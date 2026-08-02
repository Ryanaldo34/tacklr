package tacklr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// modelTelemetrySource is optionally implemented by InferenceStrategy so model
// spans can set static GenAI identity attrs at start.
type modelTelemetrySource interface {
	ModelName() string
	BaseURL() string
}

// AbsorbResult is returned by ModelTasks.Absorb after incorporating a message.
// Small value type (slice header only).
type AbsorbResult struct {
	// SummaryChunks when StreamFitSummary is true; harness streams to the client.
	SummaryChunks []LLMResponseChunk
}

// ModelTasks runs product-level model operations.
// It uses InferenceStrategy for transport and provider CountTokens, and
// ContextManager for reading/applying structured windows.
// Token counting is not a product method — it stays on InferenceStrategy.
type ModelTasks interface {
	// Turn streams the next agent step for the current context and tools.
	Turn(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error)

	// Absorb incorporates msg under window pressure (may summarize via the model).
	Absorb(ctx context.Context, msg *Message, tools []*Tool, systemPrompt string) (AbsorbResult, error)

	// Handoff rebuilds context after complete_todo or plan-document edit.
	Handoff(ctx context.Context, plan []Todo, planDoc string, tools []*Tool, systemPrompt string) error
}

// DefaultModelTasks is the standard ModelTasks implementation.
// Stored as a pointer on the harness (interface holds *DefaultModelTasks).
type DefaultModelTasks struct {
	model   InferenceStrategy
	context ContextManager
	policy  ContextPolicy
	maxSize int
	// modelSeq is a 1-based Invoke counter for tacklr.model.seq (turn lifetime).
	modelSeq int

	// countScratch is reused by absorbFit's progressive token-count search so
	// each pressure step does not allocate a new message pointer slice.
	countScratch []*Message
}

// NewDefaultModelTasks wires provider + context structure for product model ops.
func NewDefaultModelTasks(model InferenceStrategy, ctx ContextManager, policy ContextPolicy, maxSize int) *DefaultModelTasks {
	if policy.PressureRatio <= 0 && policy.CompressFraction <= 0 {
		policy = DefaultContextPolicy()
	}
	return &DefaultModelTasks{
		model:   model,
		context: ctx,
		policy:  policy,
		maxSize: maxSize,
	}
}

func (t *DefaultModelTasks) Turn(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.model == nil {
		return nil, fmt.Errorf("turn: model is required")
	}
	// Context is only reshaped on Absorb pressure (token threshold) or Handoff
	// after complete_todo / plan revision — never by dropping tool history here.
	if systemPrompt != "" {
		t.model.SetSystemPrompt(systemPrompt)
	}
	msgs := t.context.Messages()
	t.modelSeq++
	ctx = withModelIdentity(ctx, t.model)
	if telemetry.AfterToolsFromContext(ctx) {
		telemetry.EmitModelAfterTools(ctx)
	}
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, t.modelSeq, windowShape(msgs))
	ch, err := t.model.Invoke(ctx, msgs, tools)
	if err != nil {
		endModelFromErr(ctx, span, telemetry.ModelPhaseTurn, err, telemetry.TokenUsage{})
		return nil, err
	}
	return watchModelStream(ctx, span, telemetry.ModelPhaseTurn, ch), nil
}

func (t *DefaultModelTasks) Absorb(ctx context.Context, msg *Message, tools []*Tool, systemPrompt string) (AbsorbResult, error) {
	if err := ctx.Err(); err != nil {
		return AbsorbResult{}, err
	}
	if msg == nil {
		return AbsorbResult{}, nil
	}
	// Absorb/compress is context plumbing — not a turn-lifecycle span.
	window, chunks, _, err := t.absorbFit(ctx, t.context.Messages(), msg, tools, systemPrompt)
	if err != nil {
		return AbsorbResult{}, err
	}
	// window is freshly allocated; Replace takes ownership (no second copy).
	t.context.Replace(window)
	return AbsorbResult{SummaryChunks: chunks}, nil
}

func (t *DefaultModelTasks) Handoff(ctx context.Context, plan []Todo, planDoc string, tools []*Tool, systemPrompt string) error {
	open := 0
	for i := range plan {
		if plan[i].Status != streaming.TodoStatusCompleted {
			open++
		}
	}
	// Milestone: context handoff after todo complete / plan revise.
	ctx, span := telemetry.TracerFromContext(ctx).Start(ctx, telemetry.SpanContextHandoff,
		trace.WithAttributes(
			attribute.String(telemetry.AttrArea, telemetry.AreaModelTasks),
			attribute.Int(telemetry.AttrOpenTodos, open),
		),
	)
	defer span.End()
	slog.InfoContext(ctx, "running context handoff", "area", telemetry.AreaModelTasks, "open_todos", open)

	window, usedFallback, err := handoffGenerate(ctx, t.context.Messages(), plan, planDoc, t.model, tools, systemPrompt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String(telemetry.AttrOutcome, telemetry.HandoffOutcomeError))
		telemetry.InstrumentsFromContext(ctx).RecordHandoff(ctx, telemetry.AgentIDFromContext(ctx), telemetry.HandoffOutcomeError)
		return err
	}
	t.context.Replace(window)
	outcome := telemetry.HandoffOutcomeOK
	if usedFallback {
		outcome = telemetry.HandoffOutcomeFallback
	}
	span.SetAttributes(attribute.String(telemetry.AttrOutcome, outcome))
	telemetry.InstrumentsFromContext(ctx).RecordHandoff(ctx, telemetry.AgentIDFromContext(ctx), outcome)
	return nil
}

// absorbFit collapses under pressure then appends newMsg.
// Uses InferenceStrategy.CountTokens (provider-specific) and Invoke for summary.
func (t *DefaultModelTasks) absorbFit(
	ctx context.Context,
	window []*Message,
	newMsg *Message,
	tools []*Tool,
	restoreSystemPrompt string,
) (out []*Message, chunks []LLMResponseChunk, compressed bool, err error) {
	model := t.model
	policy := t.policy
	maxSize := t.maxSize
	if policy.PressureRatio <= 0 {
		policy.PressureRatio = DefaultContextPolicy().PressureRatio
	}
	if policy.CompressFraction <= 0 {
		policy.CompressFraction = DefaultContextPolicy().CompressFraction
	}

	// countView = window + newMsg in reusable scratch (not retained as the stored window).
	fullN := len(window) + 1
	countView := t.stageMessages(fullN)
	copy(countView, window)
	countView[len(window)] = newMsg

	if model == nil {
		return slices.Clone(countView), nil, false, nil
	}

	currSize, err := model.CountTokens(ctx, countView, tools)
	if err != nil {
		slog.ErrorContext(ctx, "failed to count tokens while absorbing message", "area", "model_tasks", "error", err)
		return nil, nil, false, fmt.Errorf("count tokens: %w", err)
	}
	if len(window) == 0 || float64(currSize) <= float64(maxSize)*policy.PressureRatio {
		return slices.Clone(countView), nil, false, nil
	}

	slog.InfoContext(ctx, "max context window size exceeded or approaching, compressing context window",
		"area", telemetry.AreaModelTasks, "max_size", maxSize, "current_size", currSize)
	telemetry.InstrumentsFromContext(ctx).RecordCompress(ctx, telemetry.AgentIDFromContext(ctx))

	var sumPrompt strings.Builder
	sumPrompt.Grow(280 + len(newMsg.Content))
	sumPrompt.WriteString("Please summarize the entire message history into a single, concise summary including key items for your current and past tasks with a primary focus on your current task. Current task or follow-up question to answer: ")
	sumPrompt.WriteString(newMsg.Content)
	model.SetSystemPrompt(sumPrompt.String())

	anchorLen := protectedPrefixLen(window)
	if anchorLen < 1 {
		anchorLen = 1
	}
	if anchorLen > len(window) {
		anchorLen = len(window)
	}
	anchors := window[:anchorLen]
	unprotected := window[anchorLen:]
	if len(unprotected) == 0 {
		if restoreSystemPrompt != "" {
			model.SetSystemPrompt(restoreSystemPrompt)
		}
		return slices.Clone(countView), nil, false, nil
	}

	// Progressive CountTokens probes reuse countScratch (no alloc per step).
	stageCount := func(from int) []*Message {
		n := anchorLen + (len(unprotected) - from) + 1
		buf := t.stageMessages(n)
		copy(buf, anchors)
		copy(buf[anchorLen:], unprotected[from:])
		buf[n-1] = newMsg
		return buf
	}

	numMessagesToCompress := int(math.Round(float64(len(unprotected)) * policy.CompressFraction))
	if currSize > maxSize {
		diff := currSize - maxSize
		start := int(math.Round(float64(diff) * policy.CompressFraction))
		if start < 1 {
			start = 1
		}
		if start > len(unprotected) {
			start = len(unprotected)
		}
		for start < len(unprotected) {
			count, err := model.CountTokens(ctx, stageCount(start), tools)
			if err != nil {
				return nil, nil, true, fmt.Errorf("count tokens: %w", err)
			}
			if float64(count) <= float64(maxSize)*policy.PressureRatio {
				break
			}
			start++
		}
		numMessagesToCompress = start
	}
	if numMessagesToCompress < 1 {
		numMessagesToCompress = 1
	}
	if numMessagesToCompress > len(unprotected) {
		numMessagesToCompress = len(unprotected)
	}

	// Summarization only — no tool catalog. The unprotected slice is the real
	// history under pressure (including tool results); do not invent alternate
	// windows for provider quirks.
	compressSrc := unprotected[:numMessagesToCompress]
	t.modelSeq++
	mctx := withModelIdentity(ctx, model)
	mctx, mspan := telemetry.StartModelSpan(mctx, telemetry.ModelPhaseCompress, t.modelSeq, windowShape(compressSrc))
	events, err := model.Invoke(mctx, compressSrc, nil)
	if err != nil {
		endModelFromErr(mctx, mspan, telemetry.ModelPhaseCompress, err, telemetry.TokenUsage{})
		return nil, nil, true, fmt.Errorf("compress invoke: %w", err)
	}

	summaryMsg := &Message{Role: RoleAssistant}
	var summary strings.Builder
	var outChunks []LLMResponseChunk
	var streamErr error
	var usage telemetry.TokenUsage
	for chunk := range events {
		mergeTokenUsage(&usage, chunk)
		if chunk.Type == StreamEventError {
			streamErr = fmt.Errorf("compress: %s", chunk.Content)
			if chunk.Error != nil {
				streamErr = fmt.Errorf("compress: %w", chunk.Error)
			}
			break
		}
		if policy.StreamFitSummary {
			outChunks = append(outChunks, chunk)
		}
		summary.WriteString(chunk.Content)
	}
	endModelFromErr(mctx, mspan, telemetry.ModelPhaseCompress, streamErr, usage)
	if streamErr != nil {
		return nil, nil, true, streamErr
	}
	summaryMsg.Content = summary.String()

	// Single final allocation: anchors + summary + remaining unprotected + newMsg.
	rest := unprotected[numMessagesToCompress:]
	out = make([]*Message, 0, anchorLen+1+len(rest)+1)
	out = append(out, anchors...)
	out = append(out, summaryMsg)
	out = append(out, rest...)
	out = append(out, newMsg)

	if restoreSystemPrompt != "" {
		model.SetSystemPrompt(restoreSystemPrompt)
	}
	return out, outChunks, true, nil
}

// stageMessages returns t.countScratch resized to n (grows capacity when needed).
func (t *DefaultModelTasks) stageMessages(n int) []*Message {
	if cap(t.countScratch) < n {
		t.countScratch = make([]*Message, n)
	} else {
		t.countScratch = t.countScratch[:n]
	}
	return t.countScratch
}

func handoffGenerate(
	ctx context.Context,
	window []*Message,
	plan []Todo,
	planDoc string,
	model InferenceStrategy,
	tools []*Tool,
	restoreSystemPrompt string,
) ([]*Message, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if model == nil {
		return nil, false, fmt.Errorf("handoff: model is required")
	}
	if len(window) == 0 || window[0] == nil {
		return nil, false, fmt.Errorf("handoff: empty window")
	}

	var planB strings.Builder
	planB.Grow(len(plan) * 64)
	for i := range plan {
		todo := &plan[i]
		fmt.Fprintf(&planB, "- %s: %s\nStatus: %s\n", todo.Title, todo.Description, todo.Status)
	}

	const handoffPreamble = `Your task is to produce a handoff for someone to complete the remaining todo items in the plan, not a summary of the completed work, but rather, an informative overview of the process that has completed the work so far. You will ensure to inform the handoff recipient that this is a work in progress and that they should expect to complete the remaining todo items. This is your only task, and you will not add any additional commentary, thoughts, etc. This is not a generic summary as the handoff needs to include the following sections:
	Objective: Overall mission and success criteria (brief; a durable PROJECT PLAN document will remain in context — do not restate the full blueprint).
	Completed Work: What is now true because of the completed todo(s) and an overview of the current state of the plan & implementation. Someone should know exactly what was done & what work is remaining and be able to pick up the remaining work seamlessly.
	Key Decisions: Architectural or implementation choices that should not be revisited.
	State Changes: Files changed, APIs added/removed, new abstractions, configuration changes, etc.
	Discoveries: Facts learned that affect remaining work (including anything that forced a plan revision).
	Constraints: Requirements, assumptions, and invariants that future todos must respect.
	Remaining Work: Newly discovered tasks, blockers, or dependencies.
	Validation: What was verified and what still requires verification.
	Relevant Context for Remaining Todos: Only information the next todos are likely to need which was gathered or observed in the completed work.

Current plan todos:
`
	var prompt strings.Builder
	prompt.Grow(len(handoffPreamble) + planB.Len())
	prompt.WriteString(handoffPreamble)
	prompt.WriteString(planB.String())

	model.SetSystemPrompt(prompt.String())
	// Handoff is a pure writing task over the current window — tools are not
	// offered. On model failure, install a plan-derived handoff so ACM still
	// rebuilds context (todo complete must not leave a half-applied effect).
	// modelSeq is on the receiver only when called via *DefaultModelTasks methods;
	// handoffGenerate is a free function — start span without seq when 0.
	mctx := withModelIdentity(ctx, model)
	mctx, mspan := telemetry.StartModelSpan(mctx, telemetry.ModelPhaseHandoff, 0, windowShape(window))
	events, err := model.Invoke(mctx, window, nil)
	var lastCompletedMessage string
	usedFallback := false
	if err != nil {
		endModelFromErr(mctx, mspan, telemetry.ModelPhaseHandoff, err, telemetry.TokenUsage{})
		slog.ErrorContext(ctx, "handoff invoke failed; using plan-derived fallback",
			"area", telemetry.AreaModelTasks, "error", err)
		lastCompletedMessage = fallbackHandoffContent(plan)
		usedFallback = true
	} else {
		asm := newStreamAssembler()
		var streamErr error
		var usage telemetry.TokenUsage
		for chunk := range events {
			mergeTokenUsage(&usage, chunk)
			if chunk.Type == StreamEventError {
				streamErr = fmt.Errorf("handoff: %s", chunk.Content)
				if chunk.Error != nil {
					streamErr = fmt.Errorf("handoff: %w", chunk.Error)
				}
				break
			}
			asm.AddDelta(chunk)
			if chunk.IsComplete && chunk.Type == StreamEventMessage {
				if content := asm.CompleteContent(chunk); content != "" {
					lastCompletedMessage = content
				}
			}
		}
		endModelFromErr(mctx, mspan, telemetry.ModelPhaseHandoff, streamErr, usage)
		if streamErr != nil {
			slog.ErrorContext(ctx, "handoff model stream failed; using plan-derived fallback",
				"area", telemetry.AreaModelTasks, "error", streamErr)
			lastCompletedMessage = fallbackHandoffContent(plan)
			usedFallback = true
		} else if strings.TrimSpace(lastCompletedMessage) == "" {
			lastCompletedMessage = fallbackHandoffContent(plan)
			usedFallback = true
		}
	}

	// Reuse window[0] pointer (original user). Cap 4: user, plan?, handoff, nudge?
	out := make([]*Message, 0, 4)
	out = append(out, window[0])
	if planDoc != "" {
		out = append(out, buildPlanDocumentMessage(planDoc))
	}
	out = append(out, &Message{Role: RoleDeveloper, Content: lastCompletedMessage})
	if planHasOpenTodos(plan) {
		out = append(out, &Message{
			Role:    RoleDeveloper,
			Content: continuePlanNudge,
		})
	}
	if restoreSystemPrompt != "" {
		model.SetSystemPrompt(restoreSystemPrompt)
	}
	return out, usedFallback, nil
}

func windowShape(msgs []*Message) telemetry.WindowShape {
	pairs := 0
	for _, m := range msgs {
		if m != nil && m.Role == RoleAssistant && len(m.ToolCalls) > 0 {
			pairs++
		}
	}
	return telemetry.WindowShape{Messages: len(msgs), ToolPairs: pairs}
}

func withModelIdentity(ctx context.Context, model InferenceStrategy) context.Context {
	if model == nil {
		return ctx
	}
	src, ok := model.(modelTelemetrySource)
	if !ok {
		return ctx
	}
	return telemetry.ContextWithModelIdentity(ctx, telemetry.ModelIdentity{
		Provider:  telemetry.InferProviderName(src.BaseURL()),
		Model:     src.ModelName(),
		Operation: telemetry.GenAIOperationChat,
	})
}

func mergeTokenUsage(u *telemetry.TokenUsage, chunk LLMResponseChunk) {
	if u == nil {
		return
	}
	if chunk.InputTokens > 0 {
		u.Input = chunk.InputTokens
	}
	if chunk.OutputTokens > 0 {
		u.Output = chunk.OutputTokens
	}
	if chunk.ReasoningTokens > 0 {
		u.Reasoning = chunk.ReasoningTokens
	}
}

func endModelFromErr(ctx context.Context, span trace.Span, phase string, err error, usage telemetry.TokenUsage) {
	httpStatus, code := 0, ""
	if err != nil {
		var ps ProviderStatus
		if errors.As(err, &ps) {
			httpStatus = ps.ProviderHTTPStatus()
			code = ps.ProviderErrorCode()
		}
	}
	telemetry.EndModelSpan(ctx, span, phase, err, httpStatus, code, usage)
}

// watchModelStream ends the model span when the provider stream closes, on
// cancel, or on a terminal stream error. Span end is idempotent so cancel and
// close cannot double-End. After a stream error (or when the consumer stops
// reading), remaining provider chunks are drained without blocking forever so
// the span always finishes even if the harness returns early.
func watchModelStream(ctx context.Context, span trace.Span, phase string, in <-chan LLMResponseChunk) <-chan LLMResponseChunk {
	out := make(chan LLMResponseChunk, 16)
	go func() {
		defer close(out)
		var streamErr error
		var usage telemetry.TokenUsage
		var endOnce sync.Once
		end := func(err error) {
			endOnce.Do(func() {
				endModelFromErr(ctx, span, phase, err, usage)
			})
		}
		// Always end the span if we exit for any reason (panic-safe bookkeeping).
		// Read streamErr at exit time (not when defer is registered).
		defer func() { end(streamErr) }()

		forward := func(chunk LLMResponseChunk) (stop bool) {
			select {
			case out <- chunk:
				return false
			case <-ctx.Done():
				streamErr = ctx.Err()
				end(streamErr)
				return true
			}
		}

		// Drain provider channel without blocking on a dead consumer.
		drainIn := func() {
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-in:
					if !ok {
						return
					}
				}
			}
		}

		for {
			select {
			case <-ctx.Done():
				streamErr = ctx.Err()
				end(streamErr)
				// Best-effort: drop remaining provider chunks so the HTTP goroutine can exit.
				drainIn()
				return
			case chunk, ok := <-in:
				if !ok {
					end(streamErr)
					return
				}
				mergeTokenUsage(&usage, chunk)
				if chunk.Type == StreamEventError || chunk.Error != nil {
					streamErr = chunk.Error
					if streamErr == nil && chunk.Content != "" {
						streamErr = errors.New(chunk.Content)
					}
					// Deliver the error chunk when possible, then end the span
					// immediately so mid-stream failures are visible even if the
					// harness stops reading before the provider channel closes.
					_ = forward(chunk)
					end(streamErr)
					drainIn()
					return
				}
				if forward(chunk) {
					drainIn()
					return
				}
			}
		}
	}()
	return out
}

// fallbackHandoffContent builds a plan-derived handoff when the model stream
// fails or returns no message so the window can still be rebuilt to the ACM
// handoff shape (user, plan document, handoff, optional continue nudge).
func fallbackHandoffContent(plan []Todo) string {
	var b strings.Builder
	b.WriteString("Handoff (fallback — model handoff stream failed or was empty).\n")
	b.WriteString("This is work in progress; complete remaining todos from the plan document and list below.\n\n")
	b.WriteString("Current plan todos:\n")
	for i := range plan {
		todo := &plan[i]
		fmt.Fprintf(&b, "- [%s] %s: %s\n", todo.Status, todo.Title, todo.Description)
	}
	return b.String()
}
