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

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// streamAssembler accumulates stream deltas by type and message id.
type streamAssembler struct {
	buf map[string]string
}

func newStreamAssembler() *streamAssembler {
	return &streamAssembler{buf: make(map[string]string)}
}

func (s *streamAssembler) AddDelta(chunk LLMResponseChunk) {
	if chunk.IsComplete || chunk.Content == "" {
		return
	}
	if chunk.Type != StreamEventMessage && chunk.Type != StreamEventReasoning {
		return
	}
	s.buf[string(chunk.Type)+":"+chunk.MessageId] += chunk.Content
}

// CompleteContent returns chunk.Content, or accumulated deltas if Content is empty.
func (s *streamAssembler) CompleteContent(chunk LLMResponseChunk) string {
	if chunk.Content != "" {
		return chunk.Content
	}
	return s.buf[string(chunk.Type)+":"+chunk.MessageId]
}

// MessageFromComplete builds a context-window Message for a completed
// message or reasoning chunk (not function calls).
func (s *streamAssembler) MessageFromComplete(chunk LLMResponseChunk) *Message {
	role := RoleAssistant
	if chunk.Type == StreamEventReasoning {
		role = RoleReasoning
	}
	return &Message{
		Role:             role,
		Content:          s.CompleteContent(chunk),
		ToolCalls:        append([]ToolCall(nil), chunk.ToolCalls...),
		MessageID:        chunk.MessageId,
		EncryptedContent: chunk.EncryptedContent,
	}
}

// modelIdentityProvider is optionally implemented by InferenceStrategy so model
// spans can set static GenAI identity attrs at start without exposing provider
// URL/model fields on the public InferenceStrategy interface.
type modelIdentityProvider interface {
	ModelTelemetryIdentity() telemetry.ModelIdentity
}

// AbsorbResult is returned by Absorb after incorporating a message.
type AbsorbResult struct {
	// SummaryChunks are compress summaries to stream when StreamFitSummary is true.
	SummaryChunks []LLMResponseChunk
}

// modelTasks is Turn, Absorb, and Handoff against InferenceStrategy and contextManager.
type modelTasks interface {
	Turn(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error)
	Absorb(ctx context.Context, msg *Message, tools []*Tool, systemPrompt string) (AbsorbResult, error)
	Handoff(ctx context.Context, plan []Todo, planDoc string, tools []*Tool, systemPrompt string) error
}

// defaultModelTasks is the product modelTasks implementation.
type defaultModelTasks struct {
	mu sync.Mutex // Absorb/Handoff/Turn snapshot; parallel tool results serialize here

	model    InferenceStrategy
	context  contextManager
	policy   ContextPolicy
	maxSize  int
	modelSeq int // 1-based Invoke count for model span seq

	countScratch []*Message // reused for progressive token counts
}

func newDefaultModelTasks(model InferenceStrategy, ctx contextManager, policy ContextPolicy, maxSize int) *defaultModelTasks {
	return &defaultModelTasks{
		model:   model,
		context: ctx,
		policy:  policy,
		maxSize: maxSize,
	}
}

func (t *defaultModelTasks) Turn(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Context is only reshaped on Absorb pressure (token threshold) or Handoff
	// after complete_todo / plan revision — never by dropping tool history here.
	t.mu.Lock()
	msgs := t.context.Messages()
	t.modelSeq++
	seq := t.modelSeq
	t.mu.Unlock()
	if src, ok := t.model.(modelIdentityProvider); ok {
		ctx = telemetry.ContextWithModelIdentity(ctx, src.ModelTelemetryIdentity())
	}
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, seq, windowShape(msgs))
	ch, err := t.model.Invoke(ctx, msgs, tools, systemPrompt)
	if err != nil {
		span.End(err, telemetry.TokenUsage{})
		return nil, err
	}
	return watchModelStream(ctx, span, ch), nil
}

func (t *defaultModelTasks) Absorb(ctx context.Context, msg *Message, tools []*Tool, systemPrompt string) (AbsorbResult, error) {
	if err := ctx.Err(); err != nil {
		return AbsorbResult{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	window, chunks, _, err := t.absorbFit(ctx, t.context.Messages(), msg, tools)
	if err != nil {
		return AbsorbResult{}, err
	}
	t.context.Replace(window)
	return AbsorbResult{SummaryChunks: chunks}, nil
}

func (t *defaultModelTasks) Handoff(ctx context.Context, plan []Todo, planDoc string, tools []*Tool, systemPrompt string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	open := 0
	for i := range plan {
		if plan[i].Status != streaming.TodoStatusCompleted {
			open++
		}
	}
	ctx, span := telemetry.StartHandoffSpan(ctx, open)

	window, usedFallback, err := handoffGenerate(ctx, t.context.Messages(), plan, planDoc, t.model, tools)
	if err != nil {
		span.End(telemetry.HandoffOutcomeError, err)
		return err
	}
	t.context.Replace(window)
	outcome := telemetry.HandoffOutcomeOK
	if usedFallback {
		outcome = telemetry.HandoffOutcomeFallback
	}
	span.End(outcome, nil)
	return nil
}

// absorbFit collapses under pressure then appends newMsg.
// Uses InferenceStrategy.CountTokens (provider-specific) and Invoke for summary.
func (t *defaultModelTasks) absorbFit(
	ctx context.Context,
	window []*Message,
	newMsg *Message,
	tools []*Tool,
) (out []*Message, chunks []LLMResponseChunk, compressed bool, err error) {
	model := t.model
	policy := t.policy
	maxSize := t.maxSize

	// countView = window + newMsg in reusable scratch (not retained as the stored window).
	fullN := len(window) + 1
	countView := t.stageMessages(fullN)
	copy(countView, window)
	countView[len(window)] = newMsg

	currSize, err := model.CountTokens(ctx, countView, tools)
	if err != nil {
		slog.ErrorContext(ctx, "failed to count tokens while absorbing message", "area", telemetry.AreaModelTasks, "error", err)
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
	anchorLen := protectedPrefixLen(window)
	anchors := window[:anchorLen]
	unprotected := window[anchorLen:]
	if len(unprotected) == 0 {
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

	// Compress unprotected history only; no tools on the summarize call.
	compressSrc := unprotected[:numMessagesToCompress]
	t.modelSeq++
	mctx := ctx
	if src, ok := model.(modelIdentityProvider); ok {
		mctx = telemetry.ContextWithModelIdentity(ctx, src.ModelTelemetryIdentity())
	}
	mctx, mspan := telemetry.StartModelSpan(mctx, telemetry.ModelPhaseCompress, t.modelSeq, windowShape(compressSrc))
	events, err := model.Invoke(mctx, compressSrc, nil, sumPrompt.String())
	if err != nil {
		mspan.End(err, telemetry.TokenUsage{})
		return nil, nil, true, fmt.Errorf("context compress invoke failed: %w", err)
	}

	summaryMsg := &Message{Role: RoleAssistant}
	var summary strings.Builder
	var outChunks []LLMResponseChunk
	var streamErr error
	var usage telemetry.TokenUsage
	for chunk := range events {
		mergeTokenUsage(&usage, chunk)
		if chunk.Type == StreamEventError {
			if chunk.Error != nil {
				streamErr = fmt.Errorf("context compress stream failed: %w", chunk.Error)
			} else {
				streamErr = fmt.Errorf("context compress stream failed: %s", chunk.Content)
			}
			break
		}
		if policy.StreamFitSummary {
			outChunks = append(outChunks, chunk)
		}
		summary.WriteString(chunk.Content)
	}
	mspan.End(streamErr, usage)
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

	return out, outChunks, true, nil
}

// stageMessages returns t.countScratch resized to n (grows capacity when needed).
func (t *defaultModelTasks) stageMessages(n int) []*Message {
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
) ([]*Message, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
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

	// Handoff is a pure writing task — no tools. On model failure, install a
	// plan-derived handoff so ACM still rebuilds context.
	mctx := ctx
	if src, ok := model.(modelIdentityProvider); ok {
		mctx = telemetry.ContextWithModelIdentity(ctx, src.ModelTelemetryIdentity())
	}
	mctx, mspan := telemetry.StartModelSpan(mctx, telemetry.ModelPhaseHandoff, 0, windowShape(window))
	events, err := model.Invoke(mctx, window, nil, prompt.String())
	var lastCompletedMessage string
	usedFallback := false
	if err != nil {
		mspan.End(err, telemetry.TokenUsage{})
		slog.ErrorContext(ctx, "handoff model call failed; using plan-derived fallback text",
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
				if chunk.Error != nil {
					streamErr = fmt.Errorf("handoff model stream failed: %w", chunk.Error)
				} else {
					streamErr = fmt.Errorf("handoff model stream failed: %s", chunk.Content)
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
		mspan.End(streamErr, usage)
		if streamErr != nil {
			slog.ErrorContext(ctx, "handoff model stream failed; using plan-derived fallback text",
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

func mergeTokenUsage(u *telemetry.TokenUsage, chunk LLMResponseChunk) {
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

// watchModelStream ends the model span when the stream closes, cancels, or errors.
// Drains remaining provider chunks so the span ends if the harness stops reading.
func watchModelStream(ctx context.Context, span *telemetry.ModelSpan, in <-chan LLMResponseChunk) <-chan LLMResponseChunk {
	out := make(chan LLMResponseChunk, 16)
	go func() {
		defer close(out)
		var streamErr error
		var usage telemetry.TokenUsage
		var endOnce sync.Once
		end := func(err error) {
			endOnce.Do(func() { span.End(err, usage) })
		}
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

		// Drop already-buffered chunks without blocking. A blocking drain
		// keeps a fast producer alive and starves its ctx.Done.
		drainIn := func() {
			for {
				select {
				case _, ok := <-in:
					if !ok {
						return
					}
				default:
					return
				}
			}
		}

		for {
			if err := ctx.Err(); err != nil {
				streamErr = err
				end(streamErr)
				drainIn()
				return
			}
			select {
			case <-ctx.Done():
				streamErr = ctx.Err()
				end(streamErr)
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
