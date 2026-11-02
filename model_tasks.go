package tacklr

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

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

// invokeTurn calls the model on the committed window. The window changes
// only when a round commits or a window effect applies, never here.
func (a *TurnManager) invokeTurn(ctx context.Context) (<-chan LLMResponseChunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	msgs := a.context.Messages()
	a.modelSeq++
	ctx = a.modelIdentity(ctx)
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, a.modelSeq, windowShape(msgs))
	ch, err := a.model.Invoke(ctx, a.request(msgs))
	if err != nil {
		span.End(err, telemetry.TokenUsage{})
		return nil, err
	}
	return watchModelStream(ctx, span, ch), nil
}

// modelIdentity adds the model's static GenAI identity to ctx for spans.
func (a *TurnManager) modelIdentity(ctx context.Context) context.Context {
	return telemetry.ContextWithModelIdentity(ctx, a.model.ModelTelemetryIdentity())
}

// request is a model request on msgs with the turn's tools, system prompt
// and cache key, so a token count measures exactly what a request sends.
func (a *TurnManager) request(msgs []*Message) ModelRequest {
	return ModelRequest{Messages: msgs, Tools: a.tools, SystemPrompt: a.constructSystemPrompt(), CacheKey: a.sessionId}
}

// handoff replaces the window with the plan document and a handoff the
// model writes for the remaining todos. The messages it replaces are saved
// for search first.
func (a *TurnManager) handoff(ctx context.Context) error {
	window := a.context.Messages()
	if err := a.saveSessionMessages(ctx, window[min(len(window), 1):]); err != nil {
		return err
	}
	plan, planDoc := a.session.Plan.Get(), a.session.Plan.Document()
	open := 0
	for i := range plan {
		if plan[i].Status != TodoStatusCompleted {
			open++
		}
	}
	ctx, span := telemetry.StartHandoffSpan(ctx, open)
	next, usedFallback, err := a.handoffGenerate(a.modelIdentity(ctx), window, plan, planDoc)
	if err != nil {
		span.End(telemetry.HandoffOutcomeError, err)
		return err
	}
	a.context.Replace(next)
	outcome := telemetry.HandoffOutcomeOK
	if usedFallback {
		outcome = telemetry.HandoffOutcomeFallback
	}
	span.End(outcome, nil)
	return nil
}

// fit appends round to the window. Over MaxWindowSize × pressureRatio the
// window first collapses: everything after the protected prefix (an
// earlier summary too) is saved for search and replaced by one summary.
// The window is then small, so the next rounds append without collapsing.
// A round still over MaxWindowSize has its largest results trimmed.
func (a *TurnManager) fit(ctx context.Context, round []*Message) error {
	window := a.context.Messages()
	n, err := a.count(ctx, window, round)
	if err != nil {
		return err
	}
	keep := protectedPrefixLen(window)
	if n > int(float64(a.maxWindowSize)*pressureRatio) && len(window) > keep {
		if err := a.saveSessionMessages(ctx, window[keep:]); err != nil {
			return err
		}
		summary, err := a.summarize(ctx, window)
		if err != nil {
			return err
		}
		window = append(window[:keep:keep], summary)
		if n, err = a.count(ctx, window, round); err != nil {
			return err
		}
	}
	if n > a.maxWindowSize {
		if round, err = a.trim(ctx, window, round, n); err != nil {
			return err
		}
	}
	a.context.Replace(slices.Concat(window, round))
	return nil
}

func (a *TurnManager) count(ctx context.Context, window, round []*Message) (int, error) {
	n, err := a.model.CountTokens(ctx, a.request(slices.Concat(window, round)))
	if err != nil {
		return 0, fmt.Errorf("count tokens: %w", err)
	}
	return n, nil
}

// summarize asks the model for one summary of window. Tools and the live
// system prompt stay so the request prefix matches a normal turn;
// tool_choice=none prevents calls. When the collapsed messages are saved,
// the summary tells the agent that search finds them.
func (a *TurnManager) summarize(ctx context.Context, window []*Message) (*Message, error) {
	slog.InfoContext(ctx, "context window over pressure, collapsing it into a summary",
		"area", telemetry.AreaModelTasks, "max_size", a.maxWindowSize, "messages", len(window))
	telemetry.InstrumentsFromContext(ctx).RecordCompress(ctx, telemetry.AgentIDFromContext(ctx))
	a.modelSeq++
	invokeMsgs := slices.Concat(window, []*Message{{Role: RoleDeveloper, Content: summaryInstruction}})
	mctx, mspan := telemetry.StartModelSpan(a.modelIdentity(ctx), telemetry.ModelPhaseCompress, a.modelSeq, windowShape(invokeMsgs))
	events, err := a.model.Invoke(ContextWithToolChoiceNone(mctx), a.request(invokeMsgs))
	if err != nil {
		mspan.End(err, telemetry.TokenUsage{})
		return nil, fmt.Errorf("context compress invoke failed: %w", err)
	}
	var body strings.Builder
	var streamErr error
	var usage telemetry.TokenUsage
	for chunk := range events {
		mergeTokenUsage(&usage, chunk)
		if chunk.Type == StreamEventError {
			streamErr = fmt.Errorf("context compress stream failed: %w", cmp.Or(chunk.Error, errors.New(chunk.Content)))
			break
		}
		body.WriteString(chunk.Content)
	}
	mspan.End(streamErr, usage)
	if streamErr != nil {
		return nil, streamErr
	}
	content := contextSummaryPrefix + body.String()
	if a.keepsHistory() {
		content += "\n\n" + searchableHistoryNote
	}
	return &Message{Role: RoleDeveloper, Content: content}, nil
}

// summaryInstruction asks for the summary that replaces a collapsed window.
const summaryInstruction = "<instructions>\nSummarize the entire message history into a single, concise summary including key items for your current and past tasks, with a primary focus on the most recent user request.\n</instructions>"

// searchableHistoryNote ends a summary when the collapsed messages are saved.
const searchableHistoryNote = "The full earlier conversation is saved. Use the search tool to find its exact messages, and open to read one."

// trim cuts the round's largest results until the window and round fit
// MaxWindowSize. Before a result's first cut, its full text is staged
// under OverflowPath when that is set, and the cut result says where.
func (a *TurnManager) trim(ctx context.Context, window, round []*Message, n int) ([]*Message, error) {
	round = slices.Clone(round)
	full := map[int]string{}
	kept := map[int]int{}
	where := map[int]string{}
	size := func(i int) int {
		if k, ok := kept[i]; ok {
			return k
		}
		return len(round[i].Content)
	}
	for n > a.maxWindowSize {
		i := -1
		for j, m := range round {
			if m.Role == RoleTool && size(j) > 0 && (i < 0 || size(j) > size(i)) {
				i = j
			}
		}
		if i < 0 {
			return nil, fmt.Errorf("%w: the round needs %d tokens and the context window holds %d", ErrFailed, n, a.maxWindowSize)
		}
		if _, ok := full[i]; !ok {
			full[i] = round[i].Content
			if a.overflowPath != "" {
				path, err := a.stageOverflow(ctx, round[i].ToolCallID, full[i])
				if err != nil {
					return nil, fmt.Errorf("stage full tool result: %w", err)
				}
				where[i] = path
			}
		}
		// Scale the result by how far over the window is; n > maxWindowSize,
		// so each pass keeps less.
		keep := int(int64(size(i)) * int64(a.maxWindowSize) / int64(n))
		for keep > 0 && !utf8.RuneStart(full[i][keep]) {
			keep--
		}
		kept[i] = keep
		cut := *round[i]
		cut.Content = full[i][:keep] + truncationNote(keep, len(full[i]), where[i])
		round[i] = &cut
		var err error
		if n, err = a.count(ctx, window, round); err != nil {
			return nil, err
		}
	}
	return round, nil
}

// truncationNote ends a cut result: how much of it is left, and where the
// full output is when it was kept.
func truncationNote(kept, total int, path string) string {
	if path == "" {
		return fmt.Sprintf("\n\n[Output truncated to fit the context window: kept the first %d of %d bytes. The full output was not kept.]", kept, total)
	}
	return fmt.Sprintf("\n\n[Output truncated to fit the context window: kept the first %d of %d bytes. The full output is at %s; read it with start and end.]", kept, total, path)
}

func (a *TurnManager) handoffGenerate(ctx context.Context, window []*Message, plan []Todo, planDoc string) ([]*Message, bool, error) {
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

	const handoffPreamble = `<instructions>
Your task is to produce a handoff for someone to complete the remaining todo items in the plan, not a summary of the completed work, but rather, an informative overview of the process that has completed the work so far. You will ensure to inform the handoff recipient that this is a work in progress and that they should expect to complete the remaining todo items. This is your only task, and you will not add any additional commentary, thoughts, etc. This is not a generic summary.
</instructions>
<required_sections>
Objective: Overall mission and success criteria (brief; a durable PROJECT PLAN document will remain in context — do not restate the full blueprint).
Completed Work: What is now true because of the completed todo(s) and an overview of the current state of the plan & implementation. Someone should know exactly what was done & what work is remaining and be able to pick up the remaining work seamlessly.
Key Decisions: Architectural or implementation choices that should not be revisited.
State Changes: Files changed, APIs added/removed, new abstractions, configuration changes, etc.
Discoveries: Facts learned that affect remaining work (including anything that forced a plan revision).
Constraints: Requirements, assumptions, and invariants that future todos must respect.
Remaining Work: Newly discovered tasks, blockers, or dependencies.
Validation: What was verified and what still requires verification.
Relevant Context for Remaining Todos: Only information the next todos are likely to need which was gathered or observed in the completed work.
</required_sections>
<current_plan>
`
	var prompt strings.Builder
	prompt.Grow(len(handoffPreamble) + planB.Len())
	prompt.WriteString(handoffPreamble)
	prompt.WriteString(planB.String())
	prompt.WriteString("</current_plan>")

	// Keep tools and the live system prompt so the cached prefix matches a
	// normal turn. The handoff task is an extra developer message; tool_choice
	// none prevents calls. On model failure, install a plan-derived handoff.
	invokeWindow := append(slices.Clone(window), &Message{Role: RoleDeveloper, Content: prompt.String()})
	mctx, mspan := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseHandoff, 0, windowShape(invokeWindow))
	events, err := a.model.Invoke(ContextWithToolChoiceNone(mctx), a.request(invokeWindow))
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
				streamErr = fmt.Errorf("handoff model stream failed: %w", cmp.Or(chunk.Error, errors.New(chunk.Content)))
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

	// Keep window[0] (the original user message), then plan document, handoff,
	// and a continue nudge when open todos remain.
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
	if chunk.CachedTokens > 0 {
		u.Cached = chunk.CachedTokens
	}
	if chunk.CacheWriteTokens > 0 {
		u.CacheWrite = chunk.CacheWriteTokens
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
