package tacklr

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// streamAssembler accumulates stream deltas by type and message id.
type streamAssembler struct {
	buf map[string]string
}

func newStreamAssembler() *streamAssembler {
	return &streamAssembler{buf: make(map[string]string)}
}

// isDelta reports whether chunk is a piece of a message or reasoning
// text: not complete, with content.
func isDelta(chunk LLMResponseChunk) bool {
	return !chunk.IsComplete && chunk.Content != "" && isText(chunk.Type)
}

func isText(t session.EventType) bool {
	return t == session.EventMessage || t == session.EventReasoning
}

func streamKey(chunk LLMResponseChunk) string {
	return string(chunk.Type) + ":" + chunk.MessageId
}

// Streamed is the text chunk's message streamed so far.
func (s *streamAssembler) Streamed(chunk LLMResponseChunk) string {
	return s.buf[streamKey(chunk)]
}

func (s *streamAssembler) AddDelta(chunk LLMResponseChunk) {
	if isDelta(chunk) {
		s.buf[streamKey(chunk)] += chunk.Content
	}
}

// CompleteContent is a completed message's text: what streamed, which
// readers already have a part of, else the completion's own Content.
func (s *streamAssembler) CompleteContent(chunk LLMResponseChunk) string {
	return cmp.Or(s.buf[streamKey(chunk)], chunk.Content)
}

// MessageFromComplete builds a context-window Message for a completed
// message or reasoning chunk (not function calls).
func (s *streamAssembler) MessageFromComplete(chunk LLMResponseChunk) *message.Message {
	role := message.RoleAssistant
	if chunk.Type == session.EventReasoning {
		role = message.RoleReasoning
	}
	return &message.Message{
		Role:             role,
		Content:          s.CompleteContent(chunk),
		ToolCalls:        append([]message.ToolCall(nil), chunk.ToolCalls...),
		MessageID:        chunk.MessageId,
		EncryptedContent: chunk.EncryptedContent,
	}
}

// invokeTurn calls the model on the committed window. The window changes
// only when a round commits or a window effect applies, never here.
func (a *Harness) invokeTurn(ctx context.Context) (<-chan LLMResponseChunk, error) {
	msgs := a.window.Messages()
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
func (a *Harness) modelIdentity(ctx context.Context) context.Context {
	return telemetry.ContextWithModelIdentity(ctx, a.model.ModelTelemetryIdentity())
}

// request is a model request on msgs with the turn's tools, system prompt
// and cache key, so a token count measures exactly what a request sends.
func (a *Harness) request(msgs []*message.Message) ModelRequest {
	return ModelRequest{Messages: msgs, Tools: a.tools, SystemPrompt: a.constructSystemPrompt(), CacheKey: a.sessionId}
}

// handoff hands off the remaining todos: the window becomes the goal, the
// plan document and a handoff the model writes (handoffText).
func (a *Harness) handoff(ctx context.Context) error {
	plan, planDoc := a.session.Plan.Get(), a.session.Plan.Document()
	open := 0
	for i := range plan {
		if plan[i].Status != TodoStatusCompleted {
			open++
		}
	}
	ctx, span := telemetry.StartHandoffSpan(ctx, open)
	usedFallback := false
	err := a.window.HandOff(ctx, planDoc, open > 0, func(ctx context.Context, window []*message.Message) (string, error) {
		text, fallback, err := a.handoffText(a.modelIdentity(ctx), window, plan)
		usedFallback = fallback
		return text, err
	})
	if err != nil {
		span.End(telemetry.HandoffOutcomeError, err)
		return err
	}
	outcome := telemetry.HandoffOutcomeOK
	if usedFallback {
		outcome = telemetry.HandoffOutcomeFallback
	}
	span.End(outcome, nil)
	return nil
}

// summarize is the model's summary of window, for a collapse. Tools and
// the live system prompt stay so the request prefix matches a normal turn;
// tool_choice=none prevents calls.
func (a *Harness) summarize(ctx context.Context, window []*message.Message) (string, error) {
	telemetry.InstrumentsFromContext(ctx).RecordCompress(ctx, telemetry.AgentIDFromContext(ctx))
	a.modelSeq++
	invokeMsgs := slices.Concat(window, []*message.Message{{Role: message.RoleDeveloper, Content: summaryInstruction}})
	mctx, mspan := telemetry.StartModelSpan(a.modelIdentity(ctx), telemetry.ModelPhaseCompress, a.modelSeq, windowShape(invokeMsgs))
	events, err := a.model.Invoke(ContextWithToolChoiceNone(mctx), a.request(invokeMsgs))
	if err != nil {
		mspan.End(err, telemetry.TokenUsage{})
		return "", fmt.Errorf("context compress invoke failed: %w", err)
	}
	var body strings.Builder
	var streamErr error
	var usage telemetry.TokenUsage
	for chunk := range events {
		mergeTokenUsage(&usage, chunk)
		if err := chunk.Err(); err != nil {
			streamErr = fmt.Errorf("context compress stream failed: %w", err)
			break
		}
		body.WriteString(chunk.Content)
	}
	mspan.End(streamErr, usage)
	return body.String(), streamErr
}

// summaryInstruction asks for the summary that replaces a collapsed window.
const summaryInstruction = "<instructions>\nSummarize the entire message history into a single, concise summary including key items for your current and past tasks, with a primary focus on the most recent user request.\n</instructions>"

// handoffText is the model's handoff of window for the plan's remaining
// todos, or a handoff made from the plan when the model fails or writes
// nothing (fallback reports it).
func (a *Harness) handoffText(ctx context.Context, window []*message.Message, plan []Todo) (text string, fallback bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
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
	invokeWindow := append(slices.Clone(window), &message.Message{Role: message.RoleDeveloper, Content: prompt.String()})
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
			if err := chunk.Err(); err != nil {
				streamErr = fmt.Errorf("handoff model stream failed: %w", err)
				break
			}
			asm.AddDelta(chunk)
			if chunk.IsComplete && chunk.Type == session.EventMessage {
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

	return lastCompletedMessage, usedFallback, nil
}

func windowShape(msgs []*message.Message) telemetry.WindowShape {
	pairs := 0
	for _, m := range msgs {
		if m != nil && m.Role == message.RoleAssistant && len(m.ToolCalls) > 0 {
			pairs++
		}
	}
	return telemetry.WindowShape{Messages: len(msgs), ToolPairs: pairs}
}

// mergeTokenUsage keeps the last count a provider reported for each kind.
func mergeTokenUsage(u *telemetry.TokenUsage, chunk LLMResponseChunk) {
	u.Input = cmp.Or(chunk.InputTokens, u.Input)
	u.Output = cmp.Or(chunk.OutputTokens, u.Output)
	u.Reasoning = cmp.Or(chunk.ReasoningTokens, u.Reasoning)
	u.Cached = cmp.Or(chunk.CachedTokens, u.Cached)
	u.CacheWrite = cmp.Or(chunk.CacheWriteTokens, u.CacheWrite)
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
				if streamErr = chunk.Err(); streamErr != nil {
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
