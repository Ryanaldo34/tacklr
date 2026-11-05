package tacklr

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

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
func (a *Harness) modelIdentity(ctx context.Context) context.Context {
	return telemetry.ContextWithModelIdentity(ctx, a.model.ModelTelemetryIdentity())
}

// request is a model request on msgs with the turn's tools, system prompt
// and cache key, so a token count measures exactly what a request sends.
func (a *Harness) request(msgs []*message.Message) ModelRequest {
	return ModelRequest{Messages: msgs, Tools: a.tools, SystemPrompt: a.constructSystemPrompt(), CacheKey: a.sessionId}
}

// handoff replaces the window with the plan document and a handoff the
// model writes for the remaining todos. The messages it replaces are saved
// for search first.
func (a *Harness) handoff(ctx context.Context) error {
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
func (a *Harness) fit(ctx context.Context, round []*message.Message) error {
	window, usage := a.context.Messages(), a.context.Usage()
	threshold := int(float64(a.maxWindowSize) * pressureRatio)
	if a.bound(usage, window, round) < threshold {
		a.context.Add(round...)
		return nil
	}
	n, err := a.size(ctx, usage, window, round)
	if err != nil {
		return err
	}
	collapsed := false
	if n > threshold && len(window) > protectedPrefixLen(window) {
		if window, err = a.collapse(ctx, window, len(window)); err != nil {
			return err
		}
		collapsed, usage = true, requestUsage{}
		if n, err = a.size(ctx, usage, window, round); err != nil {
			return err
		}
	}
	if n > a.maxWindowSize {
		if round, err = a.trim(ctx, usage, window, round, n); err != nil {
			return err
		}
	}
	if !collapsed {
		a.context.Add(round...)
		return nil
	}
	a.context.Replace(slices.Concat(window, round))
	return nil
}

// refit collapses the window after the provider refused a request on it
// as over the context window, although its count or bound was under. The
// messages after the protected prefix that the last counted request
// carried become one summary: the provider took that request, so the
// summary request fits. The messages after it stay. With no counted
// request, everything after the prefix becomes the summary. A window with
// nothing to collapse fails with session.ErrContextLength.
func (a *Harness) refit(ctx context.Context) error {
	window, usage := a.context.Messages(), a.context.Usage()
	keep := protectedPrefixLen(window)
	end := usage.messages
	if end <= keep {
		end = len(window)
	}
	if end <= keep {
		return fmt.Errorf("%w: the window has nothing to collapse", session.ErrContextLength)
	}
	window, err := a.collapse(ctx, window, end)
	if err != nil {
		return err
	}
	a.context.Replace(window)
	return nil
}

// collapse saves window[keep:end] for search and replaces it with one
// summary of window[:end], where keep is the protected prefix.
func (a *Harness) collapse(ctx context.Context, window []*message.Message, end int) ([]*message.Message, error) {
	keep := protectedPrefixLen(window)
	if err := a.saveSessionMessages(ctx, window[keep:end]); err != nil {
		return nil, err
	}
	summary, err := a.summarize(ctx, window[:end])
	if err != nil {
		return nil, err
	}
	return slices.Concat(window[:keep], []*message.Message{summary}, window[end:]), nil
}

// bound is an upper bound on the input tokens of a request with window and
// round: usage's count, plus the encoded size in bytes of the messages
// added after the messages it counted. With no count, it is the encoded
// size of the whole request. A byte-level tokenizer gives every token at
// least one byte, and the encoding is larger than a message's framing
// tokens, so the sum bounds the count from above.
func (a *Harness) bound(usage requestUsage, window, round []*message.Message) int {
	n, from := usage.tokens, usage.messages
	if n == 0 {
		n = len(a.constructSystemPrompt())
		for _, t := range a.tools {
			// It encodes: the parameters are a JSON schema.
			raw, _ := json.Marshal(t.parameters)
			n += len(t.QualifiedName()) + len(t.description) + len(raw)
		}
	}
	for _, m := range slices.Concat(window[from:], round) {
		raw, _ := json.Marshal(m)
		n += len(raw)
	}
	return n
}

// size is the input tokens of a request with window and round: the
// provider's count, or the bound when the provider cannot count.
func (a *Harness) size(ctx context.Context, usage requestUsage, window, round []*message.Message) (int, error) {
	if !a.cannotCount {
		n, err := a.model.CountTokens(ctx, a.request(slices.Concat(window, round)))
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, ErrCannotCount) {
			return 0, fmt.Errorf("count tokens: %w", err)
		}
		slog.InfoContext(ctx, "the model cannot count tokens; requests are sized from the usage the provider reports",
			"area", telemetry.AreaModelTasks, "error", err)
		a.cannotCount = true
	}
	return a.bound(usage, window, round), nil
}

// summarize asks the model for one summary of window. Tools and the live
// system prompt stay so the request prefix matches a normal turn;
// tool_choice=none prevents calls. When the collapsed messages are saved,
// the summary tells the agent that search finds them.
func (a *Harness) summarize(ctx context.Context, window []*message.Message) (*message.Message, error) {
	slog.InfoContext(ctx, "context window over pressure, collapsing it into a summary",
		"area", telemetry.AreaModelTasks, "max_size", a.maxWindowSize, "messages", len(window))
	telemetry.InstrumentsFromContext(ctx).RecordCompress(ctx, telemetry.AgentIDFromContext(ctx))
	a.modelSeq++
	invokeMsgs := slices.Concat(window, []*message.Message{{Role: message.RoleDeveloper, Content: summaryInstruction}})
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
		if err := chunk.Err(); err != nil {
			streamErr = fmt.Errorf("context compress stream failed: %w", err)
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
	return &message.Message{Role: message.RoleDeveloper, Content: content}, nil
}

// summaryInstruction asks for the summary that replaces a collapsed window.
const summaryInstruction = "<instructions>\nSummarize the entire message history into a single, concise summary including key items for your current and past tasks, with a primary focus on the most recent user request.\n</instructions>"

// searchableHistoryNote ends a summary when the collapsed messages are saved.
const searchableHistoryNote = "The full earlier conversation is saved. Use the search tool to find its exact messages, and open to read one."

// trim cuts the round's largest results until the window and round fit
// MaxWindowSize. Before a result's first cut, its full text is kept in
// Overflow when that is set, and the cut result says where.
func (a *Harness) trim(ctx context.Context, usage requestUsage, window, round []*message.Message, n int) ([]*message.Message, error) {
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
			if m.Role == message.RoleTool && size(j) > 0 && (i < 0 || size(j) > size(i)) {
				i = j
			}
		}
		if i < 0 {
			return nil, fmt.Errorf("%w: the round needs %d tokens and the context window holds %d", ErrFailed, n, a.maxWindowSize)
		}
		if _, ok := full[i]; !ok {
			full[i] = round[i].Content
			if a.overflow != nil {
				path, err := a.overflow.Keep(ctx, a.sessionId, round[i].ToolCallID, full[i])
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
		if n, err = a.size(ctx, usage, window, round); err != nil {
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

func (a *Harness) handoffGenerate(ctx context.Context, window []*message.Message, plan []Todo, planDoc string) ([]*message.Message, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
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

	// Keep window[0] (the original user message), then plan document, handoff,
	// and a continue nudge when open todos remain.
	out := make([]*message.Message, 0, 4)
	out = append(out, window[0])
	if planDoc != "" {
		out = append(out, buildPlanDocumentMessage(planDoc))
	}
	out = append(out, &message.Message{Role: message.RoleDeveloper, Content: lastCompletedMessage})
	if planHasOpenTodos(plan) {
		out = append(out, &message.Message{
			Role:    message.RoleDeveloper,
			Content: continuePlanNudge,
		})
	}
	return out, usedFallback, nil
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
