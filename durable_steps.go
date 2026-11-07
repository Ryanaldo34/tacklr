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
	"time"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// Commit is the Commit step: it applies in to the committed state (host
// state, then a round: each call's state writes in call order, then the
// round's messages; then each user message as a round of its own). A call
// of the round without its result is cancelled. The runtime saves the
// session after it; it is the only step whose state is saved.
func (a *Harness) Commit(ctx context.Context, in CommitIn, out chan session.Event) error {
	a.session.setState(in.State)
	if in.Round != nil {
		if err := a.commitReplyRound(ctx, in.Round, out); err != nil {
			return err
		}
	}
	for _, user := range in.Users {
		if bad := UnsupportedMIMEs(a.model, user.MIMETypes()); len(bad) > 0 {
			return fmt.Errorf("unsupported content type(s): %s", strings.Join(bad, ", "))
		}
		if err := a.commitRound(ctx, []*message.Message{user}, EffectNone, 0); err != nil {
			return err
		}
	}
	if in.Collapse {
		return a.window.Refit(ctx)
	}
	return nil
}

// commitReplyRound commits a reply and its calls' results, applying each
// call's writes in call order. A call without its result is cancelled.
func (a *Harness) commitReplyRound(ctx context.Context, r *Round, out chan session.Event) error {
	msgs := slices.Clone(r.Reply.Messages)
	effect := EffectNone
	for _, c := range r.Calls {
		if c.Done && c.Body == nil {
			return fmt.Errorf("tool call %s has a result but no body: %w", c.Key(), session.ErrNotFound)
		}
		result := (*message.Message)(nil)
		if b := c.Body; b != nil {
			a.session.apply(b.writes)
			a.emitPlanUpdate(out)
			result = b.result
			effect = max(effect, b.effect) // installing a plan wins over a handoff
		}
		if !c.Done {
			result, _ = a.toolResultMessage(c.Tool, CancelledToolResultContent, "error")
		}
		msgs = append(msgs, result)
	}
	return a.commitRound(ctx, msgs, effect, r.Reply.InputTokens)
}

// Infer is the Infer step: one model request on the committed window. It
// changes no session state: the reply commits as a round once its calls
// have their results.
func (a *Harness) Infer(ctx context.Context, out chan session.Event) (Reply, error) {
	afterTools := a.window.afterTools()
	turnCtx := ctx
	if afterTools {
		turnCtx = telemetry.ContextWithAfterTools(ctx)
	}
	events, err := a.invokeTurn(turnCtx)
	if err != nil {
		outErr := fmt.Errorf("model request failed: %w", err)
		if afterTools {
			outErr = fmt.Errorf("%w: %w", ErrModelAfterTools, err)
		}
		out <- session.Event{Type: session.EventError, Error: outErr}
		return Reply{}, outErr
	}
	asm := newStreamAssembler()
	var reply []*message.Message
	var toolCalls []message.ToolCall
	inputTokens := 0
	for {
		var chunk LLMResponseChunk
		var ok bool
		select {
		case <-ctx.Done():
			return Reply{}, ctx.Err()
		case chunk, ok = <-events:
		}
		if !ok {
			break
		}
		if afterTools && chunk.Err() != nil {
			chunk = tagModelAfterToolsError(chunk)
		}
		a.streamChunk(chunk, asm, out)
		if err := chunk.Err(); err != nil {
			return Reply{}, err
		}
		inputTokens = cmp.Or(chunk.InputTokens, inputTokens)
		asm.AddDelta(chunk)
		if !chunk.IsComplete {
			continue
		}
		toolCalls = append(toolCalls, chunk.ToolCalls...)
		// Each item keeps its place in the stream: a provider can put
		// reasoning between the calls of one reply, and replays it in order.
		switch {
		case isText(chunk.Type):
			reply = append(reply, asm.MessageFromComplete(chunk))
		case len(chunk.ToolCalls) > 0:
			reply = append(reply, &message.Message{Role: message.RoleAssistant, ToolCalls: slices.Clone(chunk.ToolCalls)})
		}
	}
	r := Reply{Messages: reply, Calls: toolCalls, InputTokens: inputTokens}
	for i, tc := range toolCalls {
		if tool := a.findTool(tc.Name, tc.Namespace); tool != nil && tool.timeout > 0 {
			if r.Timeouts == nil {
				r.Timeouts = make([]time.Duration, len(toolCalls))
			}
			r.Timeouts[i] = tool.timeout
		}
	}
	if len(toolCalls) == 0 {
		for _, m := range reply {
			if m.Role == message.RoleAssistant && m.Content != "" {
				r.Result = m.Content
			}
		}
	}
	return r, nil
}

// RunCall is the Tool step: it runs c once, from what its earlier runs did
// (its Body), and returns it changed. A job's result becomes the call's
// result; an answer resumes its park; otherwise its tool runs. It changes
// no session state: the call's body carries its writes for Commit.
func (a *Harness) RunCall(ctx context.Context, c Call, out chan session.Event) (Call, error) {
	answer, jobEnd, running, maxJobs := c.Answer, c.JobEnd, c.Jobs, c.MaxJobs
	c.Answer, c.JobEnd, c.Jobs, c.MaxJobs = nil, nil, nil, 0
	c.Park, c.Await, c.Starts, c.Cancels = nil, "", nil, nil
	if c.Body == nil {
		c.Body = &CallBody{}
	}
	if jobEnd != nil {
		status := "success"
		if jobEnd.Failed {
			status = "error"
		} else {
			a.keepSpecialistResult(ctx, jobEnd.Output)
		}
		a.finishCall(&c, out, jobEnd.Output, status, EffectNone)
		return c, nil
	}
	cc := newCallContext(out, a, c, running, maxJobs)
	if answer != nil {
		if err := cc.resume(answer); err != nil {
			return c, fmt.Errorf("return from interrupt %q: %w", c.Key(), err)
		}
	}
	a.executeToolCall(ctx, cc, out)
	return cc.call, nil
}

// finishCall gives c its result: the tool-result message, which it emits,
// and the window effect it asks for.
func (a *Harness) finishCall(c *Call, out chan session.Event, content, status string, effect ToolResultEffect) {
	c.Body.result, c.Body.effect = a.emitToolResult(out, c.Tool, content, status), effect
	c.Done = true
}

// executeToolCall runs cc's call through the interceptor chain and records
// how the run ended on the call: its result, its park, or the job it waits
// on.
func (a *Harness) executeToolCall(ctx context.Context, cc *CallContext, out chan session.Event) {
	c := &cc.call
	tc := c.Tool
	toolType := telemetry.ToolTypeFunction
	if tc.Namespace != "" {
		// A namespace is the MCP server the tool is from.
		toolType = telemetry.ToolTypeExtension
	}
	toolCtx, toolSpan := telemetry.StartToolSpan(ctx, telemetry.Tool{Name: tc.Name, Namespace: tc.Namespace, CallID: tc.CallID, Type: toolType})
	tool := a.findTool(tc.Name, tc.Namespace)
	if tool == nil {
		toolErr := Correctionf(ErrNotFound, "%s: not found. That is not a registered tool. Use a name from the available tools", tc.Name)
		toolSpan.Finish("error", toolErr)
		a.finishCall(c, out, toolErr.Error(), "error", EffectNone)
		return
	}
	outcome, err := a.toolRunner.Run(toolCtx, ToolInvocation{
		Tool:        tool,
		ArgsJSON:    tc.Arguments,
		CallContext: cc,
	})
	var parked Interrupt
	if err != nil && !errors.As(err, &parked) && errors.Is(err, ErrAuthExpired) {
		err = cc.park(&AuthExpired{Tool: tc.Name})
	}
	switch {
	case errors.As(err, &parked):
		serialized, _ := parked.Serialize()
		data, _ := json.Marshal(map[string]any{
			"interruptId": c.Key(),
			"type":        parked.TypeName(),
			"data":        json.RawMessage(serialized),
		})
		telemetry.Metrics().RecordInterrupt(
			toolCtx, telemetry.AgentFromContext(ctx), parked.TypeName(),
		)
		toolSpan.Finish("interrupt", nil)
		c.Park = &Park{InterruptID: c.Key(), Data: data}
	case err != nil:
		toolSpan.Finish("error", err)
		if !errors.Is(err, ErrCorrection) && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", tc.Name, "error", err)
		}
		a.finishCall(c, out, err.Error(), "error", EffectNone)
	case c.Await != "":
		toolSpan.Finish("success", nil)
	default:
		toolSpan.Finish("success", nil)
		a.finishCall(c, out, outcome.Output, "success", outcome.Effect)
	}
}

// tagModelAfterToolsError marks an error chunk as a model failure after
// the round's tools ran.
func tagModelAfterToolsError(chunk LLMResponseChunk) LLMResponseChunk {
	err := chunk.Err()
	if !errors.Is(err, ErrModelAfterTools) {
		err = fmt.Errorf("%w: %w", ErrModelAfterTools, err)
	}
	chunk.Error, chunk.Content = err, err.Error()
	return chunk
}
