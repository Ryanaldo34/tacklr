package tacklr

import (
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
		if err := a.commitRound(ctx, []*message.Message{user}, EffectNone); err != nil {
			return err
		}
	}
	return nil
}

// commitReplyRound commits a reply and its calls' results.
func (a *Harness) commitReplyRound(ctx context.Context, r *Round, out chan session.Event) error {
	msgs := slices.Clone(r.Reply.Messages)
	effect := EffectNone
	for i, call := range r.Reply.Calls {
		o := r.Outcomes[i]
		rec, err := record(&o)
		if err != nil {
			return err
		}
		a.session.apply(rec.Writes)
		a.emitPlanUpdate(out)
		result := rec.Result
		if !o.Done {
			result, _ = a.toolResultMessage(call, CancelledToolResultContent, "error")
		}
		msgs = append(msgs, result)
		effect = max(effect, rec.Effect) // installing a plan wins over a handoff
	}
	return a.commitRound(ctx, msgs, effect)
}

// Infer is the Infer step: one model request on the committed window. It
// changes no session state: the reply commits as a round once its calls
// have their results.
func (a *Harness) Infer(ctx context.Context, out chan session.Event) (Reply, error) {
	afterTools := a.afterTools()
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
		if chunk.InputTokens > 0 {
			// The provider counted this request: the window as it is now,
			// the tools and the system prompt.
			a.usage = requestUsage{tokens: chunk.InputTokens, messages: len(a.context.Messages())}
		}
		asm.AddDelta(chunk)
		if chunk.IsComplete {
			toolCalls = append(toolCalls, chunk.ToolCalls...)
			if chunk.Type == session.EventMessage || chunk.Type == session.EventReasoning {
				msg := asm.MessageFromComplete(chunk)
				reply = append(reply, msg)
			}
		}
	}
	r := Reply{Messages: reply, Calls: toolCalls}
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
		return r, nil
	}
	r.Messages = append(r.Messages, &message.Message{Role: message.RoleAssistant, ToolCalls: slices.Clone(toolCalls)})
	return r, nil
}

// afterTools reports whether the model is answering tool results: the
// window's last committed round is a tool round.
func (a *Harness) afterTools() bool {
	window := a.context.Messages()
	return len(window) > 0 && window[len(window)-1].Role == message.RoleTool
}

// toolRun is one executed call. A call that neither parked nor awaits a
// child has a result.
type toolRun struct {
	interrupted   bool
	interruptData []byte
	content       string // the model-visible result
	status        string // "success" or "error"
	// effect is the window effect the call's outcome asked for.
	effect ToolResultEffect
	err    error // a failure that is not the model's to correct; logged
}

// RunTool is the Tool step: it runs one call of the open round from its
// prior outcome. A child's result becomes the call's result; an answer
// resumes its park; otherwise the call runs. It changes no session state:
// the outcome carries the call's writes for Commit.
func (a *Harness) RunTool(ctx context.Context, run ToolRun, out chan session.Event) (CallOutcome, error) {
	o := CallOutcome{Call: run.Call}
	prior, err := record(run.Prior)
	if err != nil {
		return o, err
	}
	rec := callRecord{Layers: prior.Layers, Writes: prior.Writes}
	if c := run.Child; c != nil {
		status := "success"
		if c.Failed {
			status = "error"
		} else {
			a.keepSpecialistResult(ctx, c.Output)
		}
		rec.Result = a.emitToolResult(out, run.Call, c.Output, status)
		return o.with(rec, true), nil
	}
	cc := newCallContext(out, a, run, roundCall{Call: run.Call, Park: prior.Park, Layers: prior.Layers}, prior.Writes)
	if run.Answer != nil {
		if err := cc.resume(run.Answer); err != nil {
			return o, fmt.Errorf("return from interrupt %q: %w", run.Call.Key(), err)
		}
	}
	r := a.executeToolCall(ctx, cc)
	rec.Layers, rec.Writes = cc.call.Layers, cc.writes
	o.Starts, o.Cancels = cc.starts, cc.cancels
	switch {
	case r.interrupted:
		rec.Park = cc.call.Park
		o.Park = &Park{InterruptID: run.Call.Key(), Data: r.interruptData}
		return o.with(rec, false), nil
	case cc.await != "":
		o.AwaitID = cc.await
		return o.with(rec, false), nil
	}
	if r.err != nil {
		slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", run.Call.Name, "error", r.err)
	}
	rec.Result, rec.Effect = a.emitToolResult(out, run.Call, r.content, r.status), r.effect
	return o.with(rec, true), nil
}

// with is o carrying rec, Done when the call has its result. A record
// always encodes: its writes are JSON already and its park a registered
// interrupt.
func (o CallOutcome) with(rec callRecord, done bool) CallOutcome {
	o.Done = done
	o.Record, _ = json.Marshal(rec)
	return o
}

// executeToolCall runs rt's call through the interceptor chain. It may park
// or wait on a child.
func (a *Harness) executeToolCall(ctx context.Context, cc *CallContext) toolRun {
	tc := cc.call.Call
	tcKey := tc.Key()
	var run toolRun
	toolCtx, toolSpan := telemetry.StartToolSpan(ctx, tc.Name, tc.Namespace)
	tool := a.findTool(tc.Name, tc.Namespace)
	if tool == nil {
		toolErr := Correctionf(ErrNotFound, "%s: not found. That is not a registered tool. Use a name from the available tools", tc.Name)
		toolSpan.Finish("error", toolErr)
		run.content, run.status = toolErr.Error(), "error"
		return run
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
	if errors.As(err, &parked) {
		serialized, _ := parked.Serialize()
		data, _ := json.Marshal(map[string]any{
			"interruptId": tcKey,
			"type":        parked.TypeName(),
			"data":        json.RawMessage(serialized),
		})
		telemetry.InstrumentsFromContext(ctx).RecordInterrupt(
			toolCtx, telemetry.AgentIDFromContext(ctx), parked.TypeName(),
		)
		toolSpan.Finish("interrupt", nil)
		run.interrupted, run.interruptData = true, data
		return run
	}
	if err != nil {
		toolSpan.Finish("error", err)
		run.content, run.status = err.Error(), "error"
		if !errors.Is(err, ErrCorrection) && !errors.Is(err, context.Canceled) {
			run.err = err
		}
		return run
	}
	toolSpan.Finish("success", nil)
	if cc.await != "" {
		return run
	}
	run.content, run.status, run.effect = outcome.Output, "success", outcome.Effect
	return run
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
