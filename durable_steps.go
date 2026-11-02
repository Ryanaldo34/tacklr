package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Commit is the Commit step: it applies in to the committed state (host
// state, then a round: each call's state writes in call order, then the
// round's messages; then each user message as a round of its own). A call
// of the round without its result is cancelled. The runtime saves the
// session after it; it is the only step whose state is saved.
func (a *TurnManager) Commit(ctx context.Context, in CommitIn, out chan StreamEvent) error {
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
		if err := a.commitRound(ctx, []*Message{user}, EffectNone); err != nil {
			return err
		}
	}
	return nil
}

// commitReplyRound commits a reply and its calls' results.
func (a *TurnManager) commitReplyRound(ctx context.Context, r *Round, out chan StreamEvent) error {
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
func (a *TurnManager) Infer(ctx context.Context, out chan StreamEvent) (Reply, error) {
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
		out <- StreamEvent{Type: StreamEventError, Error: outErr}
		return Reply{}, outErr
	}
	asm := newStreamAssembler()
	var reply []*Message
	var toolCalls []ToolCall
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
		if afterTools && (chunk.Type == StreamEventError || chunk.Error != nil) {
			chunk = tagModelAfterToolsError(chunk)
		}
		a.streamChunk(chunk, out)
		if chunk.Type == StreamEventError || chunk.Error != nil {
			if chunk.Error != nil {
				return Reply{}, chunk.Error
			}
			return Reply{}, errors.New(chunk.Content)
		}
		asm.AddDelta(chunk)
		if chunk.IsComplete {
			toolCalls = append(toolCalls, chunk.ToolCalls...)
			if chunk.Type == StreamEventMessage || chunk.Type == StreamEventReasoning {
				msg := asm.MessageFromComplete(chunk)
				reply = append(reply, msg)
			}
		}
	}
	r := Reply{Messages: reply, Calls: toolCalls}
	if len(toolCalls) == 0 {
		for _, m := range reply {
			if m.Role == RoleAssistant && m.Content != "" {
				r.Result = m.Content
			}
		}
		return r, nil
	}
	r.Messages = append(r.Messages, &Message{Role: RoleAssistant, ToolCalls: slices.Clone(toolCalls)})
	return r, nil
}

// afterTools reports whether the model is answering tool results: the
// window's last committed round is a tool round.
func (a *TurnManager) afterTools() bool {
	window := a.context.Messages()
	return len(window) > 0 && window[len(window)-1].Role == RoleTool
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
func (a *TurnManager) RunTool(ctx context.Context, run ToolRun, out chan StreamEvent) (CallOutcome, error) {
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
	rt := newHarnessRuntime(out, a, run.Children, roundCall{Call: run.Call, Park: prior.Park, Layers: prior.Layers}, prior.Writes)
	if run.Answer != nil {
		if err := rt.resume(run.Answer); err != nil {
			return o, fmt.Errorf("return from interrupt %q: %w", run.Call.Key(), err)
		}
	}
	r := a.executeToolCall(ctx, rt)
	rec.Layers, rec.Writes = rt.call.Layers, rt.writes
	o.Starts, o.Cancels = rt.starts, rt.cancels
	switch {
	case r.interrupted:
		rec.Park = rt.call.Park
		o.Park = &Park{InterruptID: run.Call.Key(), Data: r.interruptData}
		return o.with(rec, false), nil
	case rt.await != "":
		o.AwaitID = rt.await
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
func (a *TurnManager) executeToolCall(ctx context.Context, rt *HarnessRuntime) toolRun {
	tc := rt.call.Call
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
		Tool:     tool,
		ArgsJSON: tc.Arguments,
		Runtime:  rt,
	})
	var parked Interrupt
	if err != nil && !errors.As(err, &parked) && (errors.Is(err, ErrAuthExpired) || errors.Is(err, vfs.ErrAuthExpired)) {
		err = rt.park(&AuthExpired{Tool: tc.Name})
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
	if rt.await != "" {
		return run
	}
	run.content, run.status, run.effect = outcome.Output, "success", outcome.Effect
	return run
}

func tagModelAfterToolsError(chunk LLMResponseChunk) LLMResponseChunk {
	if chunk.Error != nil {
		if !errors.Is(chunk.Error, ErrModelAfterTools) {
			chunk.Error = fmt.Errorf("%w: %w", ErrModelAfterTools, chunk.Error)
		}
		chunk.Content = chunk.Error.Error()
		return chunk
	}
	if chunk.Content != "" {
		chunk.Error = fmt.Errorf("%w: %s", ErrModelAfterTools, chunk.Content)
		chunk.Content = chunk.Error.Error()
	}
	return chunk
}
