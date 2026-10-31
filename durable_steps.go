package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// absorbUser commits user as a round of its own, after the open round, if
// any, ends with its calls cancelled.
func (a *TurnManager) absorbUser(ctx context.Context, user *Message, out chan StreamEvent) error {
	if bad := UnsupportedMIMEs(a.model, user.MIMETypes()); len(bad) > 0 {
		return fmt.Errorf("unsupported content type(s): %s", strings.Join(bad, ", "))
	}
	if err := a.cancelOpenRound(ctx); err != nil {
		return err
	}
	return a.commitRound(ctx, []*Message{user}, EffectNone)
}

// cancelOpenRound ends a round the user moved past with a new message: each
// call without a result gets CancelledToolResultContent, the round commits,
// and parks are dropped.
func (a *TurnManager) cancelOpenRound(ctx context.Context) error {
	sm := a.session
	sm.mu.Lock()
	if r := sm.round; r != nil {
		for i := range r.Calls {
			if c := &r.Calls[i]; c.Result == nil {
				c.Result, _ = a.toolResultMessage(c.Call, CancelledToolResultContent, "error")
				c.Park, c.AwaitID = nil, ""
			}
		}
	}
	sm.mu.Unlock()
	return a.commitOpenRound(ctx)
}

// ApplySessionState upserts host-owned userState after construct/restore.
// Durable runtimes apply CreateSession/Prompt/Resume.State here so tools
// see it via HarnessRuntime.StateGet.
func (a *TurnManager) ApplySessionState(state map[string]any) error {
	for key, value := range state {
		if err := a.session.StateSet(key, value); err != nil {
			return err
		}
	}
	return nil
}

// runInference calls the model on the committed window. A reply with no
// calls commits as a round; a reply with calls opens one.
func (a *TurnManager) runInference(ctx context.Context, out chan StreamEvent) (InferenceStep, error) {
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
		return InferenceStep{}, outErr
	}
	asm := newStreamAssembler()
	var reply []*Message
	var toolCalls []ToolCall
	for {
		var chunk LLMResponseChunk
		var ok bool
		select {
		case <-ctx.Done():
			return InferenceStep{}, ctx.Err()
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
				return InferenceStep{}, chunk.Error
			}
			return InferenceStep{}, errors.New(chunk.Content)
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
	if len(toolCalls) == 0 {
		if err := a.commitRound(ctx, reply, EffectNone); err != nil {
			return InferenceStep{}, err
		}
		out <- StreamEvent{Type: StreamEventComplete}
		step := InferenceStep{Complete: true}
		for _, m := range reply {
			if m.Role == RoleAssistant && m.Content != "" {
				step.Result = m.Content
			}
		}
		return step, nil
	}
	reply = append(reply, &Message{Role: RoleAssistant, ToolCalls: slices.Clone(toolCalls)})
	a.session.mu.Lock()
	a.session.round = newOpenRound(reply, toolCalls)
	a.session.mu.Unlock()
	return InferenceStep{}, nil
}

// afterTools reports whether the model is answering tool results: the
// window's last committed round is a tool round.
func (a *TurnManager) afterTools() bool {
	window := a.context.Messages()
	return len(window) > 0 && window[len(window)-1].Role == RoleTool
}

// maxParallelToolCalls bounds how many calls of one reply run at once, so a
// reply with very many calls cannot start them all on one worker.
const maxParallelToolCalls = 8

// toolRun is one executed call before its result is recorded. A call that
// neither parked nor awaits a child has a result.
type toolRun struct {
	step    ToolStep
	jobs    *callJobs
	content string // the model-visible result
	status  string // "success" or "error"
	// effect is the window effect the call's outcome asked for.
	effect ToolResultEffect
	err    error // a failure that is not the model's to correct; logged
}

// runTools is Tools step seq. It applies resume, then runs the open round's
// runnable calls concurrently, at most maxParallelToolCalls at a time, and
// records them in call order: results and their client events, parks, and
// awaits on the round, and the children each call starts or cancels in the
// step's report. A tool sees the session's shared state (user state,
// permissions, interrupts, search cursors) as it runs. The round commits
// once each call has a result; until then the window does not change. A
// tool's failure is its result, so the error is the step's: an unknown
// resume id, or a failed commit. A retry of a step whose first attempt
// saved runs no tool: it returns the saved report and only commits.
func (a *TurnManager) runTools(ctx context.Context, seq int, resume map[string][]byte, state map[string]any, out chan StreamEvent) ([]ToolStep, error) {
	sm := a.session
	sm.mu.RLock()
	retried := sm.report.Seq == seq
	steps := sm.report.Steps
	sm.mu.RUnlock()
	if retried {
		// The first attempt applied the host state and ran the tools, which
		// may have changed it since; applying it again would undo that.
		return steps, a.commitOpenRound(ctx)
	}
	if err := a.ApplySessionState(state); err != nil {
		return nil, err
	}
	for id, payload := range resume {
		if err := sm.Resume(id, payload); err != nil {
			return nil, fmt.Errorf("return from interrupt %q: %w", id, err)
		}
	}
	sm.mu.RLock()
	idx := sm.round.runnable()
	calls := make([]ToolCall, len(idx))
	for k, i := range idx {
		calls[k] = sm.round.Calls[i].Call
	}
	sm.mu.RUnlock()

	runs := make([]toolRun, len(calls))
	slots := make(chan struct{}, maxParallelToolCalls)
	var wg sync.WaitGroup
	for k, tc := range calls {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			runs[k] = a.executeToolCall(ctx, tc, out)
		})
	}
	wg.Wait()

	steps = make([]ToolStep, len(runs))
	parked := false
	sm.mu.Lock()
	for k, r := range runs {
		c := &sm.round.Calls[idx[k]]
		steps[k] = r.step
		steps[k].Starts, steps[k].Cancels = r.jobs.starts, r.jobs.cancels
		switch {
		case r.step.Interrupted && parked:
			// The turn parks on one interrupt. This call did not finish, so
			// it drops its park and runs again after resume.
			c.Park = nil
			steps[k].Interrupted, steps[k].InterruptID, steps[k].InterruptData = false, "", nil
		case r.step.Interrupted:
			parked = true
			out <- StreamEvent{Type: StreamEventInterrupt, MessageID: r.step.InterruptID, Data: r.step.InterruptData}
		case r.jobs.await != "":
			c.AwaitID, steps[k].AwaitJobID = r.jobs.await, r.jobs.await
		default:
			a.emitPlanUpdate(out)
			c.Result = a.emitToolResult(out, c.Call, r.content, r.status)
			sm.round.Effect = sm.round.Effect.merge(r.effect)
			if r.err != nil {
				slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", c.Call.Name, "error", r.err)
			}
		}
	}
	sm.report = stepReport{Seq: seq, Steps: steps}
	sm.mu.Unlock()
	return steps, a.commitOpenRound(ctx)
}

// executeToolCall runs one call. It may park or wait on a child, but it
// records nothing: runTools records every call in call order.
func (a *TurnManager) executeToolCall(ctx context.Context, tc ToolCall, out chan StreamEvent) toolRun {
	turnRT := newToolRuntime(out, a.session, a.jobHost)
	tcKey := tc.Key()
	run := toolRun{step: ToolStep{Call: tc}, jobs: turnRT.jobs}
	toolCtx, toolSpan := telemetry.StartToolSpan(ctx, tc.Name, tc.Namespace)
	tool := a.findTool(tc.Name, tc.Namespace)
	if tool == nil {
		toolErr := Correctionf(ErrNotFound, "%s: not found. That is not a registered tool. Use a name from the available tools", tc.Name)
		toolSpan.Finish("error", toolErr)
		run.content, run.status = toolErr.Error(), "error"
		return run
	}
	runtimeCopy := turnRT.WithToolCallID(tcKey)
	outcome, err := a.toolRunner.Run(toolCtx, ToolInvocation{
		Tool:     tool,
		ArgsJSON: tc.Arguments,
		Runtime:  runtimeCopy,
	})
	var parked Interrupt
	if err != nil && !errors.As(err, &parked) && (errors.Is(err, ErrAuthExpired) || errors.Is(err, vfs.ErrAuthExpired)) {
		err = a.session.Park(tcKey, &AuthExpired{Tool: tc.Name})
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
		run.step.Interrupted, run.step.InterruptID, run.step.InterruptData = true, tcKey, data
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
	if turnRT.jobs.await != "" {
		toolSpan.Finish("success", nil)
		return run
	}
	toolSpan.Finish("success", nil)
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
