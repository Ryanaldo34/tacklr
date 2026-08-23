package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// InferenceStep is the result of one model invocation for the durable driver.
type InferenceStep struct {
	ToolCalls []ToolCall
	Complete  bool
}

// ToolStep is the result of one tool invocation for the durable driver.
// Interrupted means the tool parked; the driver must persist, publish yield,
// and wait for Resume. It must not block inside the tool function.
type ToolStep struct {
	Interrupted   bool
	InterruptID   string
	InterruptData []byte
}

// TurnState is per-slice counters for the durable inference loop.
type TurnState struct {
	ModelRequests int
	HadToolRound  bool
}

// AbsorbUser adds a user message to the window (durable prompt start).
func (a *AgentHarness) AbsorbUser(ctx context.Context, user *Message, out chan StreamEvent) error {
	if user == nil {
		return nil
	}
	a.pairOpenToolCalls("unpaired tool call")
	if a.hasOpenToolWork() {
		a.finalizeCancelledWork(nil)
	}
	return a.addToContext(ctx, user, out)
}

// PendingToolCalls returns tool calls waiting to run (resume after yield).
func (a *AgentHarness) PendingToolCalls() []ToolCall {
	pending := a.pendingSnapshot()
	out := make([]ToolCall, 0, len(pending))
	for _, tc := range pending {
		if !tc.InterruptActive && tc.ToolCall != nil {
			out = append(out, *tc.ToolCall)
		}
	}
	return out
}

// Checkpoint captures the session blob for SnapshotStore.
func (a *AgentHarness) Checkpoint() (*stores.SessionCheckpoint, error) {
	return session.CaptureCheckpoint(a.context.Messages(), a.session, a.pendingSnapshot())
}

// RestoreCheckpoint applies a SnapshotStore blob onto this harness.
func (a *AgentHarness) RestoreCheckpoint(cp stores.SessionCheckpoint) error {
	applied, err := session.ApplyCheckpoint(cp, a.session)
	if err != nil {
		return err
	}
	a.context.Restore(applied.Window)
	a.pendingMu.Lock()
	a.pendingToolCalls = applied.PendingToolCalls
	if a.pendingToolCalls == nil {
		a.pendingToolCalls = make(map[string]stores.PendingToolCall)
	}
	a.pendingMu.Unlock()
	return nil
}

// RunInference is the inference activity body: model Invoke plus StreamEvent
// publish. Runtime drivers and in-process Run call this.
func (a *AgentHarness) RunInference(ctx context.Context, st *TurnState, out chan StreamEvent) (InferenceStep, error) {
	if st == nil {
		st = &TurnState{}
	}

	if a.maxTurnRequests > 0 && st.ModelRequests >= a.maxTurnRequests {
		err := fmt.Errorf("%w: limit %d", ErrMaxTurnRequests, a.maxTurnRequests)
		out <- StreamEvent{Type: StreamEventError, Error: err}
		return InferenceStep{}, err
	}
	turnCtx := ctx
	if st.HadToolRound {
		turnCtx = telemetry.ContextWithAfterTools(ctx)
	}
	events, err := a.tasks.Turn(turnCtx, a.tools, a.constructSystemPrompt())
	if err != nil {
		if ctx.Err() != nil {
			return InferenceStep{}, ctx.Err()
		}
		var outErr error
		if st.HadToolRound {
			outErr = fmt.Errorf("%w: %w", ErrModelAfterTools, err)
		} else {
			outErr = fmt.Errorf("model request failed: %w", err)
		}
		out <- StreamEvent{Type: StreamEventError, Error: outErr}
		return InferenceStep{}, outErr
	}
	st.ModelRequests++
	asm := newStreamAssembler()
	announced := make(map[string]ToolCall)
	announceOrder := make([]string, 0)
	failAnnounced := func(reason string) {
		for _, id := range announceOrder {
			tc := a.withToolPresentation(announced[id])
			tc.Status = "error"
			out <- StreamEvent{
				Type:      StreamEventToolResult,
				MessageID: tc.Key(),
				Content:   reason,
				ToolCalls: []ToolCall{tc},
			}
		}
		announceOrder = nil
		announced = make(map[string]ToolCall)
	}
	var toolCalls []ToolCall
	for {
		if err := ctx.Err(); err != nil {
			failAnnounced("tool call cancelled")
			return InferenceStep{}, err
		}
		var chunk LLMResponseChunk
		var ok bool
		select {
		case <-ctx.Done():
			failAnnounced("tool call cancelled")
			return InferenceStep{}, ctx.Err()
		case chunk, ok = <-events:
		}
		if !ok {
			break
		}
		if st.HadToolRound && (chunk.Type == StreamEventError || chunk.Error != nil) {
			chunk = tagModelAfterToolsError(chunk)
		}
		if !a.streamChunk(ctx, chunk, out) {
			failAnnounced("tool call cancelled")
			return InferenceStep{}, ctx.Err()
		}
		if chunk.Type == StreamEventError || chunk.Error != nil {
			failAnnounced("model error")
			if chunk.Error != nil {
				return InferenceStep{}, chunk.Error
			}
			return InferenceStep{}, errors.New(chunk.Content)
		}
		if chunk.Type == StreamEventFunctionCall {
			for _, tc := range chunk.ToolCalls {
				key := tc.Key()
				if key == "" {
					continue
				}
				if _, seen := announced[key]; !seen {
					announceOrder = append(announceOrder, key)
				}
				announced[key] = tc
			}
		}
		asm.AddDelta(chunk)
		if chunk.IsComplete {
			toolCalls = append(toolCalls, chunk.ToolCalls...)
			if chunk.Type == StreamEventMessage || chunk.Type == StreamEventReasoning {
				msg := asm.MessageFromComplete(chunk)
				a.context.Add(msg)
				a.recordWatchdog(msg)
			}
		}
	}
	if ctx.Err() != nil {
		failAnnounced("tool call cancelled")
		return InferenceStep{}, ctx.Err()
	}
	executable := make(map[string]struct{}, len(toolCalls))
	for _, tc := range toolCalls {
		if key := tc.Key(); key != "" {
			executable[key] = struct{}{}
		}
	}
	for _, id := range announceOrder {
		if _, ok := executable[id]; ok {
			continue
		}
		tc := a.withToolPresentation(announced[id])
		tc.Status = "error"
		out <- StreamEvent{
			Type:      StreamEventToolResult,
			MessageID: tc.Key(),
			Content:   "tool call incomplete",
			ToolCalls: []ToolCall{tc},
		}
	}
	if len(toolCalls) == 0 {
		if nudge := a.backgroundJobsNudge(); nudge != "" {
			if err := a.addToContext(ctx, &Message{Role: RoleUser, Content: nudge}, out); err != nil {
				return InferenceStep{}, err
			}
			st.HadToolRound = true
			return a.RunInference(ctx, st, out)
		}
		out <- StreamEvent{Type: StreamEventComplete}
		return InferenceStep{Complete: true}, nil
	}
	a.context.Add(&Message{
		Role:      RoleAssistant,
		ToolCalls: append([]ToolCall(nil), toolCalls...),
	})
	return InferenceStep{ToolCalls: toolCalls}, nil
}

// RunToolCall is the tool activity body: toolRunner.Run plus interrupt-as-outcome.
// VFS is injected by the caller (activity preamble). spawn_worker stays a tool
// here; the Temporal workflow intercepts it as a child workflow.
func (a *AgentHarness) RunToolCall(ctx context.Context, tc ToolCall, out chan StreamEvent) (ToolStep, error) {
	turnRT := session.NewRuntime(out, a.session)
	tcKey := tc.Key()
	toolCtx, toolSpan := telemetry.StartToolSpan(ctx, tc.Name, tc.Namespace)
	tool := a.findTool(tc.Name, tc.Namespace)
	if tool == nil {
		toolErr := fmt.Errorf("tool %q: %w", tc.Name, ErrNotFound)
		toolSpan.Finish("error", toolErr)
		msg := a.emitToolResult(out, tc, toolErr.Error(), "error")
		_ = a.addToContext(ctx, msg, out)
		return ToolStep{}, toolErr
	}
	runtimeCopy := turnRT.WithToolCallID(tcKey)
	output, toolDisp, err := a.toolRunner.Run(toolCtx, ToolInvocation{
		Tool:     tool,
		ArgsJSON: tc.Arguments,
		Runtime:  runtimeCopy,
	})
	var parked interrupt.Interrupt
	if errors.As(err, &parked) {
		serialized, serErr := parked.Serialize()
		if serErr != nil {
			toolSpan.Finish("error", serErr)
			out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("serialize interrupt: %w", serErr)}
			return ToolStep{}, serErr
		}
		payload := map[string]any{
			"interruptId": tcKey,
			"type":        parked.TypeName(),
			"data":        json.RawMessage(serialized),
		}
		data, marErr := json.Marshal(payload)
		if marErr != nil {
			toolSpan.Finish("error", marErr)
			out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("marshal interrupt: %w", marErr)}
			return ToolStep{}, marErr
		}
		telemetry.InstrumentsFromContext(ctx).RecordInterrupt(
			toolCtx, telemetry.AgentIDFromContext(ctx), parked.TypeName(),
		)
		toolSpan.Finish("interrupt", nil)
		a.pendingMu.Lock()
		a.pendingToolCalls[tcKey] = stores.PendingToolCall{ToolCall: &tc, InterruptActive: true}
		a.pendingMu.Unlock()
		out <- StreamEvent{Type: StreamEventInterrupt, MessageID: tcKey, Data: data}
		return ToolStep{Interrupted: true, InterruptID: tcKey, InterruptData: data}, nil
	}
	a.pendingMu.Lock()
	delete(a.pendingToolCalls, tcKey)
	a.pendingMu.Unlock()
	if err != nil {
		toolSpan.Finish("error", err)
		content := err.Error()
		if toolCtx.Err() != nil {
			content = CancelledToolResultContent
		}
		msg := a.emitToolResult(out, tc, content, "error")
		_ = a.addToContext(ctx, msg, out)
		return ToolStep{}, err
	}
	var effects batchToolResultEffects
	effects.merge(toolDisp)
	hookDisp := a.toolResultHooks.observe(toolCtx, ToolResultObservation{
		Name:     tc.Name,
		ArgsJSON: tc.Arguments,
		Output:   output,
		Runtime:  runtimeCopy,
	})
	effects.merge(hookDisp)
	toolSpan.Finish("success", nil)
	a.emitPlanUpdate(out)
	msg := a.emitToolResult(out, tc, output, "success")
	if !toolDisp.SuppressWindowMessage && !hookDisp.SuppressWindowMessage {
		if err := a.addToContext(ctx, msg, out); err != nil {
			return ToolStep{}, err
		}
	}
	if effect := effects.resolved(); effect != EffectNone {
		if err := a.applyBatchToolResultEffect(ctx, effect); err != nil {
			out <- StreamEvent{Type: StreamEventError, Error: err, Content: err.Error()}
			return ToolStep{}, err
		}
	}
	return ToolStep{}, nil
}

// PipeStreamEvents copies channel events to emit. Durable backends adapt
// emit callbacks to the harness chan StreamEvent API.
func PipeStreamEvents(emit func(StreamEvent)) (chan StreamEvent, func()) {
	out := make(chan StreamEvent, streamEventBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range out {
			if emit != nil {
				emit(ev)
			}
		}
	}()
	return out, func() {
		close(out)
		<-done
	}
}

// SpawnWorkerName is the tool the durable driver maps to a nested run
// (child workflow on Temporal).
const SpawnWorkerName = "spawn_worker"
