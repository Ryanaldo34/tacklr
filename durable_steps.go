package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/internal/drive"
	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func (a *AgentHarness) absorbUser(ctx context.Context, user *Message, out chan StreamEvent) error {
	if user == nil {
		return nil
	}
	a.pairOpenToolCalls("unpaired tool call")
	if a.hasOpenToolWork() {
		a.finalizeCancelledWork(nil)
	}
	return a.addToContext(ctx, user, out)
}

func (a *AgentHarness) runnableToolCalls() []ToolCall {
	pending := a.pendingSnapshot()
	out := make([]ToolCall, 0, len(pending))
	for _, p := range pending {
		if !p.InterruptActive && p.ToolCall != nil {
			out = append(out, *p.ToolCall)
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

func (a *AgentHarness) runInference(ctx context.Context, st *drive.TurnState, out chan StreamEvent) (drive.InferenceStep, error) {
	if st == nil {
		st = &drive.TurnState{}
	}
	// Pair any function_call still missing a tool message before the provider
	// round. Durable HITL used to drop the rest of a parallel batch; Azure then
	// 400s "No tool output found for function call".
	a.pairOpenToolCalls("unpaired tool call")

	if a.maxTurnRequests > 0 && st.ModelRequests >= a.maxTurnRequests {
		err := fmt.Errorf("%w: limit %d", ErrMaxTurnRequests, a.maxTurnRequests)
		out <- StreamEvent{Type: StreamEventError, Error: err}
		return drive.InferenceStep{}, err
	}
	turnCtx := ctx
	if st.HadToolRound {
		turnCtx = telemetry.ContextWithAfterTools(ctx)
	}
	events, err := a.tasks.Turn(turnCtx, a.tools, a.constructSystemPrompt())
	if err != nil {
		if ctx.Err() != nil {
			return drive.InferenceStep{}, ctx.Err()
		}
		var outErr error
		if st.HadToolRound {
			outErr = fmt.Errorf("%w: %w", ErrModelAfterTools, err)
		} else {
			outErr = fmt.Errorf("model request failed: %w", err)
		}
		out <- StreamEvent{Type: StreamEventError, Error: outErr}
		return drive.InferenceStep{}, outErr
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
			return drive.InferenceStep{}, err
		}
		var chunk LLMResponseChunk
		var ok bool
		select {
		case <-ctx.Done():
			failAnnounced("tool call cancelled")
			return drive.InferenceStep{}, ctx.Err()
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
			return drive.InferenceStep{}, ctx.Err()
		}
		if chunk.Type == StreamEventError || chunk.Error != nil {
			failAnnounced("model error")
			if chunk.Error != nil {
				return drive.InferenceStep{}, chunk.Error
			}
			return drive.InferenceStep{}, errors.New(chunk.Content)
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
		return drive.InferenceStep{}, ctx.Err()
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
				return drive.InferenceStep{}, err
			}
			st.HadToolRound = true
			return a.runInference(ctx, st, out)
		}
		out <- StreamEvent{Type: StreamEventComplete}
		return drive.InferenceStep{Complete: true}, nil
	}
	a.context.Add(&Message{
		Role:      RoleAssistant,
		ToolCalls: append([]ToolCall(nil), toolCalls...),
	})
	return drive.InferenceStep{ToolCalls: toolCalls}, nil
}

func (a *AgentHarness) runToolCall(ctx context.Context, tc ToolCall, out chan StreamEvent) (drive.ToolStep, error) {
	turnRT := session.NewRuntime(out, a.session)
	tcKey := tc.Key()
	toolCtx, toolSpan := telemetry.StartToolSpan(ctx, tc.Name, tc.Namespace)
	tool := a.findTool(tc.Name, tc.Namespace)
	if tool == nil {
		toolErr := fmt.Errorf("tool %q: %w", tc.Name, ErrNotFound)
		toolSpan.Finish("error", toolErr)
		msg := a.emitToolResult(out, tc, toolErr.Error(), "error")
		_ = a.addToContext(ctx, msg, out)
		return drive.ToolStep{}, toolErr
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
			return drive.ToolStep{}, serErr
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
			return drive.ToolStep{}, marErr
		}
		telemetry.InstrumentsFromContext(ctx).RecordInterrupt(
			toolCtx, telemetry.AgentIDFromContext(ctx), parked.TypeName(),
		)
		toolSpan.Finish("interrupt", nil)
		a.pendingMu.Lock()
		a.pendingToolCalls[tcKey] = stores.PendingToolCall{ToolCall: &tc, InterruptActive: true}
		a.pendingMu.Unlock()
		out <- StreamEvent{Type: StreamEventInterrupt, MessageID: tcKey, Data: data}
		return drive.ToolStep{Interrupted: true, InterruptID: tcKey, InterruptData: data}, nil
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
		return drive.ToolStep{}, err
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
			return drive.ToolStep{}, err
		}
	}
	if effect := effects.resolved(); effect != EffectNone {
		if err := a.applyBatchToolResultEffect(ctx, effect); err != nil {
			out <- StreamEvent{Type: StreamEventError, Error: err, Content: err.Error()}
			return drive.ToolStep{}, err
		}
	}
	return drive.ToolStep{}, nil
}

// SpawnWorkerName is the tool the durable driver maps to a nested run
// (child workflow on Temporal).
const SpawnWorkerName = "spawn_worker"
