package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

func (a *TurnManager) absorbUser(ctx context.Context, user *Message, out chan StreamEvent) error {
	if user != nil {
		if bad := UnsupportedMIMEs(a.model, user.MIMETypes()); len(bad) > 0 {
			return fmt.Errorf("unsupported content type(s): %s", strings.Join(bad, ", "))
		}
	}
	a.pairOpenToolCalls("unpaired tool call")
	if a.hasOpenToolWork() {
		a.pairCancelledToolResults(nil)
		a.clearInterruptParkState()
	}
	return a.addToContext(ctx, user, out)
}

func (a *TurnManager) runnableToolCalls() []ToolCall {
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
func (a *TurnManager) Checkpoint() (*SessionCheckpoint, error) {
	return captureCheckpoint(a.context.Messages(), a.session, a.pendingSnapshot())
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

// RestoreCheckpoint applies a SnapshotStore blob onto this harness.
func (a *TurnManager) RestoreCheckpoint(cp SessionCheckpoint) error {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	applied, err := applyCheckpoint(cp, a.session)
	if err != nil {
		return err
	}
	a.context.Restore(applied.Window)
	a.pendingMu.Lock()
	a.pendingToolCalls = applied.PendingToolCalls
	a.pendingMu.Unlock()
	return nil
}

func (a *TurnManager) runInference(ctx context.Context, st *TurnState, out chan StreamEvent) (InferenceStep, error) {
	// Pair any function_call still missing a tool message before the provider
	// round. Durable HITL used to drop the rest of a parallel batch; Azure then
	// 400s "No tool output found for function call".
	a.pairOpenToolCalls("unpaired tool call")

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
		outErr := fmt.Errorf("model request failed: %w", err)
		if st.HadToolRound {
			outErr = fmt.Errorf("%w: %w", ErrModelAfterTools, err)
		}
		out <- StreamEvent{Type: StreamEventError, Error: outErr}
		return InferenceStep{}, outErr
	}
	st.ModelRequests++
	asm := newStreamAssembler()
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
		if st.HadToolRound && (chunk.Type == StreamEventError || chunk.Error != nil) {
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
				a.context.Add(msg)
				if a.watchDog != nil {
					_ = a.watchDog.RecordOutput(msg)
				}
			}
		}
	}
	if len(toolCalls) == 0 {
		out <- StreamEvent{Type: StreamEventComplete}
		return InferenceStep{Complete: true}, nil
	}
	a.context.Add(&Message{
		Role:      RoleAssistant,
		ToolCalls: append([]ToolCall(nil), toolCalls...),
	})
	a.pendingMu.Lock()
	for i := range toolCalls {
		key := toolCalls[i].Key()
		cp := toolCalls[i]
		a.pendingToolCalls[key] = PendingToolCall{ToolCall: &cp, InterruptActive: false}
	}
	a.pendingMu.Unlock()
	return InferenceStep{ToolCalls: toolCalls}, nil
}

func (a *TurnManager) runToolCall(ctx context.Context, tc ToolCall, out chan StreamEvent) (ToolStep, error) {
	turnRT := newToolRuntime(out, a.session, a.childHost)
	tcKey := tc.Key()
	toolCtx, toolSpan := telemetry.StartToolSpan(ctx, tc.Name, tc.Namespace)
	tool := a.findTool(tc.Name, tc.Namespace)
	if tool == nil {
		a.pendingMu.Lock()
		delete(a.pendingToolCalls, tcKey)
		a.pendingMu.Unlock()
		toolErr := Correctionf(ErrNotFound, "%s: not found. That is not a registered tool. Use a name from the available tools", tc.Name)
		toolSpan.Finish("error", toolErr)
		msg := a.emitToolResult(out, tc, toolErr.Error(), "error")
		_ = a.addToContext(ctx, msg, out)
		return ToolStep{}, nil
	}
	runtimeCopy := turnRT.WithToolCallID(tcKey)
	output, toolDisp, err := a.toolRunner.Run(toolCtx, ToolInvocation{
		Tool:     tool,
		ArgsJSON: tc.Arguments,
		Runtime:  runtimeCopy,
	})
	var parked interrupt.Interrupt
	if err != nil && !errors.As(err, &parked) && (errors.Is(err, ErrAuthExpired) || errors.Is(err, vfs.ErrAuthExpired)) {
		err = a.session.Park(tcKey, &interrupt.AuthExpired{Tool: tc.Name})
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
		a.pendingMu.Lock()
		a.pendingToolCalls[tcKey] = PendingToolCall{ToolCall: &tc, InterruptActive: true}
		a.pendingMu.Unlock()
		out <- StreamEvent{Type: StreamEventInterrupt, MessageID: tcKey, Data: data}
		return ToolStep{Interrupted: true, InterruptID: tcKey, InterruptData: data}, nil
	}
	a.pendingMu.Lock()
	delete(a.pendingToolCalls, tcKey)
	a.pendingMu.Unlock()
	if err != nil {
		toolSpan.Finish("error", err)
		msg := a.emitToolResult(out, tc, err.Error(), "error")
		_ = a.addToContext(ctx, msg, out)
		if errors.Is(err, ErrCorrection) || errors.Is(err, context.Canceled) {
			return ToolStep{}, nil
		}
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
		_ = a.addToContext(ctx, msg, out)
	}
	if effect := effects.resolved(); effect != EffectNone {
		_ = a.applyBatchToolResultEffect(ctx, effect)
	}
	return ToolStep{}, nil
}

// SpawnSpecialistName is the built-in that calls HarnessRuntime.SpawnChild.
const SpawnSpecialistName = "spawn_specialist"

// ListChildrenName, GetChildName, and CancelChildName are built-ins on HarnessRuntime.Children / AwaitChild / CancelChild.
const (
	ListChildrenName = "list_children"
	GetChildName     = "get_child"
	CancelChildName  = "cancel_child"
)

func (a *TurnManager) applyResume(finishedInterrupts map[string][]byte) error {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	for id, payload := range finishedInterrupts {
		tc, ok := a.pendingToolCalls[id]
		if !ok {
			return fmt.Errorf("no tool call id found for interrupt %s: %w", id, interrupt.ErrInterruptNotFound)
		}
		if _, err := a.session.Resume(id, payload); err != nil {
			return fmt.Errorf("return from interrupt %q: %w", id, err)
		}
		a.pendingToolCalls[id] = PendingToolCall{ToolCall: tc.ToolCall, InterruptActive: false}
	}
	return nil
}

// pairOpenToolCalls appends error tool results for assistant tool_calls that
// have no matching tool message. Restored dirty windows become valid before
// the next model turn; new turns never commit unpaired calls.
//
// Pair on WireID (Responses call_id), matching toolResultMessage. Do not
// invent a result for a still-pending call — Resume re-runs those, and a
// Key()-keyed phantom (fc_ item id) is rejected by Azure as
// "No tool call found for function call output".
func (a *TurnManager) pairOpenToolCalls(reason string) {
	hasOutput := toolOutputIDs(a.context.Messages())
	pending := a.pendingSnapshot()
	for _, m := range a.context.Messages() {
		if m == nil || m.Role != RoleAssistant {
			continue
		}
		for _, tc := range m.ToolCalls {
			if _, ok := hasOutput[tc.WireID()]; ok {
				continue
			}
			if _, ok := pending[tc.Key()]; ok {
				continue
			}
			msg, _ := a.toolResultMessage(tc, reason, "error")
			a.context.Add(msg)
			hasOutput[msg.ToolCallID] = struct{}{}
		}
	}
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
