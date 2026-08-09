package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func (a *AgentHarness) Run(ctx context.Context, prompt string) (<-chan StreamEvent, error) {
	if a.context == nil || a.tasks == nil {
		return nil, fmt.Errorf("agent harness: Run called on uninitialized harness")
	}
	if err := a.initSkills(ctx); err != nil {
		return nil, fmt.Errorf("load skills: %w", err)
	}
	a.initMCP(ctx)
	a.injectBuiltinTools()
	out := make(chan StreamEvent, streamEventBuffer)
	// Turn-scoped Runtime: event bus for this Run only; durable state is on a.session.
	// Tools receive a value copy via invoke; plan tools emit plan_update through it.
	turnRT := session.NewRuntime(out, a.store, a.session)

	emitCancelled := func() {
		// Pair open tools into the window before the cancel error event so the
		// checkpoint (and any client still draining) sees consistent tool results.
		a.pairCancelledToolResults(out)
		a.clearInterruptParkState()
		out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: context cancelled: %w", ctx.Err())}
	}

	// Run work is async so the caller can drain out. Every exit path checkpoints;
	// interrupt park also saves mid-turn for resume after restart. runMu ensures
	// ReturnFromInterrupt's follow-on Run cannot overlap this loop.
	go func() {
		a.runMu.Lock()
		defer a.runMu.Unlock()
		defer close(out)
		defer a.persistSession(ctx, "run_exit")
		if err := a.addToContext(ctx, &Message{Role: RoleUser, Content: prompt}, out); err != nil {
			if ctx.Err() != nil {
				emitCancelled()
				return
			}
			a.stripUnpairedToolCallsAfterInferenceError()
			out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: %w", err)}
			return
		}
		turnModelRequests := 0
		// hadToolRound: true after a tool batch so model errors are not treated as tool failures.
		hadToolRound := false
		for {
			if ctx.Err() != nil {
				emitCancelled()
				return
			}
			var toolResults []*Message
			var toolCalls []ToolCall
			if len(a.pendingToolCalls) == 0 {
				if a.maxTurnRequests > 0 && turnModelRequests >= a.maxTurnRequests {
					out <- StreamEvent{
						Type:  StreamEventError,
						Error: WrapStopReason(ErrMaxTurnRequests, fmt.Errorf("limit %d", a.maxTurnRequests)),
					}
					return
				}
				turnCtx := ctx
				if hadToolRound {
					turnCtx = telemetry.ContextWithAfterTools(ctx)
				}
				events, err := a.tasks.Turn(turnCtx, a.tools, a.constructSystemPrompt())
				if err != nil {
					if ctx.Err() != nil {
						emitCancelled()
						return
					}
					var outErr error
					if hadToolRound {
						outErr = fmt.Errorf("%w: %w", ErrModelAfterTools, err)
					} else {
						outErr = fmt.Errorf("model request failed: %w", err)
					}
					a.stripUnpairedToolCallsAfterInferenceError()
					out <- StreamEvent{Type: StreamEventError, Error: outErr}
					return
				}
				turnModelRequests++
				asm := newStreamAssembler()
				// Track announced function_calls so incomplete ones get a failed result
				// (clients otherwise stay in_progress).
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
				// Select on cancel: bare range would ignore ctx if the model keeps the channel open.
				modelFailed := false
				for {
					var chunk LLMResponseChunk
					var ok bool
					select {
					case <-ctx.Done():
						// Announced-only tools never entered the window; stream cancel
						// status for clients. Window pairing is handled by emitCancelled.
						failAnnounced("tool call cancelled")
						emitCancelled()
						return
					case chunk, ok = <-events:
					}
					if !ok {
						break
					}
					if hadToolRound && (chunk.Type == StreamEventError || chunk.Error != nil) {
						chunk = tagModelAfterToolsError(chunk)
					}
					if !a.streamChunk(ctx, chunk, out) {
						if ctx.Err() != nil {
							failAnnounced("tool call cancelled")
							emitCancelled()
							return
						}
						failAnnounced("tool call cancelled")
						emitCancelled()
						return
					}
					if chunk.Type == StreamEventError || chunk.Error != nil {
						failAnnounced("model error")
						modelFailed = true
						break
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
				if modelFailed {
					a.stripUnpairedToolCallsAfterInferenceError()
					return
				}
				if ctx.Err() != nil {
					failAnnounced("tool call cancelled")
					emitCancelled()
					return
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
					if ctx.Err() != nil {
						emitCancelled()
						return
					}
				}
				if len(toolCalls) == 0 {
					out <- StreamEvent{Type: StreamEventComplete}
					if ctx.Err() != nil {
						emitCancelled()
						return
					}
					a.persistSession(ctx, "turn_complete")
					return
				}
				// Record the assistant tool-call turn before tool results (Responses pairing).
				a.context.Add(&Message{
					Role:      RoleAssistant,
					ToolCalls: append([]ToolCall(nil), toolCalls...),
				})
				toolResults = make([]*Message, len(toolCalls))
			} else {
				toolResults = make([]*Message, len(a.pendingToolCalls))
				toolCalls = make([]ToolCall, 0, len(a.pendingToolCalls))
				for _, tc := range a.pendingToolCalls {
					if !tc.InterruptActive {
						toolCalls = append(toolCalls, *tc.ToolCall)
					}
				}
			}

			if ctx.Err() != nil {
				emitCancelled()
				return
			}

			var runningTools sync.WaitGroup
			var batchEffects batchToolResultEffects
			suppressWindow := make([]atomic.Bool, len(toolCalls))
			for i, tc := range toolCalls {
				runningTools.Add(1)
				go func(i int, tc ToolCall) {
					defer runningTools.Done()
					if ctx.Err() != nil {
						return
					}
					tcKey := tc.Key()
					toolCtx, toolSpan := telemetry.StartToolSpan(ctx, tc.Name, tc.Namespace)

					tool := a.findTool(tc.Name, tc.Namespace)
					if tool == nil {
						toolErr := fmt.Errorf("tool %q: %w", tc.Name, ErrToolNotFound)
						toolSpan.Finish("error", toolErr)
						toolResults[i] = a.emitToolResult(out, tc, toolErr.Error(), "error")
						return
					}
					runtimeCopy := turnRT
					runtimeCopy.CurrentToolCallID = tcKey
					output, toolDisp, err := a.toolRunner.Run(toolCtx, ToolInvocation{
						Tool:     tool,
						ArgsJSON: tc.Arguments,
						Runtime:  runtimeCopy,
					})
					var interrupt interrupt.Interrupt
					if errors.As(err, &interrupt) {
						intrId := uuid.New().String()
						serialized, err := interrupt.Serialize()
						if err != nil {
							toolSpan.Finish("error", err)
							out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("serialize interrupt: %w", err)}
							return
						}
						// Use json.RawMessage so interrupt data is nested JSON, not base64.
						payload := map[string]any{
							"interruptId": intrId,
							"type":        interrupt.TypeName(),
							"data":        json.RawMessage(serialized),
						}
						data, err := json.Marshal(payload)
						if err != nil {
							toolSpan.Finish("error", err)
							out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("marshal interrupt: %w", err)}
							return
						}
						telemetry.InstrumentsFromContext(ctx).RecordInterrupt(
							toolCtx, telemetry.AgentIDFromContext(ctx), interrupt.TypeName(),
						)
						toolSpan.Finish("interrupt", nil)
						out <- StreamEvent{Type: StreamEventInterrupt, MessageID: tcKey, Data: data}
						a.pendingMu.Lock()
						a.pendingToolCalls[tcKey] = stores.PendingToolCall{ToolCall: &tc, InterruptActive: true}
						a.interruptToRequester[intrId] = tcKey
						a.pendingMu.Unlock()
						a.persistSession(toolCtx, "interrupt")
						return
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
						toolResults[i] = a.emitToolResult(out, tc, content, "error")
						return
					}
					batchEffects.merge(toolDisp)
					if toolDisp.SuppressWindowMessage {
						suppressWindow[i].Store(true)
					}
					hookDisp := a.toolResultHooks.observe(toolCtx, ToolResultObservation{
						Name:     tc.Name,
						ArgsJSON: tc.Arguments,
						Output:   output,
						Runtime:  runtimeCopy,
					})
					batchEffects.merge(hookDisp)
					if hookDisp.SuppressWindowMessage {
						suppressWindow[i].Store(true)
					}
					toolSpan.Finish("success", nil)
					toolResults[i] = a.emitToolResult(out, tc, output, "success")
				}(i, tc)
			}
			runningTools.Wait()
			if ctx.Err() != nil {
				// Keep results that finished before cancel; open slots get cancelled pairing.
				for i, r := range toolResults {
					if r == nil || suppressWindow[i].Load() {
						continue
					}
					a.context.Add(r)
				}
				emitCancelled()
				return
			}
			if len(toolCalls) > 0 {
				hadToolRound = true
			}
			for i, r := range toolResults {
				if r == nil || suppressWindow[i].Load() {
					continue
				}
				if err := a.addToContext(ctx, r, out); err != nil {
					if ctx.Err() != nil {
						emitCancelled()
						return
					}
					a.stripUnpairedToolCallsAfterInferenceError()
					out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: %w", err)}
					return
				}
			}
			a.pendingMu.Lock()
			hasPending := len(a.pendingToolCalls) > 0
			a.pendingMu.Unlock()
			if hasPending {
				a.persistSession(ctx, "interrupt_park")
				return
			}
			if effect := batchEffects.resolved(); effect != EffectNone {
				if err := a.applyBatchToolResultEffect(ctx, effect); err != nil {
					slog.ErrorContext(ctx, "failed to apply tool result context effect", "session_id", a.sessionId, "effect", effect, "error", err)
					out <- StreamEvent{Type: StreamEventError, Content: err.Error()}
					return
				}
				a.persistSession(ctx, "effect_applied")
			}
		}
	}()
	return out, nil
}

func (a *AgentHarness) ReturnFromInterrupt(ctx context.Context, finishedInterrupts map[string][]byte) (<-chan StreamEvent, error) {
	a.pendingMu.Lock()
	if a.interruptPayloads == nil {
		a.interruptPayloads = make(map[string][]byte)
	}
	for interruptId, payload := range finishedInterrupts {
		toolCallId, ok := a.interruptToRequester[interruptId]
		if !ok {
			a.pendingMu.Unlock()
			return nil, fmt.Errorf("no tool call id found for interrupt %s: %w", interruptId, interrupt.ErrInterruptNotFound)
		}
		a.interruptPayloads[toolCallId] = payload
		if _, err := a.session.ReturnInterrupt(toolCallId, payload); err != nil {
			a.pendingMu.Unlock()
			return nil, fmt.Errorf("return from interrupt %q: %w", interruptId, err)
		}
		delete(a.interruptToRequester, interruptId)
		if tc, ok := a.pendingToolCalls[toolCallId]; ok {
			a.pendingToolCalls[toolCallId] = stores.PendingToolCall{ToolCall: tc.ToolCall, InterruptActive: false}
		} else {
			a.pendingMu.Unlock()
			return nil, fmt.Errorf("no pending tool call found for tool call id %s", toolCallId)
		}
	}
	a.pendingMu.Unlock()
	return a.Run(ctx, "")
}

// tagModelAfterToolsError wraps a stream error with ErrModelAfterTools.
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
