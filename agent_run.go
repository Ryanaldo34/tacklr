package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func (a *AgentHarness) Run(ctx context.Context, prompt string) (<-chan StreamEvent, error) {
	if a.out == nil {
		return nil, fmt.Errorf("agent harness: Run called on uninitialized harness")
	}
	if err := a.initSkills(); err != nil {
		return nil, fmt.Errorf("load skills: %w", err)
	}
	a.initMCP(ctx)
	a.injectBuiltinTools()
	// Buffered so tool EmitUpdate (non-blocking) is not dropped while the Run
	// loop is busy or the consumer has not yet entered its receive loop.
	out := make(chan StreamEvent, streamEventBuffer)
	a.runtime.SetOutputChannel(out)

	emitCancelled := func() {
		emitNonBlocking(out, StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: context cancelled: %w", ctx.Err())})
	}

	// All work that may stream into out runs in this goroutine so callers can
	// drain the channel (addToContext window-pressure compress streams summaries).
	// Clear the runtime channel on exit so post-Run EmitUpdate does not send
	// on a closed channel (output channel is nil until the first Run).
	//
	// Durability: every Run exit path persists session state (complete, interrupt
	// park, cancel, model/tool/effect errors). Mid-turn interrupt also persists
	// immediately so clients can resume after process restart.
	go func() {
		defer close(out)
		defer a.runtime.SetOutputChannel(nil)
		defer a.persistSession(ctx, "run_exit")
		if err := a.addToContext(ctx, &Message{Role: RoleUser, Content: prompt}, out); err != nil {
			if ctx.Err() != nil {
				emitCancelled()
				return
			}
			// Absorb may fail during pressure compress (model invoke).
			a.stripUnpairedToolCallsAfterInferenceError()
			_ = emit(ctx, out, StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: %w", err)})
			return
		}
		a.runtime.EnsureInitialized()
		turnModelRequests := 0
		// True after at least one tool batch was executed this Run. Used so a later
		// model failure is not misread as "the last tool failed" (common in Zed ACP).
		hadToolRound := false
		for {
			// Non-blocking poll: use Err(), not select+default (same Done channel,
			// clearer cooperative cancel between turn phases).
			if ctx.Err() != nil {
				emitCancelled()
				return
			}
			var toolResults []*Message
			var toolCalls []ToolCall
			if len(a.pendingToolCalls) == 0 {
				if a.maxTurnRequests > 0 && turnModelRequests >= a.maxTurnRequests {
					_ = emit(ctx, out, StreamEvent{
						Type:  StreamEventError,
						Error: WrapStopReason(ErrMaxTurnRequests, fmt.Errorf("limit %d", a.maxTurnRequests)),
					})
					return
				}
				events, err := a.tasks.Turn(ctx, a.tools, a.constructSystemPrompt())
				if err != nil {
					if ctx.Err() != nil {
						emitCancelled()
						return
					}
					phase := "run: invoke"
					if hadToolRound {
						phase = "run: model after tools"
					}
					a.stripUnpairedToolCallsAfterInferenceError()
					_ = emit(ctx, out, StreamEvent{Type: StreamEventError, Error: fmt.Errorf("%s: %w", phase, err)})
					return
				}
				turnModelRequests++
				asm := newStreamAssembler()
				// Lifecycle bookkeeping: every function_call forwarded to the client is
				// announced. Incomplete calls (IsComplete=false) are not executed, so we
				// must emit a terminal failed result or the UI stays on in_progress.
				announced := make(map[string]ToolCall)
				announceOrder := make([]string, 0)
				failAnnounced := func(reason string) {
					for _, id := range announceOrder {
						tc := announced[id]
						tc.Status = "error"
						_ = emit(ctx, out, StreamEvent{
							Type:      StreamEventToolResult,
							MessageID: toolCallKey(tc),
							Content:   reason,
							ToolCalls: []ToolCall{tc},
						})
					}
					announceOrder = nil
					announced = make(map[string]ToolCall)
				}
				// Wait on stream or cancel — do not use bare range (blocks until
				// producer closes even after ctx cancel if the model ignores ctx).
				modelFailed := false
				for {
					chunk, ok, err := recvChunk(ctx, events)
					if err != nil {
						failAnnounced("tool call cancelled")
						emitCancelled()
						return
					}
					if !ok {
						break
					}
					// Tag provider failures that follow a successful tool batch so hosts
					// (e.g. Zed) do not attribute response.failed to list_plan/etc.
					if hadToolRound && (chunk.Type == StreamEventError || chunk.Error != nil) {
						chunk = tagModelAfterToolsError(chunk)
					}
					if !a.streamChunk(ctx, chunk, out) {
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
							key := toolCallKey(tc)
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
					// Checkpoint (via run_exit) must not re-submit unpaired function_calls.
					a.stripUnpairedToolCallsAfterInferenceError()
					return
				}
				if ctx.Err() != nil {
					failAnnounced("tool call cancelled")
					emitCancelled()
					return
				}
				// Close announced tool calls that will not be executed (incomplete status).
				executable := make(map[string]struct{}, len(toolCalls))
				for _, tc := range toolCalls {
					if key := toolCallKey(tc); key != "" {
						executable[key] = struct{}{}
					}
				}
				for _, id := range announceOrder {
					if _, ok := executable[id]; ok {
						continue
					}
					tc := announced[id]
					tc.Status = "error"
					if !emit(ctx, out, StreamEvent{
						Type:      StreamEventToolResult,
						MessageID: toolCallKey(tc),
						Content:   "tool call incomplete",
						ToolCalls: []ToolCall{tc},
					}) {
						emitCancelled()
						return
					}
				}
				// No executable tool calls so the turn ends
				if len(toolCalls) == 0 {
					if !emit(ctx, out, StreamEvent{Type: StreamEventComplete}) {
						emitCancelled()
						return
					}
					// defer run_exit also persists; explicit reason for metrics/logs clarity.
					a.persistSession(ctx, "turn_complete")
					return
				}
				// Responses multi-turn input needs each function_call_output paired
				// with a prior function_call (same call_id). Record the assistant
				// tool-call turn before tool results.
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
					tcKey := toolCallKey(tc)
					// Milestone: each tool call (create_plan, complete_todo, work tools, …).
					toolStart := time.Now()
					toolCtx, toolSpan := telemetry.TracerFromContext(ctx).Start(ctx, telemetry.SpanTool,
						trace.WithAttributes(
							attribute.String(telemetry.AttrArea, telemetry.AreaHarness),
							attribute.String(telemetry.AttrToolName, tc.Name),
							attribute.String(telemetry.AttrToolNS, tc.Namespace),
						),
					)
					defer toolSpan.End()

					finishTool := func(status string, err error) {
						toolSpan.SetAttributes(attribute.String(telemetry.AttrToolStatus, status))
						if err != nil {
							toolSpan.RecordError(err)
							toolSpan.SetStatus(codes.Error, err.Error())
							toolSpan.SetAttributes(attribute.String(telemetry.AttrOutcome, telemetry.OutcomeError))
						} else if status == "error" {
							toolSpan.SetAttributes(attribute.String(telemetry.AttrOutcome, telemetry.OutcomeError))
						} else {
							toolSpan.SetAttributes(attribute.String(telemetry.AttrOutcome, telemetry.OutcomeOK))
						}
						telemetry.InstrumentsFromContext(ctx).RecordTool(
							toolCtx,
							telemetry.AgentIDFromContext(ctx),
							tc.Name,
							tc.Namespace,
							status,
							time.Since(toolStart),
						)
					}

					tool := a.findTool(tc.Name, tc.Namespace)
					if tool == nil {
						toolErr := fmt.Errorf("tool %q: %w", tc.Name, ErrToolNotFound)
						finishTool("error", toolErr)
						toolResults[i] = a.emitToolResult(toolCtx, out, tc, toolErr.Error(), "error")
						return
					}
					runtimeCopy := a.runtime
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
							finishTool("error", err)
							_ = emit(toolCtx, out, StreamEvent{Type: StreamEventError, Error: fmt.Errorf("serialize interrupt: %w", err)})
							return
						}
						// json.RawMessage embeds the already-serialized interrupt as a
						// nested object. Plain []byte would base64-encode as a string
						// and break consumers that unmarshal data into the interrupt type.
						payload := map[string]any{
							"interruptId": intrId,
							"type":        interrupt.TypeName(),
							"data":        json.RawMessage(serialized),
						}
						data, err := json.Marshal(payload)
						if err != nil {
							finishTool("error", err)
							_ = emit(toolCtx, out, StreamEvent{Type: StreamEventError, Error: fmt.Errorf("marshal interrupt: %w", err)})
							return
						}
						telemetry.InstrumentsFromContext(ctx).RecordInterrupt(
							toolCtx, telemetry.AgentIDFromContext(ctx), interrupt.TypeName(),
						)
						finishTool("interrupt", nil)
						_ = emit(toolCtx, out, StreamEvent{Type: StreamEventInterrupt, MessageID: tcKey, Data: data})
						a.pendingMu.Lock()
						a.pendingToolCalls[tcKey] = stores.PendingToolCall{ToolCall: &tc, InterruptActive: true}
						a.interruptToRequester[intrId] = tcKey
						a.pendingMu.Unlock()
						// Persist before parking so resume works after process restart.
						a.persistSession(toolCtx, "interrupt")
						return
					}
					a.pendingMu.Lock()
					delete(a.pendingToolCalls, tcKey)
					a.pendingMu.Unlock()
					if err != nil {
						finishTool("error", err)
						toolResults[i] = a.emitToolResult(toolCtx, out, tc, fmt.Sprintf("An error occurred: %s", err.Error()), "error")
						return
					}
					// BuiltinResult effects first; optional host ToolResultHooks merge after.
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
					finishTool("success", nil)
					toolResults[i] = a.emitToolResult(toolCtx, out, tc, output, "success")
				}(i, tc)
			}
			runningTools.Wait()
			if ctx.Err() != nil {
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
					// Pressure compress is a model call; sanitize before checkpoint.
					a.stripUnpairedToolCallsAfterInferenceError()
					_ = emit(ctx, out, StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: %w", err)})
					return
				}
			}
			// There are pending interrupts to be resumed after user input is gathered
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
					_ = emit(ctx, out, StreamEvent{Type: StreamEventError, Content: err.Error()})
					// Still durable: plan may already have been updated by the tool.
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
		// Stash payload so spawn_worker can forward it to a parked child.
		a.interruptPayloads[toolCallId] = payload
		if _, err := a.runtime.ReturnInterrupt(toolCallId, payload); err != nil {
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

// tagModelAfterToolsError rewrites a provider error chunk so clients can tell
// the tool batch succeeded and the subsequent model request failed.
func tagModelAfterToolsError(chunk LLMResponseChunk) LLMResponseChunk {
	const prefix = "model after tools: "
	if chunk.Error != nil {
		if !strings.Contains(chunk.Error.Error(), "model after tools") {
			chunk.Error = fmt.Errorf("%s%w", prefix, chunk.Error)
		}
		if chunk.Content == "" {
			chunk.Content = chunk.Error.Error()
		} else if !strings.Contains(chunk.Content, "model after tools") {
			chunk.Content = prefix + chunk.Content
		}
		return chunk
	}
	if chunk.Content != "" && !strings.Contains(chunk.Content, "model after tools") {
		chunk.Content = prefix + chunk.Content
		chunk.Error = errors.New(chunk.Content)
	}
	return chunk
}
