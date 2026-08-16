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

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// Run starts a turn with a plain-text user message (SSE and simple hosts).
func (a *AgentHarness) Run(ctx context.Context, prompt string) (<-chan StreamEvent, error) {
	return a.RunMessage(ctx, &Message{Role: RoleUser, Content: prompt})
}

// RunMessage starts a turn with a full user Message (Content and optional ContentParts).
func (a *AgentHarness) RunMessage(ctx context.Context, user *Message) (<-chan StreamEvent, error) {
	if user == nil {
		return nil, fmt.Errorf("agent harness: RunMessage requires a user message")
	}
	if user.Role == "" {
		user.Role = RoleUser
	}
	if bad := UnsupportedMIMEs(a.model, user.MIMETypes()); len(bad) > 0 {
		return nil, fmt.Errorf("unsupported content type(s): %s", strings.Join(bad, ", "))
	}
	return a.startTurn(ctx, user)
}

// ReturnFromInterrupt applies host resolutions and resumes the parked tool batch.
// Keys are tool call ids (also the wire interrupt ids). Old checkpoints that
// stored a separate wire id are resolved through legacyInterruptIDs.
func (a *AgentHarness) ReturnFromInterrupt(ctx context.Context, finishedInterrupts map[string][]byte) (<-chan StreamEvent, error) {
	if err := a.applyInterruptResolutions(finishedInterrupts); err != nil {
		return nil, err
	}
	return a.startTurn(ctx, nil)
}

func (a *AgentHarness) applyInterruptResolutions(finishedInterrupts map[string][]byte) error {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	if a.interruptPayloads == nil {
		a.interruptPayloads = make(map[string][]byte)
	}
	for id, payload := range finishedInterrupts {
		toolCallID, ok := a.lookupToolCallID(id)
		if !ok {
			return fmt.Errorf("no tool call id found for interrupt %s: %w", id, interrupt.ErrInterruptNotFound)
		}
		a.interruptPayloads[toolCallID] = payload
		if _, err := a.session.ReturnInterrupt(toolCallID, payload); err != nil {
			return fmt.Errorf("return from interrupt %q: %w", id, err)
		}
		delete(a.legacyInterruptIDs, id)
		tc, ok := a.pendingToolCalls[toolCallID]
		if !ok {
			return fmt.Errorf("no pending tool call found for tool call id %s", toolCallID)
		}
		a.pendingToolCalls[toolCallID] = stores.PendingToolCall{ToolCall: tc.ToolCall, InterruptActive: false}
	}
	return nil
}

func (a *AgentHarness) startTurn(ctx context.Context, user *Message) (<-chan StreamEvent, error) {
	if err := a.ensureReady(ctx); err != nil {
		return nil, err
	}
	out := make(chan StreamEvent, streamEventBuffer)
	turnRT := session.NewRuntime(out, a.session)

	go func() {
		a.runMu.Lock()
		defer a.runMu.Unlock()
		defer close(out)
		defer a.persistSession(ctx)

		emitCancelled := func() {
			a.finalizeCancelledWork(out)
			out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: context cancelled: %w", ctx.Err())}
		}

		a.pairOpenToolCalls("unpaired tool call")
		if user != nil {
			if err := a.addToContext(ctx, user, out); err != nil {
				if ctx.Err() != nil {
					emitCancelled()
					return
				}
				out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: %w", err)}
				return
			}
		}
		a.runTurnLoop(ctx, out, turnRT, emitCancelled)
	}()
	return out, nil
}

func (a *AgentHarness) runTurnLoop(ctx context.Context, out chan StreamEvent, turnRT session.Runtime, emitCancelled func()) {
	turnModelRequests := 0
	hadToolRound := false
	for {
		if ctx.Err() != nil {
			emitCancelled()
			return
		}
		var toolResults []*Message
		var toolCalls []ToolCall
		pending := a.pendingSnapshot()
		if len(pending) == 0 {
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
				out <- StreamEvent{Type: StreamEventError, Error: outErr}
				return
			}
			turnModelRequests++
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
			modelFailed := false
			for {
				var chunk LLMResponseChunk
				var ok bool
				select {
				case <-ctx.Done():
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
				return
			}
			a.context.Add(&Message{
				Role:      RoleAssistant,
				ToolCalls: append([]ToolCall(nil), toolCalls...),
			})
			toolResults = make([]*Message, len(toolCalls))
		} else {
			toolResults = make([]*Message, len(pending))
			toolCalls = make([]ToolCall, 0, len(pending))
			for _, tc := range pending {
				if !tc.InterruptActive && tc.ToolCall != nil {
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
				runtimeCopy := turnRT.WithToolCallID(tcKey)
				output, toolDisp, err := a.toolRunner.Run(toolCtx, ToolInvocation{
					Tool:     tool,
					ArgsJSON: tc.Arguments,
					Runtime:  runtimeCopy,
				})
				var parked interrupt.Interrupt
				if errors.As(err, &parked) {
					serialized, err := parked.Serialize()
					if err != nil {
						toolSpan.Finish("error", err)
						out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("serialize interrupt: %w", err)}
						return
					}
					payload := map[string]any{
						"interruptId": tcKey,
						"type":        parked.TypeName(),
						"data":        json.RawMessage(serialized),
					}
					data, err := json.Marshal(payload)
					if err != nil {
						toolSpan.Finish("error", err)
						out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("marshal interrupt: %w", err)}
						return
					}
					telemetry.InstrumentsFromContext(ctx).RecordInterrupt(
						toolCtx, telemetry.AgentIDFromContext(ctx), parked.TypeName(),
					)
					toolSpan.Finish("interrupt", nil)
					a.pendingMu.Lock()
					a.pendingToolCalls[tcKey] = stores.PendingToolCall{ToolCall: &tc, InterruptActive: true}
					a.pendingMu.Unlock()
					out <- StreamEvent{Type: StreamEventInterrupt, MessageID: tcKey, Data: data}
					a.persistSession(toolCtx)
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
				a.emitPlanUpdate(out)
				toolResults[i] = a.emitToolResult(out, tc, output, "success")
			}(i, tc)
		}
		runningTools.Wait()
		if ctx.Err() != nil {
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
				a.pairCancelledToolResults(out)
				out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: %w", err)}
				return
			}
		}
		if len(a.pendingSnapshot()) > 0 {
			a.persistSession(ctx)
			return
		}
		if effect := batchEffects.resolved(); effect != EffectNone {
			if err := a.applyBatchToolResultEffect(ctx, effect); err != nil {
				slog.ErrorContext(ctx, "failed to apply tool result context effect", "session_id", a.sessionId, "effect", effect, "error", err)
				out <- StreamEvent{Type: StreamEventError, Error: err, Content: err.Error()}
				return
			}
		}
	}
}

// tagModelAfterToolsError wraps a stream error with ErrModelAfterTools.
// pairOpenToolCalls appends error tool results for assistant tool_calls that
// have no matching tool message. Restored dirty windows become valid before
// the next model turn; new turns never commit unpaired calls.
func (a *AgentHarness) pairOpenToolCalls(reason string) {
	if a.context == nil {
		return
	}
	msgs := a.context.Messages()
	haveResult := make(map[string]struct{})
	for _, m := range msgs {
		if m != nil && m.Role == RoleTool && m.ToolCallID != "" {
			haveResult[m.ToolCallID] = struct{}{}
		}
	}
	for _, m := range msgs {
		if m == nil || m.Role != RoleAssistant {
			continue
		}
		for _, tc := range m.ToolCalls {
			key := tc.Key()
			if key == "" {
				continue
			}
			if _, ok := haveResult[key]; ok {
				continue
			}
			a.context.Add(&Message{Role: RoleTool, ToolCallID: key, Content: reason})
			haveResult[key] = struct{}{}
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
