package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ryanaldo34/tacklr/internal/drive"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// Run starts a turn with a plain-text user message.
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
	return a.startTurn(ctx, user, nil)
}

// ReturnFromInterrupt applies host resolutions and resumes the parked tool batch.
// Keys are tool call ids, which are also the wire interrupt ids.
func (a *AgentHarness) ReturnFromInterrupt(ctx context.Context, finishedInterrupts map[string][]byte) (<-chan StreamEvent, error) {
	if err := a.resumeIDsExist(finishedInterrupts); err != nil {
		return nil, err
	}
	return a.startTurn(ctx, nil, finishedInterrupts)
}

func (a *AgentHarness) resumeIDsExist(finishedInterrupts map[string][]byte) error {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	for id := range finishedInterrupts {
		if _, ok := a.pendingToolCalls[id]; !ok {
			return fmt.Errorf("no tool call id found for interrupt %s: %w", id, interrupt.ErrInterruptNotFound)
		}
	}
	return nil
}

func (a *AgentHarness) applyResume(finishedInterrupts map[string][]byte) error {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	if a.interruptPayloads == nil {
		a.interruptPayloads = make(map[string][]byte)
	}
	for id, payload := range finishedInterrupts {
		tc, ok := a.pendingToolCalls[id]
		if !ok {
			return fmt.Errorf("no tool call id found for interrupt %s: %w", id, interrupt.ErrInterruptNotFound)
		}
		a.interruptPayloads[id] = payload
		if _, err := a.session.ReturnInterrupt(id, payload); err != nil {
			return fmt.Errorf("return from interrupt %q: %w", id, err)
		}
		a.pendingToolCalls[id] = stores.PendingToolCall{ToolCall: tc.ToolCall, InterruptActive: false}
	}
	return nil
}

func (a *AgentHarness) startTurn(ctx context.Context, user *Message, resume map[string][]byte) (<-chan StreamEvent, error) {
	if err := a.ensureReady(ctx); err != nil {
		return nil, err
	}
	out := make(chan StreamEvent, streamEventBuffer)

	go func() {
		a.runMu.Lock()
		defer a.runMu.Unlock()
		defer close(out)

		kind := telemetry.TurnKindPrompt
		promptLen := 0
		if user == nil {
			kind = telemetry.TurnKindResume
		} else {
			promptLen = len(user.Content)
		}
		ctx = telemetry.BindTurnContext(ctx, "", a.sessionId)
		ctx, span := telemetry.StartTurnSpan(ctx, telemetry.TurnAttrs{
			SessionID: a.sessionId,
			ThreadID:  a.sessionId,
			Kind:      kind,
			Runtime:   telemetry.RuntimeEmbed,
		})
		telemetry.EmitTurnReceived(ctx, kind, promptLen, 0)
		outcome := telemetry.OutcomeOK
		defer func() { span.End(outcome) }()

		cancelled := false
		emitCancelled := func() {
			if cancelled {
				return
			}
			cancelled = true
			outcome = telemetry.OutcomeCancelled
			a.finalizeCancelledWork(out)
			out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: context cancelled: %w", ctx.Err())}
		}

		if len(resume) > 0 {
			if err := a.applyResume(resume); err != nil {
				outcome = telemetry.OutcomeError
				out <- StreamEvent{Type: StreamEventError, Error: err}
				return
			}
		}

		if user != nil {
			if err := a.absorbUser(ctx, user, out); err != nil {
				if ctx.Err() != nil {
					emitCancelled()
					return
				}
				outcome = telemetry.OutcomeError
				out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: %w", err)}
				return
			}
		} else {
			a.pairOpenToolCalls("unpaired tool call")
		}
		loopOut := a.runTurnLoop(ctx, out, emitCancelled)
		if outcome == telemetry.OutcomeOK {
			outcome = loopOut
		}
	}()
	return out, nil
}

func (a *AgentHarness) runTurnLoop(ctx context.Context, out chan StreamEvent, emitCancelled func()) string {
	st := &drive.TurnState{}
	for {
		if ctx.Err() != nil {
			emitCancelled()
			return telemetry.OutcomeCancelled
		}
		var toolCalls []ToolCall
		pending := a.pendingSnapshot()
		if len(pending) == 0 {
			step, err := a.runInference(ctx, st, out)
			if ctx.Err() != nil {
				emitCancelled()
				return telemetry.OutcomeCancelled
			}
			if err != nil {
				return telemetry.OutcomeError
			}
			if step.Complete {
				return telemetry.OutcomeOK
			}
			toolCalls = step.ToolCalls
		} else {
			toolCalls = make([]ToolCall, 0, len(pending))
			for _, tc := range pending {
				if !tc.InterruptActive && tc.ToolCall != nil {
					toolCalls = append(toolCalls, *tc.ToolCall)
				}
			}
		}

		if ctx.Err() != nil {
			emitCancelled()
			return telemetry.OutcomeCancelled
		}

		st.HadToolRound = st.HadToolRound || len(toolCalls) > 0
		var wg sync.WaitGroup
		var parked atomic.Bool
		for _, tc := range toolCalls {
			wg.Add(1)
			go func(tc ToolCall) {
				defer wg.Done()
				if ctx.Err() != nil {
					return
				}
				step, _ := a.runToolCall(ctx, tc, out)
				if step.Interrupted {
					parked.Store(true)
				}
			}(tc)
		}
		wg.Wait()
		if ctx.Err() != nil {
			emitCancelled()
			return telemetry.OutcomeCancelled
		}
		// A park ends this slice even if Resume already resolved the interrupt.
		if parked.Load() || a.hasBlockingToolWork() {
			return telemetry.OutcomeYield
		}
	}
}

// tagModelAfterToolsError wraps a stream error with ErrModelAfterTools.
// pairOpenToolCalls appends error tool results for assistant tool_calls that
// have no matching tool message. Restored dirty windows become valid before
// the next model turn; new turns never commit unpaired calls.
//
// Pair on WireID (Responses call_id), matching toolResultMessage. Do not
// invent a result for a still-pending call — ReturnFromInterrupt resumes
// those, and a Key()-keyed phantom (fc_ item id) is rejected by Azure as
// "No tool call found for function call output".
func (a *AgentHarness) pairOpenToolCalls(reason string) {
	hasOutput := toolOutputIDs(a.context.Messages())
	pending := a.pendingSnapshot()
	for _, m := range a.context.Messages() {
		if m == nil || m.Role != RoleAssistant {
			continue
		}
		for _, tc := range m.ToolCalls {
			if tc.WireID() == "" {
				continue
			}
			if toolCallHasResult(hasOutput, tc) || toolCallIsPending(pending, tc) {
				continue
			}
			msg, _ := a.toolResultMessage(tc, reason, "error")
			a.context.Add(msg)
			hasOutput[msg.ToolCallID] = struct{}{}
		}
	}
}

func toolCallHasResult(hasOutput map[string]struct{}, tc ToolCall) bool {
	if _, ok := hasOutput[tc.WireID()]; ok {
		return true
	}
	if key := tc.Key(); key != "" {
		if _, ok := hasOutput[key]; ok {
			return true
		}
	}
	return false
}

func toolCallIsPending(pending map[string]stores.PendingToolCall, tc ToolCall) bool {
	if _, ok := pending[tc.Key()]; ok {
		return true
	}
	if _, ok := pending[tc.WireID()]; ok {
		return true
	}
	for _, p := range pending {
		if p.ToolCall == nil {
			continue
		}
		if p.ToolCall.Key() == tc.Key() || p.ToolCall.WireID() == tc.WireID() {
			return true
		}
	}
	return false
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
