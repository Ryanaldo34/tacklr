package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestResolveInterruptViaACP_dispatchOutcomes(t *testing.T) {
	// Unsupported kind.
	data, _ := json.Marshal(map[string]any{
		"interruptId": "i1",
		"type":        "not_a_real_kind",
		"data":        map[string]any{},
	})
	_, err := resolveInterruptViaACP(context.Background(), ProtocolEnv{Conn: &Conn{RPC: NewClientBridge(&recordingWriter{})}}, "s", &EventStream{}, &streaming.StreamEvent{Data: data})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported: %v", err)
	}

	// Bad envelope.
	_, err = resolveInterruptViaACP(context.Background(), ProtocolEnv{Conn: &Conn{RPC: NewClientBridge(&recordingWriter{})}}, "s", &EventStream{}, &streaming.StreamEvent{Data: []byte(`{`)})
	if err == nil {
		t.Fatal("expected parse error")
	}

	// User selection without elicitation form capability → park (nil, nil).
	usi := interrupt.UserSelectionInterrupt{Options: []interrupt.UserChoice{{Title: "A"}, {Title: "B"}}}
	ser, _ := usi.Serialize()
	selData, _ := json.Marshal(map[string]any{
		"interruptId": "i2",
		"type":        "user_selection_choice",
		"data":        json.RawMessage(ser),
	})
	ch, err := resolveInterruptViaACP(context.Background(), ProtocolEnv{Conn: &Conn{
		RPC:  NewClientBridge(&recordingWriter{}),
		Caps: ClientCapabilities{ElicitationForm: false},
	}}, "s", &EventStream{}, &streaming.StreamEvent{Data: selData, MessageID: "tc"})
	if err != nil || ch != nil {
		t.Fatalf("no-form park: ch=%v err=%v", ch, err)
	}

	// Empty type defaults to user_selection_choice (same park without form).
	legacy, _ := json.Marshal(map[string]any{
		"interruptId": "i3",
		"data":        json.RawMessage(ser),
	})
	ch, err = resolveInterruptViaACP(context.Background(), ProtocolEnv{Conn: &Conn{
		RPC: NewClientBridge(&recordingWriter{}),
	}}, "s", &EventStream{}, &streaming.StreamEvent{Data: legacy})
	if err != nil || ch != nil {
		t.Fatalf("legacy type park: ch=%v err=%v", ch, err)
	}

	// Stale Conn.Caps snapshot (false) but bridge has form support after initialize.
	// Mid-turn resolution must use live GetCaps, not the dispatch-time copy.
	bridge := NewClientBridge(&recordingWriter{})
	bridge.SetCaps(ClientCapabilities{ElicitationForm: true})
	// Without a matching client response Call will block; just assert we do not
	// park as "no form" — parse will proceed to elicitation Call. Cancel via ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = resolveInterruptViaACP(ctx, ProtocolEnv{Conn: &Conn{
		RPC:  bridge,
		Caps: ClientCapabilities{ElicitationForm: false}, // stale snapshot
	}}, "s", &EventStream{}, &streaming.StreamEvent{Data: selData, MessageID: "tc"})
	// Must not park (nil, nil): should attempt elicitation and fail via ctx/call.
	if err == nil {
		t.Fatal("expected error from elicitation call, not silent park")
	}
	if !strings.Contains(err.Error(), "elicitation") && !strings.Contains(err.Error(), "context") {
		// Accept either elicitation/create failure or context deadline.
		t.Logf("got err (ok if call attempted): %v", err)
	}

	wa := interrupt.WriteApprovalInterrupt{ToolName: "mutate", Title: "Write: /a", Args: `{"path":"/a"}`}
	waSer, _ := wa.Serialize()
	waData, _ := json.Marshal(map[string]any{
		"interruptId": "i4",
		"type":        interrupt.WriteApprovalType,
		"data":        json.RawMessage(waSer),
	})
	ch, err = resolveInterruptViaACP(context.Background(), ProtocolEnv{Conn: &Conn{
		RPC:  NewClientBridge(&recordingWriter{}),
		Caps: ClientCapabilities{ElicitationForm: false},
	}}, "s", &EventStream{}, &streaming.StreamEvent{Data: waData, MessageID: "tc"})
	if err != nil || ch != nil {
		t.Fatalf("write approval no-form park: ch=%v err=%v", ch, err)
	}
	id, parsed, err := ParseWriteApprovalFromInterruptData(waData)
	if err != nil || id != "i4" || parsed.ToolName != "mutate" {
		t.Fatalf("parse write approval: id=%s parsed=%+v err=%v", id, parsed, err)
	}
	if _, _, err := ParseWriteApprovalFromInterruptData([]byte(`{`)); err == nil {
		t.Fatal("parse write approval bad envelope")
	}
}

// TestResolvePermissionViaRequest_outcomes exercises every return path of
// resolvePermissionViaRequest through a real ClientBridge.
func TestResolvePermissionViaRequest_outcomes(t *testing.T) {
	mkEvent := func(data []byte) *streaming.StreamEvent {
		return &streaming.StreamEvent{
			Type:      streaming.StreamEventInterrupt,
			MessageID: "call_1",
			Data:      data,
		}
	}
	goodData := func() []byte {
		perm := interrupt.ToolPermissionInterrupt{
			ToolName: "sensitive",
			Options:  interrupt.DefaultPermissionOptions(),
		}
		ser, err := perm.Serialize()
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(map[string]any{
			"interruptId": "intr-1",
			"type":        "tool_permission",
			"data":        json.RawMessage(ser),
		})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	t.Run("parse error", func(t *testing.T) {
		env := ProtocolEnv{Conn: &Conn{RPC: NewClientBridge(&recordingWriter{})}}
		_, err := resolvePermissionViaRequest(context.Background(), env, "sess", &EventStream{}, mkEvent([]byte(`{`)))
		if err == nil {
			t.Fatal("expected parse error")
		}
	})

	t.Run("rpc error", func(t *testing.T) {
		w := &recordingWriter{}
		bridge := NewClientBridge(w)
		env := ProtocolEnv{Conn: &Conn{RPC: bridge}}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, err := resolvePermissionViaRequest(ctx, env, "sess", &EventStream{}, mkEvent(goodData()))
		if err == nil {
			t.Fatal("expected rpc/context error")
		}
	})

	t.Run("invalid result payload", func(t *testing.T) {
		w := &recordingWriter{}
		bridge := NewClientBridge(w)
		env := ProtocolEnv{Conn: &Conn{RPC: bridge}}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			_, err := resolvePermissionViaRequest(ctx, env, "sess", &EventStream{}, mkEvent(goodData()))
			errCh <- err
		}()
		respondToBridge(t, bridge, w, map[string]any{
			"outcome": map[string]any{"outcome": "selected"}, // missing optionId
		})
		if err := <-errCh; err == nil {
			t.Fatal("expected invalid result error")
		}
	})

	t.Run("cancelled outcome", func(t *testing.T) {
		w := &recordingWriter{}
		bridge := NewClientBridge(w)
		env := ProtocolEnv{Conn: &Conn{RPC: bridge}}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			_, err := resolvePermissionViaRequest(ctx, env, "sess", &EventStream{}, mkEvent(goodData()))
			errCh <- err
		}()
		respondToBridge(t, bridge, w, map[string]any{
			"outcome": map[string]any{"outcome": "cancelled"},
		})
		err := <-errCh
		if err == nil || !strings.Contains(err.Error(), "cancelled") {
			t.Fatalf("got %v, want cancelled", err)
		}
	})

	t.Run("selected resumes interrupts", func(t *testing.T) {
		store := testStore(t)
		sensitive := tacklr.NewTool(tacklr.ToolConfig{
			Name:    "sensitive",
			OnCall:  tacklr.OnCalls(tacklr.ToolPermissionOnCall),
			Handler: func(ctx context.Context) (string, error) { return "ok", nil },
		})
		ms := &mockInferenceStrategy{
			invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "call_1", CallID: "call_1", Name: "sensitive", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
			},
		}
		h := mustAgent(t, tacklr.AgentOptions{
			Config:    tacklr.Config{MaxWindowSize: 8192},
			SessionID: "sess-perm-resolve",
			Model:     ms,
			Store:     store,
			Tools:     []*tacklr.Tool{sensitive},
		})
		events, err := h.Run(context.Background(), "go")
		if err != nil {
			t.Fatal(err)
		}
		var interruptEv streaming.StreamEvent
		for ev := range events {
			if ev.Type == streaming.StreamEventInterrupt {
				interruptEv = ev
			}
		}
		if len(interruptEv.Data) == 0 {
			t.Fatal("expected interrupt event")
		}

		// After park, model on resume should finish cleanly.
		ms.invokeFn = func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
		}

		w := &recordingWriter{}
		bridge := NewClientBridge(w)
		env := ProtocolEnv{Conn: &Conn{RPC: bridge}}
		stream := &EventStream{harness: h, runCtx: context.Background()}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		type result struct {
			ch  <-chan streaming.StreamEvent
			err error
		}
		resCh := make(chan result, 1)
		go func() {
			ch, err := resolvePermissionViaRequest(ctx, env, "sess-perm-resolve", stream, &interruptEv)
			resCh <- result{ch, err}
		}()
		respondToBridge(t, bridge, w, map[string]any{
			"outcome": map[string]any{"outcome": "selected", "optionId": "allow-once"},
		})
		res := <-resCh
		if res.err != nil {
			t.Fatal(res.err)
		}
		if res.ch == nil {
			t.Fatal("expected resume events channel")
		}
		for range res.ch {
		}
	})
}

func respondToBridge(t *testing.T, b *ClientBridge, w *recordingWriter, result any) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var frame []byte
	for {
		w.mu.Lock()
		if len(w.frames) > 0 {
			frame = append([]byte(nil), w.frames[len(w.frames)-1]...)
			w.mu.Unlock()
			break
		}
		w.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for request frame")
		}
		time.Sleep(5 * time.Millisecond)
	}
	var req struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(frame, &req); err != nil {
		t.Fatal(err)
	}
	resp, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result":  result,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !b.TryCompleteResponse(resp) {
		t.Fatal("TryCompleteResponse failed")
	}
}
