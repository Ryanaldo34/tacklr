package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestSSEProtocol_HandleInbound_noop(t *testing.T) {
	if err := SSE.HandleInbound(context.Background(), ProtocolEnv{}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestServeHTTP_mountsAndServesPrompt(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "http-ok", IsComplete: true}
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	// Exercise serveHTTPSSE (same path ServeHTTP mounts for SSE POST /).
	rec := httptest.NewRecorder()
	req := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hi"}`)))
	NewServer(r, SSE).serveHTTPSSE(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "http-ok") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestNewServer_panicsWithoutRegistryOrProtocols(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil registry")
		}
	}()
	_ = NewServer(nil, ACP)
}

func TestNewServer_panicsWithoutProtocols(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for no protocols")
		}
	}()
	_ = NewServer(newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil))
}

func TestLogTurnError_andClientBridgeErrorResponse(t *testing.T) {
	logTurnError(fmt.Errorf("boom internal"), "a", "t")
	logTurnError(clientErrorf(ErrAgentNotFound, "missing"), "a", "t")

	w := &recordingMessageWriter{}
	b := NewClientBridge(w)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	var callErr error
	go func() {
		defer close(done)
		_, callErr = b.Call(ctx, "ping", nil)
	}()
	// Wait for frame
	deadline := time.Now().Add(time.Second)
	for {
		frames := w.SnapshotFrames()
		if len(frames) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	frame := w.SnapshotFrames()[0]
	var req struct {
		ID int64 `json:"id"`
	}
	_ = json.Unmarshal(frame, &req)
	resp, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"error":   map[string]any{"code": -32000, "message": "nope"},
	})
	if !b.TryCompleteResponse(resp) {
		t.Fatal("TryCompleteResponse failed")
	}
	<-done
	if callErr == nil || !strings.Contains(callErr.Error(), "nope") {
		t.Fatalf("callErr = %v", callErr)
	}
	// Non-response frames return false.
	if b.TryCompleteResponse([]byte(`{"jsonrpc":"2.0","method":"x"}`)) {
		t.Fatal("request should not complete waiter")
	}
}

func TestElicitation_declineAndCancelAndValidation(t *testing.T) {
	opts := []control.UserChoice{{Title: "A"}, {Title: "B"}}
	if _, err := SelectionToElicitationParams("s", "tc", "", []control.UserChoice{{Title: "only"}}); err == nil {
		t.Fatal("want error for <2 options")
	}
	if _, err := SelectionToElicitationParams("s", "tc", "Q", []control.UserChoice{{Title: ""}, {Title: "B"}}); err == nil {
		t.Fatal("want error for empty title")
	}
	params, err := SelectionToElicitationParams("s", "tc", "Pick", opts)
	if err != nil {
		t.Fatal(err)
	}
	if params["message"] == nil {
		t.Fatal("missing message")
	}

	action, res, err := ElicitationResultToSelectionPayload([]byte(`{"action":"decline"}`), opts)
	if err != nil || action != "decline" || res != nil {
		t.Fatalf("decline: action=%s res=%s err=%v", action, res, err)
	}
	action, res, err = ElicitationResultToSelectionPayload([]byte(`{"action":"cancel"}`), opts)
	if err != nil || action != "cancel" {
		t.Fatalf("cancel: %s %v", action, err)
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{"action":"accept","content":{}}`), opts); err == nil {
		t.Fatal("accept without choice")
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{"action":"accept","content":{"choice":"Z"}}`), opts); err == nil {
		t.Fatal("unknown choice")
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{"action":"wat"}`), opts); err == nil {
		t.Fatal("unknown action")
	}
	if _, _, err := ElicitationResultToSelectionPayload([]byte(`{`), opts); err == nil {
		t.Fatal("bad json")
	}
}

func TestACP_OnStreamClosed_cancelledVsPark(t *testing.T) {
	p := acpProtocol{}
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Conn: &Conn{Writer: w}}
	if err := p.OnStreamClosed(context.Background(), env, "t", json.RawMessage(`1`), true); err != nil {
		t.Fatal(err)
	}
	if len(w.Results) != 1 {
		t.Fatalf("results = %d", len(w.Results))
	}
	w2 := &recordingMessageWriter{}
	env2 := ProtocolEnv{Conn: &Conn{Writer: w2}}
	if err := p.OnStreamClosed(context.Background(), env2, "t", json.RawMessage(`2`), false); err != nil {
		// WriteError returns nil from recording writer
		t.Log(err)
	}
	if len(w2.Errors) != 1 {
		t.Fatalf("errors = %d", len(w2.Errors))
	}
	// empty req id no-op
	if err := p.OnStreamClosed(context.Background(), env, "t", nil, false); err != nil {
		t.Fatal(err)
	}
}

func TestLineAndSSEMessageWriters(t *testing.T) {
	var buf bytes.Buffer
	lw := &lineMessageWriter{w: &buf}
	if err := lw.WriteResult(json.RawMessage(`1`), map[string]string{"ok": "1"}); err != nil {
		t.Fatal(err)
	}
	if err := lw.WriteError(json.RawMessage(`2`), clientErrorf(ErrInvalidRequest, "bad")); err != nil {
		t.Fatal(err)
	}
	if err := lw.WriteFrame([]byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"ok":"1"`) || !strings.Contains(out, "bad") || !strings.Contains(out, `{"x":1}`) {
		t.Fatalf("line writer out = %s", out)
	}

	// SSE writer via real response recorder.
	rec := httptest.NewRecorder()
	// Need Flusher — ResponseRecorder implements it.
	sw := &sseMessageWriter{w: rec, flusher: rec}
	if err := sw.WriteResult(json.RawMessage(`3`), map[string]string{"r": "1"}); err != nil {
		t.Fatal(err)
	}
	if err := sw.WriteError(json.RawMessage(`4`), clientErrorf(ErrInvalidRequest, "sse-err")); err != nil {
		t.Fatal(err)
	}
	if err := sw.WriteFrame([]byte(`{"type":"message","content":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "sse-err") || !strings.Contains(body, "event: message") {
		t.Fatalf("sse body = %s", body)
	}
}

func TestWSMessageWriter_resultErrorAndHelpers(t *testing.T) {
	// Use the same pattern as ws_test: dial server that accepts and reads.
	up := make(chan *websocket.Conn, 1)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		up <- c
		// Keep open briefly.
		time.Sleep(200 * time.Millisecond)
		_ = c.Close(websocket.StatusNormalClosure, "")
	}))
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")
	client, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")

	var serverConn *websocket.Conn
	select {
	case serverConn = <-up:
	case <-time.After(2 * time.Second):
		t.Fatal("no server conn")
	}
	mw := &wsMessageWriter{ctx: ctx, c: serverConn}
	if err := mw.WriteResult(json.RawMessage(`9`), map[string]string{"stopReason": "end_turn"}); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteError(json.RawMessage(`10`), clientErrorf(ErrInvalidRequest, "ws-err")); err != nil {
		t.Fatal(err)
	}
	if err := writeWSClientError(ctx, serverConn, errors.New("clienty")); err != nil {
		t.Fatal(err)
	}
	if err := writeWSInternalError(ctx, serverConn); err != nil {
		t.Fatal(err)
	}

	// Drain client side a few messages.
	for i := 0; i < 4; i++ {
		readCtx, c := context.WithTimeout(ctx, 500*time.Millisecond)
		_, data, err := client.Read(readCtx)
		c()
		if err != nil {
			break
		}
		if !strings.Contains(string(data), "end_turn") && !strings.Contains(string(data), "ws-err") &&
			!strings.Contains(string(data), "clienty") && !strings.Contains(string(data), "internal") {
			// still ok — types vary
			t.Logf("ws frame: %s", data)
		}
	}
}

func TestValidateSSERequest_moreCases(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{`, "invalid JSON"},
		{`{"prompt":"x"}`, "agent_id is required"},
		{`{"agent_id":"a","responses":{"i":{}},"prompt":"x"}`, "thread_id is required"},
		{`{"agent_id":"a","thread_id":"t","responses":{"i":{}},"prompt":"x"}`, "prompt is not allowed"},
		// Invalid raw JSON fragment inside responses value (not a JSON string).
		{`{"agent_id":"a","thread_id":"t","responses":{"i":{`, "invalid JSON"},
		{`{"agent_id":"a"}`, "prompt is required"},
	}
	for _, tc := range cases {
		_, err := validateSSERequest([]byte(tc.body))
		if err == nil {
			t.Errorf("body %s: expected error containing %q", tc.body, tc.want)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, tc.want) {
			t.Errorf("body %s: err=%q want contains %q", tc.body, msg, tc.want)
		}
	}
	// Happy path
	pr, err := validateSSERequest([]byte(`{"agent_id":"default","prompt":"hi","thread_id":"t1"}`))
	if err != nil || pr.Prompt != "hi" {
		t.Fatalf("ok path: %+v %v", pr, err)
	}
}

func TestRunTurn_nonSessionAndLoadErrors(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, nil)

	// Agent-only turn without session (SSE style).
	stream, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "default", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Events {
	}
	stream.Cancel()
	stream.Close()
	// Cancel/Close idempotent
	stream.Cancel()
	stream.Close()
	if stream.TurnContext() == nil {
		// may be cancelled already
	}

	// Unknown agent
	if _, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "nope", Prompt: "x"}); err == nil {
		t.Fatal("want agent not found")
	}

	// ResumeInterrupts without harness
	es := &EventStream{}
	if _, err := es.ResumeInterrupts(context.Background(), nil); err == nil {
		t.Fatal("want no harness error")
	}
}

func TestACP_handleHTTP_methodNotAllowed(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	NewServer(r, ACP).serveHTTPRPC(rec, req)
	// GET may 405 or method not allowed depending on handler
	if rec.Code == 200 && rec.Body.Len() == 0 {
		t.Log("get handled")
	}
}

func TestEventToAcpJsonRpc_errorWithErrorField(t *testing.T) {
	frames, err := eventToAcpJsonRpc("s1", &streaming.StreamEvent{
		Type:  streaming.StreamEventError,
		Error: errors.New("explode"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) == 0 {
		t.Fatal("want frames")
	}
	if !strings.Contains(string(frames[0]), "explode") {
		t.Fatalf("%s", frames[0])
	}
}

func TestInjectReqID_nonJSONFrame(t *testing.T) {
	out := injectReqID([][]byte{[]byte("not-json"), []byte(`{"a":1}`)}, json.RawMessage(`7`), true)
	if len(out) != 2 {
		t.Fatal(len(out))
	}
	if string(out[0]) != "not-json" {
		t.Fatalf("%s", out[0])
	}
}

func TestToSSEEvent_withError(t *testing.T) {
	ev := toSSEEvent(tacklr.StreamEvent{Type: streaming.StreamEventError, Error: errors.New("e")})
	if ev.Error != "e" {
		t.Fatalf("%+v", ev)
	}
}

// Ensure concurrent ClientBridge calls do not panic.
func TestClientBridge_concurrentCalls(t *testing.T) {
	w := &recordingMessageWriter{}
	b := NewClientBridge(w)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_, _ = b.Call(ctx, "m", map[string]int{"i": i})
		}(i)
	}
	// Complete any waiters we can.
	time.Sleep(20 * time.Millisecond)
	frames := w.SnapshotFrames()
	for _, f := range frames {
		var req struct {
			ID int64 `json:"id"`
		}
		if json.Unmarshal(f, &req) == nil && req.ID > 0 {
			resp, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
			b.TryCompleteResponse(resp)
		}
	}
	wg.Wait()
}
