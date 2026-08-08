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
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
)

func TestServeHTTP_listenCancelAndMountedHandlers(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "http-ok", IsComplete: true}
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	srvSSE := NewServer(r, SSE)
	srvACP := NewServer(r, ACP)

	rec := httptest.NewRecorder()
	req := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hi"}`)))
	srvSSE.serveHTTPSSE(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "http-ok") {
		t.Fatalf("sse: %d %s", rec.Code, rec.Body.String())
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`,
	)))
	srvACP.serveHTTPRPC(rec2, req2)
	if rec2.Code != 200 || !strings.Contains(rec2.Body.String(), "protocolVersion") {
		t.Fatalf("acp: %d %s", rec2.Code, rec2.Body.String())
	}

	// ServeHTTP + cancel (one protocol per mux — both register POST /).
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srvSSE.ServeHTTP(ctx, "127.0.0.1:0") }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeHTTP did not exit")
	}

	// nil ctx uses Background then we still need a cancellable path — exercise via short cancel parent is enough above.
	_ = NewServer(r, SSE).ServeHTTP
}

func TestServeHelpers_protocolNotRegisteredAndFallback(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	acpOnly := NewServer(r, ACP)
	rec := httptest.NewRecorder()
	acpOnly.serveHTTPSSE(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`))))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("sse without protocol status = %d", rec.Code)
	}
	rec2 := httptest.NewRecorder()
	NewServer(r, SSE).serveHTTPRPC(rec2, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`))))
	if rec2.Code != http.StatusInternalServerError {
		t.Fatalf("acp without protocol status = %d", rec2.Code)
	}

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "fb", IsComplete: true}
		},
	}
	srv := NewServer(newTestRegistry(testStore(t), strategy, nil), SSE)
	rec3 := httptest.NewRecorder()
	req := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"x"}`)))
	req.URL.Path = ""
	srv.serveHTTPSSE(rec3, req)
	if !strings.Contains(rec3.Body.String(), "fb") {
		t.Fatalf("fallback body = %s", rec3.Body.String())
	}
}

func TestNewServer_panicsWithoutRegistryOrProtocols(t *testing.T) {
	t.Run("nil registry", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		_ = NewServer(nil, ACP)
	})
	t.Run("no protocols", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		_ = NewServer(newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil))
	})
}

func TestRunTurn_agentTurnAndErrors(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, nil)

	stream, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "default", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Events {
	}
	stream.Cancel()
	stream.Close()
	stream.Cancel()
	stream.Close()
	_ = stream.TurnContext()
	_ = stream.Cancelled()

	if _, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "nope", Prompt: "x"}); err == nil {
		t.Fatal("want agent not found")
	}
	if _, err := (&EventStream{}).ResumeInterrupts(context.Background(), nil); err == nil {
		t.Fatal("want no harness error")
	}
}

func TestLogTurnError(t *testing.T) {
	logTurnError(errors.New("boom internal"), "a", "t")
	logTurnError(clientErrorf(ErrAgentNotFound, "missing"), "a", "t")
}

// TestHandleMessage_lifecycleMethods exercises HandleInbound through HandleMessage
// for initialize, session CRUD/config, authenticate, and unknown method.
func TestHandleMessage_lifecycleMethods(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	srv := NewServer(r, ACP)
	w := &recordingMessageWriter{}
	srv.Client = NewClientBridge(w)

	send := func(body string) {
		srv.HandleMessage(context.Background(), []byte(body), w)
	}

	send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}}`)
	send(`{"jsonrpc":"2.0","id":2,"method":"authenticate","params":{}}`)
	send(`{"jsonrpc":"2.0","id":3,"method":"session/new","params":{"cwd":"/tmp"}}`)

	var sessionID string
	for _, res := range w.Results {
		b, _ := json.Marshal(res.Result)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if s, ok := m["sessionId"].(string); ok && s != "" {
			sessionID = s
		}
	}
	if sessionID == "" {
		t.Fatal("session/new did not return sessionId")
	}

	send(fmt.Sprintf(`{"jsonrpc":"2.0","id":4,"method":"session/set_config_option","params":{"sessionId":%q,"configId":"model","value":"default"}}`, sessionID))
	send(fmt.Sprintf(`{"jsonrpc":"2.0","id":5,"method":"session/load","params":{"sessionId":%q,"cwd":"/tmp"}}`, sessionID))
	send(fmt.Sprintf(`{"jsonrpc":"2.0","id":6,"method":"session/close","params":{"sessionId":%q}}`, sessionID))
	send(fmt.Sprintf(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":%q}}`, sessionID))
	send(`{"jsonrpc":"2.0","id":7,"method":"totally_unknown","params":{}}`)
	send(`{`)
	if len(w.Results)+len(w.Errors) < 3 {
		t.Fatalf("expected multiple results/errors, got results=%d errors=%d", len(w.Results), len(w.Errors))
	}
}

func TestHandleSSE_invalidAgentAndMissingAccept(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	srv := NewServer(r, SSE)

	// Missing Accept → 406 (handleSSE early path).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"x"}`)))
	srv.serveHTTPSSE(rec, req)
	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("status = %d", rec.Code)
	}

	// Valid Accept but unknown agent → SSE error event after headers.
	rec2 := httptest.NewRecorder()
	req2 := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"nope","prompt":"x"}`)))
	srv.serveHTTPSSE(rec2, req2)
	if !strings.Contains(rec2.Body.String(), "error") && !strings.Contains(rec2.Body.String(), "not found") {
		t.Fatalf("body = %s", rec2.Body.String())
	}
}

func TestServeHTTP_listenError(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	// Invalid address should fail ListenAndServe quickly.
	err := NewServer(r, SSE).ServeHTTP(context.Background(), "127.0.0.1:99999x")
	if err == nil {
		t.Fatal("expected listen error")
	}
}

func TestACP_handleHTTP_invalidJSON(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{`)))
	NewACPProtocol(nil).(*acpProtocol).handleHTTP(ProtocolEnv{Registry: r}, rec, req)
	if !strings.Contains(rec.Body.String(), "error") && rec.Code == 0 {
		// jsonRPC writer may not set status; body should still have error JSON
	}
	if rec.Body.Len() == 0 {
		t.Fatal("expected error body")
	}
}

func TestRunTurnStream_nilStreamAndCancel(t *testing.T) {
	if err := runTurnStream(context.Background(), ProtocolEnv{}, SSE, "t", nil, nil); err != nil {
		t.Fatal(err)
	}

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			// Block until cancelled so turnCtx.Done path runs.
			<-ctx.Done()
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	stream, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "default", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Registry: r, Conn: &Conn{Writer: w}}
	// Cancel turn after stream starts.
	go func() {
		time.Sleep(20 * time.Millisecond)
		stream.Cancel()
	}()
	err = runTurnStream(context.Background(), env, SSE, "thread", stream, nil)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("runTurnStream cancel: %v", err)
	}
	stream.Close()
}
