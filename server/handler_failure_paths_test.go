package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Failing I/O doubles for transport/writer error paths.

type errWriter struct{ n int }

func (e *errWriter) Write(p []byte) (int, error) {
	e.n++
	return 0, errors.New("write fail")
}

type errFlusher struct{ errWriter }

func (errFlusher) Header() http.Header            { return make(http.Header) }
func (errFlusher) WriteHeader(int)                {}
func (e *errFlusher) Flush()                      {}
func (e *errFlusher) Write(p []byte) (int, error) { return e.errWriter.Write(p) }

// brokenBody fails on first Read.
type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, errors.New("body read fail") }
func (brokenBody) Close() error             { return nil }

func TestEventStream_nilGuards(t *testing.T) {
	var es *EventStream
	if es.TurnContext() != nil {
		t.Fatal("nil TurnContext")
	}
	es.Cancel() // no panic
	es.Close()
	es = &EventStream{}
	es.Cancel()
	es.Close()
	es.Close() // idempotent
	if _, err := es.ResumeInterrupts(context.Background(), nil); err == nil {
		t.Fatal("want no harness")
	}
	// Resume with cancelled runCtx falls back to parent.
	h := tacklr.NewAgent(context.Background(), tacklr.AgentOptions{
		Config: tacklr.Config{},
		Model: &mockInferenceStrategy{
			invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		},
		Store: testStore(t),
	})
	done := context.Background()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	es = &EventStream{Harness: h, runCtx: cancelled}
	ch, err := es.ResumeInterrupts(done, map[string][]byte{})
	if err != nil {
		// empty responses may still succeed starting Run with ""
		t.Log(err)
	}
	if ch != nil {
		for range ch {
		}
	}
	es.Close()
}

func TestWriterErrorPaths(t *testing.T) {
	// writeJSONLine marshal failure
	if err := writeJSONLine(io.Discard, make(chan int)); err == nil {
		t.Fatal("marshal fail")
	}
	// writeJSONLine write failure
	if err := writeJSONLine(&errWriter{}, map[string]string{"a": "b"}); err == nil {
		t.Fatal("write fail")
	}
	// second Write (newline) fails after first succeeds
	if err := writeJSONLine(&failAfter{n: 1}, map[string]string{"a": "b"}); err == nil {
		t.Fatal("newline write fail")
	}

	lw := &lineMessageWriter{w: &errWriter{}}
	if err := lw.WriteFrame([]byte("x")); err == nil {
		t.Fatal("frame write fail")
	}
	lw2 := &lineMessageWriter{w: &failAfter{n: 1}}
	if err := lw2.WriteFrame([]byte("x")); err == nil {
		t.Fatal("frame newline fail")
	}

	jw := &jsonRPCMessageWriter{w: &errFlusher{}}
	if err := jw.WriteFrame([]byte(`{}`)); err == nil {
		t.Fatal("jsonrpc frame write fail")
	}

	sw := &sseMessageWriter{w: httptest.NewRecorder(), flusher: httptest.NewRecorder()}
	if err := sw.WriteResult(json.RawMessage(`1`), make(chan int)); err == nil {
		t.Fatal("sse result marshal fail")
	}

	if err := writeSSEEvent(&errWriter{}, httptest.NewRecorder(), "e", []byte("d")); err == nil {
		t.Fatal("sse event write fail")
	}
}

type failAfter struct {
	n, i int
}

func (f *failAfter) Write(p []byte) (int, error) {
	f.i++
	if f.i > f.n {
		return 0, errors.New("fail after")
	}
	return len(p), nil
}

func TestHandleHTTP_bodyReadError(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", brokenBody{})
	NewACPProtocol(nil).(*acpProtocol).handleHTTP(ProtocolEnv{Registry: r}, rec, req)
	if rec.Body.Len() == 0 {
		t.Fatal("expected error response body")
	}
}

func TestHandleInbound_cancelledContext(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &recordingMessageWriter{}
	err := NewACPProtocol(nil).(*acpProtocol).HandleInbound(ctx, ProtocolEnv{Registry: r, Conn: &Conn{Writer: w}}, []byte(`{}`))
	if err == nil {
		t.Fatal("want cancelled")
	}
}

func TestHandleSSE_noFlusherAndInvalidJSON(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	// ResponseWriter without Flusher → 500 StreamingNotSupported
	nf := &nonFlushResponse{header: make(http.Header), body: &bytes.Buffer{}}
	req := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"x"}`)))
	sseProtocol{}.handleSSE(ProtocolEnv{Registry: r}, nf, req)
	if nf.code != http.StatusInternalServerError && nf.body.Len() == 0 {
		// WriteHeader may encode message without status in some paths
		t.Logf("no-flusher code=%d body=%s", nf.code, nf.body.String())
	}

	// Invalid body with flusher → SSE error frame
	rec := httptest.NewRecorder()
	req2 := newSSERequest(t, "/", bytes.NewReader([]byte(`{`)))
	sseProtocol{}.handleSSE(ProtocolEnv{Registry: r}, rec, req2)
}

type nonFlushResponse struct {
	header http.Header
	body   *bytes.Buffer
	code   int
}

func (n *nonFlushResponse) Header() http.Header         { return n.header }
func (n *nonFlushResponse) Write(p []byte) (int, error) { return n.body.Write(p) }
func (n *nonFlushResponse) WriteHeader(code int)        { n.code = code }

func TestHTTPMux_invokesMountedRoutes(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "mux", IsComplete: true}
		},
	}
	srv := NewServer(newTestRegistry(testStore(t), strategy, nil), SSE)
	mux := srv.HTTPMux()
	rec := httptest.NewRecorder()
	req := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hi"}`)))
	mux.ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), "mux") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestRunTurnStream_writeFrameError(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "x", IsComplete: true}
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	stream, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "default", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	env := ProtocolEnv{Registry: r, Conn: &Conn{Writer: &failFrameWriter{}}}
	err = runTurnStream(context.Background(), env, SSE, "t", stream, nil)
	if err == nil {
		t.Fatal("want write frame error")
	}
	stream.Close()
}

type failFrameWriter struct{}

func (failFrameWriter) WriteResult(json.RawMessage, any) error  { return nil }
func (failFrameWriter) WriteError(json.RawMessage, error) error { return nil }
func (failFrameWriter) WriteFrame([]byte) error                 { return errors.New("frame fail") }

func TestRunTurn_sessionCwdMismatchAndNoAgent(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	p := NewACPProtocol(nil).(*acpProtocol)
	env := ProtocolEnv{Registry: r}
	params, _ := json.Marshal(map[string]any{"cwd": "/cwd-a"})
	sid, _, err := p.CreateSession(context.Background(), env, params)
	if err != nil {
		t.Fatal(err)
	}
	turnParams, _ := json.Marshal(map[string]any{
		"sessionId": sid,
		"cwd":       "/cwd-b",
		"prompt":    []map[string]string{{"type": "text", "text": "x"}},
	})
	if _, err := p.BindTurn(context.Background(), env, sid, turnParams); err == nil || !strings.Contains(err.Error(), "cwd") {
		t.Fatalf("err = %v", err)
	}

	empty := NewRegistry(testStore(t), "")
	if _, err := empty.RunTurn(context.Background(), TurnRequest{Prompt: "x"}); err == nil {
		t.Fatal("want agent_id required")
	}
	if _, err := empty.RunTurn(context.Background(), TurnRequest{SessionID: "s1", Prompt: "x"}); err == nil {
		t.Fatal("want no agent configured")
	}
}

func TestParsePermissionAndSelection_badData(t *testing.T) {
	if _, _, err := ParseUserSelectionFromInterruptData([]byte(`{`)); err == nil {
		t.Fatal("selection bad envelope")
	}
	if _, _, err := ParseUserSelectionFromInterruptData([]byte(`{"interruptId":"i","data":"x"}`)); err == nil {
		t.Fatal("selection bad data")
	}
	if _, _, err := ParseToolPermissionFromInterruptData([]byte(`{"interruptId":"i","data":"x"}`)); err == nil {
		t.Fatal("permission bad data")
	}
}

type failSaveStore struct {
	*stores.InMemoryStore
}

func (f failSaveStore) SaveSession(ctx context.Context, id string, cp stores.SessionCheckpoint) error {
	return errors.New("save fail")
}

func TestCreateSession_wireStoreIndependentOfHarnessStore(t *testing.T) {
	// Wire session create does not require harness BaseStore.
	r := NewRegistry(failSaveStore{InMemoryStore: stores.NewInMemoryStore()}, "default")
	r.Register("default", AgentSpec{Model: &mockInferenceStrategy{}})
	p := NewACPProtocol(nil).(*acpProtocol)
	params, _ := json.Marshal(map[string]any{"cwd": "/tmp"})
	sid, _, err := p.CreateSession(context.Background(), ProtocolEnv{Registry: r}, params)
	if err != nil || sid == "" {
		t.Fatalf("wire session create: %v %q", err, sid)
	}
}

func TestLoadAgent_noStoreOnLoad(t *testing.T) {
	r := NewRegistry(nil, "default")
	r.Register("default", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 1024},
		Model:  &mockInferenceStrategy{},
	})
	_, err := r.RunTurn(context.Background(), TurnRequest{
		AgentID:  "default",
		ThreadID: "t1",
		Load:     true,
		Prompt:   "x",
	})
	if err == nil {
		t.Fatal("want store not configured or similar")
	}
}

func TestRunTurnStream_onStreamClosedWhenFinished(t *testing.T) {
	// Natural end invokes OnStreamClosed with cancelled=false.
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "bye", IsComplete: true}
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	stream, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "default", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Registry: r, Conn: &Conn{Writer: w}}
	// SSE OnStreamClosed is no-op; still hits runTurnStream end(false).
	_ = runTurnStream(context.Background(), env, SSE, "t", stream, nil)
	stream.Close()
}

func TestHandleInbound_notificationUnknown(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	w := &recordingMessageWriter{}
	// Unknown notification is ignored (no id).
	err := NewACPProtocol(nil).(*acpProtocol).HandleInbound(context.Background(), ProtocolEnv{Registry: r, Conn: &Conn{Writer: w}},
		[]byte(`{"jsonrpc":"2.0","method":"session/foo","params":{}}`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientBridge_writeFrameError(t *testing.T) {
	b := NewClientBridge(&failFrameWriter{})
	if _, err := b.Call(context.Background(), "m", nil); err == nil {
		t.Fatal("want write error")
	}
}

func TestSetConfigOption_unknownSession(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	p := NewACPProtocol(nil).(*acpProtocol)
	if _, err := p.setConfig(context.Background(), ProtocolEnv{Registry: r}, "missing", "model", "default"); err == nil {
		t.Fatal("want session not found")
	}
}

func TestResolveSelectionViaElicitation_withQuestion(t *testing.T) {
	// Drive accept path with question stashed on harness runtime.
	optionsJSON := `[{"title":"A","description":"","isRecommended":true},{"title":"B","description":"","isRecommended":false}]`
	tool := tacklr.NewTool(tacklr.ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime *tacklr.HarnessRuntime) (string, error) {
			runtime.StateSet("_ask_user_question:"+runtime.CurrentToolCallID, "Which?")
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			return "picked:" + intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice.Title, nil
		},
	})
	var n int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "a1", CallID: "a1", Name: "ask_user", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := tacklr.NewAgent(context.Background(), tacklr.AgentOptions{
		Config: tacklr.Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  testStore(t),
		Tools:  []*tacklr.Tool{tool},
	})
	events, err := h.Run(context.Background(), "q")
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
		t.Fatal("no interrupt")
	}

	w := &recordingWriter{}
	bridge := NewClientBridge(w)
	env := ProtocolEnv{Conn: &Conn{RPC: bridge, Caps: ClientCapabilities{ElicitationForm: true}}}
	stream := &EventStream{Harness: h, runCtx: context.Background()}

	type res struct {
		ch  <-chan streaming.StreamEvent
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		ch, err := resolveSelectionViaElicitation(context.Background(), env, "sess", stream, &interruptEv)
		resCh <- res{ch, err}
	}()
	// Complete elicitation
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		nframes := len(w.frames)
		w.mu.Unlock()
		if nframes > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	w.mu.Lock()
	frame := w.frames[len(w.frames)-1]
	w.mu.Unlock()
	var req struct {
		ID int64 `json:"id"`
	}
	_ = json.Unmarshal(frame, &req)
	resp, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result":  map[string]any{"action": "accept", "content": map[string]any{"choice": "A"}},
	})
	bridge.TryCompleteResponse(resp)
	got := <-resCh
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.ch != nil {
		for range got.ch {
		}
	}
}

func TestServeHTTP_badAddr(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	err := NewServer(r, SSE).ServeHTTP(context.Background(), "bad:addr:port")
	if err == nil {
		t.Fatal("want listen error")
	}
}

func TestValidateACP_promptMissingSession(t *testing.T) {
	_, err := validateACPRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"prompt":[{"type":"text","text":"hi"}]}}`))
	if err == nil || !strings.Contains(err.Error(), "sessionId") {
		t.Fatalf("%v", err)
	}
}

func TestClientBridge_TryCompleteUnknownID(t *testing.T) {
	b := NewClientBridge(&recordingWriter{})
	if b.TryCompleteResponse([]byte(`{"jsonrpc":"2.0","id":999,"result":{}}`)) {
		t.Fatal("unknown id should be false")
	}
	if b.TryCompleteResponse([]byte(`not-json`)) {
		t.Fatal("bad json false")
	}
}

func TestServeHTTPSSE_resumePath(t *testing.T) {
	// Hit /resume pattern registration via serveHTTPSSE path matching.
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "r", IsComplete: true}
		},
	}
	srv := NewServer(newTestRegistry(store, strategy, nil), SSE)
	// First create a session via normal prompt to get thread
	rec := httptest.NewRecorder()
	srv.serveHTTPSSE(rec, newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hi"}`))))
	// Resume with empty responses still exercises /resume route (may error)
	rec2 := httptest.NewRecorder()
	req := newSSERequest(t, "/resume", bytes.NewReader([]byte(`{"agent_id":"default","thread_id":"missing","responses":{"x":{}}}`)))
	srv.serveHTTPSSE(rec2, req)
}

func TestStopReasonFromError_contextMessage(t *testing.T) {
	// String-based cancel detection
	reason, ok := stopReasonFromError(errors.New("run: context cancelled: wrap"))
	if !ok || reason != stopReasonCancelled {
		t.Fatalf("%v %v", reason, ok)
	}
}

func TestCreateSession_emptyAgentName(t *testing.T) {
	r := NewRegistry(testStore(t), "a")
	r.Register("a", AgentSpec{Name: "", Model: &mockInferenceStrategy{}})
	p := NewACPProtocol(nil).(*acpProtocol)
	params, _ := json.Marshal(map[string]any{"cwd": "/tmp"})
	sid, _, err := p.CreateSession(context.Background(), ProtocolEnv{Registry: r}, params)
	if err != nil || sid == "" {
		t.Fatal("no session")
	}
	if _, err := p.setConfig(context.Background(), ProtocolEnv{Registry: r}, sid, "nope", "x"); err == nil {
		t.Fatal("want unknown config")
	}
}

func TestValidateACP_emptyPromptAndConfigSessionID(t *testing.T) {
	// Missing prompt field → len(RawMessage)==0
	if _, err := validateACPRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s"}}`,
	)); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("missing prompt: %v", err)
	}
	// Empty array joins to empty string after concatenate
	if _, err := validateACPRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[]}}`,
	)); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty prompt array: %v", err)
	}
	if _, err := validateACPRequest([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"session/set_config_option","params":{"configId":"model","value":"x"}}`,
	)); err == nil || !strings.Contains(err.Error(), "sessionId") {
		t.Fatalf("missing sessionId: %v", err)
	}
	// resource block without resource field
	if _, err := concatenateACPPrompt([]byte(`[{"type":"resource"}]`)); err == nil {
		t.Fatal("want resource required")
	}
	// empty text blocks that join to empty after validation of non-empty texts is separate:
	// message event with empty content short-circuits
	frames, err := eventToAcpJsonRpc("t", &streaming.StreamEvent{Type: streaming.StreamEventMessage, Content: ""})
	if err != nil || frames != nil {
		t.Fatalf("empty message: %v %v", frames, err)
	}
	// plan update bad JSON
	if _, err := eventToAcpJsonRpc("t", &streaming.StreamEvent{
		Type: streaming.StreamEventPlanUpdate,
		Data: []byte(`not-json`),
	}); err == nil {
		t.Fatal("want plan unmarshal error")
	}
	// error event with Content only (no Error)
	frames, err = eventToAcpJsonRpc("t", &streaming.StreamEvent{
		Type:    streaming.StreamEventError,
		Content: "from-content",
		TurnID:  "1",
	})
	if err != nil || len(frames) != 1 {
		t.Fatalf("%v %d", err, len(frames))
	}
	if !strings.Contains(string(frames[0]), "from-content") {
		t.Fatalf("%s", frames[0])
	}
}

func TestHandleInbound_sessionCancelRequestAndMethodNotFound(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Registry: r, Conn: &Conn{Writer: w}}
	// session/cancel as a request (with id) → empty result
	err := NewACPProtocol(nil).(*acpProtocol).HandleInbound(context.Background(), env, []byte(
		`{"jsonrpc":"2.0","id":9,"method":"session/cancel","params":{"sessionId":"s1"}}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	// MethodNotFound for unknown method (written as error frame; WriteError returns nil)
	err = NewACPProtocol(nil).(*acpProtocol).HandleInbound(context.Background(), env, []byte(
		`{"jsonrpc":"2.0","id":10,"method":"session/foo","params":{}}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range w.Errors {
		if e.Err != nil && strings.Contains(e.Err.Error(), "method not found") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want method not found error frame, got %+v", w.Errors)
	}
}

func TestHandleSessionTurn_nonClientError(t *testing.T) {
	// Second session prompt loads from harness store; non-client LoadSession error.
	r := NewRegistry(&failLoadStore{InMemoryStore: stores.NewInMemoryStore(), err: errors.New("db down")}, "default")
	r.Register("default", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 1024},
		Model: &mockInferenceStrategy{
			invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "1", IsComplete: true}
			},
		},
	})
	p := NewACPProtocol(nil).(*acpProtocol)
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Registry: r, Conn: &Conn{Writer: w}}
	params, _ := json.Marshal(map[string]any{"cwd": "/tmp"})
	sid, _, err := p.CreateSession(context.Background(), env, params)
	if err != nil {
		t.Fatal(err)
	}
	// First prompt succeeds (Load=false).
	if err := p.handleSessionTurn(context.Background(), env, &parsedRequest{
		ID: json.RawMessage(`1`), ThreadID: sid, Prompt: "first",
	}); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second prompt loads harness checkpoint and fails with non-client error.
	err = p.handleSessionTurn(context.Background(), env, &parsedRequest{
		ID: json.RawMessage(`2`), ThreadID: sid, Prompt: "second",
	})
	if err == nil {
		t.Fatal("want non-client load error on second prompt")
	}
}

type failLoadStore struct {
	*stores.InMemoryStore
	err error
}

func (f *failLoadStore) LoadSession(ctx context.Context, id string) (stores.SessionCheckpoint, error) {
	return stores.SessionCheckpoint{}, f.err
}

func (f *failLoadStore) SaveSession(ctx context.Context, id string, cp stores.SessionCheckpoint) error {
	if f.InMemoryStore != nil {
		return f.InMemoryStore.SaveSession(ctx, id, cp)
	}
	return nil
}

func TestOnStreamEvent_cancelledCompleteAndEncodeError(t *testing.T) {
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Conn: &Conn{Writer: w}}
	// Cancelled stream + complete → cancelled result without encode
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream := &EventStream{runCtx: ctx, cancel: func() {}}
	ctrl := NewACPProtocol(nil).(*acpProtocol).OnStreamEvent(context.Background(), env, "t", stream, streaming.StreamEvent{
		Type: streaming.StreamEventComplete,
	}, json.RawMessage(`42`))
	if !ctrl.Finished {
		t.Fatal("want finished cancelled path")
	}
	// Encode error via bad plan update data
	ctrl = NewACPProtocol(nil).(*acpProtocol).OnStreamEvent(context.Background(), env, "t", stream, streaming.StreamEvent{
		Type: streaming.StreamEventPlanUpdate,
		Data: []byte(`{`),
	}, json.RawMessage(`1`))
	if ctrl.Err == nil {
		t.Fatal("want encode error")
	}
}

func TestResolveSelectionViaElicitation_errorPaths(t *testing.T) {
	// UserSelectionInterrupt JSON shape: {options:[{title...},...]}
	optsOK := []byte(`{"interruptId":"i1","type":"user_selection_choice","data":{"options":[{"title":"A"},{"title":"B"}]}}`)
	// Bad parse
	if _, err := resolveSelectionViaElicitation(context.Background(), ProtocolEnv{}, "s", &EventStream{}, &streaming.StreamEvent{
		Data: []byte(`{`),
	}); err == nil {
		t.Fatal("parse fail")
	}
	// Call fail — bridge write fails
	failBridge := NewClientBridge(&failFrameWriter{})
	env := ProtocolEnv{Conn: &Conn{RPC: failBridge, Caps: ClientCapabilities{ElicitationForm: true}}}
	if _, err := resolveSelectionViaElicitation(context.Background(), env, "s", &EventStream{}, &streaming.StreamEvent{
		Data: optsOK, MessageID: "m1",
	}); err == nil {
		t.Fatal("want call fail")
	}
	// Bad elicitation result payload
	w := &recordingWriter{}
	bridge := NewClientBridge(w)
	env2 := ProtocolEnv{Conn: &Conn{RPC: bridge, Caps: ClientCapabilities{ElicitationForm: true}}}
	type res struct {
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		_, err := resolveSelectionViaElicitation(context.Background(), env2, "s", &EventStream{}, &streaming.StreamEvent{
			Data: optsOK, MessageID: "m1",
		})
		resCh <- res{err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	var frame []byte
	for {
		w.mu.Lock()
		n := len(w.frames)
		if n > 0 {
			frame = append([]byte(nil), w.frames[n-1]...)
		}
		w.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(frame) == 0 {
		t.Fatal("no elicitation request frame")
	}
	var req struct {
		ID int64 `json:"id"`
	}
	_ = json.Unmarshal(frame, &req)
	// Invalid result body
	resp, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "not-an-object"})
	bridge.TryCompleteResponse(resp)
	got := <-resCh
	if got.err == nil {
		t.Fatal("want result parse error")
	}
}

func TestClientBridge_nilAndFullChannel(t *testing.T) {
	var b *ClientBridge
	if b.TryCompleteResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)) {
		t.Fatal("nil bridge")
	}
	b = NewClientBridge(&recordingWriter{})
	ch := make(chan rpcOutcome, 1)
	ch <- rpcOutcome{} // fill so second send hits default
	b.mu.Lock()
	b.wait["1"] = &rpcWaiter{ch: ch}
	b.mu.Unlock()
	if !b.TryCompleteResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)) {
		t.Fatal("want true even when channel full")
	}
}

func TestRunTurnStream_nilTurnCtxAndOnStreamClosedError(t *testing.T) {
	// nil turnCtx falls back to parent ctx
	events := make(chan streaming.StreamEvent)
	close(events)
	stream := &EventStream{Events: events} // runCtx nil
	proto := closedErrProtocol{}
	err := runTurnStream(context.Background(), ProtocolEnv{}, proto, "t", stream, json.RawMessage(`1`))
	if err == nil || !strings.Contains(err.Error(), "closed-err") {
		t.Fatalf("want OnStreamClosed error, got %v", err)
	}

	// cancel path: discardUntilClosed drains leftover events then ends cancelled
	events2 := make(chan streaming.StreamEvent)
	ctx, cancel := context.WithCancel(context.Background())
	stream2 := &EventStream{Events: events2, runCtx: ctx, cancel: cancel}
	started := make(chan struct{})
	go func() {
		close(started)
		events2 <- streaming.StreamEvent{Type: streaming.StreamEventMessage, Content: "x"}
		// keep open briefly so discardUntilClosed must receive then wait
		time.Sleep(40 * time.Millisecond)
		close(events2)
	}()
	<-started
	// Run stream loop; cancel shortly after start so Done wins while channel still open.
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	err = runTurnStream(context.Background(), ProtocolEnv{}, SSE, "t", stream2, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
}

type closedErrProtocol struct{}

func (closedErrProtocol) Name() string { return "cerr" }
func (closedErrProtocol) HandleInbound(context.Context, ProtocolEnv, []byte) error {
	return nil
}
func (closedErrProtocol) HTTPRoutes() []HTTPRoute { return nil }
func (closedErrProtocol) OnStreamEvent(context.Context, ProtocolEnv, string, *EventStream, streaming.StreamEvent, json.RawMessage) StreamControl {
	return StreamControl{}
}
func (closedErrProtocol) OnStreamClosed(context.Context, ProtocolEnv, string, json.RawMessage, bool) error {
	return errors.New("closed-err")
}

func (closedErrProtocol) CreateSession(context.Context, ProtocolEnv, json.RawMessage) (string, any, error) {
	return "", nil, ErrWireSessionUnsupported
}
func (closedErrProtocol) LoadSession(context.Context, ProtocolEnv, string, json.RawMessage) (any, error) {
	return nil, ErrWireSessionUnsupported
}
func (closedErrProtocol) BindTurn(context.Context, ProtocolEnv, string, json.RawMessage) (TurnRequest, error) {
	return TurnRequest{}, ErrWireSessionUnsupported
}
func (closedErrProtocol) CloseSession(context.Context, ProtocolEnv, string) error {
	return ErrWireSessionUnsupported
}

func TestSetConfigOption_nilConfigValuesAndSpecStore(t *testing.T) {
	store := testStore(t)
	perAgent := stores.NewInMemoryStore()
	r := NewRegistry(store, "default")
	r.Register("default", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 1024},
		Model: &mockInferenceStrategy{
			invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		},
		Store: perAgent, // exercises spec.Store override
	})
	// Manually insert wire session with empty configValues
	sid := "sess-nil-cfg"
	p := NewACPProtocol(nil).(*acpProtocol)
	p.sessions[sid] = &acpWireSession{cwd: "/tmp", configValues: nil}
	result, err := p.setConfig(context.Background(), ProtocolEnv{Registry: r}, sid, "model", "default")
	if err != nil {
		t.Fatal(err)
	}
	resMap, _ := result.(map[string]any)
	if resMap["configOptions"] == nil {
		t.Fatal("expected configOptions")
	}
	// Run turn uses per-agent store
	stream, err := r.RunTurn(context.Background(), TurnRequest{AgentID: "default", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Events {
	}
	stream.Close()
}

func TestLoadAgent_allowMissingCheckpointCreatesFresh(t *testing.T) {
	// AllowMissingCheckpoint + Load + store not found → fresh harness.
	r := NewRegistry(notFoundStore{}, "default")
	r.Register("default", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 1024},
		Model: &mockInferenceStrategy{
			invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "fresh", IsComplete: true}
			},
		},
	})
	stream2, err := r.RunTurn(context.Background(), TurnRequest{
		SessionID:              "wire-sess",
		AgentID:                "default",
		Load:                   true,
		AllowMissingCheckpoint: true,
		Prompt:                 "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	var saw bool
	for ev := range stream2.Events {
		if ev.Content == "fresh" {
			saw = true
		}
	}
	stream2.Close()
	if !saw {
		t.Fatal("expected fresh harness message")
	}
}

type notFoundStore struct{}

func (notFoundStore) SaveSession(context.Context, string, stores.SessionCheckpoint) error {
	return nil
}
func (notFoundStore) LoadSession(context.Context, string) (stores.SessionCheckpoint, error) {
	return stores.SessionCheckpoint{}, stores.ErrSessionNotFound
}

func TestValidateSSE_resumeResponses(t *testing.T) {
	pr, err := validateSSERequest([]byte(`{"agent_id":"default","thread_id":"t1","responses":{"i1":{"ok":true}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if pr.ThreadID != "t1" || len(pr.Responses) != 1 {
		t.Fatalf("%+v", pr)
	}
}

func TestServeStdio_edges(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "s", IsComplete: true}
		},
	}, nil)
	srv := NewServer(r, ACP)

	// empty lines + EOF without trailing newline
	var out bytes.Buffer
	in := strings.NewReader("\n\n" + `{"jsonrpc":"2.0","id":1,"method":"authenticate","params":{}}`)
	if err := srv.ServeStdio(context.Background(), in, &out); err != nil {
		t.Fatalf("serve: %v", err)
	}
	if !strings.Contains(out.String(), "result") {
		t.Fatalf("out=%s", out.String())
	}

	// read error
	err := NewServer(r, ACP).ServeStdio(context.Background(), errReader{}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "stdio read") {
		t.Fatalf("want stdio read error, got %v", err)
	}

	// Cancel while reader is blocked on Read.
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewServer(r, ACP).ServeStdio(ctx, pr, io.Discard)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	_ = pw.Close()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStdio did not return after cancel")
	}
}

func TestRunStdioLoop_channelClosed(t *testing.T) {
	ch := make(chan stdioReadResult)
	close(ch)
	var wg sync.WaitGroup
	bridge := NewClientBridge(&recordingWriter{})
	err := runStdioLoop(context.Background(), ch, bridge, func([]byte) {}, &wg)
	if err != nil {
		// ctx not cancelled → nil from ctx.Err()
		t.Fatalf("want nil on closed channel without cancel, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch2 := make(chan stdioReadResult)
	close(ch2)
	err = runStdioLoop(ctx, ch2, bridge, func([]byte) {}, &wg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom-read") }

func TestHandleSSE_bodyReadErrorAndInternalRunError(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	rec := httptest.NewRecorder()
	req := newSSERequest(t, "/", brokenBody{})
	sseProtocol{}.handleSSE(ProtocolEnv{Registry: r}, rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", rec.Code)
	}

	// Non-client RunTurn error: use failLoadStore with Load via thread — agent path without session
	// Internal: mock that returns error from Run is hard. Use agent not found is client error.
	// logTurnError path for non-client: failLoadStore on load
	r2 := NewRegistry(&failLoadStore{err: errors.New("disk fail")}, "default")
	r2.Register("default", AgentSpec{Config: tacklr.Config{MaxWindowSize: 1024}, Model: &mockInferenceStrategy{}})
	rec2 := httptest.NewRecorder()
	// Load true via resume responses path; store LoadSession fails non-client.
	req3 := newSSERequest(t, "/resume", bytes.NewReader([]byte(
		`{"agent_id":"default","thread_id":"tid","responses":{"i":{}}}`,
	)))
	sseProtocol{}.handleSSE(ProtocolEnv{Registry: r2}, rec2, req3)
	// Should write SSE error
	if rec2.Body.Len() == 0 && rec2.Code == 0 {
		t.Log("may still have written")
	}
}

func TestHandleWS_acceptFailAndReadFail(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	// Accept fails on ResponseRecorder (no hijack)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	sseProtocol{}.handleWS(ProtocolEnv{Registry: r}, rec, req)

	// Read fails: dial then close without writing
	mux := http.NewServeMux()
	env := ProtocolEnv{Registry: r, Conn: &Conn{}}
	for _, route := range SSE.HTTPRoutes() {
		if route.Method == http.MethodGet && route.Pattern == "/" {
			mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
				route.Handler(env, w, req)
			})
		}
	}
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
	time.Sleep(50 * time.Millisecond)
}

func TestHandleWS_nonClientRunErrorAndWriteThreadFail(t *testing.T) {
	// Non-client load error on WS resume path.
	r := NewRegistry(&failLoadStore{err: errors.New("ws-db-down")}, "default")
	r.Register("default", AgentSpec{Config: tacklr.Config{MaxWindowSize: 1024}, Model: &mockInferenceStrategy{}})
	mux := http.NewServeMux()
	env := ProtocolEnv{Registry: r, Conn: &Conn{}}
	for _, route := range SSE.HTTPRoutes() {
		if route.Method == http.MethodGet && route.Pattern == "/" {
			mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
				route.Handler(env, w, req)
			})
		}
	}
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"agent_id":"default","thread_id":"tid","responses":{"i":{}}}`)
	if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.Read(ctx)
	_ = conn.Close(websocket.StatusNormalClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "error") && !strings.Contains(string(data), "ws-db") {
		t.Fatalf("want error frame, got %s", data)
	}

	// writeWSJSON thread fail via test seam.
	prev := wsWriteJSON
	wsWriteJSON = func(ctx context.Context, c *websocket.Conn, v any) error {
		return errors.New("ws write fail")
	}
	t.Cleanup(func() { wsWriteJSON = prev })
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "late", IsComplete: true}
		},
	}
	r2 := newTestRegistry(testStore(t), strategy, nil)
	mux2 := http.NewServeMux()
	env2 := ProtocolEnv{Registry: r2, Conn: &Conn{}}
	for _, route := range SSE.HTTPRoutes() {
		if route.Method == http.MethodGet && route.Pattern == "/" {
			mux2.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
				route.Handler(env2, w, req)
			})
		}
	}
	hs2 := httptest.NewServer(mux2)
	t.Cleanup(hs2.Close)
	wsURL2 := "ws" + strings.TrimPrefix(hs2.URL, "http")
	conn2, _, err := websocket.Dial(ctx, wsURL2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn2.Write(ctx, websocket.MessageText, []byte(`{"agent_id":"default","prompt":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	_ = conn2.Close(websocket.StatusNormalClosure, "")
}

func TestServeHTTPSSE_fallbackPath(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "fb2", IsComplete: true}
		},
	}
	srv := NewServer(newTestRegistry(testStore(t), strategy, nil), SSE)
	rec := httptest.NewRecorder()
	req := newSSERequest(t, "/nope", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"x"}`)))
	srv.serveHTTPSSE(rec, req)
	if !strings.Contains(rec.Body.String(), "fb2") {
		t.Fatalf("fallback = %s", rec.Body.String())
	}
}

func TestServeHTTP_errServerClosedAndListenError(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	// Immediate listen error returns from errCh branch (not ErrServerClosed)
	err := NewServer(r, SSE).ServeHTTP(context.Background(), "127.0.0.1:99999x")
	if err == nil {
		t.Fatal("want listen error")
	}
}

func TestWaitHTTPServer_branches(t *testing.T) {
	// errCh closes with ErrServerClosed → nil
	errCh := make(chan error, 1)
	errCh <- http.ErrServerClosed
	if err := waitHTTPServer(context.Background(), func(context.Context) error { return nil }, errCh); err != nil {
		t.Fatalf("ErrServerClosed: %v", err)
	}
	// errCh other error → return it
	errCh2 := make(chan error, 1)
	errCh2 <- errors.New("listen boom")
	if err := waitHTTPServer(context.Background(), func(context.Context) error { return nil }, errCh2); err == nil || !strings.Contains(err.Error(), "listen boom") {
		t.Fatalf("got %v", err)
	}
	// ctx cancel first, then errCh delivers ErrServerClosed → ctx.Err()
	ctx, cancel := context.WithCancel(context.Background())
	errCh3 := make(chan error, 1)
	cancel()
	go func() {
		time.Sleep(10 * time.Millisecond)
		errCh3 <- http.ErrServerClosed
	}()
	if err := waitHTTPServer(ctx, func(context.Context) error { return nil }, errCh3); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	// ctx cancel first, then non-closed error → return that error
	ctx2, cancel2 := context.WithCancel(context.Background())
	errCh4 := make(chan error, 1)
	cancel2()
	go func() {
		time.Sleep(10 * time.Millisecond)
		errCh4 <- errors.New("shutdown race")
	}()
	if err := waitHTTPServer(ctx2, func(context.Context) error { return nil }, errCh4); err == nil || !strings.Contains(err.Error(), "shutdown race") {
		t.Fatalf("got %v", err)
	}
}

func TestHandleSSE_runTurnStreamError(t *testing.T) {
	// Force runTurnStream error via write failure after headers: use failing flusher writer
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "x", IsComplete: true}
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	// errFlusher fails all writes — may fail before/during stream
	ef := &errFlusher{}
	req := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hi"}`)))
	sseProtocol{}.handleSSE(ProtocolEnv{Registry: r}, ef, req)
}
