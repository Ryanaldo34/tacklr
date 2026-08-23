package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
)

// failWire is a ProtocolWireStore that can fail Put/Get/Delete for error outcomes.
type failWire struct {
	base    *MemoryWireStore
	putErr  error
	getErr  error
	delErr  error
	putOnce int // succeed first N puts when putErr set
}

func (f *failWire) Put(ctx context.Context, sessionID string, payload []byte) error {
	if f.putErr != nil {
		if f.putOnce > 0 {
			f.putOnce--
			return f.base.Put(ctx, sessionID, payload)
		}
		return f.putErr
	}
	return f.base.Put(ctx, sessionID, payload)
}

func (f *failWire) Get(ctx context.Context, sessionID string) ([]byte, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.base.Get(ctx, sessionID)
}

func (f *failWire) Delete(ctx context.Context, sessionID string) error {
	if f.delErr != nil {
		return f.delErr
	}
	return f.base.Delete(ctx, sessionID)
}

// TestWireAndConstruct_outcomes covers wire-store / construct / streamable edge
// paths in few high-value cases (no permission/prompt suite duplication).
func TestWireAndConstruct_outcomes(t *testing.T) {
	r := newTestKernel(t, &mockInferenceStrategy{}, durable.AgentSpec{})

	if p := NewACPProtocol(nil); p == nil {
		t.Fatal("NewACPProtocol")
	}
	wire := NewMemoryWireStore()
	srvWire := NewServer(r.Runtime, r.Catalog, NewACPProtocol(wire))
	s := &acpTestServer{t: t, r: r, proto: srvWire.Protocols[0], wire: wire}
	rec := s.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}`)
	sid, _ := acpRPCResult(t, rec)["sessionId"].(string)
	if sid == "" {
		t.Fatal("missing sessionId")
	}
	if _, err := wire.Get(t.Context(), sid); err != nil {
		t.Fatalf("wire should hold envelope: %v", err)
	}

	w := NewMemoryWireStore()
	if _, err := w.Get(context.Background(), "missing"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing get: %v", err)
	}
	if err := w.Put(context.Background(), "k", []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}

	// --- Streamable HTTP protocol error matrix (one server, many outcomes) ---
	hs, _ := startACPStreamServer(t, r)
	assertStatus := func(name string, method, path, body string, headers map[string]string, want int) {
		t.Helper()
		var req *http.Request
		var err error
		if method == http.MethodGet || method == http.MethodDelete {
			req, err = http.NewRequest(method, hs.URL+path, nil)
		} else {
			req, err = http.NewRequest(method, hs.URL+path, strings.NewReader(body))
		}
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := streamHTTPClient().Do(req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s: status=%d want %d", name, resp.StatusCode, want)
		}
	}

	// DELETE without connection id → 400
	assertStatus("DELETE no id", http.MethodDelete, "/acp", "", nil, http.StatusBadRequest)
	// DELETE unknown → 404
	assertStatus("DELETE unknown", http.MethodDelete, "/acp", "", map[string]string{
		HeaderAcpConnectionID: "nope",
	}, http.StatusNotFound)
	// Wrong Content-Type → 415
	assertStatus("POST no content-type", http.MethodPost, "/acp", `{}`, nil, http.StatusUnsupportedMediaType)
	assertStatus("POST text/plain", http.MethodPost, "/acp", `{}`, map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusUnsupportedMediaType)
	// Empty body → 400
	assertStatus("empty body", http.MethodPost, "/acp", "", map[string]string{
		"Content-Type": "application/json",
	}, http.StatusBadRequest)
	// Batch → 501
	assertStatus("batch", http.MethodPost, "/acp", `[{}]`, map[string]string{
		"Content-Type": "application/json",
	}, http.StatusNotImplemented)
	// Invalid JSON → 400
	assertStatus("bad json", http.MethodPost, "/acp", `{`, map[string]string{
		"Content-Type": "application/json",
	}, http.StatusBadRequest)
	// POST without connection after non-init → 400
	assertStatus("post no conn", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`,
		map[string]string{"Content-Type": "application/json"},
		http.StatusBadRequest)
	// POST with charset accepted
	req, _ := http.NewRequest(http.MethodPost, hs.URL+"/acp", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "protocolVersion") {
		t.Fatalf("init charset: %d %s", resp.StatusCode, body)
	}
	// initialize with connection id → 400
	assertStatus("init with conn", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":1}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: "already",
		}, http.StatusBadRequest)
	// Unsupported protocol version still yields JSON-RPC error body (200)
	resp = acpPOST(t, hs, `{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":0}}`, nil)
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bad version status=%d", resp.StatusCode)
	}
	_ = b

	connID, _ := acpInitialize(t, hs)
	// SSE missing Accept → 406
	assertStatus("sse no accept", http.MethodGet, "/acp", "", map[string]string{
		HeaderAcpConnectionID: connID,
	}, http.StatusNotAcceptable)
	// SSE missing connection → 400
	assertStatus("sse no conn", http.MethodGet, "/acp", "", map[string]string{
		"Accept": "text/event-stream",
	}, http.StatusBadRequest)
	// SSE unknown connection → 404
	assertStatus("sse unknown", http.MethodGet, "/acp", "", map[string]string{
		"Accept":              "text/event-stream",
		HeaderAcpConnectionID: "missing-conn",
	}, http.StatusNotFound)
	// session/prompt without session header → 400
	assertStatus("prompt no session hdr", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":9,"method":"session/prompt","params":{"sessionId":"x","prompt":[{"type":"text","text":"hi"}]}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: connID,
		}, http.StatusBadRequest)
	// session-scoped method with a session this connection does not own → 403
	assertStatus("prompt unattached session", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":11,"method":"session/prompt","params":{"sessionId":"ghost","prompt":[{"type":"text","text":"hi"}]}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: connID,
			HeaderAcpSessionID:    "ghost",
		}, http.StatusForbidden)
	// session/load may name a session this connection has not seen yet → 202
	assertStatus("load unattached session", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":10,"method":"session/load","params":{"sessionId":"ghost","cwd":"/tmp"}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: connID,
		}, http.StatusAccepted)
	// client JSON-RPC response demux on unknown conn → 404
	assertStatus("response unknown conn", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":99,"result":{}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: "ghost",
		}, http.StatusNotFound)
	// client response on known conn → 202
	assertStatus("response ok", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":100,"result":{}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: connID,
		}, http.StatusAccepted)

	// Double connection SSE → 409
	sse1 := openACPSSE(t, hs, connID, "")
	defer sse1.Body.Close()
	req, _ = http.NewRequest(http.MethodGet, hs.URL+"/acp", nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(HeaderAcpConnectionID, connID)
	resp, err = streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second SSE status=%d want 409", resp.StatusCode)
	}

	sessSSE := openACPSSE(t, hs, connID, "sess-1")
	defer sessSSE.Body.Close()
	req, _ = http.NewRequest(http.MethodGet, hs.URL+"/acp", nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(HeaderAcpConnectionID, connID)
	req.Header.Set(HeaderAcpSessionID, "sess-1")
	resp, err = streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second session SSE status=%d want 409", resp.StatusCode)
	}

	del, err := http.NewRequest(http.MethodDelete, hs.URL+"/acp", nil)
	if err != nil {
		t.Fatal(err)
	}
	del.Header.Set(HeaderAcpConnectionID, connID)
	delResp, err := streamHTTPClient().Do(del)
	if err != nil {
		t.Fatal(err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusAccepted {
		t.Fatalf("DELETE with SSE open: %d", delResp.StatusCode)
	}
}

func TestHandleInbound_wireStoreFailures(t *testing.T) {
	k := newTestKernel(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	inbound := func(proto Protocol, body string) *httptest.ResponseRecorder {
		t.Helper()
		return serveACPInbound(t, k, proto, body)
	}

	fw := &failWire{base: NewMemoryWireStore(), putErr: fmt.Errorf("put fail")}
	rec := inbound(NewACPProtocol(fw), `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/t"}}`)
	_ = acpRPCError(t, rec)

	fw2 := &failWire{base: NewMemoryWireStore(), putErr: fmt.Errorf("put2"), putOnce: 1}
	pFail := NewACPProtocol(fw2)
	rec = inbound(pFail, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/t"}}`)
	sid := acpRPCResult(t, rec)["sessionId"].(string)
	rec = inbound(pFail, `{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sid+`","cwd":"/t"}}`)
	_ = acpRPCError(t, rec)

	badWire := NewMemoryWireStore()
	if err := badWire.Put(t.Context(), "corrupt", []byte(`{not-json`)); err != nil {
		t.Fatal(err)
	}
	rec = inbound(NewACPProtocol(badWire), `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"corrupt"}}`)
	_ = acpRPCError(t, rec)

	fw3 := &failWire{base: NewMemoryWireStore(), getErr: fmt.Errorf("db down")}
	rec = inbound(NewACPProtocol(fw3), `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"any"}}`)
	_ = acpRPCError(t, rec)

	fw4 := &failWire{base: NewMemoryWireStore(), delErr: fmt.Errorf("del fail")}
	pDel := NewACPProtocol(fw4)
	rec = inbound(pDel, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/x"}}`)
	sid = acpRPCResult(t, rec)["sessionId"].(string)
	rec = inbound(pDel, `{"jsonrpc":"2.0","id":2,"method":"session/close","params":{"sessionId":"`+sid+`"}}`)
	_ = acpRPCError(t, rec)
}

// TestJSONRPCWSMessageWriter_error covers the ACP WebSocket writer error path.
func TestJSONRPCWSMessageWriter_error(t *testing.T) {
	up := make(chan *websocket.Conn, 1)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		up <- c
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
	mw := &jsonRPCWSMessageWriter{ctx: ctx, c: serverConn}
	if err := mw.WriteResult(json.RawMessage(`1`), map[string]string{"ok": "1"}); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteError(json.RawMessage(`2`), clientErrorf(ErrInvalidRequest, "ws-jsonrpc-err")); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteFrame([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	// Drain client reads so writes complete
	for i := 0; i < 3; i++ {
		readCtx, c := context.WithTimeout(ctx, 300*time.Millisecond)
		_, data, err := client.Read(readCtx)
		c()
		if err != nil {
			break
		}
		if i == 1 && !strings.Contains(string(data), "ws-jsonrpc-err") {
			t.Fatalf("error frame = %s", data)
		}
	}
}

// TestStreamable_promptOnSessionSSE is a single happy-path streamable outcome:
// initialize → dual SSE → session/new → prompt → end_turn on session stream.
func TestStreamable_promptOnSessionSSE(t *testing.T) {
	r := newTestKernel(t, &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hi", IsComplete: true}
		},
	}, durable.AgentSpec{})
	hs, _ := startACPStreamServer(t, r)
	connID, _ := acpInitialize(t, hs)
	sessionID, frames, closeFn := streamableSession(t, hs, connID)
	defer closeFn()

	_ = acpPOST(t, hs, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":%q,"prompt":[{"type":"text","text":"hello"}]}}`,
		sessionID), map[string]string{
		HeaderAcpConnectionID: connID,
		HeaderAcpSessionID:    sessionID,
	}).Body.Close()

	// Wait for end_turn result
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		frame := frames.next(t, time.Until(deadline))
		if res, ok := frame["result"].(map[string]any); ok {
			if res["stopReason"] == "end_turn" {
				return
			}
		}
	}
	t.Fatal("no end_turn")
}
