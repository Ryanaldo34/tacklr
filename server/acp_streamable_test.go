package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
)

func startACPStreamServer(t *testing.T, r *testKernel) (*httptest.Server, *Server) {
	t.Helper()
	// Fresh ACP protocol per server — no package-level ACP singleton.
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))
	hs := httptest.NewServer(srv.HTTPMux())
	t.Cleanup(hs.Close)
	return hs, srv
}

// streamHTTPClient allows concurrent connection SSE + session SSE + POSTs.
func streamHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
		},
	}
}

func acpPOST(t *testing.T, hs *httptest.Server, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, hs.URL+"/acp", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := streamHTTPClient().Do(req)
	if err != nil {
		t.Fatalf("POST /acp: %v", err)
	}
	return resp
}

func acpInitialize(t *testing.T, hs *httptest.Server) (connID string, initBody map[string]any) {
	t.Helper()
	resp := acpPOST(t, hs, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("initialize status=%d body=%s", resp.StatusCode, b)
	}
	connID = resp.Header.Get(HeaderAcpConnectionID)
	if connID == "" {
		t.Fatal("missing Acp-Connection-Id on initialize")
	}
	if c := resp.Cookies(); len(c) == 0 {
		t.Error("expected affinity cookie on initialize")
	}
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes.TrimSpace(raw), &initBody); err != nil {
		t.Fatalf("initialize body: %v raw=%s", err, raw)
	}
	return connID, initBody
}

func openACPSSE(t *testing.T, hs *httptest.Server, connID, sessionID string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, hs.URL+"/acp", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(HeaderAcpConnectionID, connID)
	if sessionID != "" {
		req.Header.Set(HeaderAcpSessionID, sessionID)
	}
	resp, err := streamHTTPClient().Do(req)
	if err != nil {
		t.Fatalf("GET SSE: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("GET SSE status=%d body=%s", resp.StatusCode, b)
	}
	return resp
}

// sseFrameReader continuously demuxes JSON-RPC payloads from one SSE body.
type sseFrameReader struct {
	ch <-chan map[string]any
}

func newSSEFrameReader(r io.Reader) *sseFrameReader {
	ch := make(chan map[string]any, 64)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			payload := strings.TrimPrefix(line, "data: ")
			var frame map[string]any
			if err := json.Unmarshal([]byte(payload), &frame); err != nil {
				return
			}
			ch <- frame
		}
	}()
	return &sseFrameReader{ch: ch}
}

func (s *sseFrameReader) next(t *testing.T, timeout time.Duration) map[string]any {
	t.Helper()
	select {
	case frame, ok := <-s.ch:
		if !ok || frame == nil {
			t.Fatal("sse closed without data")
		}
		return frame
	case <-time.After(timeout):
		t.Fatal("timed out waiting for SSE JSON-RPC event")
	}
	return nil
}

func streamableSession(t *testing.T, hs *httptest.Server, connID string) (sessionID string, sessFrames *sseFrameReader, closeFn func()) {
	t.Helper()
	connSSE := openACPSSE(t, hs, connID, "")
	connFrames := newSSEFrameReader(connSSE.Body)
	_ = acpPOST(t, hs, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`, map[string]string{
		HeaderAcpConnectionID: connID,
	}).Body.Close()
	sessionID = connFrames.next(t, 4*time.Second)["result"].(map[string]any)["sessionId"].(string)

	sessSSE := openACPSSE(t, hs, connID, sessionID)
	sessFrames = newSSEFrameReader(sessSSE.Body)
	closeFn = func() {
		_ = sessSSE.Body.Close()
		_ = connSSE.Body.Close()
	}
	return sessionID, sessFrames, closeFn
}

// TestACP_Streamable_permissionMidTurn: 202 POST + dual SSE + permission reply demux
// (and covers initialize / session/new / prompt / end_turn as part of the flow).
func TestACP_Streamable_permissionMidTurn(t *testing.T) {
	var ran bool
	sensitive := tacklr.NewTool(tacklr.ToolConfig{
		Name:   "sensitive",
		OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			ran = true
			return "ok", nil
		},
	})
	var n int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "c1", CallID: "c1", Name: "sensitive", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	r := newTestRegistry(testStore(t), strategy, []*tacklr.Tool{sensitive})
	hs, _ := startACPStreamServer(t, r)

	connID, initBody := acpInitialize(t, hs)
	if res, ok := initBody["result"].(map[string]any); !ok || res["protocolVersion"] == nil {
		t.Fatalf("initialize result missing: %v", initBody)
	}
	sessionID, sessFrames, closeFn := streamableSession(t, hs, connID)
	defer closeFn()

	_ = acpPOST(t, hs, `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"x"}]}}`, map[string]string{
		HeaderAcpConnectionID: connID,
		HeaderAcpSessionID:    sessionID,
	}).Body.Close()

	var sawPermission, endTurn bool
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && !endTurn {
		fr := sessFrames.next(t, 5*time.Second)
		if fr["method"] == "session/request_permission" {
			sawPermission = true
			reply, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      fr["id"],
				"result": map[string]any{
					"outcome": map[string]any{"outcome": "selected", "optionId": "allow-once"},
				},
			})
			resp := acpPOST(t, hs, string(reply), map[string]string{
				HeaderAcpConnectionID: connID,
				HeaderAcpSessionID:    sessionID,
			})
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("permission reply status=%d", resp.StatusCode)
			}
		}
		if res, ok := fr["result"].(map[string]any); ok && res["stopReason"] == "end_turn" {
			endTurn = true
		}
	}
	if !sawPermission {
		t.Fatal("expected session/request_permission on session SSE")
	}
	if !ran {
		t.Error("expected tool to run after allow")
	}
	if !endTurn {
		t.Error("expected end_turn")
	}
}

// TestACP_Streamable_cancelDuringPrompt: concurrent POST cancel while prompt streams
// over Streamable HTTP (stdio cancel is covered elsewhere; this is the duplex HTTP path).
// TestACP_Streamable_deleteConnection: DELETE removes connection; further POST → 404.
func TestACP_Streamable_deleteConnection(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	hs, srv := startACPStreamServer(t, r)
	connID, _ := acpInitialize(t, hs)
	if srv.Connections.Get(connID) == nil {
		t.Fatal("connection should exist")
	}

	req, _ := http.NewRequest(http.MethodDelete, hs.URL+"/acp", nil)
	req.Header.Set(HeaderAcpConnectionID, connID)
	resp, err := streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("DELETE status=%d", resp.StatusCode)
	}
	if srv.Connections.Get(connID) != nil {
		t.Fatal("connection should be removed")
	}

	resp2 := acpPOST(t, hs, `{"jsonrpc":"2.0","id":9,"method":"session/new","params":{"cwd":"/tmp"}}`, map[string]string{
		HeaderAcpConnectionID: connID,
	})
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("POST after DELETE status=%d, want 404", resp2.StatusCode)
	}
}

// TestACP_Streamable_contentNegotiation: 415 / 406 / 501 / 400 / 404 paths.
func TestACP_Streamable_contentNegotiation(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	hs, _ := startACPStreamServer(t, r)

	req, _ := http.NewRequest(http.MethodPost, hs.URL+"/acp", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("want 415, got %d", resp.StatusCode)
	}

	resp = acpPOST(t, hs, `[{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}]`, nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("want 501 batch, got %d", resp.StatusCode)
	}

	connID, _ := acpInitialize(t, hs)
	req, _ = http.NewRequest(http.MethodGet, hs.URL+"/acp", nil)
	req.Header.Set(HeaderAcpConnectionID, connID)
	resp, err = streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotAcceptable {
		t.Fatalf("want 406, got %d", resp.StatusCode)
	}

	resp = acpPOST(t, hs, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`, nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}

	resp = acpPOST(t, hs, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`, map[string]string{
		HeaderAcpConnectionID: "does-not-exist",
	})
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}
