package acp_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
)

// dialACPWebSocket opens a WebSocket to GET /acp and returns the connection
// plus Acp-Connection-Id from the upgrade response.
func dialACPWebSocket(t *testing.T, hs *httptest.Server) (*websocket.Conn, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http") + "/acp"
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial /acp: %v", err)
	}
	connID := ""
	if resp != nil {
		connID = resp.Header.Get(acp.HeaderConnectionID)
	}
	t.Cleanup(func() {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	return conn, connID
}

func startACPWSServer(t *testing.T, r *testRuntime) (*httptest.Server, *server.Server) {
	t.Helper()
	srv := server.NewServer(r.Client, r.Agent, acp.New(server.NewMemoryWireStore()))
	hs := httptest.NewServer(srv.HTTPMux())
	t.Cleanup(hs.Close)
	return hs, srv
}

func wsWriteJSONRPC(ctx context.Context, t *testing.T, c *websocket.Conn, msg any) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := c.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}

func wsReadFrame(ctx context.Context, t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	_, data, err := c.Read(readCtx)
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var frame map[string]any
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("unmarshal frame %q: %v", data, err)
	}
	return frame
}

// TestACP_WS_permissionMidTurn: duplex acp.ClientBridge over WebSocket — init →
// session/new → prompt → mid-turn request_permission reply → tool runs → end_turn.
// Also asserts Acp-Connection-Id is registered and removed when the socket closes.
func TestACP_WS_permissionMidTurn(t *testing.T) {
	var ran bool
	sensitive := tacklr.NewTool(tacklr.ToolConfig{
		Name:   "sensitive",
		OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			ran = true
			return "secret-ok", nil
		},
	})
	var invokeCount int
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		invokeCount++
		if invokeCount == 1 {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: []message.ToolCall{
				{ID: "c1", CallID: "c1", Name: "sensitive", Arguments: `{}`},
			}, IsComplete: true}
			ch <- tacklr.LLMResponseChunk{IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
	})
	r := newTestRuntime(t, strategy, tacklr.AgentOptions{Tools: []*tacklr.Tool{sensitive}})
	hs, srv := startACPWSServer(t, r)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, connID := dialACPWebSocket(t, hs)
	if connID == "" {
		t.Fatal("expected Acp-Connection-Id on WebSocket upgrade response")
	}
	if srv.Connections.Get(connID) == nil {
		t.Fatal("connection should be registered while socket is open")
	}

	if err := conn.Write(ctx, websocket.MessageText, nil); err != nil {
		t.Fatalf("empty frame: %v", err)
	}
	wsWriteJSONRPC(ctx, t, conn, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
		"params": map[string]any{"protocolVersion": 1},
	})
	wsWriteJSONRPC(ctx, t, conn, map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "session/new",
		"params": map[string]any{"cwd": "/tmp"},
	})

	var sessionID string
	var sawPermission, endTurn, promptSent bool

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && !endTurn {
		frame := wsReadFrame(ctx, t, conn)

		if res, ok := frame["result"].(map[string]any); ok {
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
			if idMatch(frame["id"], 3) && res["stopReason"] == "end_turn" {
				endTurn = true
			}
		}
		if frame["method"] == "session/request_permission" {
			sawPermission = true
			wsWriteJSONRPC(ctx, t, conn, map[string]any{
				"jsonrpc": "2.0",
				"id":      frame["id"],
				"result": map[string]any{
					"outcome": map[string]any{
						"outcome":  "selected",
						"optionId": "allow-once",
					},
				},
			})
		}
		if sessionID != "" && !promptSent {
			promptSent = true
			wsWriteJSONRPC(ctx, t, conn, map[string]any{
				"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
				"params": map[string]any{
					"sessionId": sessionID,
					"prompt":    []map[string]string{{"type": "text", "text": "run"}},
				},
			})
		}
	}

	if !sawPermission {
		t.Fatal("expected session/request_permission on WebSocket")
	}
	if !ran {
		t.Error("expected permission-approved tool to run")
	}
	if !endTurn {
		t.Error("expected end_turn after permission flow")
	}

	_ = conn.Close(websocket.StatusNormalClosure, "")
	removed := time.Now().Add(2 * time.Second)
	for time.Now().Before(removed) {
		if srv.Connections.Get(connID) == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("connection still in registry after close")
}

func TestACP_WS_disconnectCancelsInFlightTurn(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	var once sync.Once
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		close(cancelled)
	})
	r := newTestRuntime(t, strategy, tacklr.AgentOptions{})
	hs, _ := startACPWSServer(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _ := dialACPWebSocket(t, hs)

	wsWriteJSONRPC(ctx, t, conn, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
		"params": map[string]any{"protocolVersion": 1},
	})
	wsWriteJSONRPC(ctx, t, conn, map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "session/new",
		"params": map[string]any{"cwd": "/tmp"},
	})
	var sessionID string
	for sessionID == "" {
		frame := wsReadFrame(ctx, t, conn)
		if res, ok := frame["result"].(map[string]any); ok {
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
		}
	}
	wsWriteJSONRPC(ctx, t, conn, map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
		"params": map[string]any{
			"sessionId": sessionID,
			"prompt":    []map[string]string{{"type": "text", "text": "hi"}},
		},
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not start")
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
	select {
	case <-cancelled:
	case <-time.After(8 * time.Second):
		t.Fatal("disconnect did not cancel in-flight inference")
	}
}

// TestACP_prompt_stopReason_refusal: model terminal ErrModelRefused → PromptResponse refusal.
func TestACP_prompt_stopReason_refusal(t *testing.T) {
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type:       session.EventError,
			Error:      session.ErrModelRefused,
			Content:    "model refused",
			IsComplete: true,
		}
	})
	assertACPStopReason(t, strategy, nil, "refusal")
}

func TestACP_prompt_stopReason_maxTokens(t *testing.T) {
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type:       session.EventError,
			Error:      session.ErrMaxTokens,
			IsComplete: true,
		}
	})
	assertACPStopReason(t, strategy, nil, "max_tokens")
}

func assertACPStopReason(t *testing.T, strategy tacklr.Model, tools []*tacklr.Tool, want string) {
	t.Helper()
	r := newTestRuntime(t, strategy, tacklr.AgentOptions{Tools: tools})
	recNew := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID := acpSessionID(t, recNew)
	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"hi"}]}}`)
	got := promptStopReason(t, rec)
	if got != want {
		blob, _ := json.Marshal(parseACPFrames(t, rec.Body))
		t.Fatalf("stopReason = %q, want %q frames=%s", got, want, blob)
	}
}

func TestACP_sessionCancel_stopReasonCancelled(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		once.Do(func() { close(started) })
		<-ctx.Done()
	})
	r := newTestRuntime(t, strategy, tacklr.AgentOptions{})
	srv := server.NewServer(r.Client, r.Agent, acp.New(server.NewMemoryWireStore()))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rpc := newACPRPC(ctx, t, srv)
	rpc.write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	rpc.write(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)

	var sessionID string
	for sessionID == "" {
		frame := rpc.frame()
		if res, ok := frame["result"].(map[string]any); ok {
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
		}
	}
	rpc.write(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not start")
	}
	rpc.write(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"` + sessionID + `"}}`)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		frame := rpc.frame()
		if res, ok := frame["result"].(map[string]any); ok && idMatch(frame["id"], 10) && res["stopReason"] == "cancelled" {
			return
		}
	}
	t.Fatal("expected prompt stopReason cancelled after session/cancel")
}

func promptStopReason(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	frames := parseACPFrames(t, rec.Body)
	for _, f := range frames {
		if res, ok := f["result"].(map[string]any); ok {
			if sr, ok := res["stopReason"].(string); ok && sr != "" {
				return sr
			}
		}
	}
	// Also accept WriteResult-only responses (single JSON object body).
	var single map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &single); err == nil {
		if res, ok := single["result"].(map[string]any); ok {
			if sr, _ := res["stopReason"].(string); sr != "" {
				return sr
			}
		}
	}
	return ""
}
