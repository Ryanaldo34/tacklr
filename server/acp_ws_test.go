package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
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
		connID = resp.Header.Get(HeaderAcpConnectionID)
	}
	t.Cleanup(func() {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	return conn, connID
}

func startACPWSServer(t *testing.T, r *Registry) (*httptest.Server, *Server) {
	t.Helper()
	srv := NewServer(r, ACP)
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

// TestACP_WS_permissionMidTurn: duplex ClientBridge over WebSocket — init →
// session/new → prompt → mid-turn request_permission reply → tool runs → end_turn.
// Also asserts Acp-Connection-Id is registered and removed when the socket closes.
func TestACP_WS_permissionMidTurn(t *testing.T) {
	var ran bool
	sensitive := tacklr.NewTool(tacklr.ToolConfig{
		Name:               "sensitive",
		PermissionRequired: true,
		Handler: func(ctx context.Context) (string, error) {
			ran = true
			return "secret-ok", nil
		},
	})
	var invokeCount int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
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
