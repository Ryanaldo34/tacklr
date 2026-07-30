package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ryanaldo34/tacklr"
)

// TestWebSocket_promptStreamsMessageAndComplete covers the SSE protocol WS path:
// client opens GET /, sends a turn body, receives thread id + message + complete.
func TestWebSocket_promptStreamsMessageAndComplete(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ws-hello", IsComplete: true}
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	mux := http.NewServeMux()
	env := ProtocolEnv{Registry: r, Conn: &Conn{}}
	for _, route := range SSE.HTTPRoutes() {
		if route.Method == "GET" && route.Pattern == "/" {
			mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
				route.Handler(env, w, req)
			})
		}
	}
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	reqBody := []byte(`{"agent_id":"default","prompt":"hi from ws"}`)
	if err := conn.Write(ctx, websocket.MessageText, reqBody); err != nil {
		t.Fatalf("write request: %v", err)
	}

	var sawThread, sawMessage, sawComplete bool
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && !(sawThread && sawMessage && sawComplete) {
		readCtx, readCancel := context.WithTimeout(ctx, time.Second)
		_, data, err := conn.Read(readCtx)
		readCancel()
		if err != nil {
			if strings.Contains(err.Error(), "deadline") || strings.Contains(err.Error(), "timeout") {
				continue
			}
			// Stream may close after complete.
			break
		}
		var ev map[string]any
		if err := json.Unmarshal(data, &ev); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		switch ev["type"] {
		case "thread":
			if s, _ := ev["content"].(string); s != "" {
				sawThread = true
			}
		case "message":
			if ev["content"] == "ws-hello" {
				sawMessage = true
			}
		case "complete":
			sawComplete = true
		case "error":
			t.Fatalf("ws error event: %v", ev)
		}
	}
	if !sawThread {
		t.Error("expected thread event")
	}
	if !sawMessage {
		t.Error("expected message content ws-hello")
	}
	if !sawComplete {
		t.Error("expected complete event")
	}
}
