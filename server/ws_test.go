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
	env := ProtocolEnv{Runtime: r.Runtime, Catalog: r.Catalog, Conn: &Conn{}}
	for _, route := range SSE.HTTPRoutes() {
		if route.Method == http.MethodGet && route.Pattern == "/{$}" {
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

// TestWebSocket_invalidRequestAndTurnError covers WS validation failure and
// RunTurn error framing on the GET / path.
func TestWebSocket_invalidRequestAndTurnError(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	mux := http.NewServeMux()
	env := ProtocolEnv{Runtime: r.Runtime, Catalog: r.Catalog, Conn: &Conn{}}
	for _, route := range SSE.HTTPRoutes() {
		if route.Method == http.MethodGet && route.Pattern == "/{$}" {
			mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
				route.Handler(env, w, req)
			})
		}
	}
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	// Invalid body → error frame.
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{`)); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.Read(ctx)
	_ = conn.Close(websocket.StatusNormalClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "error") && !strings.Contains(string(data), "invalid") {
		t.Fatalf("want error frame, got %s", data)
	}

	// Unknown agent → error frame.
	conn2, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn2.Write(ctx, websocket.MessageText, []byte(`{"agent_id":"missing","prompt":"x"}`)); err != nil {
		t.Fatal(err)
	}
	_, data2, err := conn2.Read(ctx)
	_ = conn2.Close(websocket.StatusNormalClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data2), "error") && !strings.Contains(string(data2), "not found") {
		t.Fatalf("want agent error, got %s", data2)
	}
}
