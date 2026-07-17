package streaming

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ryanaldo34/tacklr"
)

type wsEvent struct {
	Type      string             `json:"type"`
	TurnID    string             `json:"turn_id,omitempty"`
	MessageID string             `json:"message_id,omitempty"`
	Content   string             `json:"content,omitempty"`
	Data      json.RawMessage    `json:"data,omitempty"`
	ToolCalls []tacklr.ToolCall `json:"tool_calls,omitempty"`
	Error     string             `json:"error,omitempty"`
}

func startWSTestServer(t *testing.T) (*httptest.Server, chan wsEvent) {
	t.Helper()
	events := make(chan wsEvent, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept failed: %v", err)
			return
		}
		defer c.CloseNow()

		ctx := context.Background()
		for {
			var ev wsEvent
			if err := wsjson.Read(ctx, c, &ev); err != nil {
				return
			}
			events <- ev
		}
	}))
	t.Cleanup(srv.Close)
	return srv, events
}

func dialTestWS(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	url := strings.Replace(srv.URL, "http://", "ws://", 1)
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func mustReceive(t *testing.T, ch chan wsEvent) wsEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for websocket event")
	}
	return wsEvent{}
}

func TestWebSocketStreamer_Stream_writesAndForwards(t *testing.T) {
	srv, received := startWSTestServer(t)
	conn := dialTestWS(t, srv)

	ctx := context.Background()
	streamer := NewWebSocketStreamer(ctx, conn)
	out := make(chan tacklr.StreamEvent, 10)

	err := streamer.Stream(tacklr.LLMResponseChunk{
		Type:      tacklr.StreamEventMessage,
		TurnId:    "turn_1",
		MessageId: "msg_1",
		Content:   "hello",
	}, out)
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	ev := mustReceive(t, received)
	if ev.Type != "message" {
		t.Errorf("type = %q, want message", ev.Type)
	}
	if ev.TurnID != "turn_1" {
		t.Errorf("turn_id = %q, want turn_1", ev.TurnID)
	}
	if ev.MessageID != "msg_1" {
		t.Errorf("message_id = %q, want msg_1", ev.MessageID)
	}
	if ev.Content != "hello" {
		t.Errorf("content = %q, want hello", ev.Content)
	}

	select {
	case outEv := <-out:
		if outEv.Type != tacklr.StreamEventMessage || outEv.Content != "hello" {
			t.Errorf("out event = %+v", outEv)
		}
	default:
		t.Fatal("expected event on out channel")
	}
}

func TestWebSocketStreamer_Stream_writesCompleteEvent(t *testing.T) {
	srv, received := startWSTestServer(t)
	conn := dialTestWS(t, srv)

	ctx := context.Background()
	streamer := NewWebSocketStreamer(ctx, conn)
	out := make(chan tacklr.StreamEvent, 10)

	if err := streamer.Stream(tacklr.LLMResponseChunk{
		IsComplete: true,
	}, out); err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	ev := mustReceive(t, received)
	if ev.Type != "complete" {
		t.Errorf("type = %q, want complete", ev.Type)
	}

	select {
	case outEv := <-out:
		if outEv.Type != tacklr.StreamEventComplete {
			t.Errorf("out event type = %v", outEv.Type)
		}
	default:
		t.Fatal("expected event on out channel")
	}
}

func TestWebSocketStreamer_WriteEvent_nonInferenceEvent(t *testing.T) {
	srv, received := startWSTestServer(t)
	conn := dialTestWS(t, srv)

	ctx := context.Background()
	streamer := NewWebSocketStreamer(ctx, conn)

	err := streamer.WriteEvent(tacklr.StreamEvent{
		Type:      tacklr.StreamEventToolResult,
		MessageID: "msg_2",
		Content:   "tool output",
	})
	if err != nil {
		t.Fatalf("WriteEvent error: %v", err)
	}

	ev := mustReceive(t, received)
	if ev.Type != "tool_result" {
		t.Errorf("type = %q, want tool_result", ev.Type)
	}
	if ev.Content != "tool output" {
		t.Errorf("content = %q", ev.Content)
	}
}


