package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr/streaming"
)

type syncBuffer struct {
	bytes.Buffer
	synced bool
}

func (s *syncBuffer) Sync() error {
	s.synced = true
	return nil
}

func TestLineAndSSEMessageWriters(t *testing.T) {
	var buf syncBuffer
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
	if !buf.synced {
		t.Fatal("expected Sync on line writer")
	}

	rec := httptest.NewRecorder()
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
	// Frame without type → default event name.
	if err := sw.WriteFrame([]byte(`{"content":"x"}`)); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "sse-err") || !strings.Contains(body, "event: message") {
		t.Fatalf("sse body = %s", body)
	}
}

func TestWSMessageWriter_resultErrorAndHelpers(t *testing.T) {
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
	mw := &wsMessageWriter{ctx: ctx, c: serverConn}
	if err := mw.WriteResult(json.RawMessage(`9`), map[string]string{"stopReason": "end_turn"}); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteError(json.RawMessage(`10`), clientErrorf(ErrInvalidRequest, "ws-err")); err != nil {
		t.Fatal(err)
	}
	if err := wsWriteJSON(ctx, serverConn, map[string]any{"type": "error", "error": "clienty"}); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteFrame([]byte(`{"type":"message","content":"f"}`)); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4; i++ {
		readCtx, c := context.WithTimeout(ctx, 500*time.Millisecond)
		_, _, err := client.Read(readCtx)
		c()
		if err != nil {
			break
		}
	}
}

func TestPresentStreamEvent_withError(t *testing.T) {
	ev, err := presentStreamEvent(streaming.StreamEvent{Type: streaming.StreamEventError, Error: errors.New("e")})
	if err != nil {
		t.Fatal(err)
	}
	if ev.ErrorText != "e" {
		t.Fatalf("%+v", ev)
	}
	if _, err := presentStreamEvent(streaming.StreamEvent{}); err == nil {
		t.Fatal("unknown stream event type was silently accepted")
	}
}

func TestWriteSSEError_andPresentStreamEventSSE(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := writeSSEError(rec, rec, "boom"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("%s", rec.Body.String())
	}
	presented, err := presentStreamEvent(streaming.StreamEvent{Type: streaming.StreamEventMessage, Content: "x"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(presented)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("empty SSE payload")
	}
}
