package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
)

type sseEvent struct {
	Type      string            `json:"type"`
	Content   string            `json:"content,omitempty"`
	Data      json.RawMessage   `json:"data,omitempty"`
	ToolCalls []tacklr.ToolCall `json:"tool_calls,omitempty"`
	Error     string            `json:"error,omitempty"`
}

type threadEvent struct {
	ThreadID string `json:"thread_id"`
}

func acceptsSSE(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

func toSSEEvent(ev tacklr.StreamEvent) sseEvent {
	e := sseEvent{Type: string(ev.Type), Content: ev.Content, Data: ev.Data, ToolCalls: ev.ToolCalls}
	if ev.Error != nil {
		e.Error = ev.Error.Error()
	}
	return e
}

func writeSSEEvent(w io.Writer, flusher http.Flusher, evType string, data []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evType, data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func writeSSEError(w http.ResponseWriter, flusher http.Flusher, msg string) error {
	data, err := json.Marshal(sseEvent{Type: "error", Error: msg})
	if err != nil {
		return fmt.Errorf("marshal error event: %w", err)
	}
	return writeSSEEvent(w, flusher, "error", data)
}

func eventToRawSSE(threadID string, ev *streaming.StreamEvent) ([][]byte, error) {
	data, err := json.Marshal(toSSEEvent(*ev))
	if err != nil {
		return nil, fmt.Errorf("marshal sse event: %w", err)
	}
	return [][]byte{data}, nil
}
