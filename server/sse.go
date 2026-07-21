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

func validateSSERequest(body []byte) (*parsedRequest, error) {
	var req turnRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, clientErrorf(ErrInvalidRequest, "invalid JSON: %v", err)
	}
	if req.AgentID == "" {
		return nil, clientErrorf(ErrInvalidRequest, "agent_id is required")
	}
	if len(req.Responses) > 0 {
		if req.ThreadID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "thread_id is required for resume")
		}
		if req.Prompt != "" {
			return nil, clientErrorf(ErrInvalidRequest, "prompt is not allowed on resume")
		}
		for id, payload := range req.Responses {
			if !json.Valid(payload) {
				return nil, clientErrorf(ErrInvalidRequest, "response for interrupt %q is not valid JSON", id)
			}
		}
		return &parsedRequest{
			AgentID:   req.AgentID,
			ThreadID:  req.ThreadID,
			Responses: req.Responses,
		}, nil
	}
	if req.Prompt == "" {
		return nil, clientErrorf(ErrInvalidRequest, "prompt is required")
	}
	return &parsedRequest{
		AgentID:  req.AgentID,
		ThreadID: req.ThreadID,
		Prompt:   req.Prompt,
	}, nil
}
