package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ryanaldo34/tacklr/streaming"
)

type threadEvent struct {
	ThreadID string `json:"thread_id"`
}

func writeSSEEvent(w io.Writer, flusher http.Flusher, evType string, data []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evType, data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func writeSSEError(w http.ResponseWriter, flusher http.Flusher, msg string) error {
	data, _ := json.Marshal(presentationEvent{Type: "error", ErrorText: msg})
	return writeSSEEvent(w, flusher, "error", data)
}

func eventToRawSSE(threadID string, ev *streaming.StreamEvent) ([][]byte, error) {
	_ = threadID
	presented, err := presentStreamEvent(*ev)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(presented)
	if err != nil {
		return nil, err
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
		// Response payloads are json.RawMessage from a successful Unmarshal, so they
		// are already valid JSON tokens; no extra json.Valid scan is required.
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
