package server

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr"
)

// turnRequest is the common payload for both SSE and WebSocket prompt/resume
// endpoints. Fields are omitempty so the same shape can be reused across all
// four handlers while each endpoint validates only the fields it cares about.
type turnRequest struct {
	AgentID   string                     `json:"agent_id"`
	ThreadID  string                     `json:"thread_id,omitempty"`
	Prompt    string                     `json:"prompt,omitempty"`
	Responses map[string]json.RawMessage `json:"responses,omitempty"`
}

// resolveThread chooses the thread ID and whether to load an existing session
// based on the request and endpoint semantics.
func resolveThread(pr *parsedRequest, resume bool) (threadID string, load bool) {
	if resume {
		return pr.ThreadID, true
	}
	if pr.ThreadID != "" {
		return pr.ThreadID, true
	}
	return uuid.New().String(), false
}

// runHarness executes either a prompt run or a resume from interrupt. The
// caller owns the harness (and any transport-specific streaming strategy).
func runHarness(ctx context.Context, h *tacklr.AgentHarness, pr *parsedRequest, resume bool) (<-chan tacklr.StreamEvent, error) {
	if resume {
		responses := make(map[string][]byte, len(pr.Responses))
		for id, payload := range pr.Responses {
			responses[id] = []byte(payload)
		}
		return h.ReturnFromInterrupt(ctx, responses)
	}
	return h.Run(ctx, pr.Prompt)
}
