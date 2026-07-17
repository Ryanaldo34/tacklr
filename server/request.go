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

// validateRequest validates the fields required for either a prompt or resume
// turn. It is transport-agnostic; callers know which endpoint they are serving.
// Returned errors wrap ErrInvalidRequest and are safe to send to the caller.
func validateRequest(req turnRequest, resume bool) error {
	if req.AgentID == "" {
		return clientErrorf(ErrInvalidRequest, "agent_id is required")
	}
	if resume {
		if req.ThreadID == "" {
			return clientErrorf(ErrInvalidRequest, "thread_id is required")
		}
		if len(req.Responses) == 0 {
			return clientErrorf(ErrInvalidRequest, "responses is required and must not be empty")
		}
		if req.Prompt != "" {
			return clientErrorf(ErrInvalidRequest, "prompt is not allowed on the resume endpoint")
		}
		for id, payload := range req.Responses {
			if !json.Valid(payload) {
				return clientErrorf(ErrInvalidRequest, "response for interrupt %q is not valid JSON", id)
			}
		}
		return nil
	}
	if req.Prompt == "" {
		return clientErrorf(ErrInvalidRequest, "prompt is required")
	}
	if len(req.Responses) != 0 {
		return clientErrorf(ErrInvalidRequest, "responses are not allowed on the prompt endpoint")
	}
	return nil
}

// resolveThread chooses the thread ID and whether to load an existing session
// based on the request and endpoint semantics.
func resolveThread(req turnRequest, resume bool) (threadID string, load bool) {
	if resume {
		return req.ThreadID, true
	}
	if req.ThreadID != "" {
		return req.ThreadID, true
	}
	return uuid.New().String(), false
}

// runHarness executes either a prompt run or a resume from interrupt. The
// caller owns the harness (and any transport-specific streaming strategy).
func runHarness(ctx context.Context, h *tacklr.AgentHarness, req turnRequest, resume bool) (<-chan tacklr.StreamEvent, error) {
	if resume {
		responses := make(map[string][]byte, len(req.Responses))
		for id, payload := range req.Responses {
			responses[id] = []byte(payload)
		}
		return h.ReturnFromInterrupt(ctx, responses)
	}
	return h.Run(ctx, req.Prompt)
}
