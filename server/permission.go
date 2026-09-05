package server

import (
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// InterruptEventEnvelope is the harness StreamEventInterrupt Data shape.
type InterruptEventEnvelope struct {
	InterruptId string          `json:"interruptId"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
}

// PermissionToACPParams builds session/request_permission params.
func PermissionToACPParams(sessionID, toolCallID string, perm interrupt.ToolPermissionInterrupt) map[string]any {
	options := make([]map[string]any, 0, len(perm.Options))
	for _, o := range perm.Options {
		options = append(options, map[string]any{
			"optionId": o.OptionID,
			"name":     o.Name,
			"kind":     o.Kind,
		})
	}
	title := perm.Title
	if title == "" {
		title = perm.ToolName
	}
	toolCall := map[string]any{
		"toolCallId": toolCallID,
		"title":      title,
		"status":     "pending",
	}
	if perm.ToolName != "" {
		toolCall["name"] = perm.ToolName
	}
	return map[string]any{
		"sessionId": sessionID,
		"toolCall":  toolCall,
		"options":   options,
	}
}

// RequestPermissionResult is the Client response to session/request_permission.
type RequestPermissionResult struct {
	Outcome struct {
		Outcome  string `json:"outcome"`
		OptionID string `json:"optionId,omitempty"`
	} `json:"outcome"`
}

// RequestPermissionResultToPayload maps a client permission response to the
// harness resolution payload. cancelled yields a non-nil err suitable for ending the turn.
func RequestPermissionResultToPayload(raw json.RawMessage) (resolution []byte, cancelled bool, err error) {
	var res RequestPermissionResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, false, fmt.Errorf("unmarshal permission result: %w", err)
	}
	switch res.Outcome.Outcome {
	case "cancelled":
		return nil, true, nil
	case "selected":
		if res.Outcome.OptionID == "" {
			return nil, false, fmt.Errorf("selected outcome missing optionId")
		}
		resolution, err = json.Marshal(interrupt.ToolPermissionPayload{OptionID: res.Outcome.OptionID})
		return resolution, false, err
	default:
		return nil, false, fmt.Errorf("unknown permission outcome %q", res.Outcome.Outcome)
	}
}
