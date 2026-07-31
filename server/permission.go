package server

import (
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr/control"
)

// InterruptEventEnvelope is the harness StreamEventInterrupt Data shape.
type InterruptEventEnvelope struct {
	InterruptId string          `json:"interruptId"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
}

// ParseInterruptEnvelope extracts interrupt id, type, and raw data from a yield event.
func ParseInterruptEnvelope(data []byte) (InterruptEventEnvelope, error) {
	var env InterruptEventEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return env, err
	}
	if env.InterruptId == "" {
		return env, fmt.Errorf("interrupt envelope missing interruptId")
	}
	return env, nil
}

// ParseUserSelectionFromInterruptData extracts options from StreamEventInterrupt Data
// payload shape {"interruptId":"...","data":<serialized UserSelectionInterrupt>}.
func ParseUserSelectionFromInterruptData(data []byte) (interruptID string, opts []control.UserChoice, err error) {
	env, err := ParseInterruptEnvelope(data)
	if err != nil {
		return "", nil, err
	}
	var usi control.UserSelectionInterrupt
	if err := json.Unmarshal(env.Data, &usi); err != nil {
		return "", nil, fmt.Errorf("unmarshal selection interrupt: %w", err)
	}
	return env.InterruptId, usi.Options, nil
}

// ParseToolPermissionFromInterruptData extracts a tool permission interrupt from yield data.
func ParseToolPermissionFromInterruptData(data []byte) (interruptID string, perm control.ToolPermissionInterrupt, err error) {
	env, err := ParseInterruptEnvelope(data)
	if err != nil {
		return "", perm, err
	}
	if err := json.Unmarshal(env.Data, &perm); err != nil {
		return "", perm, fmt.Errorf("unmarshal permission interrupt: %w", err)
	}
	if len(perm.Options) == 0 {
		perm.Options = control.DefaultPermissionOptions()
	}
	return env.InterruptId, perm, nil
}

// PermissionToACPParams builds session/request_permission params.
func PermissionToACPParams(sessionID, toolCallID string, perm control.ToolPermissionInterrupt) map[string]any {
	options := make([]map[string]any, 0, len(perm.Options))
	for _, o := range perm.Options {
		options = append(options, map[string]any{
			"optionId": o.OptionID,
			"name":     o.Name,
			"kind":     o.Kind,
		})
	}
	title := perm.ToolName
	if title == "" {
		title = "Tool call"
	}
	toolCall := map[string]any{
		"toolCallId": toolCallID,
		"title":      title,
		"status":     "pending",
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
		resolution, err = json.Marshal(control.ToolPermissionPayload{OptionID: res.Outcome.OptionID})
		return resolution, false, err
	default:
		return nil, false, fmt.Errorf("unknown permission outcome %q", res.Outcome.Outcome)
	}
}
