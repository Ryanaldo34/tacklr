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

func unmarshalInterruptData[T any](data []byte) (interruptID string, v T, err error) {
	env, err := ParseInterruptEnvelope(data)
	if err != nil {
		return "", v, err
	}
	if err := json.Unmarshal(env.Data, &v); err != nil {
		return "", v, fmt.Errorf("unmarshal interrupt data: %w", err)
	}
	return env.InterruptId, v, nil
}

// ParseUserSelectionFromInterruptData extracts options from StreamEventInterrupt Data
// payload shape {"interruptId":"...","data":<serialized UserSelectionInterrupt>}.
func ParseUserSelectionFromInterruptData(data []byte) (interruptID string, opts []interrupt.UserChoice, err error) {
	id, usi, err := unmarshalInterruptData[interrupt.UserSelectionInterrupt](data)
	if err != nil {
		return "", nil, err
	}
	return id, usi.Options, nil
}

// ParseToolPermissionFromInterruptData extracts a tool permission interrupt from yield data.
func ParseToolPermissionFromInterruptData(data []byte) (interruptID string, perm interrupt.ToolPermissionInterrupt, err error) {
	id, perm, err := unmarshalInterruptData[interrupt.ToolPermissionInterrupt](data)
	if err != nil {
		return "", perm, err
	}
	if len(perm.Options) == 0 {
		perm.Options = interrupt.DefaultPermissionOptions()
	}
	return id, perm, nil
}

// ParseWriteApprovalFromInterruptData extracts a write-approval interrupt from yield data.
func ParseWriteApprovalFromInterruptData(data []byte) (interruptID string, wa interrupt.WriteApprovalInterrupt, err error) {
	return unmarshalInterruptData[interrupt.WriteApprovalInterrupt](data)
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
	if title == "" {
		title = "Tool call"
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
