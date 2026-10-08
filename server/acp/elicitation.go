package acp

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr"
)

// ElicitationResult is the Client response to elicitation/create.
type ElicitationResult struct {
	Action  string         `json:"action"`
	Content map[string]any `json:"content"`
}

// SelectionToElicitationParams builds form-mode elicitation/create params from
// a user-selection interrupt. ask_user_choice already requires ≥2 titled options.
func SelectionToElicitationParams(sessionID, toolCallID, question string, opts []tacklr.UserChoice) map[string]any {
	titles := make([]string, 0, len(opts))
	var msg strings.Builder
	if question != "" {
		msg.WriteString(question)
		msg.WriteString("\n\n")
	}
	msg.WriteString("Options:\n")
	for i, o := range opts {
		titles = append(titles, o.Title)
		fmt.Fprintf(&msg, "%d. %s", i+1, o.Title)
		if o.Description != "" {
			msg.WriteString(" — ")
			msg.WriteString(o.Description)
		}
		if o.IsRecommended {
			msg.WriteString(" (recommended)")
		}
		msg.WriteByte('\n')
	}

	return elicitationFormParams(sessionID, toolCallID, strings.TrimSpace(msg.String()), map[string]any{
		"choice": map[string]any{
			"type":  "string",
			"title": "Your choice",
			"enum":  titles,
		},
	}, []string{"choice"})
}

func elicitationFormParams(sessionID, toolCallID, message string, properties map[string]any, required []string) map[string]any {
	params := map[string]any{
		"sessionId": sessionID,
		"mode":      "form",
		"message":   message,
		"requestedSchema": map[string]any{
			"type":       "object",
			"properties": properties,
			"required":   required,
		},
	}
	if toolCallID != "" {
		params["toolCallId"] = toolCallID
	}
	return params
}

func parseElicitationResult(raw json.RawMessage) (ElicitationResult, error) {
	var res ElicitationResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return res, fmt.Errorf("unmarshal elicitation result: %w", err)
	}
	switch res.Action {
	case "accept", "decline", "cancel":
		return res, nil
	default:
		return res, fmt.Errorf("unknown elicitation action %q", res.Action)
	}
}

// ElicitationResultToSelectionPayload maps an accept response to the harness
// interrupt resolution payload. Returns action and optional selection JSON.
func ElicitationResultToSelectionPayload(raw json.RawMessage, opts []tacklr.UserChoice) (action string, resolution []byte, err error) {
	res, err := parseElicitationResult(raw)
	if err != nil {
		return "", nil, err
	}
	action = res.Action
	if action != "accept" {
		return action, nil, nil
	}
	choice, _ := res.Content["choice"].(string)
	if choice == "" {
		return action, nil, fmt.Errorf("accept missing content.choice")
	}
	idx := slices.IndexFunc(opts, func(o tacklr.UserChoice) bool {
		return o.Title == choice
	})
	if idx < 0 {
		return action, nil, fmt.Errorf("unknown choice %q", choice)
	}
	resolution, err = json.Marshal(map[string]any{"selectionIdx": idx})
	return action, resolution, err
}

// InterruptEventEnvelope is the harness StreamEventInterrupt Data shape.
type InterruptEventEnvelope struct {
	InterruptId string          `json:"interruptId"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
}

// PermissionToACPParams builds session/request_permission params.
func PermissionToACPParams(sessionID, toolCallID string, perm tacklr.ToolPermissionInterrupt) map[string]any {
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
		resolution, err = json.Marshal(tacklr.ToolPermissionPayload{OptionID: res.Outcome.OptionID})
		return resolution, false, err
	default:
		return nil, false, fmt.Errorf("unknown permission outcome %q", res.Outcome.Outcome)
	}
}
