package server

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/control"
)

// ElicitationResult is the Client response to elicitation/create.
type ElicitationResult struct {
	Action  string         `json:"action"`
	Content map[string]any `json:"content"`
}

// SelectionToElicitationParams builds form-mode elicitation/create params from
// a user-selection interrupt.
func SelectionToElicitationParams(sessionID, toolCallID, question string, opts []control.UserChoice) (map[string]any, error) {
	if len(opts) < 2 {
		return nil, fmt.Errorf("elicitation requires at least 2 options")
	}
	titles := make([]string, 0, len(opts))
	var msg strings.Builder
	if question != "" {
		msg.WriteString(question)
		msg.WriteString("\n\n")
	}
	msg.WriteString("Options:\n")
	for i, o := range opts {
		if o.Title == "" {
			return nil, fmt.Errorf("option %d: empty title", i)
		}
		titles = append(titles, o.Title)
		msg.WriteString(fmt.Sprintf("%d. %s", i+1, o.Title))
		if o.Description != "" {
			msg.WriteString(" — ")
			msg.WriteString(o.Description)
		}
		if o.IsRecommended {
			msg.WriteString(" (recommended)")
		}
		msg.WriteByte('\n')
	}

	params := map[string]any{
		"sessionId": sessionID,
		"mode":      "form",
		"message":   strings.TrimSpace(msg.String()),
		"requestedSchema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"choice": map[string]any{
					"type":  "string",
					"title": "Your choice",
					"enum":  titles,
				},
			},
			"required": []string{"choice"},
		},
	}
	if toolCallID != "" {
		params["toolCallId"] = toolCallID
	}
	return params, nil
}

// ElicitationResultToSelectionPayload maps an accept response to the harness
// interrupt resolution payload. Returns action and optional selection JSON.
func ElicitationResultToSelectionPayload(raw json.RawMessage, opts []control.UserChoice) (action string, resolution []byte, err error) {
	var res ElicitationResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return "", nil, fmt.Errorf("unmarshal elicitation result: %w", err)
	}
	action = res.Action
	switch action {
	case "accept":
		choice, _ := res.Content["choice"].(string)
		if choice == "" {
			return action, nil, fmt.Errorf("accept missing content.choice")
		}
		idx := -1
		for i, o := range opts {
			if o.Title == choice {
				idx = i
				break
			}
		}
		if idx < 0 {
			return action, nil, fmt.Errorf("unknown choice %q", choice)
		}
		resolution, err = json.Marshal(map[string]any{"selectionIdx": idx})
		return action, resolution, err
	case "decline", "cancel":
		return action, nil, nil
	default:
		return action, nil, fmt.Errorf("unknown elicitation action %q", action)
	}
}

// ParseUserSelectionFromInterruptData extracts options from StreamEventInterrupt Data
// payload shape {"interruptId":"...","data":<serialized UserSelectionInterrupt>}.
func ParseUserSelectionFromInterruptData(data []byte) (interruptID string, opts []control.UserChoice, err error) {
	var envelope struct {
		InterruptId string          `json:"interruptId"`
		Data        json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", nil, err
	}
	var usi control.UserSelectionInterrupt
	if err := json.Unmarshal(envelope.Data, &usi); err != nil {
		return "", nil, fmt.Errorf("unmarshal selection interrupt: %w", err)
	}
	return envelope.InterruptId, usi.Options, nil
}
