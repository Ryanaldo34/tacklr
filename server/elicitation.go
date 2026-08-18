package server

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// ElicitationResult is the Client response to elicitation/create.
type ElicitationResult struct {
	Action  string         `json:"action"`
	Content map[string]any `json:"content"`
}

// SelectionToElicitationParams builds form-mode elicitation/create params from
// a user-selection interrupt.
func SelectionToElicitationParams(sessionID, toolCallID, question string, opts []interrupt.UserChoice) (map[string]any, error) {
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
	}, []string{"choice"}), nil
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
func ElicitationResultToSelectionPayload(raw json.RawMessage, opts []interrupt.UserChoice) (action string, resolution []byte, err error) {
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
	idx := slices.IndexFunc(opts, func(o interrupt.UserChoice) bool {
		return o.Title == choice
	})
	if idx < 0 {
		return action, nil, fmt.Errorf("unknown choice %q", choice)
	}
	resolution, err = json.Marshal(map[string]any{"selectionIdx": idx})
	return action, resolution, err
}
