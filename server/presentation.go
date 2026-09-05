package server

import (
	"encoding/json"
	"slices"

	"github.com/ryanaldo34/tacklr"
)

// presentationEvent is the protocol-neutral client-facing meaning of one
// harness event. Protocol serializers may reshape it, but semantic conversion
// from tacklr.StreamEvent happens only in presentStreamEvent.
type presentationEvent struct {
	Type      string            `json:"type"`
	TurnID    string            `json:"turn_id,omitempty"`
	MessageID string            `json:"message_id,omitempty"`
	Content   string            `json:"content,omitempty"`
	Data      json.RawMessage   `json:"data,omitempty"`
	ToolCalls []tacklr.ToolCall `json:"tool_calls,omitempty"`
	ErrorText string            `json:"error,omitempty"`
	Error     error             `json:"-"`
}

func presentStreamEvent(event tacklr.StreamEvent) presentationEvent {
	presented := presentationEvent{
		Type:      string(event.Type),
		TurnID:    event.TurnID,
		MessageID: event.MessageID,
		Content:   event.Content,
		Data:      append(json.RawMessage(nil), event.Data...),
		ToolCalls: slices.Clone(event.ToolCalls),
		Error:     event.Error,
	}
	if event.Error != nil {
		presented.ErrorText = event.Error.Error()
	}
	return presented
}
