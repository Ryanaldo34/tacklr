package server

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/ryanaldo34/tacklr/streaming"
)

// presentationEvent is the protocol-neutral client-facing meaning of one
// harness event. Protocol serializers may reshape it, but semantic conversion
// from streaming.StreamEvent happens only in presentStreamEvent.
type presentationEvent struct {
	Type      string               `json:"type"`
	TurnID    string               `json:"turn_id,omitempty"`
	MessageID string               `json:"message_id,omitempty"`
	Content   string               `json:"content,omitempty"`
	Data      json.RawMessage      `json:"data,omitempty"`
	ToolCalls []streaming.ToolCall `json:"tool_calls,omitempty"`
	ErrorText string               `json:"error,omitempty"`
	Error     error                `json:"-"`
}

func presentStreamEvent(event streaming.StreamEvent) (presentationEvent, error) {
	switch event.Type {
	case streaming.StreamEventMessage,
		streaming.StreamEventReasoning,
		streaming.StreamEventFunctionCall,
		streaming.StreamEventToolResult,
		streaming.StreamEventComplete,
		streaming.StreamEventError,
		streaming.StreamEventInterrupt,
		streaming.StreamEventToolUpdate,
		streaming.StreamEventPlanUpdate:
	default:
		return presentationEvent{}, fmt.Errorf("server: unsupported stream event type %q", event.Type)
	}
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
	return presented, nil
}

func presentationError(err error) presentationEvent {
	message := PublicError(err).Error()
	return presentationEvent{Type: string(streaming.StreamEventError), Content: message, ErrorText: message, Error: err}
}
