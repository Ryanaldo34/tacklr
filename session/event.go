package session

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr/message"
)

// EventType categorizes events sent to the caller.
type EventType string

const (
	EventMessage      EventType = "message"
	EventReasoning    EventType = "reasoning"
	EventFunctionCall EventType = "function_call"
	EventToolResult   EventType = "tool_result"
	EventComplete     EventType = "complete"
	EventError        EventType = "error"
	EventInterrupt    EventType = "yield"
	EventToolUpdate   EventType = "tool_update"
	EventPlanUpdate   EventType = "plan_update"
)

// Event is the harness interior event bus. Protocols map these events
// to wire formats; the harness does not own protocol framing.
type Event struct {
	Type      EventType
	TurnID    string
	MessageID string
	Content   string
	Data      []byte
	ToolCalls []message.ToolCall
	// Error is in-process only. Workflow Streams cannot encode error values;
	// Fail is the durable stand-in (the error's text) and FailKind names the
	// sentinel a client acts on, so Kind.Err rebuilds it after the wire.
	Error    error    `json:"-"`
	Fail     string   `json:"fail,omitempty"`
	FailKind FailKind `json:"failKind,omitempty"`
}

// FailKind names a turn failure a client acts on: a refusal, the token
// limit, or a cancel. Empty is any other failure.
type FailKind string

const (
	FailRefused   FailKind = "refused"
	FailMaxTokens FailKind = "max_tokens"
	FailCancelled FailKind = "cancelled"
)

// FailureEvent is the error event that reports a turn ended with err.
func FailureEvent(err error) Event {
	var kind FailKind
	switch {
	case errors.Is(err, ErrModelRefused):
		kind = FailRefused
	case errors.Is(err, ErrMaxTokens):
		kind = FailMaxTokens
	case errors.Is(err, context.Canceled):
		kind, err = FailCancelled, context.Canceled
	}
	return Event{Type: EventError, Fail: err.Error(), Content: err.Error(), FailKind: kind}
}

// Err is the failure a FailureEvent carried: the sentinel kind names, else
// an error with text.
func (k FailKind) Err(text string) error {
	switch k {
	case FailRefused:
		return ErrModelRefused
	case FailMaxTokens:
		return ErrMaxTokens
	case FailCancelled:
		return context.Canceled
	}
	return errors.New(text)
}
