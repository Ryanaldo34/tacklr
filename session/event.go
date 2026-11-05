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
	// Delta marks a piece of a message or reasoning text as the model
	// streams it. A delta goes live to the subscribers that listen now and
	// is never stored: the message's complete event, which is stored,
	// carries its full text. Offset is where the delta's Content starts in
	// that text, in bytes, so a reader sees a gap or a repeat.
	Delta  bool `json:"delta,omitempty"`
	Offset int  `json:"offset,omitempty"`
	// Streamed is how many bytes of a complete event's Content the reader
	// already got as deltas. The pump (server.RunTurn) sets it, so a
	// protocol sends only the rest. It is not on the wire.
	Streamed int `json:"-"`
	// Error is in-process only. Workflow Streams cannot encode error values;
	// Fail is the durable stand-in (the error's text) and FailKind names the
	// sentinel a client acts on, so Kind.Err rebuilds it after the wire.
	Error    error    `json:"-"`
	Fail     string   `json:"fail,omitempty"`
	FailKind FailKind `json:"failKind,omitempty"`
}

// Live reports whether ev goes only to the subscribers that listen now and
// is never stored: a delta, or a tool's progress update.
func (ev Event) Live() bool { return ev.Delta || ev.Type == EventToolUpdate }

// FailKind names a turn failure a client acts on: a refusal, the token
// limit, a request over the context window, or a cancel. Empty is any
// other failure.
type FailKind string

const (
	FailRefused       FailKind = "refused"
	FailMaxTokens     FailKind = "max_tokens"
	FailContextLength FailKind = "context_length"
	FailCancelled     FailKind = "cancelled"
)

// KindOf is the kind of failure err is, or empty.
func KindOf(err error) FailKind {
	switch {
	case errors.Is(err, ErrModelRefused):
		return FailRefused
	case errors.Is(err, ErrContextLength):
		return FailContextLength
	case errors.Is(err, ErrMaxTokens):
		return FailMaxTokens
	case errors.Is(err, context.Canceled):
		return FailCancelled
	}
	return ""
}

// FailureEvent is the error event that reports a turn ended with err.
func FailureEvent(err error) Event {
	kind := KindOf(err)
	if kind == FailCancelled {
		err = context.Canceled
	}
	return Event{Type: EventError, Fail: err.Error(), Content: err.Error(), FailKind: kind}
}

// WithText is a failure of kind k with its text: errors.Is finds k's
// sentinel (Err) after the failure crossed a step boundary as text. With no
// kind it is an error with text.
func (k FailKind) WithText(text string) error {
	if k == "" {
		return errors.New(text)
	}
	return kindError{text: text, kind: k.Err(text)}
}

// kindError is a failure's text that unwraps to its kind's sentinel.
type kindError struct {
	text string
	kind error
}

func (e kindError) Error() string { return e.text }

func (e kindError) Unwrap() error { return e.kind }

// Err is the failure a FailureEvent carried: the sentinel kind names, else
// an error with text.
func (k FailKind) Err(text string) error {
	switch k {
	case FailRefused:
		return ErrModelRefused
	case FailMaxTokens:
		return ErrMaxTokens
	case FailContextLength:
		return ErrContextLength
	case FailCancelled:
		return context.Canceled
	}
	return errors.New(text)
}
