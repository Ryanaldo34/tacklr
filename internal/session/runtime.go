package session

import (
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Runtime is the tool-facing hook for one harness turn:
// EmitUpdate, StateGet/Set/Delete, RaiseInterrupt, CurrentToolCallID.
//
// Session modules (plan, permissions, on-call, parks) are not on this type.
// Lifetime: create with NewRuntime at Run start (with the turn event channel);
// discard when the turn ends. Session state lives on SessionManager and outlives
// the turn. Value copies share the same channel and session pointers.
//
// Invariants: out and session are always non-nil after NewRuntime.
type Runtime struct {
	session    *SessionManager
	out        chan streaming.StreamEvent
	toolCallID string
}

// NewRuntime builds a turn-scoped Runtime. ch and sm must be non-nil.
func NewRuntime(ch chan streaming.StreamEvent, sm *SessionManager) Runtime {
	if ch == nil {
		panic("session.NewRuntime: nil event channel")
	}
	if sm == nil {
		panic("session.NewRuntime: nil SessionManager")
	}
	return Runtime{
		session: sm,
		out:     ch,
	}
}

// WithToolCallID returns a copy bound to the given tool call id.
func (rt Runtime) WithToolCallID(id string) Runtime {
	rt.toolCallID = id
	return rt
}

// CurrentToolCallID is the tool call this Runtime is serving, or empty.
func (rt Runtime) CurrentToolCallID() string {
	return rt.toolCallID
}

// EmitUpdate sends a non-blocking tool progress update for the current call.
func (rt Runtime) EmitUpdate(message string) {
	select {
	case rt.out <- streaming.StreamEvent{
		Type:      streaming.StreamEventToolUpdate,
		Content:   message,
		MessageID: rt.toolCallID,
	}:
	default:
	}
}

// StateGet returns a session value stored with StateSet.
func (rt Runtime) StateGet(key string) (any, bool) {
	return rt.session.StateGet(key)
}

// StateSet stores a session value for tools and interceptors.
func (rt Runtime) StateSet(key string, value any) error {
	return rt.session.StateSet(key, value)
}

// StateDelete removes a session value.
func (rt Runtime) StateDelete(key string) {
	rt.session.StateDelete(key)
}

// RaiseInterrupt parks the current tool until the host resumes with a payload.
func (rt Runtime) RaiseInterrupt(kind string, payload []byte) (interrupt.Interrupt, error) {
	return rt.session.raiseInterrupt(rt.toolCallID, kind, payload)
}
