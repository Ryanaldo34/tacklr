package session

import (
	"fmt"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Runtime is the tool-facing hook for one harness turn:
// EmitUpdate, StateGet/Set/Delete, Park, CurrentToolCallID.
//
// Session modules (plan, permissions, on-call) are not on this type.
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

// NewRuntime builds a turn-scoped Runtime.
func NewRuntime(ch chan streaming.StreamEvent, sm *SessionManager) Runtime {
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

// Park writes pending for this tool call and returns the interrupt as error.
// After Resume, re-entry returns the resolved interrupt with a nil error.
func (rt Runtime) Park(kind string, payload []byte) (interrupt.Interrupt, error) {
	if resolved, ok := rt.session.TakeResolved(rt.toolCallID); ok {
		return resolved, nil
	}
	intr, ok := interrupt.New(kind)
	if !ok {
		return nil, fmt.Errorf("%q is not a valid interrupt type", kind)
	}
	if init, ok := intr.(interrupt.PayloadInitializer); ok {
		_ = init.InitFromPayload(payload)
	}
	return nil, rt.session.Park(rt.toolCallID, intr)
}
