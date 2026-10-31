package tacklr

import (
	"fmt"
)

// sessionRuntime is the tool-facing hook for one harness turn:
// EmitUpdate, StateGet/Set/Delete, Park, CurrentToolCallID.
//
// Session modules (plan, permissions, on-call) are not on this type.
// Lifetime: create with newSessionRuntime at Run start (with the turn event
// channel); discard when the turn ends. Session state lives on sessionManager
// and outlives the turn. Value copies share the same channel and session
// pointers.
//
// Invariants: out and session are always non-nil after newSessionRuntime.
type sessionRuntime struct {
	session    *sessionManager
	out        chan StreamEvent
	toolCallID string
}

// newSessionRuntime builds a turn-scoped Runtime.
func newSessionRuntime(ch chan StreamEvent, sm *sessionManager) sessionRuntime {
	return sessionRuntime{
		session: sm,
		out:     ch,
	}
}

// WithToolCallID returns a copy bound to the given tool call id.
func (rt sessionRuntime) WithToolCallID(id string) sessionRuntime {
	rt.toolCallID = id
	return rt
}

// CurrentToolCallID is the tool call this sessionRuntime is serving, or empty.
func (rt sessionRuntime) CurrentToolCallID() string {
	return rt.toolCallID
}

// EmitUpdate sends a non-blocking tool progress update for the current call.
func (rt sessionRuntime) EmitUpdate(message string) {
	select {
	case rt.out <- StreamEvent{
		Type:      StreamEventToolUpdate,
		Content:   message,
		MessageID: rt.toolCallID,
	}:
	default:
	}
}

// StateGet returns a session value stored with StateSet.
func (rt sessionRuntime) StateGet(key string) (any, bool) {
	return rt.session.StateGet(key)
}

// StateSet stores a session value for tools and interceptors.
func (rt sessionRuntime) StateSet(key string, value any) error {
	return rt.session.StateSet(key, value)
}

// StateDelete removes a session value.
func (rt sessionRuntime) StateDelete(key string) {
	rt.session.StateDelete(key)
}

// Park parks this tool call and returns the interrupt as the error. The
// call's run after resume gets the answered interrupt and a nil error.
func (rt sessionRuntime) Park(kind string, payload []byte) (Interrupt, error) {
	if answered, ok := rt.session.TakeAnswer(rt.toolCallID, kind); ok {
		return answered, nil
	}
	intr, ok := NewInterrupt(kind)
	if !ok {
		return nil, fmt.Errorf("%q is not a valid interrupt type", kind)
	}
	if init, ok := intr.(PayloadInitializer); ok {
		_ = init.InitFromPayload(payload)
	}
	return nil, rt.session.Park(rt.toolCallID, intr)
}
