package tacklr

import (
	"fmt"

	"github.com/ryanaldo34/tacklr/interrupt"
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

// Park writes pending for this tool call and returns the interrupt as error.
// After Resume, re-entry returns the resolved interrupt with a nil error.
func (rt sessionRuntime) Park(kind string, payload []byte) (interrupt.Interrupt, error) {
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
