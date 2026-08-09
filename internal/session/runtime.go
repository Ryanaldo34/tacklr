package session

import (
	"encoding/json"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Runtime is the tool-facing surface for a single harness turn:
// EmitUpdate, StateGet/Set/Delete, RaiseInterrupt, Store, CurrentToolCallID.
//
// Lifetime: create with NewRuntime at Run start (with the turn event channel);
// discard when the turn ends. Session state lives on SessionManager and outlives
// the turn. Value copies share the same channel and session pointers.
//
// Invariants: out and session are always non-nil after NewRuntime.
type Runtime struct {
	Store             stores.BaseStore
	CurrentToolCallID string

	session *SessionManager
	out     chan streaming.StreamEvent
}

// NewRuntime builds a turn-scoped Runtime. ch and sm must be non-nil.
func NewRuntime(ch chan streaming.StreamEvent, store stores.BaseStore, sm *SessionManager) Runtime {
	if ch == nil {
		panic("session.NewRuntime: nil event channel")
	}
	if sm == nil {
		panic("session.NewRuntime: nil SessionManager")
	}
	sm.ensure()
	return Runtime{
		Store:   store,
		session: sm,
		out:     ch,
	}
}

// EmitUpdate sends a non-blocking tool progress update for the current call.
func (rt Runtime) EmitUpdate(message string) {
	select {
	case rt.out <- streaming.StreamEvent{
		Type:      streaming.StreamEventToolUpdate,
		Content:   message,
		MessageID: rt.CurrentToolCallID,
	}:
	default:
	}
}

// EmitPlanUpdate sends a non-blocking plan_update stream event.
func (rt Runtime) EmitPlanUpdate(plan []Todo) {
	data, _ := json.Marshal(plan)
	select {
	case rt.out <- streaming.StreamEvent{
		Type: streaming.StreamEventPlanUpdate,
		Data: data,
	}:
	default:
	}
}

// StateGet returns a session value stored with StateSet.
func (rt Runtime) StateGet(key string) (any, bool) {
	return rt.session.stateGet(key)
}

// StateSet stores a session value for tools and interceptors.
func (rt Runtime) StateSet(key string, value any) {
	rt.session.stateSet(key, value)
}

// StateDelete removes a session value.
func (rt Runtime) StateDelete(key string) {
	rt.session.stateDelete(key)
}

// RaiseInterrupt parks the current tool until the host resumes with a payload.
func (rt Runtime) RaiseInterrupt(kind string, payload []byte) (interrupt.Interrupt, error) {
	return rt.session.raiseInterrupt(rt.CurrentToolCallID, kind, payload)
}

// AdoptInterrupt attaches a child interrupt to the current tool call.
func (rt Runtime) AdoptInterrupt(intr interrupt.Interrupt) (interrupt.Interrupt, error) {
	return rt.session.adoptInterrupt(rt.CurrentToolCallID, intr)
}

// TakeResolvedInterrupt removes and returns a resolved interrupt if present.
func (rt Runtime) TakeResolvedInterrupt(id string) (interrupt.Interrupt, bool) {
	return rt.session.takeResolvedInterrupt(id)
}

// PendingInterrupt returns an open interrupt for tool-call id if any.
func (rt Runtime) PendingInterrupt(id string) (interrupt.Interrupt, bool) {
	return rt.session.pendingInterrupt(id)
}

// ReturnInterrupt resolves a parked interrupt with the host payload.
// Prefer SessionManager.ReturnInterrupt when no turn Runtime is in hand.
func (rt Runtime) ReturnInterrupt(id string, result []byte) (interrupt.Interrupt, error) {
	return rt.session.returnInterrupt(id, result)
}

// HasPendingInterrupt is true when any interrupt is still open.
// Prefer SessionManager.HasPendingInterrupt when no turn Runtime is in hand.
func (rt Runtime) HasPendingInterrupt() bool {
	return rt.session.HasPendingInterrupt()
}
