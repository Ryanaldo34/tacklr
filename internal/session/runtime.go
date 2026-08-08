package session

import (
	"encoding/json"
	"sync"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// runtimeOutput is the turn event channel shared across Runtime value copies.
// ch is never nil: live turn bus, or IdleOutput between turns.
type runtimeOutput struct {
	mu sync.RWMutex
	ch chan streaming.StreamEvent
}

// idleOutput is drained in the background so EmitUpdate never needs a nil check
// between turns (after the live turn channel is closed).
var idleOutput = func() chan streaming.StreamEvent {
	ch := make(chan streaming.StreamEvent, 64)
	go func() {
		for range ch {
		}
	}()
	return ch
}()

// IdleOutput is the between-turn sink. Use after a turn ends instead of nil.
func IdleOutput() chan streaming.StreamEvent { return idleOutput }

// Runtime is the tool hook surface (re-exported as tacklr.HarnessRuntime):
// EmitUpdate, StateGet/Set/Delete, RaiseInterrupt, Store, CurrentToolCallID.
// Turn lifecycle helpers are package-level functions in this package, not methods.
type Runtime struct {
	Store             stores.BaseStore
	CurrentToolCallID string

	session *SessionManager
	out     *runtimeOutput
}

// NewRuntime builds a Runtime over sm. ch must be non-nil (use IdleOutput if idle).
func NewRuntime(ch chan streaming.StreamEvent, store stores.BaseStore, sm *SessionManager) Runtime {
	if sm == nil {
		sm = NewSessionManager()
	}
	if ch == nil {
		panic("session.NewRuntime: nil event channel; use IdleOutput()")
	}
	sm.ensure()
	return Runtime{
		Store:   store,
		session: sm,
		out:     &runtimeOutput{ch: ch},
	}
}

func (rt *Runtime) ensureInitialized() {
	if rt.session == nil {
		rt.session = NewSessionManager()
	}
	rt.session.ensure()
	if rt.out == nil {
		rt.out = &runtimeOutput{ch: idleOutput}
	}
}

// EmitUpdate sends a non-blocking tool progress update for the current call.
func (rt *Runtime) EmitUpdate(message string) {
	event := streaming.StreamEvent{
		Type:      streaming.StreamEventToolUpdate,
		Content:   message,
		MessageID: rt.CurrentToolCallID,
	}
	select {
	case rt.outputChannel() <- event:
	default:
	}
}

func (rt *Runtime) outputChannel() chan streaming.StreamEvent {
	rt.ensureInitialized()
	rt.out.mu.RLock()
	ch := rt.out.ch
	rt.out.mu.RUnlock()
	return ch
}

// StateGet returns a session value stored with StateSet.
func (rt *Runtime) StateGet(key string) (any, bool) {
	rt.ensureInitialized()
	return rt.session.stateGet(key)
}

// StateSet stores a session value for tools and interceptors.
func (rt *Runtime) StateSet(key string, value any) {
	rt.ensureInitialized()
	rt.session.stateSet(key, value)
}

// StateDelete removes a session value.
func (rt *Runtime) StateDelete(key string) {
	rt.ensureInitialized()
	rt.session.stateDelete(key)
}

// RaiseInterrupt parks the current tool until the host resumes with a payload.
func (rt *Runtime) RaiseInterrupt(kind string, payload []byte) (interrupt.Interrupt, error) {
	rt.ensureInitialized()
	return rt.session.raiseInterrupt(rt.CurrentToolCallID, kind, payload)
}

// Module-only harness control (not methods on Runtime / HarnessRuntime).

// EnsureInitialized prepares session state when missing.
func EnsureInitialized(rt *Runtime) {
	if rt == nil {
		return
	}
	rt.ensureInitialized()
}

// SetOutputChannel binds the turn event channel for EmitUpdate and plan updates.
// ch must be non-nil; use IdleOutput() when no turn is live.
func SetOutputChannel(rt *Runtime, ch chan streaming.StreamEvent) {
	if rt == nil {
		return
	}
	if ch == nil {
		panic("session.SetOutputChannel: nil event channel; use IdleOutput()")
	}
	rt.ensureInitialized()
	rt.out.mu.Lock()
	rt.out.ch = ch
	rt.out.mu.Unlock()
}

// EmitPlanUpdate sends a plan_update stream event (plan tools).
func EmitPlanUpdate(rt *Runtime, plan []Todo) {
	if rt == nil {
		return
	}
	data, _ := json.Marshal(plan)
	select {
	case rt.outputChannel() <- streaming.StreamEvent{
		Type: streaming.StreamEventPlanUpdate,
		Data: data,
	}:
	default:
	}
}

// HasPendingInterrupt is true when any interrupt is still open.
func HasPendingInterrupt(rt *Runtime) bool {
	if rt == nil {
		return false
	}
	rt.ensureInitialized()
	return rt.session.HasPendingInterrupt()
}

// ReturnInterrupt resolves a parked interrupt with the host payload.
func ReturnInterrupt(rt *Runtime, id string, result []byte) (interrupt.Interrupt, error) {
	rt.ensureInitialized()
	return rt.session.returnInterrupt(id, result)
}

// AdoptInterrupt attaches a child interrupt to the current tool call.
func AdoptInterrupt(rt *Runtime, intr interrupt.Interrupt) (interrupt.Interrupt, error) {
	rt.ensureInitialized()
	return rt.session.adoptInterrupt(rt.CurrentToolCallID, intr)
}

// TakeResolvedInterrupt removes and returns a resolved interrupt if present.
func TakeResolvedInterrupt(rt *Runtime, id string) (interrupt.Interrupt, bool) {
	rt.ensureInitialized()
	return rt.session.takeResolvedInterrupt(id)
}

// PendingInterrupt returns an open interrupt for tool-call id if any.
func PendingInterrupt(rt *Runtime, id string) (interrupt.Interrupt, bool) {
	rt.ensureInitialized()
	return rt.session.pendingInterrupt(id)
}
