package session

import (
	"encoding/json"
	"sync"

	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// runtimeOutput holds the turn event channel behind a pointer shared across
// Runtime shallow copies.
type runtimeOutput struct {
	mu sync.RWMutex
	ch chan streaming.StreamEvent
}

// Runtime is the tool-facing hook surface: StateGet/Set, interrupts, EmitUpdate,
// Store, CurrentToolCallID. Session data lives on SessionManager (unexported field).
// Re-exported publicly as tacklr.HarnessRuntime.
type Runtime struct {
	VectorDB          *milvusclient.Client
	Store             stores.BaseStore
	Mode              string
	CurrentToolCallID string

	session *SessionManager
	out     *runtimeOutput
}

// NewRuntime builds a tool-facing Runtime facade over sm.
func NewRuntime(ch chan streaming.StreamEvent, store stores.BaseStore, sm *SessionManager) Runtime {
	if sm == nil {
		sm = NewSessionManager()
	}
	sm.ensure()
	return Runtime{
		Store:   store,
		session: sm,
		out:     &runtimeOutput{ch: ch},
	}
}

// EnsureInitialized attaches a SessionManager and output box when missing.
func (rt *Runtime) EnsureInitialized() {
	if rt.session == nil {
		rt.session = NewSessionManager()
	}
	rt.session.ensure()
	if rt.out == nil {
		rt.out = &runtimeOutput{}
	}
}

// Session returns the backend manager (harness/builtins only; same module).
func (rt *Runtime) Session() *SessionManager {
	rt.EnsureInitialized()
	return rt.session
}

func (rt *Runtime) EmitUpdate(message string) {
	ch := rt.outputChannel()
	if ch == nil {
		return
	}
	event := streaming.StreamEvent{
		Type:      streaming.StreamEventToolUpdate,
		Content:   message,
		MessageID: rt.CurrentToolCallID,
	}
	select {
	case ch <- event:
	default:
	}
}

func (rt *Runtime) EmitPlanUpdate(plan []Todo) {
	ch := rt.outputChannel()
	if ch == nil {
		return
	}
	data, _ := json.Marshal(plan)
	// Non-blocking: never hang tests or tool goroutines if the turn consumer
	// has stopped draining (cancel/teardown). Mid-turn the harness buffer is
	// sized so plan updates are not dropped under normal load.
	select {
	case ch <- streaming.StreamEvent{
		Type: streaming.StreamEventPlanUpdate,
		Data: data,
	}:
	default:
	}
}

func (rt *Runtime) SetOutputChannel(ch chan streaming.StreamEvent) {
	rt.EnsureInitialized()
	rt.out.mu.Lock()
	rt.out.ch = ch
	rt.out.mu.Unlock()
}

func (rt *Runtime) outputChannel() chan streaming.StreamEvent {
	if rt.out == nil {
		return nil
	}
	rt.out.mu.RLock()
	ch := rt.out.ch
	rt.out.mu.RUnlock()
	return ch
}

func (rt *Runtime) StateGet(key string) (any, bool) {
	rt.EnsureInitialized()
	return rt.session.stateGet(key)
}

func (rt *Runtime) StateSet(key string, value any) {
	rt.EnsureInitialized()
	rt.session.stateSet(key, value)
}

func (rt *Runtime) StateDelete(key string) {
	rt.EnsureInitialized()
	rt.session.stateDelete(key)
}

func (rt *Runtime) HasPendingInterrupt() bool {
	rt.EnsureInitialized()
	return rt.session.hasPendingInterrupt()
}

func (rt *Runtime) ReturnInterrupt(id string, result []byte) (interrupt.Interrupt, error) {
	rt.EnsureInitialized()
	return rt.session.returnInterrupt(id, result)
}

func (rt *Runtime) AdoptInterrupt(intr interrupt.Interrupt) (interrupt.Interrupt, error) {
	rt.EnsureInitialized()
	return rt.session.adoptInterrupt(rt.CurrentToolCallID, intr)
}

func (rt *Runtime) TakeResolvedInterrupt(id string) (interrupt.Interrupt, bool) {
	rt.EnsureInitialized()
	return rt.session.takeResolvedInterrupt(id)
}

func (rt *Runtime) PendingInterrupt(id string) (interrupt.Interrupt, bool) {
	rt.EnsureInitialized()
	return rt.session.pendingInterrupt(id)
}

func (rt *Runtime) RaiseInterrupt(kind string, payload []byte) (interrupt.Interrupt, error) {
	rt.EnsureInitialized()
	return rt.session.raiseInterrupt(rt.CurrentToolCallID, kind, payload)
}
