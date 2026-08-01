package control

import (
	"encoding/json"
	"sync"

	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// runtimeOutput holds the turn event channel behind a pointer shared across
// HarnessRuntime shallow copies. By-value Runtime copies must not race with
// SetOutputChannel (which would otherwise write a channel field in the struct).
type runtimeOutput struct {
	mu sync.RWMutex
	ch chan streaming.StreamEvent
}

// HarnessRuntime is the public hook surface for user-defined tools:
// StateGet/Set/Delete, interrupts, EmitUpdate, Store, and CurrentToolCallID.
//
// It does not own session data or checkpoint logic. Those live on SessionManager
// (backend) and Checkpointer (wire format). Runtime is a thin facade, shallow-copied
// per tool call with only CurrentToolCallID unique per copy.
//
// Framework modules (plan, …) are not reachable through Runtime.
// Prefer NewRuntime so the SessionManager backend is set.
type HarnessRuntime struct {
	// Optional DI fields for product tools (not used by built-ins).
	VectorDB *milvusclient.Client
	Store    stores.BaseStore
	Mode     string
	// CurrentToolCallID is set per tool invocation on a shallow copy.
	CurrentToolCallID string

	// session is the shared backend. Unexported so external packages cannot
	// reach SessionManager through Runtime.
	session *SessionManager
	out     *runtimeOutput
}

// NewRuntime builds a tool-facing Runtime facade over sm.
// If sm is nil, a new SessionManager is created. ch may be nil until SetOutputChannel.
func NewRuntime(ch chan streaming.StreamEvent, store stores.BaseStore, sm *SessionManager) HarnessRuntime {
	if sm == nil {
		sm = NewSessionManager()
	}
	sm.ensure()
	return HarnessRuntime{
		Store:   store,
		session: sm,
		out:     &runtimeOutput{ch: ch},
	}
}

// EnsureInitialized attaches a SessionManager and output box when missing
// (zero-value Runtime in tests). NewRuntime already initializes both.
func (rt *HarnessRuntime) EnsureInitialized() {
	if rt.session == nil {
		rt.session = NewSessionManager()
	}
	rt.session.ensure()
	if rt.out == nil {
		rt.out = &runtimeOutput{}
	}
}

// Runtime hook to emit custom events as updates from tool calls.
// Non-blocking: drops if no listener or channel full so tools never hang.
func (rt *HarnessRuntime) EmitUpdate(message string) {
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

// EmitPlanUpdate publishes a StreamEventPlanUpdate (todo list reshape) when an
// output channel is attached. Stream utility only — SessionManager holds plan state.
func (rt *HarnessRuntime) EmitPlanUpdate(plan []Todo) {
	ch := rt.outputChannel()
	if ch == nil {
		return
	}
	data, _ := json.Marshal(plan)
	ch <- streaming.StreamEvent{
		Type: streaming.StreamEventPlanUpdate,
		Data: data,
	}
}

// SetOutputChannel updates the channel used by EmitUpdate and EmitPlanUpdate.
// Harness Run attaches the turn channel and clears it on exit.
func (rt *HarnessRuntime) SetOutputChannel(ch chan streaming.StreamEvent) {
	rt.EnsureInitialized()
	rt.out.mu.Lock()
	rt.out.ch = ch
	rt.out.mu.Unlock()
}

func (rt *HarnessRuntime) outputChannel() chan streaming.StreamEvent {
	if rt.out == nil {
		return nil
	}
	rt.out.mu.RLock()
	ch := rt.out.ch
	rt.out.mu.RUnlock()
	return ch
}

// StateGet returns user DI state for key. Reserved plan keys are never returned.
func (rt *HarnessRuntime) StateGet(key string) (any, bool) {
	rt.EnsureInitialized()
	return rt.session.stateGet(key)
}

// StateSet stores user DI state. Reserved plan keys are ignored.
func (rt *HarnessRuntime) StateSet(key string, value any) {
	rt.EnsureInitialized()
	rt.session.stateSet(key, value)
}

// StateDelete removes a user DI key. Reserved plan keys are ignored.
func (rt *HarnessRuntime) StateDelete(key string) {
	rt.EnsureInitialized()
	rt.session.stateDelete(key)
}

func (rt *HarnessRuntime) HasPendingInterrupt() bool {
	rt.EnsureInitialized()
	return rt.session.hasPendingInterrupt()
}

// ReturnInterrupt resolves a pending interrupt with the consumer's response.
func (rt *HarnessRuntime) ReturnInterrupt(id string, result []byte) (Interrupt, error) {
	rt.EnsureInitialized()
	return rt.session.returnInterrupt(id, result)
}

// AdoptInterrupt parks an existing Interrupt under CurrentToolCallID.
func (rt *HarnessRuntime) AdoptInterrupt(intr Interrupt) (Interrupt, error) {
	rt.EnsureInitialized()
	return rt.session.adoptInterrupt(rt.CurrentToolCallID, intr)
}

// TakeResolvedInterrupt removes and returns a resolved interrupt for id, if any.
func (rt *HarnessRuntime) TakeResolvedInterrupt(id string) (Interrupt, bool) {
	rt.EnsureInitialized()
	return rt.session.takeResolvedInterrupt(id)
}

// PendingInterrupt returns the pending interrupt for id, if any.
func (rt *HarnessRuntime) PendingInterrupt(id string) (Interrupt, bool) {
	rt.EnsureInitialized()
	return rt.session.pendingInterrupt(id)
}

// RaiseInterrupt is the hook for tools to raise interrupts and yield control.
func (rt *HarnessRuntime) RaiseInterrupt(kind string, payload []byte) (Interrupt, error) {
	rt.EnsureInitialized()
	return rt.session.raiseInterrupt(rt.CurrentToolCallID, kind, payload)
}
