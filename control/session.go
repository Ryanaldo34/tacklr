package control

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

type ActivityStatus int

const (
	ActivityPending ActivityStatus = iota
	ActivityInProgress
	ActivityCompleted
	ActivityFailed
)

// HarnessRuntime is the runtime hook into global state and services used by the
// harness and its tools. It is shallow-copied per tool invocation with
// CurrentToolCallID set uniquely per copy. All pointer and map fields are
// shared across copies; only CurrentToolCallID is per-call.
//
// Concurrent access to State, PendingInterrupts, and ResolvedInterrupts is
// guarded by mu. Tool handlers that mutate State must use the StateGet,
// StateSet, and StateDelete helpers rather than direct map access.
//
// Callers must initialize the runtime via EnsureInitialized before use.
type HarnessRuntime struct {
	VectorDB           *milvusclient.Client
	Store              stores.BaseStore
	State              map[string]any
	PendingInterrupts  interruptMap
	ResolvedInterrupts interruptMap
	CurrentToolCallID  string
	Mode               string
	mu                 *sync.RWMutex
	ch                 chan streaming.StreamEvent
}

// Runtime hook to emit custom events as updates from tool calls
func (rt *HarnessRuntime) EmitUpdate(message string) {
	event := streaming.StreamEvent{
		Type:      streaming.StreamEventToolUpdate,
		Content:   message,
		MessageID: rt.CurrentToolCallID,
	}
	rt.ch <- event
}

// SetOutputChannel updates the channel used by EmitUpdate to send tool progress
// events. Each Run call creates a fresh output channel so that previous closes
// don't affect active runs.
func (rt *HarnessRuntime) SetOutputChannel(ch chan streaming.StreamEvent) {
	rt.ch = ch
}

// EnsureInitialized initializes the mutex and all maps on the runtime.
// The harness calls this once when creating or loading a harness so that
// subsequent shallow copies share already-allocated map references.
func (rt *HarnessRuntime) EnsureInitialized() {
	if rt.mu == nil {
		rt.mu = &sync.RWMutex{}
	}
	if rt.PendingInterrupts == nil {
		rt.PendingInterrupts = interruptMap{}
	}
	if rt.ResolvedInterrupts == nil {
		rt.ResolvedInterrupts = interruptMap{}
	}
	if rt.State == nil {
		rt.State = map[string]any{}
	}
}

// StateGet returns a value for the given key from the state map.
// Safe for concurrent access across tool goroutines.
func (rt *HarnessRuntime) StateGet(key string) (any, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	v, ok := rt.State[key]
	return v, ok
}

// StateSet stores a key-value pair in the state map.
// Safe for concurrent access across tool goroutines.
func (rt *HarnessRuntime) StateSet(key string, value any) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.State[key] = value
}

// StateDelete removes a key from the state map.
// Safe for concurrent access across tool goroutines.
func (rt *HarnessRuntime) StateDelete(key string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	delete(rt.State, key)
}

func (rt *HarnessRuntime) HasPendingInterrupt() bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return len(rt.PendingInterrupts) > 0
}

// ReturnInterrupt resolves a pending interrupt with the consumer's response.
// The resolved interrupt is stored in ResolvedInterrupts (keyed by the same
// id) so that the next RaiseInterrupt call from the re-executed tool returns
// the resolved interrupt with a nil error instead of creating a new one.
func (rt *HarnessRuntime) ReturnInterrupt(id string, result []byte) (Interrupt, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	intr, ok := rt.PendingInterrupts[id]
	if !ok {
		return nil, fmt.Errorf("interrupt %q: %w", id, ErrInterruptNotFound)
	}
	if validator, ok := intr.(PayloadValidator); ok {
		if err := validator.ValidatePayload(result); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidPayload, err)
		}
	}
	if err := intr.Return(result); err != nil {
		return nil, fmt.Errorf("return interrupt: %w", err)
	}
	delete(rt.PendingInterrupts, id)
	rt.ResolvedInterrupts[id] = intr
	return intr, nil
}

// RaiseInterrupt is the hook for tools to raise interrupts and yield control
// back to the consumer for additional input or confirmation.
//
// On first call for a given tool call (identified by rt.CurrentToolCallID,
// which the harness sets before each tool invocation), it creates and stores
// the interrupt in PendingInterrupts, then returns (nil, interrupt) — the
// interrupt itself implements error, so returning it causes the harness to
// detect it via errors.As and yield the StreamEventInterrupt to the consumer.
//
// On re-execution after the consumer resolves the interrupt via
// ReturnFromInterrupt, it finds the resolved interrupt in ResolvedInterrupts,
// deletes it, and returns (resolved, nil) so the tool can read the consumer's
// response and continue execution.
func (rt *HarnessRuntime) RaiseInterrupt(kind string, payload []byte) (Interrupt, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Check for a resolved interrupt from a previous RaiseInterrupt
	if resolved, ok := rt.ResolvedInterrupts[rt.CurrentToolCallID]; ok {
		delete(rt.ResolvedInterrupts, rt.CurrentToolCallID)
		return resolved, nil
	}

	// First raise: create, store, return as error
	factory, ok := interruptFactories[kind]
	if !ok {
		return nil, fmt.Errorf("%q is not a valid interrupt type", kind)
	}
	intr := factory()
	if init, ok := intr.(payloadInitializer); ok {
		if err := init.InitFromPayload(payload); err != nil {
			return nil, fmt.Errorf("init interrupt payload: %w", err)
		}
	}
	rt.PendingInterrupts[rt.CurrentToolCallID] = intr
	return nil, intr
}

func NewRuntime(ch chan streaming.StreamEvent, store stores.BaseStore, state map[string]any) HarnessRuntime {
	if state == nil {
		state = make(map[string]any)
	}
	rt := HarnessRuntime{ch: ch, Store: store, State: state}
	return rt
}

// --- Workflow types ---

type Activity struct {
	Status              ActivityStatus `json:"status"`
	AcceptanceCriteria  string         `json:"acceptanceCriteria"`
	Order               int            `json:"order"`
	PredecessorActivity *Activity
	SuccessorActivity   *Activity
}

type TurnGoal struct {
	Description        string `json:"description"`
	AcceptanceCriteria string `json:"acceptanceCriteria"`
}

type ExecutionSession struct {
	CurrentGoal TurnGoal
	Runtime     *HarnessRuntime
}
