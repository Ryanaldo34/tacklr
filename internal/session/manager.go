package session

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
)

// SessionManager owns durable and live data for one agent harness thread
// (checkpoint id), not an ACP client session id: plan, user tool state,
// interrupts, and the optional virtual filesystem mount table.
// Knowledge namespace + ResultSet live on brain.SearchContext.
// Builtins close over it; user tools use Runtime.
type SessionManager struct {
	mu        sync.RWMutex
	plan      *PlanStore
	userState map[string]any
	pending   interruptMap
	resolved  interruptMap
	// VFS is the session-owned mount table. Hosts attach/detach mounts on this
	// object (or the same pointer via AgentOptions.MountSession). Nil when unused.
	// Not a harness concern — the agent only checkpoints Specs() at save time.
	VFS *vfs.MountSession
}

// NewSessionManager returns an empty manager ready for use.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		plan:      NewPlanStore(),
		userState: map[string]any{},
		pending:   interruptMap{},
		resolved:  interruptMap{},
	}
}

func (s *SessionManager) ensure() {
	if s.plan == nil {
		s.plan = NewPlanStore()
	}
	if s.userState == nil {
		s.userState = map[string]any{}
	}
	if s.pending == nil {
		s.pending = interruptMap{}
	}
	if s.resolved == nil {
		s.resolved = interruptMap{}
	}
}

// Plan returns the plan module. Never nil after NewSessionManager.
func (s *SessionManager) Plan() *PlanStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.plan == nil {
		s.plan = NewPlanStore()
	}
	return s.plan
}

// HasActivePlan reports whether a non-empty todo list is present.
func (s *SessionManager) HasActivePlan() bool {
	return s.Plan().HasActive()
}

// --- user State bag (facade target for Runtime.StateGet/Set) ---

// StateGet returns a host/tool state value without a turn Runtime.
func (s *SessionManager) StateGet(key string) (any, bool) {
	return s.stateGet(key)
}

// StateSet stores a host/tool state value without a turn Runtime.
func (s *SessionManager) StateSet(key string, value any) {
	s.stateSet(key, value)
}

// StateDelete removes a host/tool state value without a turn Runtime.
func (s *SessionManager) StateDelete(key string) {
	s.stateDelete(key)
}

// PendingInterrupt returns an open interrupt for id if any.
func (s *SessionManager) PendingInterrupt(id string) (interrupt.Interrupt, bool) {
	return s.pendingInterrupt(id)
}

func (s *SessionManager) stateGet(key string) (any, bool) {
	if IsReservedRuntimeStateKey(key) {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.userState[key]
	return v, ok
}

func (s *SessionManager) stateSet(key string, value any) {
	if IsReservedRuntimeStateKey(key) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.userState == nil {
		s.userState = map[string]any{}
	}
	s.userState[key] = value
}

func (s *SessionManager) stateDelete(key string) {
	if IsReservedRuntimeStateKey(key) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.userState, key)
}

// --- interrupts (facade target for Runtime Raise/Return/…) ---

// HasPendingInterrupt reports whether any interrupt is still awaiting a client payload.
func (s *SessionManager) HasPendingInterrupt() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pending) > 0
}

// ClearInterrupts drops pending and resolved interrupt maps (steer / cancel finalize).
func (s *SessionManager) ClearInterrupts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = interruptMap{}
	s.resolved = interruptMap{}
}

// ReturnInterrupt resolves a parked interrupt (session-scoped; no turn bus needed).
func (s *SessionManager) ReturnInterrupt(id string, result []byte) (interrupt.Interrupt, error) {
	return s.returnInterrupt(id, result)
}

func (s *SessionManager) returnInterrupt(id string, result []byte) (interrupt.Interrupt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensure()
	intr, ok := s.pending[id]
	if !ok {
		return nil, fmt.Errorf("interrupt %q: %w", id, interrupt.ErrInterruptNotFound)
	}
	if validator, ok := intr.(interrupt.PayloadValidator); ok {
		if err := validator.ValidatePayload(result); err != nil {
			return nil, fmt.Errorf("%w: %w", interrupt.ErrInvalidPayload, err)
		}
	}
	if err := intr.Return(result); err != nil {
		return nil, fmt.Errorf("return interrupt: %w", err)
	}
	delete(s.pending, id)
	s.resolved[id] = intr
	return intr, nil
}

func (s *SessionManager) adoptInterrupt(toolCallID string, intr interrupt.Interrupt) (interrupt.Interrupt, error) {
	if intr == nil {
		return nil, fmt.Errorf("adopt interrupt: interrupt is nil")
	}
	if toolCallID == "" {
		return nil, fmt.Errorf("adopt interrupt: CurrentToolCallID is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensure()
	if resolved, ok := s.resolved[toolCallID]; ok {
		delete(s.resolved, toolCallID)
		return resolved, nil
	}
	s.pending[toolCallID] = intr
	return nil, intr
}

func (s *SessionManager) takeResolvedInterrupt(id string) (interrupt.Interrupt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	intr, ok := s.resolved[id]
	if !ok {
		return nil, false
	}
	delete(s.resolved, id)
	return intr, true
}

func (s *SessionManager) pendingInterrupt(id string) (interrupt.Interrupt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	intr, ok := s.pending[id]
	return intr, ok
}

func (s *SessionManager) raiseInterrupt(toolCallID string, kind string, payload []byte) (interrupt.Interrupt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensure()
	if resolved, ok := s.resolved[toolCallID]; ok {
		delete(s.resolved, toolCallID)
		return resolved, nil
	}
	intr, ok := interrupt.New(kind)
	if !ok {
		return nil, fmt.Errorf("%q is not a valid interrupt type", kind)
	}
	if init, ok := intr.(interrupt.PayloadInitializer); ok {
		if err := init.InitFromPayload(payload); err != nil {
			return nil, fmt.Errorf("init interrupt payload: %w", err)
		}
	}
	s.pending[toolCallID] = intr
	return nil, intr
}

// SnapshotDurable copies user state, plan modules, and interrupt maps for
// checkpointing. Interrupts are deep-cloned so marshal does not race live maps.
// Reserved plan keys in userState are never exported as user keys; plan is
// written via PlanStore.ExportInto.
func (s *SessionManager) SnapshotDurable() (runtimeState map[string]any, pending, resolved interruptMap) {
	s.mu.RLock()
	runtimeState = make(map[string]any, len(s.userState))
	for k, v := range s.userState {
		if IsReservedRuntimeStateKey(k) {
			continue
		}
		runtimeState[k] = v
	}
	pending = make(interruptMap, len(s.pending))
	for k, v := range s.pending {
		if cp := interrupt.Clone(v); cp != nil {
			pending[k] = cp
		}
	}
	resolved = make(interruptMap, len(s.resolved))
	for k, v := range s.resolved {
		if cp := interrupt.Clone(v); cp != nil {
			resolved[k] = cp
		}
	}
	plan := s.plan
	s.mu.RUnlock()

	if plan != nil {
		plan.ExportInto(runtimeState)
	}
	return runtimeState, pending, resolved
}

// LoadUserAndPlanState hydrates user State and plan from checkpoint RuntimeState.
// Reserved keys (including legacy _search_namespace) are not left as user keys.
func (s *SessionManager) LoadUserAndPlanState(state map[string]any) {
	s.ensure()
	s.plan.LoadFromState(state)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.userState == nil {
		s.userState = map[string]any{}
	}
	for k, v := range state {
		if IsReservedRuntimeStateKey(k) {
			continue
		}
		s.userState[k] = v
	}
}

// LoadInterruptsJSON restores interrupt maps from checkpoint JSON blobs.
func (s *SessionManager) LoadInterruptsJSON(pendingJSON, resolvedJSON []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensure()
	if len(pendingJSON) > 0 {
		if err := json.Unmarshal(pendingJSON, &s.pending); err != nil {
			return fmt.Errorf("unmarshal pending interrupts: %w", err)
		}
	}
	if len(resolvedJSON) > 0 {
		if err := json.Unmarshal(resolvedJSON, &s.resolved); err != nil {
			return fmt.Errorf("unmarshal resolved interrupts: %w", err)
		}
	}
	return nil
}
