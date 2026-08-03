package session

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/interrupt"
)

// SessionManager owns durable and live data for one agent harness thread
// (checkpoint id), not an ACP client session id: plan, search namespace, user
// tool state, and interrupts. Builtins close over it; user tools use Runtime.
type SessionManager struct {
	mu              sync.RWMutex
	plan            *PlanStore
	searchNamespace *uuid.UUID // nil = no filter; host-set, checkpointed
	userState       map[string]any
	pending         interruptMap
	resolved        interruptMap
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
	if s == nil {
		return
	}
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
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.plan == nil {
		s.plan = NewPlanStore()
	}
	return s.plan
}

// HasActivePlan reports whether a non-empty todo list is present.
func (s *SessionManager) HasActivePlan() bool {
	if s == nil {
		return false
	}
	return s.Plan().HasActive()
}

// SearchNamespace returns the host-set search namespace, if any.
func (s *SessionManager) SearchNamespace() (id uuid.UUID, ok bool) {
	if s == nil {
		return uuid.UUID{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.searchNamespace == nil {
		return uuid.UUID{}, false
	}
	return *s.searchNamespace, true
}

// SetSearchNamespace sets host retrieval isolation for this session.
func (s *SessionManager) SetSearchNamespace(id uuid.UUID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := id
	s.searchNamespace = &cp
}

// ClearSearchNamespace clears retrieval isolation for this session.
func (s *SessionManager) ClearSearchNamespace() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.searchNamespace = nil
}

// --- user State bag (facade target for Runtime.StateGet/Set) ---

func (s *SessionManager) stateGet(key string) (any, bool) {
	if s == nil || IsReservedRuntimeStateKey(key) {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.userState[key]
	return v, ok
}

func (s *SessionManager) stateSet(key string, value any) {
	if s == nil || IsReservedRuntimeStateKey(key) {
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
	if s == nil || IsReservedRuntimeStateKey(key) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.userState, key)
}

// --- interrupts (facade target for Runtime Raise/Return/…) ---

func (s *SessionManager) hasPendingInterrupt() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pending) > 0
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
	if s == nil {
		return map[string]any{}, interruptMap{}, interruptMap{}
	}
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
	var ns *uuid.UUID
	if s.searchNamespace != nil {
		cp := *s.searchNamespace
		ns = &cp
	}
	s.mu.RUnlock()

	if plan != nil {
		plan.ExportInto(runtimeState)
	}
	if ns != nil {
		runtimeState[searchNamespaceStateKey] = ns.String()
	}
	return runtimeState, pending, resolved
}

// LoadUserAndPlanState hydrates user State, plan, and search namespace from
// checkpoint RuntimeState. Reserved keys are consumed into modules and not left
// as user keys.
func (s *SessionManager) LoadUserAndPlanState(state map[string]any) {
	if s == nil {
		return
	}
	s.ensure()
	s.plan.LoadFromState(state)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.searchNamespace = nil
	if raw, ok := state[searchNamespaceStateKey]; ok {
		switch v := raw.(type) {
		case string:
			if id, err := uuid.Parse(v); err == nil {
				cp := id
				s.searchNamespace = &cp
			}
		case uuid.UUID:
			cp := v
			s.searchNamespace = &cp
		case [16]byte:
			cp := uuid.UUID(v)
			s.searchNamespace = &cp
		}
	}
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
	if s == nil {
		return nil
	}
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
