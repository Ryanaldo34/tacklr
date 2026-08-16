package session

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
)

// SessionManager owns durable and live data for one agent harness thread
// (checkpoint id), not an ACP client session id: plan, user tool state,
// permission memory, parked workers, search context, interrupts, and the
// optional virtual filesystem mount table.
//
// VFS is host-owned and attached with SetVFS. Knowledge namespace + ResultSet
// live on Search(). Builtins close over the manager; user tools use Runtime.
type SessionManager struct {
	mu        sync.RWMutex
	plan      *PlanStore
	userState map[string]any
	pending   interruptMap
	resolved  interruptMap
	perms     *permissionBag
	parks     *parkBag
	search    *brain.SearchContext
	vfs       *vfs.MountSession
}

// NewSessionManager returns an empty manager ready for use.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		plan:      NewPlanStore(),
		userState: map[string]any{},
		pending:   interruptMap{},
		resolved:  interruptMap{},
		perms:     newPermissionBag(),
		parks:     newParkBag(),
		search:    brain.NewSearchContext(),
	}
}

// Plan returns the plan module. Never nil after NewSessionManager.
func (s *SessionManager) Plan() *PlanStore {
	return s.plan
}

// HasActivePlan reports whether a non-empty todo list is present.
func (s *SessionManager) HasActivePlan() bool {
	return s.Plan().HasActive()
}

// Search returns the knowledge retrieval session (namespace + ResultSet).
func (s *SessionManager) Search() *brain.SearchContext {
	return s.search
}

// SetSearch replaces the search context. Nil resets to an empty context.
func (s *SessionManager) SetSearch(sc *brain.SearchContext) {
	if sc == nil {
		sc = brain.NewSearchContext()
	}
	s.search = sc
}

// VFS returns the host-owned mount table, or nil.
func (s *SessionManager) VFS() *vfs.MountSession {
	if s == nil {
		return nil
	}
	return s.vfs
}

// SetVFS attaches a host-owned mount table. The harness does not create,
// persist, or close it.
func (s *SessionManager) SetVFS(ms *vfs.MountSession) {
	if s == nil {
		return
	}
	s.vfs = ms
}

// StateGet returns a host/tool state value without a turn Runtime.
func (s *SessionManager) StateGet(key string) (any, bool) {
	return s.stateGet(key)
}

// StateSet stores a host/tool state value without a turn Runtime.
// Reserved module keys return an error.
func (s *SessionManager) StateSet(key string, value any) error {
	return s.stateSet(key, value)
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

func (s *SessionManager) stateSet(key string, value any) error {
	if IsReservedRuntimeStateKey(key) {
		return fmt.Errorf("session: reserved state key %q cannot be set", key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userState[key] = value
	return nil
}

func (s *SessionManager) stateDelete(key string) {
	if IsReservedRuntimeStateKey(key) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.userState, key)
}

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

// SnapshotDurable copies user state, session modules, and interrupt maps for
// checkpointing. Interrupts are deep-cloned so marshal does not race live maps.
// Reserved keys in userState are never exported as user keys; modules write
// their own reserved keys via ExportInto.
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
	perms := s.perms
	parks := s.parks
	s.mu.RUnlock()

	plan.ExportInto(runtimeState)
	perms.exportInto(runtimeState)
	parks.exportInto(runtimeState)
	return runtimeState, pending, resolved
}

// LoadUserAndPlanState hydrates user State and session modules from checkpoint
// RuntimeState. Reserved keys (including legacy _search_namespace) are not
// left as user keys. A string _search_namespace restores Search().
func (s *SessionManager) LoadUserAndPlanState(state map[string]any) {
	s.plan.LoadFromState(state)
	s.perms.loadFromState(state)
	s.parks.loadFromState(state)
	if raw, ok := state[searchNamespaceStateKey]; ok {
		if ns, ok := raw.(string); ok && ns != "" {
			if id, err := uuid.Parse(ns); err == nil {
				s.search.SetNamespace(id)
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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
