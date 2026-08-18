package session

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
)

// SessionManager owns durable and live data for one agent harness thread
// (checkpoint id), not an ACP client session id: plan, user tool state,
// permission memory, on-call stages, parked workers,
// search context, interrupts, and the optional virtual filesystem mount table.
//
// VFS is host-owned and attached by assigning SessionManager.VFS. Knowledge
// namespace + ResultSet live on Search. Builtins close over the manager; user
// tools use Runtime.
type SessionManager struct {
	mu          sync.RWMutex
	Plan        *PlanStore
	userState   map[string]any
	pending     interruptMap
	resolved    interruptMap
	Permissions Permissions
	parks       parkBag
	OnCall      OnCallStore
	Search      *brain.SearchContext
	VFS         *vfs.MountSession
}

// NewSessionManager returns an empty manager ready for use.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		Plan:        NewPlanStore(),
		userState:   map[string]any{},
		pending:     interruptMap{},
		resolved:    interruptMap{},
		Permissions: NewPermissions(),
		parks:       newParkBag(),
		Search:      brain.NewSearchContext(),
	}
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.userState[key]
	return v, ok
}

func (s *SessionManager) stateSet(key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userState[key] = value
	return nil
}

func (s *SessionManager) stateDelete(key string) {
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

// DropInterrupt removes one interrupt after a durability failure prevents it
// from being safely exposed to a client.
func (s *SessionManager) DropInterrupt(id string) {
	if s == nil || id == "" {
		return
	}
	s.mu.Lock()
	delete(s.pending, id)
	delete(s.resolved, id)
	s.mu.Unlock()
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

// AdoptInterrupt attaches an interrupt to toolCallID. Parks when none is
// resolved yet; returns the resolved interrupt on re-entry.
func (s *SessionManager) AdoptInterrupt(toolCallID string, intr interrupt.Interrupt) (interrupt.Interrupt, error) {
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

// TakeResolvedInterrupt removes and returns a resolved interrupt if present.
func (s *SessionManager) TakeResolvedInterrupt(id string) (interrupt.Interrupt, bool) {
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

// LoadInterruptsJSON restores interrupt maps from checkpoint JSON blobs.
func (s *SessionManager) LoadInterruptsJSON(pendingJSON, resolvedJSON []byte) error {
	pending, resolved, err := decodeInterruptMaps(pendingJSON, resolvedJSON)
	if err != nil {
		return err
	}
	s.replaceInterrupts(pending, resolved)
	return nil
}

func decodeInterruptMaps(pendingJSON, resolvedJSON []byte) (interruptMap, interruptMap, error) {
	pending := interruptMap{}
	resolved := interruptMap{}
	if len(pendingJSON) > 0 {
		if err := json.Unmarshal(pendingJSON, &pending); err != nil {
			return nil, nil, fmt.Errorf("unmarshal pending interrupts: %w", err)
		}
	}
	if len(resolvedJSON) > 0 {
		if err := json.Unmarshal(resolvedJSON, &resolved); err != nil {
			return nil, nil, fmt.Errorf("unmarshal resolved interrupts: %w", err)
		}
	}
	return pending, resolved, nil
}

func (s *SessionManager) replaceInterrupts(pending, resolved interruptMap) {
	s.mu.Lock()
	s.pending = pending
	s.resolved = resolved
	s.mu.Unlock()
}
