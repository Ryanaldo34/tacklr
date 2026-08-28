package tacklr

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
)

// sessionManager owns durable and live data for one agent harness thread
// (checkpoint id), not an ACP client session id: plan, user tool state,
// permission memory, on-call stages, search context, interrupts, and the
// optional virtual filesystem mount table.
//
// VFS is host-owned and attached by assigning sessionManager.VFS. Knowledge
// namespace + ResultSet live on Search. Builtins close over the manager; user
// tools use sessionRuntime.
type sessionManager struct {
	mu          sync.RWMutex
	Plan        *planStore
	userState   map[string]any
	pending     interruptMap
	resolved    interruptMap
	Permissions permissions
	OnCall      onCallStore
	Search      *brain.SearchContext
	VFS         *vfs.MountSession
}

// newSessionManager returns an empty manager ready for use.
func newSessionManager() *sessionManager {
	return &sessionManager{
		Plan:        newPlanStore(),
		userState:   map[string]any{},
		pending:     interruptMap{},
		resolved:    interruptMap{},
		Permissions: newPermissions(),
		Search:      brain.NewSearchContext(),
	}
}

// StateGet returns a host/tool state value without a turn Runtime.
func (s *sessionManager) StateGet(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.userState[key]
	return v, ok
}

// StateSet stores a host/tool state value without a turn Runtime.
func (s *sessionManager) StateSet(key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userState[key] = value
	return nil
}

// StateDelete removes a host/tool state value without a turn Runtime.
func (s *sessionManager) StateDelete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.userState, key)
}

// PendingInterrupt returns an open interrupt for id if any.
func (s *sessionManager) PendingInterrupt(id string) (interrupt.Interrupt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	intr, ok := s.pending[id]
	return intr, ok
}

// HasPendingInterrupt reports whether any interrupt is still awaiting a client payload.
func (s *sessionManager) HasPendingInterrupt() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pending) > 0
}

// ClearInterrupts drops pending and resolved interrupt maps (steer / cancel finalize).
func (s *sessionManager) ClearInterrupts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = interruptMap{}
	s.resolved = interruptMap{}
}

// DropInterrupt removes one interrupt after a durability failure prevents it
// from being safely exposed to a client.
func (s *sessionManager) DropInterrupt(id string) {
	s.mu.Lock()
	delete(s.pending, id)
	delete(s.resolved, id)
	s.mu.Unlock()
}

// Park is the only writer of pending. It stores intr under callID and returns
// that interrupt as the error tools propagate (`return "", err`).
func (s *sessionManager) Park(callID string, intr interrupt.Interrupt) error {
	if intr == nil {
		return fmt.Errorf("park: interrupt is nil")
	}
	if callID == "" {
		return fmt.Errorf("park: CurrentToolCallID is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[callID] = intr
	return intr
}

// Resume validates via Interrupt.Return, then moves pending → resolved.
// An invalid payload leaves the park in place.
func (s *sessionManager) Resume(callID string, payload []byte) (interrupt.Interrupt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	intr, ok := s.pending[callID]
	if !ok {
		return nil, fmt.Errorf("interrupt %q: %w", callID, interrupt.ErrInterruptNotFound)
	}
	if err := intr.Return(payload); err != nil {
		return nil, err
	}
	delete(s.pending, callID)
	s.resolved[callID] = intr
	return intr, nil
}

// TakeResolved removes and returns a resolved interrupt if present (re-entry after Resume).
func (s *sessionManager) TakeResolved(id string) (interrupt.Interrupt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	intr, ok := s.resolved[id]
	if !ok {
		return nil, false
	}
	delete(s.resolved, id)
	return intr, true
}

// Pending returns a clone of open interrupts (checkpoint / ACP translators).
func (s *sessionManager) Pending() map[string]interrupt.Interrupt {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneInterruptMap(s.pending)
}

// LoadInterruptsJSON restores interrupt maps from checkpoint JSON blobs.
func (s *sessionManager) LoadInterruptsJSON(pendingJSON, resolvedJSON []byte) error {
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

func (s *sessionManager) replaceInterrupts(pending, resolved interruptMap) {
	s.mu.Lock()
	s.pending = pending
	s.resolved = resolved
	s.mu.Unlock()
}
