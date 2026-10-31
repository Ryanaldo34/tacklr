package tacklr

import (
	"fmt"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// sessionManager is one session's state: plan, user tool state, permission
// memory, search context, the open round (with each call's park and OnCall
// layers), the last Tools step's report, and the workspace mount table.
// Builtins close over it; user tools reach it through sessionRuntime.
type sessionManager struct {
	mu          sync.RWMutex
	Plan        *planStore
	userState   map[string]any
	Permissions permissions
	Search      *brain.SearchContext
	VFS         *vfs.MountSession
	// round is the reply whose calls do not all have results yet; nil when
	// none is open. The window gets it whole when it commits.
	round *openRound
	// report is the last Tools step's report, for a retry of that step.
	report stepReport
}

// newSessionManager returns an empty manager ready for use.
func newSessionManager() *sessionManager {
	return &sessionManager{
		Plan:        newPlanStore(),
		userState:   map[string]any{},
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

// Park makes intr the park of the open round's call callID and returns it
// as the error tools propagate (`return "", err`).
func (s *sessionManager) Park(callID string, intr Interrupt) error {
	if intr == nil {
		return fmt.Errorf("park: interrupt is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.round.call(callID)
	if c == nil {
		return fmt.Errorf("park: tool call %q is not in the open round", callID)
	}
	c.Park = &callPark{Interrupt: intr}
	return intr
}

// Resume answers callID's park through Interrupt.Return. An invalid payload
// leaves the park unanswered.
func (s *sessionManager) Resume(callID string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.round.call(callID)
	if c == nil || c.Park == nil || c.Park.Answered {
		return fmt.Errorf("interrupt %q: %w", callID, ErrInterruptNotFound)
	}
	if err := c.Park.Interrupt.Return(payload); err != nil {
		return err
	}
	c.Park.Answered = true
	return nil
}

// TakeAnswer removes and returns callID's answered park of typeName, for
// the call's run after resume.
func (s *sessionManager) TakeAnswer(callID, typeName string) (Interrupt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.round.call(callID)
	if c == nil || c.Park == nil || !c.Park.Answered || c.Park.Interrupt.TypeName() != typeName {
		return nil, false
	}
	intr := c.Park.Interrupt
	c.Park = nil
	return intr, true
}

// Layer is callID's completed OnCall layer of typeName.
func (s *sessionManager) Layer(callID, typeName string) (onCallLayer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.round.call(callID)
	if c == nil {
		return onCallLayer{}, false
	}
	return c.layer(typeName)
}

// RecordLayer stores a completed OnCall layer on callID, so its next run
// skips it.
func (s *sessionManager) RecordLayer(callID string, layer onCallLayer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.round.call(callID); c != nil {
		c.Layers = append(c.Layers, layer)
	}
}
