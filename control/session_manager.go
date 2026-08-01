package control

// SessionManager owns framework-mutable session state for one agent harness
// instance (checkpoint / thread), not an ACP client session id.
//
// It is the internal control plane for built-in tools and harness interceptors.
// User-defined tools must not receive a SessionManager; they use HarnessRuntime
// only (DI, interrupts, EmitUpdate).
//
// Modules (Plan today; memory, parks, policy later) live here so new internal
// state does not grow the public Runtime surface.
//
// SessionManager does not own client streaming. Todo-list reshape notifications
// (StreamEventPlanUpdate) are emitted separately via HarnessRuntime.EmitPlanUpdate
// when a builtin changes the plan — that is a stream utility, not session state.
type SessionManager struct {
	plan *PlanStore
}

// NewSessionManager returns a manager with an empty plan module.
func NewSessionManager() *SessionManager {
	return &SessionManager{plan: NewPlanStore()}
}

// Plan returns the plan module. Never nil after NewSessionManager.
func (s *SessionManager) Plan() *PlanStore {
	if s == nil {
		return nil
	}
	if s.plan == nil {
		s.plan = NewPlanStore()
	}
	return s.plan
}

// HasActivePlan reports whether a non-empty todo list is present
// (write-lock unlock condition).
func (s *SessionManager) HasActivePlan() bool {
	if s == nil || s.plan == nil {
		return false
	}
	return s.plan.HasActive()
}

// ExportInto writes all session modules into a runtime-state map for checkpoints.
func (s *SessionManager) ExportInto(state map[string]any) {
	if state == nil {
		return
	}
	if s == nil || s.plan == nil {
		StripPlanKeys(state)
		return
	}
	s.plan.ExportInto(state)
}

// LoadFromState hydrates modules from checkpoint RuntimeState.
func (s *SessionManager) LoadFromState(state map[string]any) {
	if s == nil {
		return
	}
	if s.plan == nil {
		s.plan = NewPlanStore()
	}
	s.plan.LoadFromState(state)
}
