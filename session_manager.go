package tacklr

import (
	"encoding/json"
	"maps"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
)

// sessionManager is one session's committed state: plan, user state,
// permission memory, search context, and the workspace mount table. A tool
// call reads it through its CallContext and writes only there; Commit
// applies the writes here.
type sessionManager struct {
	mu          sync.RWMutex
	Plan        *planStore
	userState   map[string]any
	Permissions permissions
	Search      *brain.SearchContext
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

// state is the committed user-state value of key.
func (s *sessionManager) state(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.userState[key]
	return v, ok
}

// setState upserts host values (CreateSession, Prompt, or Resume State).
func (s *sessionManager) setState(state map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.Copy(s.userState, state)
}

// apply applies one call's writes to the committed state.
func (s *sessionManager) apply(w callWrites) {
	s.mu.Lock()
	for _, sw := range w.State {
		if sw.Deleted {
			delete(s.userState, sw.Key)
			continue
		}
		var v any
		_ = json.Unmarshal(sw.Value, &v)
		s.userState[sw.Key] = v
	}
	s.mu.Unlock()
	for _, pw := range w.Permissions {
		s.Permissions.set(pw.Tool, pw.Decision)
	}
	for _, rw := range w.Rests {
		s.Search.SetRest(rw.Key, rw.Rest)
	}
	for _, op := range w.Plan {
		s.replayPlanOp(op)
	}
}
