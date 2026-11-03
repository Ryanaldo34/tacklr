package tacklr

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/brain"
)

// checkpointVersion is the schema of checkpointState. A checkpoint of
// another version fails to restore.
const checkpointVersion = 5

// checkpointState is what a checkpoint encodes. Host configuration (the
// search namespace and principals, AllowTools) is not in it: each step
// takes that from AgentOptions.
type checkpointState struct {
	Version     int                           `json:"version"`
	Window      []*message.Message            `json:"window"`
	Todos       []Todo                        `json:"todos,omitempty"`
	Plan        string                        `json:"plan,omitempty"`
	UserState   map[string]any                `json:"userState,omitempty"`
	Permissions map[string]permissionDecision `json:"permissions,omitempty"`
	Rests       map[string]brain.Rest         `json:"rests,omitempty"`
}

// Checkpoint encodes the session's committed state: the window of complete
// rounds, the plan, user state, permission decisions and search rests. A
// round in progress is not in it. A runtime stores the bytes whole
// (session.Store) and only the harness reads them.
func (a *Harness) Checkpoint() ([]byte, error) {
	st := checkpointState{
		Version:     checkpointVersion,
		Window:      a.context.Messages(),
		Permissions: a.session.Permissions.snapshot(),
		Rests:       a.session.Search.Rests(),
	}
	st.Todos, st.Plan = a.session.Plan.snapshot()
	sm := a.session
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	st.UserState = sm.userState
	data, err := json.Marshal(st)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	return data, nil
}

// RestoreCheckpoint replaces the session's durable state with cp's.
func (a *Harness) RestoreCheckpoint(cp []byte) error {
	var st checkpointState
	if err := json.Unmarshal(cp, &st); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	if st.Version != checkpointVersion {
		return fmt.Errorf("checkpoint: unsupported version %d", st.Version)
	}
	if err := ValidateMessages(st.Window); err != nil {
		return fmt.Errorf("checkpoint: invalid context window: %w", err)
	}
	a.context.Restore(st.Window)
	a.usage = requestUsage{}
	a.session.Plan.restore(st.Todos, st.Plan)
	a.session.Permissions.restore(st.Permissions)
	a.session.Search.SetRests(st.Rests)
	sm := a.session
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.userState = maps.Clone(st.UserState)
	if sm.userState == nil {
		sm.userState = map[string]any{}
	}
	return nil
}
