package tacklr

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr/brain"
)

// SessionCheckpoint is a session's committed state, encoded: the window of
// complete rounds, the plan, user state, permission decisions, and search
// rests. A round in progress is not in it: the runtime records each step's
// result and holds the open round. A runtime stores it whole
// (session.SnapshotStore) and only the harness reads it. It is an encoding,
// so it shares nothing with the live session it came from.
type SessionCheckpoint struct {
	data json.RawMessage
}

func (c SessionCheckpoint) MarshalJSON() ([]byte, error) {
	if len(c.data) == 0 {
		return []byte("null"), nil
	}
	return c.data, nil
}

func (c *SessionCheckpoint) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		c.data = nil
		return nil
	}
	c.data = slices.Clone(b)
	return nil
}

// checkpointVersion is the schema of checkpointState. A checkpoint of
// another version fails to restore.
const checkpointVersion = 5

// checkpointState is what a checkpoint encodes. Host configuration (the
// search namespace and principals, AllowTools) is not in it: each step
// takes that from AgentOptions.
type checkpointState struct {
	Version     int                           `json:"version"`
	Window      []*Message                    `json:"window"`
	Todos       []Todo                        `json:"todos,omitempty"`
	Plan        string                        `json:"plan,omitempty"`
	UserState   map[string]any                `json:"userState,omitempty"`
	Permissions map[string]permissionDecision `json:"permissions,omitempty"`
	Rests       map[string]brain.Rest         `json:"rests,omitempty"`
}

// Checkpoint encodes the session's durable state.
func (a *TurnManager) Checkpoint() (SessionCheckpoint, error) {
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
		return SessionCheckpoint{}, fmt.Errorf("checkpoint: %w", err)
	}
	return SessionCheckpoint{data: data}, nil
}

// RestoreCheckpoint replaces the session's durable state with cp's.
func (a *TurnManager) RestoreCheckpoint(cp SessionCheckpoint) error {
	var st checkpointState
	if err := json.Unmarshal(cp.data, &st); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	if st.Version != checkpointVersion {
		return fmt.Errorf("checkpoint: unsupported version %d", st.Version)
	}
	if err := ValidateMessages(st.Window); err != nil {
		return fmt.Errorf("checkpoint: invalid context window: %w", err)
	}
	a.context.Restore(st.Window)
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
