package tacklr

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// checkpointVersion is the schema of the checkpoint's rows. A checkpoint of
// another version fails to restore.
const checkpointVersion = 6

// The kinds of a checkpoint's rows. Host configuration (the search
// namespace and principals, AllowTools) is not in it: each step takes that
// from AgentOptions.
const (
	// rowVersion is the one row of the schema version.
	rowVersion = "version"
	// rowMessage is one message of the window, keyed by its position, so
	// an append writes only the new positions.
	rowMessage = "message"
	// rowTodo is one todo of the plan, keyed by its position.
	rowTodo = "todo"
	// rowPlan is the one row of the plan document.
	rowPlan = "plan"
	// rowState is one user-state key.
	rowState = "state"
	// rowPermission is one tool's permission decision.
	rowPermission = "permission"
	// rowRest is one search rest (search, related).
	rowRest = "rest"
)

// position is the key of the n-th row of an ordered kind: fixed-width, so
// keys sort in order.
func position(n int) string { return fmt.Sprintf("%08d", n) }

// Checkpoint is the session's committed state as rows: the window of
// complete rounds, the plan, user state, permission decisions and search
// rests. A round in progress is not in it. A runtime stores the rows
// (session.Store) and diffs them against the last ones it saved, so a
// commit writes only what changed; only the harness reads their values.
func (a *Harness) Checkpoint() ([]session.CheckpointEntry, error) {
	rows := []session.CheckpointEntry{{Kind: rowVersion, Value: []byte(strconv.Itoa(checkpointVersion))}}
	add := func(kind, key string, v any) error {
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("checkpoint %s %s: %w", kind, key, err)
		}
		rows = append(rows, session.CheckpointEntry{Kind: kind, Key: key, Value: raw})
		return nil
	}
	for i, m := range a.context.Messages() {
		_ = add(rowMessage, position(i), m) // messages are plain data
	}
	todos, plan := a.session.Plan.snapshot()
	for i, td := range todos {
		_ = add(rowTodo, position(i), td) // strings only
	}
	if plan != "" {
		rows = append(rows, session.CheckpointEntry{Kind: rowPlan, Value: []byte(plan)})
	}
	for tool, d := range sortedMap(a.session.Permissions.snapshot()) {
		_ = add(rowPermission, tool, d) // an int
	}
	for name, r := range sortedMap(a.session.Search.Rests()) {
		_ = add(rowRest, name, r) // ids, scores and evidence
	}
	sm := a.session
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for k, v := range sortedMap(sm.userState) {
		if err := add(rowState, k, v); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// sortedMap yields m's entries in key order.
func sortedMap[V any](m map[string]V) func(func(string, V) bool) {
	return func(yield func(string, V) bool) {
		for _, k := range slices.Sorted(maps.Keys(m)) {
			if !yield(k, m[k]) {
				return
			}
		}
	}
}

// RestoreCheckpoint replaces the session's durable state with the rows'.
func (a *Harness) RestoreCheckpoint(rows []session.CheckpointEntry) error {
	rows = slices.Clone(rows)
	slices.SortFunc(rows, func(x, y session.CheckpointEntry) int {
		return cmp.Or(cmp.Compare(x.Kind, y.Kind), cmp.Compare(x.Key, y.Key))
	})
	var (
		version     int
		window      []*message.Message
		todos       []Todo
		plan        string
		permissions = map[string]permissionDecision{}
		rests       = map[string]brain.Rest{}
		userState   = map[string]any{}
	)
	for _, r := range rows {
		var err error
		switch r.Kind {
		case rowVersion:
			version, err = strconv.Atoi(string(r.Value))
		case rowMessage:
			m := new(message.Message)
			err = json.Unmarshal(r.Value, m)
			window = append(window, m)
		case rowTodo:
			var td Todo
			err = json.Unmarshal(r.Value, &td)
			todos = append(todos, td)
		case rowPlan:
			plan = string(r.Value)
		case rowPermission:
			var d permissionDecision
			err = json.Unmarshal(r.Value, &d)
			permissions[r.Key] = d
		case rowRest:
			var rest brain.Rest
			err = json.Unmarshal(r.Value, &rest)
			rests[r.Key] = rest
		case rowState:
			var v any
			err = json.Unmarshal(r.Value, &v)
			userState[r.Key] = v
		default:
			err = fmt.Errorf("unknown row kind %q", r.Kind)
		}
		if err != nil {
			return fmt.Errorf("checkpoint %s %s: %w", r.Kind, r.Key, err)
		}
	}
	if version != checkpointVersion {
		return fmt.Errorf("checkpoint: unsupported version %d", version)
	}
	if err := ValidateMessages(window); err != nil {
		return fmt.Errorf("checkpoint: invalid context window: %w", err)
	}
	a.context.Restore(window)
	a.session.Plan.restore(todos, plan)
	a.session.Permissions.restore(permissions)
	a.session.Search.SetRests(rests)
	sm := a.session
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.userState = userState
	return nil
}
