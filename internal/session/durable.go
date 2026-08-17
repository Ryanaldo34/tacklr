package session

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/interrupt"
)

const (
	modulePlan        = "plan"
	modulePermissions = "permissions"
	moduleParks       = "parks"
	moduleOnCall      = "onCall"
	moduleSearch      = "search"
)

type planCheckpoint struct {
	Todos           []Todo `json:"todos"`
	Document        string `json:"document,omitempty"`
	DocumentUpdated bool   `json:"documentUpdated,omitempty"`
}

type permissionsCheckpoint struct {
	Allow map[string]bool `json:"allow,omitempty"`
	Deny  map[string]bool `json:"deny,omitempty"`
}

type parksCheckpoint struct {
	Workers map[string]ParkedWorkerMeta `json:"workers,omitempty"`
}

type onCallCheckpoint struct {
	Stages []onCallStage `json:"stages,omitempty"`
}

func (s *SessionManager) snapshotCheckpoint() (
	userState map[string]json.RawMessage,
	modules map[string]json.RawMessage,
	pending, resolved interruptMap,
	err error,
) {
	s.mu.RLock()
	userValues := maps.Clone(s.userState)
	pending = cloneInterruptMap(s.pending)
	resolved = cloneInterruptMap(s.resolved)
	s.mu.RUnlock()

	userState = make(map[string]json.RawMessage, len(userValues))
	for key, value := range userValues {
		if IsReservedRuntimeStateKey(key) {
			continue
		}
		raw, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("checkpoint user state %q: %w", key, marshalErr)
		}
		userState[key] = raw
	}

	modules = make(map[string]json.RawMessage, 5)
	s.Plan.mu.RLock()
	plan := planCheckpoint{
		Todos:           slices.Clone(s.Plan.todos),
		Document:        s.Plan.document,
		DocumentUpdated: s.Plan.documentUpdated,
	}
	s.Plan.mu.RUnlock()
	if modules[modulePlan], err = json.Marshal(plan); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("checkpoint plan: %w", err)
	}

	s.Permissions.mu.RLock()
	permissions := permissionsCheckpoint{
		Allow: maps.Clone(s.Permissions.allow),
		Deny:  maps.Clone(s.Permissions.deny),
	}
	s.Permissions.mu.RUnlock()
	if modules[modulePermissions], err = json.Marshal(permissions); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("checkpoint permissions: %w", err)
	}

	parks := parksCheckpoint{Workers: s.parks.clone()}
	if modules[moduleParks], err = json.Marshal(parks); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("checkpoint parks: %w", err)
	}

	s.OnCall.mu.RLock()
	onCall := onCallCheckpoint{Stages: slices.Clone(s.OnCall.stages)}
	s.OnCall.mu.RUnlock()
	if modules[moduleOnCall], err = json.Marshal(onCall); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("checkpoint on-call: %w", err)
	}

	if s.Search != nil {
		if modules[moduleSearch], err = s.Search.Export(); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("checkpoint search: %w", err)
		}
	}
	return userState, modules, pending, resolved, nil
}

func (s *SessionManager) applyCheckpoint(userState, modules map[string]json.RawMessage) error {
	var plan planCheckpoint
	if err := decodeModule(modules, modulePlan, &plan); err != nil {
		return err
	}
	var permissions permissionsCheckpoint
	if err := decodeModule(modules, modulePermissions, &permissions); err != nil {
		return err
	}
	var parks parksCheckpoint
	if err := decodeModule(modules, moduleParks, &parks); err != nil {
		return err
	}
	var onCall onCallCheckpoint
	if err := decodeModule(modules, moduleOnCall, &onCall); err != nil {
		return err
	}

	search := brain.NewSearchContext()
	if raw := modules[moduleSearch]; len(raw) > 0 {
		if err := search.Restore(raw); err != nil {
			return fmt.Errorf("checkpoint module %q: %w", moduleSearch, err)
		}
	}

	decodedUser := make(map[string]any, len(userState))
	for key, raw := range userState {
		if IsReservedRuntimeStateKey(key) {
			return fmt.Errorf("checkpoint user state %q uses a reserved module key", key)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("checkpoint user state %q: %w", key, err)
		}
		decodedUser[key] = value
	}

	s.Plan.mu.Lock()
	s.Plan.todos = slices.Clone(plan.Todos)
	s.Plan.document = plan.Document
	s.Plan.documentUpdated = plan.DocumentUpdated
	s.Plan.todosUpdated = false
	s.Plan.mu.Unlock()

	s.Permissions.mu.Lock()
	s.Permissions.allow = maps.Clone(permissions.Allow)
	s.Permissions.deny = maps.Clone(permissions.Deny)
	if s.Permissions.allow == nil {
		s.Permissions.allow = map[string]bool{}
	}
	if s.Permissions.deny == nil {
		s.Permissions.deny = map[string]bool{}
	}
	s.Permissions.mu.Unlock()

	s.parks.replace(parks.Workers)
	s.OnCall.mu.Lock()
	s.OnCall.stages = slices.Clone(onCall.Stages)
	s.OnCall.mu.Unlock()

	s.mu.Lock()
	s.userState = decodedUser
	s.Search = search
	s.mu.Unlock()
	return nil
}

func decodeModule(modules map[string]json.RawMessage, name string, target any) error {
	raw := modules[name]
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("checkpoint module %q: %w", name, err)
	}
	return nil
}

func cloneInterruptMap(values interruptMap) interruptMap {
	out := make(interruptMap, len(values))
	for key, value := range values {
		if cloned := interrupt.Clone(value); cloned != nil {
			out[key] = cloned
		}
	}
	return out
}
