package session

import (
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr/internal/codec"
)

// The functions in this file are the only generic-map coercion path. They read
// version 1 checkpoints and are never used for newly captured state.

// LoadFromState imports a legacy plan stored in RuntimeState.
func (p *PlanStore) LoadFromState(state map[string]any) {
	if state == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.todos = nil
	p.document = ""
	p.documentUpdated = false
	p.todosUpdated = false
	if value, ok := state[planStateKey]; ok && value != nil {
		if plan, decoded := codec.As[[]Todo](value); decoded {
			p.todos = slices.Clone(plan)
		}
	}
	if document, ok := state[planDocumentStateKey].(string); ok {
		p.document = document
	}
	if updated, ok := state[planDocumentUpdatedKey].(bool); ok {
		p.documentUpdated = updated
	}
}

func decodeBoolSet(raw any) map[string]bool {
	values, ok := codec.As[map[string]bool](raw)
	if !ok || values == nil {
		return map[string]bool{}
	}
	return maps.Clone(values)
}

func (p *Permissions) loadFromState(state map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allow = map[string]bool{}
	p.deny = map[string]bool{}
	if state == nil {
		return
	}
	if raw, ok := state[permissionAllowKey]; ok && raw != nil {
		p.allow = decodeBoolSet(raw)
	}
	if raw, ok := state[permissionDenyKey]; ok && raw != nil {
		p.deny = decodeBoolSet(raw)
	}
}

func (p *parkBag) loadFromState(state map[string]any) {
	raw, ok := state[parkedWorkersStateKey]
	if !ok || raw == nil {
		p.replace(nil)
		return
	}
	if values, decoded := codec.As[map[string]ParkedWorkerMeta](raw); decoded && values != nil {
		p.replace(values)
		return
	}
	p.replace(nil)
}

func (s *OnCallStore) loadFromState(state map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = nil
	if state == nil {
		return
	}
	raw, ok := state[onCallStagesKey]
	if !ok || raw == nil {
		return
	}
	s.stages = decodeOnCallStages(raw)
}

func decodeOnCallStages(raw any) []onCallStage {
	stages, ok := codec.As[[]onCallStage](raw)
	if !ok || len(stages) == 0 {
		return nil
	}
	return slices.Clone(stages)
}
