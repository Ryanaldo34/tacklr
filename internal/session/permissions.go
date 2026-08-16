package session

import (
	"encoding/json"
	"maps"
	"slices"
	"sync"
)

const (
	permissionAllowKey = "_permission_always_allow"
	permissionDenyKey  = "_permission_always_deny"
	onCallStagesKey    = "_on_call_stages"
)

// onCallStage is one completed OnCall middleware layer for a tool call.
type onCallStage struct {
	ToolCallID string `json:"toolCallID"`
	TypeName   string `json:"typeName"`
	Denied     bool   `json:"denied"`
	Args       string `json:"args"`
}

type permissionBag struct {
	mu     sync.RWMutex
	allow  map[string]bool
	deny   map[string]bool
	stages []onCallStage
}

func newPermissionBag() *permissionBag {
	return &permissionBag{
		allow: map[string]bool{},
		deny:  map[string]bool{},
	}
}

func (p *permissionBag) has(set map[string]bool, name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return set[name]
}

func (p *permissionBag) remember(set map[string]bool, name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	set[name] = true
}

func (p *permissionBag) alwaysAllowed(name string) bool {
	return p.has(p.allow, name)
}

func (p *permissionBag) alwaysDenied(name string) bool {
	return p.has(p.deny, name)
}

func (p *permissionBag) rememberAllow(name string) {
	p.remember(p.allow, name)
}

func (p *permissionBag) rememberDeny(name string) {
	p.remember(p.deny, name)
}

func (p *permissionBag) stageFor(toolCallID, typeName string) (onCallStage, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	i := slices.IndexFunc(p.stages, func(st onCallStage) bool {
		return st.ToolCallID == toolCallID && st.TypeName == typeName
	})
	if i < 0 {
		return onCallStage{}, false
	}
	return p.stages[i], true
}

func (p *permissionBag) recordStage(st onCallStage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.stages {
		if p.stages[i].ToolCallID == st.ToolCallID && p.stages[i].TypeName == st.TypeName {
			p.stages[i] = st
			return
		}
	}
	p.stages = append(p.stages, st)
}

func decodeAs[T any](raw any) (T, bool) {
	var zero T
	if v, ok := raw.(T); ok {
		return v, true
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return zero, false
	}
	var v T
	if json.Unmarshal(b, &v) != nil {
		return zero, false
	}
	return v, true
}

func decodeBoolSet(raw any) map[string]bool {
	m, ok := decodeAs[map[string]bool](raw)
	if !ok || m == nil {
		return map[string]bool{}
	}
	return maps.Clone(m)
}

func (p *permissionBag) exportInto(state map[string]any) {
	p.mu.RLock()
	allow := maps.Clone(p.allow)
	deny := maps.Clone(p.deny)
	stages := slices.Clone(p.stages)
	p.mu.RUnlock()
	if len(allow) == 0 {
		delete(state, permissionAllowKey)
	} else {
		state[permissionAllowKey] = allow
	}
	if len(deny) == 0 {
		delete(state, permissionDenyKey)
	} else {
		state[permissionDenyKey] = deny
	}
	if len(stages) == 0 {
		delete(state, onCallStagesKey)
	} else {
		state[onCallStagesKey] = stages
	}
}

func (p *permissionBag) loadFromState(state map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allow = map[string]bool{}
	p.deny = map[string]bool{}
	p.stages = nil
	if state == nil {
		return
	}
	if raw, ok := state[permissionAllowKey]; ok && raw != nil {
		p.allow = decodeBoolSet(raw)
	}
	if raw, ok := state[permissionDenyKey]; ok && raw != nil {
		p.deny = decodeBoolSet(raw)
	}
	if raw, ok := state[onCallStagesKey]; ok && raw != nil {
		p.stages = decodeOnCallStages(raw)
	}
}

func decodeOnCallStages(raw any) []onCallStage {
	recs, ok := decodeAs[[]onCallStage](raw)
	if !ok || len(recs) == 0 {
		return nil
	}
	return slices.Clone(recs)
}

// PermissionAlwaysAllowed reports whether toolName was granted allow-always.
func (s *SessionManager) PermissionAlwaysAllowed(toolName string) bool {
	return s.perms.alwaysAllowed(toolName)
}

// PermissionAlwaysDenied reports whether toolName was granted reject-always.
func (s *SessionManager) PermissionAlwaysDenied(toolName string) bool {
	return s.perms.alwaysDenied(toolName)
}

// RememberPermissionAllow records allow-always for toolName.
func (s *SessionManager) RememberPermissionAllow(toolName string) {
	s.perms.rememberAllow(toolName)
}

// RememberPermissionDeny records reject-always for toolName.
func (s *SessionManager) RememberPermissionDeny(toolName string) {
	s.perms.rememberDeny(toolName)
}

// OnCallStage returns the completed OnCall layer for toolCallID and typeName.
func (s *SessionManager) OnCallStage(toolCallID, typeName string) (args string, denied bool, ok bool) {
	st, ok := s.perms.stageFor(toolCallID, typeName)
	if !ok {
		return "", false, false
	}
	return st.Args, st.Denied, true
}

// RecordOnCallStage stores a completed OnCall layer for re-entry.
func (s *SessionManager) RecordOnCallStage(toolCallID, typeName, args string, denied bool) {
	s.perms.recordStage(onCallStage{
		ToolCallID: toolCallID,
		TypeName:   typeName,
		Denied:     denied,
		Args:       args,
	})
}
