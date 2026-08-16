package session

import (
	"maps"
	"sync"
)

const (
	permissionAllowKey = "_permission_always_allow"
	permissionDenyKey  = "_permission_always_deny"
)

func init() {
	reserveStateKeys(permissionAllowKey, permissionDenyKey)
}

// PermissionDecision is session memory for a tool's allow-always / deny-always.
type PermissionDecision int

const (
	PermissionNone PermissionDecision = iota
	PermissionAllowAlways
	PermissionDenyAlways
)

// Permissions is the session module for durable tool-permission memory.
// It is not exposed on HarnessRuntime.
type Permissions struct {
	mu    sync.RWMutex
	allow map[string]bool
	deny  map[string]bool
}

// NewPermissions returns an empty permission store.
func NewPermissions() *Permissions {
	return &Permissions{
		allow: map[string]bool{},
		deny:  map[string]bool{},
	}
}

// Decision returns remembered allow-always or deny-always for toolName.
// Deny wins if both are set.
func (p *Permissions) Decision(toolName string) PermissionDecision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.deny[toolName] {
		return PermissionDenyAlways
	}
	if p.allow[toolName] {
		return PermissionAllowAlways
	}
	return PermissionNone
}

// Remember stores an always-allow or always-deny decision. Other values are a no-op.
func (p *Permissions) Remember(toolName string, d PermissionDecision) {
	if d != PermissionAllowAlways && d != PermissionDenyAlways {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch d {
	case PermissionAllowAlways:
		p.allow[toolName] = true
		delete(p.deny, toolName)
	case PermissionDenyAlways:
		p.deny[toolName] = true
		delete(p.allow, toolName)
	}
}

func decodeBoolSet(raw any) map[string]bool {
	m, ok := decodeAs[map[string]bool](raw)
	if !ok || m == nil {
		return map[string]bool{}
	}
	return maps.Clone(m)
}

func (p *Permissions) exportInto(state map[string]any) {
	p.mu.RLock()
	allow := maps.Clone(p.allow)
	deny := maps.Clone(p.deny)
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

// Permissions returns the permission-memory module. Never nil after NewSessionManager.
func (s *SessionManager) Permissions() *Permissions {
	return s.perms
}
