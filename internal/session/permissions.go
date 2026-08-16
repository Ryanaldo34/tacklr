package session

import (
	"encoding/json"
	"sync"
)

const (
	permissionAllowKey = "_permission_always_allow"
	permissionDenyKey  = "_permission_always_deny"
)

type permissionBag struct {
	mu    sync.RWMutex
	allow map[string]bool
	deny  map[string]bool
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

func cloneBoolSet(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func decodeBoolSet(raw any) map[string]bool {
	if m, ok := raw.(map[string]bool); ok && m != nil {
		return cloneBoolSet(m)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return map[string]bool{}
	}
	var m map[string]bool
	if json.Unmarshal(b, &m) != nil || m == nil {
		return map[string]bool{}
	}
	return m
}

func (p *permissionBag) exportInto(state map[string]any) {
	p.mu.RLock()
	allow := cloneBoolSet(p.allow)
	deny := cloneBoolSet(p.deny)
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

func (p *permissionBag) loadFromState(state map[string]any) {
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
