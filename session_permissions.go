package tacklr

import (
	"sync"
)

// permissionDecision is session memory for a tool's allow-always / deny-always.
type permissionDecision int

const (
	permNone permissionDecision = iota
	permAllowAlways
	permDenyAlways
)

// permissions is the session module for durable tool-permission memory.
// It is not exposed on HarnessRuntime.
type permissions struct {
	mu    sync.RWMutex
	allow map[string]bool
	deny  map[string]bool
}

// newPermissions returns an empty permission store.
func newPermissions() permissions {
	return permissions{
		allow: map[string]bool{},
		deny:  map[string]bool{},
	}
}

// Decision returns remembered allow-always or deny-always for toolName.
// Deny wins if both are set.
func (p *permissions) Decision(toolName string) permissionDecision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.deny[toolName] {
		return permDenyAlways
	}
	if p.allow[toolName] {
		return permAllowAlways
	}
	return permNone
}

// Remember stores an always-allow or always-deny decision.
func (p *permissions) Remember(toolName string, d permissionDecision) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch d {
	case permAllowAlways:
		p.allow[toolName] = true
		delete(p.deny, toolName)
	case permDenyAlways:
		p.deny[toolName] = true
		delete(p.allow, toolName)
	}
}
