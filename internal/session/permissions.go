package session

import (
	"sync"
)

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
func NewPermissions() Permissions {
	return Permissions{
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

// Remember stores an always-allow or always-deny decision.
func (p *Permissions) Remember(toolName string, d PermissionDecision) {
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
