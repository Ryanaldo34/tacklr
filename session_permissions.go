package tacklr

import (
	"maps"
	"sync"
)

// permissionDecision is a tool's remembered allow-always or deny-always.
type permissionDecision int

const (
	permNone permissionDecision = iota
	permAllowAlways
	permDenyAlways
)

// permissions answers a permission-gated call without a park. Decisions are
// the user's allow-always and reject-always answers (checkpointed); granted
// is the host's AllowTools (configuration, never checkpointed). A user's
// decision wins over a grant.
type permissions struct {
	mu        sync.RWMutex
	decisions map[string]permissionDecision
	granted   map[string]bool
}

func newPermissions() permissions {
	return permissions{decisions: map[string]permissionDecision{}}
}

// Decision is the answer for the tool qualified-named name, or permNone.
func (p *permissions) Decision(name string) permissionDecision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if d := p.decisions[name]; d != permNone {
		return d
	}
	if p.granted[name] {
		return permAllowAlways
	}
	return permNone
}

// rememberAnswer stores the always-answer a resolved permission prompt
// carries; a once-answer is not remembered.
func (p *permissions) rememberAnswer(perm *ToolPermissionInterrupt) {
	var d permissionDecision
	switch perm.SelectedKind {
	case PermissionAllowAlways:
		d = permAllowAlways
	case PermissionRejectAlways:
		d = permDenyAlways
	default:
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.decisions[perm.ToolName] = d
}

func (p *permissions) snapshot() map[string]permissionDecision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return maps.Clone(p.decisions)
}

func (p *permissions) restore(decisions map[string]permissionDecision) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.decisions = maps.Clone(decisions)
	if p.decisions == nil {
		p.decisions = map[string]permissionDecision{}
	}
}
