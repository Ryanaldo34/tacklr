package session

import (
	"encoding/json"
	"maps"
	"slices"
	"sync"
)

const (
	permissionAllowKey    = "_permission_always_allow"
	permissionDenyKey     = "_permission_always_deny"
	writeApprovalAuditKey = "_write_approval_audit"
)

// WriteApprovalRecord is one resolved write-approval decision.
type WriteApprovalRecord struct {
	ToolName   string `json:"toolName"`
	ToolCallID string `json:"toolCallID"`
	Action     string `json:"action"`
	Args       string `json:"args"`
	UnixTime   int64  `json:"unixTime"`
}

type permissionBag struct {
	mu        sync.RWMutex
	allow     map[string]bool
	deny      map[string]bool
	approvals []WriteApprovalRecord
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

func (p *permissionBag) appendApproval(rec WriteApprovalRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.approvals = append(p.approvals, rec)
}

func (p *permissionBag) approvalList() []WriteApprovalRecord {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slices.Clone(p.approvals)
}

func (p *permissionBag) approvalFor(toolCallID string) (WriteApprovalRecord, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	i := slices.IndexFunc(p.approvals, func(rec WriteApprovalRecord) bool {
		return rec.ToolCallID == toolCallID
	})
	if i < 0 {
		return WriteApprovalRecord{}, false
	}
	return p.approvals[i], true
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

func decodeApprovalRecords(raw any) []WriteApprovalRecord {
	recs, ok := decodeAs[[]WriteApprovalRecord](raw)
	if !ok || len(recs) == 0 {
		return nil
	}
	return slices.Clone(recs)
}

func (p *permissionBag) exportInto(state map[string]any) {
	p.mu.RLock()
	allow := maps.Clone(p.allow)
	deny := maps.Clone(p.deny)
	recs := slices.Clone(p.approvals)
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
	if len(recs) == 0 {
		delete(state, writeApprovalAuditKey)
	} else {
		state[writeApprovalAuditKey] = recs
	}
}

func (p *permissionBag) loadFromState(state map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allow = map[string]bool{}
	p.deny = map[string]bool{}
	p.approvals = nil
	if state == nil {
		return
	}
	if raw, ok := state[permissionAllowKey]; ok && raw != nil {
		p.allow = decodeBoolSet(raw)
	}
	if raw, ok := state[permissionDenyKey]; ok && raw != nil {
		p.deny = decodeBoolSet(raw)
	}
	if raw, ok := state[writeApprovalAuditKey]; ok && raw != nil {
		p.approvals = decodeApprovalRecords(raw)
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

// WriteApprovals returns a copy of checkpointed write-approval decisions.
func (s *SessionManager) WriteApprovals() []WriteApprovalRecord {
	return s.perms.approvalList()
}

// WriteApprovalFor returns the decision recorded for toolCallID, if any.
func (s *SessionManager) WriteApprovalFor(toolCallID string) (WriteApprovalRecord, bool) {
	return s.perms.approvalFor(toolCallID)
}

// RecordWriteApproval appends a resolved write-approval decision.
func (s *SessionManager) RecordWriteApproval(rec WriteApprovalRecord) {
	s.perms.appendApproval(rec)
}
