package session

import (
	"encoding/json"
	"sync"
)

const writeApprovalAuditKey = "_write_approval_audit"

// WriteApprovalRecord is one resolved write-approval decision.
type WriteApprovalRecord struct {
	ToolName   string `json:"toolName"`
	ToolCallID string `json:"toolCallID"`
	Action     string `json:"action"`
	Args       string `json:"args"`
	UnixTime   int64  `json:"unixTime"`
}

type approvalBag struct {
	mu      sync.RWMutex
	records []WriteApprovalRecord
}

func newApprovalBag() *approvalBag {
	return &approvalBag{}
}

func (a *approvalBag) append(rec WriteApprovalRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, rec)
}

func (a *approvalBag) list() []WriteApprovalRecord {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.records) == 0 {
		return nil
	}
	out := make([]WriteApprovalRecord, len(a.records))
	copy(out, a.records)
	return out
}

func (a *approvalBag) exportInto(state map[string]any) {
	recs := a.list()
	if len(recs) == 0 {
		delete(state, writeApprovalAuditKey)
		return
	}
	state[writeApprovalAuditKey] = recs
}

func decodeApprovalRecords(raw any) []WriteApprovalRecord {
	if recs, ok := raw.([]WriteApprovalRecord); ok {
		out := make([]WriteApprovalRecord, len(recs))
		copy(out, recs)
		return out
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var recs []WriteApprovalRecord
	if json.Unmarshal(b, &recs) != nil || len(recs) == 0 {
		return nil
	}
	return recs
}

func (a *approvalBag) loadFromState(state map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = nil
	if state == nil {
		return
	}
	if raw, ok := state[writeApprovalAuditKey]; ok && raw != nil {
		a.records = decodeApprovalRecords(raw)
	}
}

// WriteApprovals returns a copy of checkpointed write-approval decisions.
func (s *SessionManager) WriteApprovals() []WriteApprovalRecord {
	return s.approvals.list()
}

// RecordWriteApproval appends a resolved write-approval decision.
func (s *SessionManager) RecordWriteApproval(rec WriteApprovalRecord) {
	s.approvals.append(rec)
}
