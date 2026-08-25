package session

import (
	"sync"

	"github.com/ryanaldo34/tacklr/stores"
)

// ParkedWorkerMeta is durable park metadata for a spawn_specialist tool call.
// Live harness pointers are not stored here. Checkpoint is the child's
// SnapshotStore blob so a reconstructed parent can restore the worker.
type ParkedWorkerMeta struct {
	Specialist        string                    `json:"specialist"`
	WorkerSessionID   string                    `json:"workerSessionId"`
	Task              string                    `json:"task"`
	ChildInterruptIDs []string                  `json:"childInterruptIds"`
	Checkpoint        *stores.SessionCheckpoint `json:"checkpoint,omitempty"`
}

type parkBag struct {
	mu   sync.RWMutex
	byID map[string]ParkedWorkerMeta
}

func newParkBag() parkBag {
	return parkBag{byID: map[string]ParkedWorkerMeta{}}
}

func (p *parkBag) clone() map[string]ParkedWorkerMeta {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]ParkedWorkerMeta, len(p.byID))
	for k, v := range p.byID {
		out[k] = v
	}
	return out
}

func (p *parkBag) replace(m map[string]ParkedWorkerMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byID = m
	if p.byID == nil {
		p.byID = map[string]ParkedWorkerMeta{}
	}
}

// ParkedWorker returns durable park metadata for a spawn_specialist tool call.
func (s *SessionManager) ParkedWorker(toolCallID string) (ParkedWorkerMeta, bool) {
	s.parks.mu.RLock()
	defer s.parks.mu.RUnlock()
	m, ok := s.parks.byID[toolCallID]
	return m, ok
}

// SetParkedWorker stores durable park metadata for a spawn_specialist tool call.
func (s *SessionManager) SetParkedWorker(toolCallID string, meta ParkedWorkerMeta) {
	s.parks.mu.Lock()
	s.parks.byID[toolCallID] = meta
	s.parks.mu.Unlock()
}

// DeleteParkedWorker removes durable park metadata for a spawn_specialist tool call.
func (s *SessionManager) DeleteParkedWorker(toolCallID string) {
	s.parks.mu.Lock()
	delete(s.parks.byID, toolCallID)
	s.parks.mu.Unlock()
}
