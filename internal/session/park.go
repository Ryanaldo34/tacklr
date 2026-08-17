package session

import "sync"

// ParkedWorkerMeta is durable park metadata for a spawn_worker tool call.
// Live harness pointers are not stored here.
type ParkedWorkerMeta struct {
	WorkerName        string   `json:"workerName"`
	WorkerSessionID   string   `json:"workerSessionId"`
	Task              string   `json:"task"`
	ChildInterruptIDs []string `json:"childInterruptIds"`
}

type parkBag struct {
	mu   sync.RWMutex
	byID map[string]ParkedWorkerMeta
}

func newParkBag() parkBag {
	return parkBag{byID: map[string]ParkedWorkerMeta{}}
}

func (p *parkBag) get(id string) (ParkedWorkerMeta, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	m, ok := p.byID[id]
	return m, ok
}

func (p *parkBag) set(id string, meta ParkedWorkerMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byID[id] = meta
}

func (p *parkBag) delete(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.byID, id)
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

// ParkedWorker returns durable park metadata for a spawn_worker tool call.
func (s *SessionManager) ParkedWorker(toolCallID string) (ParkedWorkerMeta, bool) {
	return s.parks.get(toolCallID)
}

// SetParkedWorker stores durable park metadata for a spawn_worker tool call.
func (s *SessionManager) SetParkedWorker(toolCallID string, meta ParkedWorkerMeta) {
	s.parks.set(toolCallID, meta)
}

// DeleteParkedWorker removes durable park metadata for a spawn_worker tool call.
func (s *SessionManager) DeleteParkedWorker(toolCallID string) {
	s.parks.delete(toolCallID)
}
