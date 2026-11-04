package session

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu       sync.Mutex
	records  map[ID]snapRecord
	outcomes map[ID]map[string]Outcome
}

type snapRecord struct {
	cp  []byte
	gen uint64
}

// NewMemoryStore returns an empty Store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[ID]snapRecord), outcomes: make(map[ID]map[string]Outcome)}
}

func revisionOf(gen uint64) Revision {
	return Revision(strconv.FormatUint(gen, 10))
}

// Save implements Store. expected must equal the revision
// from the last Load (zero if the row does not exist).
func (s *MemoryStore) Save(_ context.Context, sessionID ID, cp []byte, expected Revision) (Revision, error) {
	if sessionID == "" {
		return "", fmt.Errorf("snapshot: session id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	var current Revision
	if ok {
		current = revisionOf(cur.gen)
	}
	if expected != current {
		return "", ErrStaleCheckpoint
	}
	cur.gen++
	cur.cp = slices.Clone(cp)
	s.records[sessionID] = cur
	return revisionOf(cur.gen), nil
}

// Load implements Store.
func (s *MemoryStore) Load(_ context.Context, sessionID ID) ([]byte, Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	if !ok {
		return nil, "", fmt.Errorf("load snapshot %q: %w", sessionID, ErrNotFound)
	}
	return cur.cp, revisionOf(cur.gen), nil
}

// Revision implements Store.
func (s *MemoryStore) Revision(_ context.Context, sessionID ID) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	if !ok {
		return "", nil
	}
	return revisionOf(cur.gen), nil
}

// PutOutcome implements Store.
func (s *MemoryStore) PutOutcome(_ context.Context, sessionID ID, callKey string, o Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outcomes[sessionID] == nil {
		s.outcomes[sessionID] = map[string]Outcome{}
	}
	o.Record = slices.Clone(o.Record)
	s.outcomes[sessionID][callKey] = o
	return nil
}

// Outcomes implements Store.
func (s *MemoryStore) Outcomes(_ context.Context, sessionID ID, callKeys []string) (map[string]Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Outcome, len(callKeys))
	for _, key := range callKeys {
		if rec, ok := s.outcomes[sessionID][key]; ok {
			out[key] = rec
		}
	}
	return out, nil
}

// ClearOutcomes implements Store.
func (s *MemoryStore) ClearOutcomes(_ context.Context, sessionID ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.outcomes, sessionID)
	return nil
}

// Delete implements Store.
func (s *MemoryStore) Delete(_ context.Context, sessionID ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, sessionID)
	delete(s.outcomes, sessionID)
	return nil
}

var _ Store = (*MemoryStore)(nil)
