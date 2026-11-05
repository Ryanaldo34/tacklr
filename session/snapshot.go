package session

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu      sync.Mutex
	records map[ID]snapRecord
	calls   map[ID]map[string][]byte
}

type snapRecord struct {
	rows map[CheckpointRow][]byte
	gen  uint64
}

// NewMemoryStore returns an empty Store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[ID]snapRecord), calls: make(map[ID]map[string][]byte)}
}

func revisionOf(gen uint64) Revision {
	return Revision(strconv.FormatUint(gen, 10))
}

// Save implements Store. expected must equal the revision
// from the last Load (zero if the row does not exist).
func (s *MemoryStore) Save(_ context.Context, sessionID ID, changes []CheckpointChange, expected Revision) (Revision, error) {
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
	rows := maps.Clone(cur.rows)
	if rows == nil {
		rows = map[CheckpointRow][]byte{}
	}
	for _, c := range changes {
		if c.Delete {
			delete(rows, CheckpointRow{c.Kind, c.Key})
			continue
		}
		rows[CheckpointRow{c.Kind, c.Key}] = slices.Clone(c.Value)
	}
	cur.gen++
	cur.rows = rows
	s.records[sessionID] = cur
	return revisionOf(cur.gen), nil
}

// Load implements Store.
func (s *MemoryStore) Load(_ context.Context, sessionID ID) ([]CheckpointEntry, Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	if !ok {
		return nil, "", fmt.Errorf("load snapshot %q: %w", sessionID, ErrNotFound)
	}
	entries := make([]CheckpointEntry, 0, len(cur.rows))
	for row, v := range cur.rows {
		entries = append(entries, CheckpointEntry{Kind: row.Kind, Key: row.Key, Value: slices.Clone(v)})
	}
	return entries, revisionOf(cur.gen), nil
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

// PutCall implements Store.
func (s *MemoryStore) PutCall(_ context.Context, sessionID ID, callKey string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls[sessionID] == nil {
		s.calls[sessionID] = map[string][]byte{}
	}
	s.calls[sessionID][callKey] = slices.Clone(body)
	return nil
}

// Calls implements Store.
func (s *MemoryStore) Calls(_ context.Context, sessionID ID, callKeys []string) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(callKeys))
	for _, key := range callKeys {
		if body, ok := s.calls[sessionID][key]; ok {
			out[key] = slices.Clone(body)
		}
	}
	return out, nil
}

// ClearCalls implements Store.
func (s *MemoryStore) ClearCalls(_ context.Context, sessionID ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.calls, sessionID)
	return nil
}

// Delete implements Store.
func (s *MemoryStore) Delete(_ context.Context, sessionID ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, sessionID)
	delete(s.calls, sessionID)
	return nil
}

var _ Store = (*MemoryStore)(nil)
