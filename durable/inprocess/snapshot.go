package inprocess

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/ryanaldo34/tacklr/durable"
)

// MemorySnapshot is an in-memory SnapshotStore with etag concurrency.
type MemorySnapshot struct {
	mu      sync.Mutex
	records map[durable.SessionID]snapRecord
}

type snapRecord struct {
	snap durable.Snapshot
	etag string
	gen  uint64
}

// NewMemorySnapshot returns an empty SnapshotStore.
func NewMemorySnapshot() *MemorySnapshot {
	return &MemorySnapshot{records: make(map[durable.SessionID]snapRecord)}
}

// Save implements durable.SnapshotStore. Empty etag creates or overwrites.
// A non-empty etag must match the stored value.
func (s *MemorySnapshot) Save(_ context.Context, sessionID durable.SessionID, snap durable.Snapshot, etag string) (string, error) {
	if sessionID == "" {
		return "", fmt.Errorf("snapshot: session id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	if ok && etag != "" && etag != cur.etag {
		return "", durable.ErrEtagMismatch
	}
	cur.gen++
	cur.snap = snap
	cur.etag = strconv.FormatUint(cur.gen, 10)
	s.records[sessionID] = cur
	return cur.etag, nil
}

// Load implements durable.SnapshotStore.
func (s *MemorySnapshot) Load(_ context.Context, sessionID durable.SessionID) (durable.Snapshot, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	if !ok {
		return durable.Snapshot{}, "", fmt.Errorf("load snapshot %q: %w", sessionID, durable.ErrSessionNotFound)
	}
	return cur.snap, cur.etag, nil
}

// Delete implements durable.SnapshotStore.
func (s *MemorySnapshot) Delete(_ context.Context, sessionID durable.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, sessionID)
	return nil
}

var _ durable.SnapshotStore = (*MemorySnapshot)(nil)
