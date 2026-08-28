package inprocess

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/ryanaldo34/tacklr/durable"
)

// MemorySnapshot is an in-memory SnapshotStore.
type MemorySnapshot struct {
	mu      sync.Mutex
	records map[durable.SessionID]snapRecord
}

type snapRecord struct {
	snap durable.Snapshot
	gen  uint64
}

// NewMemorySnapshot returns an empty SnapshotStore.
func NewMemorySnapshot() *MemorySnapshot {
	return &MemorySnapshot{records: make(map[durable.SessionID]snapRecord)}
}

func revisionOf(gen uint64) durable.Revision {
	return durable.Revision(strconv.FormatUint(gen, 10))
}

// Save implements durable.SnapshotStore. expected must equal the revision
// from the last Load (zero if the row does not exist).
func (s *MemorySnapshot) Save(_ context.Context, sessionID durable.SessionID, snap durable.Snapshot, expected durable.Revision) (durable.Revision, error) {
	if sessionID == "" {
		return "", fmt.Errorf("snapshot: session id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	var current durable.Revision
	if ok {
		current = revisionOf(cur.gen)
	}
	if expected != current {
		return "", durable.ErrStaleCheckpoint
	}
	cur.gen++
	cur.snap = snap
	s.records[sessionID] = cur
	return revisionOf(cur.gen), nil
}

// Load implements durable.SnapshotStore.
func (s *MemorySnapshot) Load(_ context.Context, sessionID durable.SessionID) (durable.Snapshot, durable.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	if !ok {
		return durable.Snapshot{}, "", fmt.Errorf("load snapshot %q: %w", sessionID, durable.ErrSessionNotFound)
	}
	return cur.snap, revisionOf(cur.gen), nil
}

// Delete implements durable.SnapshotStore.
func (s *MemorySnapshot) Delete(_ context.Context, sessionID durable.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, sessionID)
	return nil
}

var _ durable.SnapshotStore = (*MemorySnapshot)(nil)
