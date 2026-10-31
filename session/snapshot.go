package session

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/ryanaldo34/tacklr"
)

// MemorySnapshot is an in-memory SnapshotStore.
type MemorySnapshot struct {
	mu      sync.Mutex
	records map[SessionID]snapRecord
}

type snapRecord struct {
	cp  tacklr.SessionCheckpoint
	gen uint64
}

// NewMemorySnapshot returns an empty SnapshotStore.
func NewMemorySnapshot() *MemorySnapshot {
	return &MemorySnapshot{records: make(map[SessionID]snapRecord)}
}

func revisionOf(gen uint64) Revision {
	return Revision(strconv.FormatUint(gen, 10))
}

// Save implements SnapshotStore. expected must equal the revision
// from the last Load (zero if the row does not exist).
func (s *MemorySnapshot) Save(_ context.Context, sessionID SessionID, cp tacklr.SessionCheckpoint, expected Revision) (Revision, error) {
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
	cur.cp = cp
	s.records[sessionID] = cur
	return revisionOf(cur.gen), nil
}

// Load implements SnapshotStore.
func (s *MemorySnapshot) Load(_ context.Context, sessionID SessionID) (tacklr.SessionCheckpoint, Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.records[sessionID]
	if !ok {
		return tacklr.SessionCheckpoint{}, "", fmt.Errorf("load snapshot %q: %w", sessionID, ErrSessionNotFound)
	}
	return cur.cp, revisionOf(cur.gen), nil
}

// Delete implements SnapshotStore.
func (s *MemorySnapshot) Delete(_ context.Context, sessionID SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, sessionID)
	return nil
}

var _ SnapshotStore = (*MemorySnapshot)(nil)
