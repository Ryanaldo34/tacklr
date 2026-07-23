package stores

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type sessionRecord struct {
	data []byte
}

// InMemoryStore implements BaseStore entirely in memory.
// Sessions are lost when the process exits.
type InMemoryStore struct {
	mu       sync.RWMutex
	sessions map[string]sessionRecord
}

// NewInMemoryStore creates an empty in-memory session store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		sessions: make(map[string]sessionRecord),
	}
}

// SaveSession stores the session checkpoint for the given sessionID.
func (s *InMemoryStore) SaveSession(_ context.Context, sessionID string, checkpoint SessionCheckpoint) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = sessionRecord{data: data}
	return nil
}

// LoadSession retrieves the session checkpoint for the given sessionID.
func (s *InMemoryStore) LoadSession(_ context.Context, sessionID string) (SessionCheckpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.sessions[sessionID]
	if !ok {
		return SessionCheckpoint{}, fmt.Errorf("load session %q: %w", sessionID, ErrSessionNotFound)
	}

	var checkpoint SessionCheckpoint
	if err := json.Unmarshal(record.data, &checkpoint); err != nil {
		return SessionCheckpoint{}, fmt.Errorf("unmarshal checkpoint: %w", err)
	}
	return checkpoint, nil
}
