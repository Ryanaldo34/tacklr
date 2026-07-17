package stores

import (
	"context"
	"fmt"
	"sync"
)

type sessionRecord struct {
	contextWindow []byte
	state         []byte
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

// SaveSession stores the context window and state for the given sessionID.
func (s *InMemoryStore) SaveSession(_ context.Context, sessionID string, contextWindow, state []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sessions[sessionID] = sessionRecord{
		contextWindow: append([]byte(nil), contextWindow...),
		state:         append([]byte(nil), state...),
	}
	return nil
}

// LoadSession retrieves the context window and state for the given sessionID.
func (s *InMemoryStore) LoadSession(_ context.Context, sessionID string) ([]byte, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.sessions[sessionID]
	if !ok {
		return nil, nil, fmt.Errorf("load session %q: %w", sessionID, ErrSessionNotFound)
	}

	contextWindow := append([]byte(nil), record.contextWindow...)
	state := append([]byte(nil), record.state...)
	return contextWindow, state, nil
}
