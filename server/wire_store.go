package server

import (
	"context"
	"fmt"
	"sync"
)

// ProtocolWireStore persists protocol-owned session envelopes (not harness
// checkpoints). Payload is opaque JSON defined by each protocol.
// May share a database connection with other Tacklr stores without sharing schema.
type ProtocolWireStore interface {
	Put(ctx context.Context, sessionID string, payload []byte) error
	Get(ctx context.Context, sessionID string) ([]byte, error)
	Delete(ctx context.Context, sessionID string) error
}

// MemoryWireStore is an in-process ProtocolWireStore.
type MemoryWireStore struct {
	mu   sync.RWMutex
	byID map[string][]byte
}

// NewMemoryWireStore returns an empty in-memory wire store.
func NewMemoryWireStore() *MemoryWireStore {
	return &MemoryWireStore{byID: make(map[string][]byte)}
}

func (s *MemoryWireStore) Put(_ context.Context, sessionID string, payload []byte) error {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	s.mu.Lock()
	s.byID[sessionID] = cp
	s.mu.Unlock()
	return nil
}

func (s *MemoryWireStore) Get(_ context.Context, sessionID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	raw, ok := s.byID[sessionID]
	if !ok {
		return nil, fmt.Errorf("wire session %q: %w", sessionID, ErrSessionNotFound)
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return cp, nil
}

func (s *MemoryWireStore) Delete(_ context.Context, sessionID string) error {
	s.mu.Lock()
	delete(s.byID, sessionID)
	s.mu.Unlock()
	return nil
}
