package durable

import (
	"context"
	"fmt"
	"sync"
)

// Secrets is the session-scoped secret bag that must not enter Temporal
// history or SnapshotStore. Auth holds VFS credentials. Add fields here when
// more work-item secrets need the same path.
type Secrets struct {
	Auth AuthContext
}

// SecretStorage holds Secrets for Temporal activities.
// Runtime client and worker must share one instance. It is not SnapshotStore.
type SecretStorage interface {
	// Put replaces Auth when secrets.Auth.Bindings is non-empty.
	// Drop-only Auth is a no-op on the bag (recipes drop in the workflow).
	Put(ctx context.Context, sessionID SessionID, secrets Secrets) error
	// Get returns the last Put bag. Missing session: zero Secrets, nil error.
	Get(ctx context.Context, sessionID SessionID) (Secrets, error)
	Delete(ctx context.Context, sessionID SessionID) error
}

// MemorySecretStorage is an in-memory SecretStorage for tests and
// single-process workers. Client and worker must share the same instance.
type MemorySecretStorage struct {
	mu   sync.Mutex
	byID map[SessionID]Secrets
}

func NewMemorySecretStorage() *MemorySecretStorage {
	return &MemorySecretStorage{byID: make(map[SessionID]Secrets)}
}

func (s *MemorySecretStorage) Put(_ context.Context, sessionID SessionID, secrets Secrets) error {
	if sessionID == "" {
		return fmt.Errorf("secrets: session id is required")
	}
	if len(secrets.Auth.Bindings) == 0 {
		return nil
	}
	s.mu.Lock()
	s.byID[sessionID] = Secrets{Auth: cloneAuth(secrets.Auth)}
	s.mu.Unlock()
	return nil
}

func (s *MemorySecretStorage) Get(_ context.Context, sessionID SessionID) (Secrets, error) {
	if sessionID == "" {
		return Secrets{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byID[sessionID]
	if !ok {
		return Secrets{}, nil
	}
	return Secrets{Auth: cloneAuth(cur.Auth)}, nil
}

func (s *MemorySecretStorage) Delete(_ context.Context, sessionID SessionID) error {
	if sessionID == "" {
		return nil
	}
	s.mu.Lock()
	delete(s.byID, sessionID)
	s.mu.Unlock()
	return nil
}
