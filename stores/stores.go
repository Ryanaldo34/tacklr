package stores

import (
	"context"
	"errors"
)

// BaseStore is the minimal persistence interface for agent session state.
//
// Implementations included in this package:
//   - InMemoryStore — sessions live in memory, lost on restart.
//   - PostgresStore — sessions persisted via PostgreSQL/pgx.
//
// Users may provide their own implementation (e.g. Redis, SQLite, custom DB).
type BaseStore interface {
	SaveSession(ctx context.Context, sessionID string, contextWindow, state []byte) error
	LoadSession(ctx context.Context, sessionID string) (contextWindow, state []byte, err error)
}

// ErrSessionNotFound is returned by LoadSession when the requested session
// does not exist.
var ErrSessionNotFound = errors.New("session not found")
