package brain

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrResultSetNotFound is returned when a ResultSet id is unknown or expired.
var ErrResultSetNotFound = errors.New("brain: result set not found")

// ResultSet is a ranked-list snapshot for deterministic continue() pagination.
type ResultSet struct {
	ID        uuid.UUID
	ObjectIDs []uuid.UUID
	CreatedAt time.Time
}

// ResultSetStore holds ResultSet snapshots. Not harness Runtime state.
// Wiring into the agent is deferred until search/continue.
type ResultSetStore interface {
	Put(ctx context.Context, set ResultSet) error
	Get(ctx context.Context, id uuid.UUID) (ResultSet, error)
}
