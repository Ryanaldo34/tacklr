package brain

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrResultSetNotFound is returned when a ResultSet id is unknown or replaced.
var ErrResultSetNotFound = errors.New("brain: result set not found")

// ResultSet is a ranked-list snapshot for deterministic continue() pagination.
type ResultSet struct {
	ID        uuid.UUID   `json:"id"`
	ObjectIDs []uuid.UUID `json:"object_ids"`
	Offset    int         `json:"offset"`
	CreatedAt time.Time   `json:"created_at"`
}

// ResultSetStore holds ResultSet snapshots for continue().
// SearchContext is the production implementation (single active set).
// Offset is advanced by Put of the same id with an updated Offset field.
type ResultSetStore interface {
	Put(ctx context.Context, set ResultSet) error
	Get(ctx context.Context, id uuid.UUID) (ResultSet, error)
}
