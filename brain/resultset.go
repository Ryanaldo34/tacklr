package brain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ResultSet is a ranked-list snapshot for deterministic continue() pagination.
type ResultSet struct {
	ID        uuid.UUID   `json:"id"`
	ObjectIDs []uuid.UUID `json:"object_ids"`
	// Relations carries expand hop metadata keyed by object id so continue
	// re-attaches relation fields on later pages (JSON keys are UUID strings).
	Relations map[uuid.UUID]Relation `json:"relations,omitempty"`
	Namespace Namespace              `json:"namespace,omitempty"`
	Offset    int                    `json:"offset"`
	CreatedAt time.Time              `json:"created_at"`
}

// ResultSetStore holds ResultSet snapshots for continue().
// SearchContext is the production implementation (single active set).
// Offset is advanced by Put of the same id with an updated Offset field.
type ResultSetStore interface {
	Put(ctx context.Context, set ResultSet) error
	Get(ctx context.Context, id uuid.UUID) (ResultSet, error)
}
