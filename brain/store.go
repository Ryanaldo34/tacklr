package brain

import (
	"context"

	"github.com/google/uuid"
)

// ObjectReader is the read port for knowledge objects.
type ObjectReader interface {
	Get(ctx context.Context, scope Scope, id uuid.UUID) (Object, error)
	// ListChildren returns parts ordered by position.
	ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]Object, error)
}

// KindReader backs schema() discovery.
type KindReader interface {
	GetKind(ctx context.Context, kind string) (ObjectKind, error)
	ListKinds(ctx context.Context) ([]ObjectKind, error)
}

// PartSearcher is the candidate retrieval port for hybrid / exact search.
// MemoryStore and PostgresStore implement this.
type PartSearcher interface {
	SearchLexical(ctx context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error)
	SearchVector(ctx context.Context, scope Scope, embedding []float32, filters Filters, k int) ([]ScoredID, error)
	SearchTrigram(ctx context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error)
}

// Store is the combined read surface for the engine.
type Store interface {
	ObjectReader
	KindReader
}
