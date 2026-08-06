package brain

import (
	"context"

	"github.com/google/uuid"
)

// ObjectReader is the read port for knowledge objects.
type ObjectReader interface {
	Get(ctx context.Context, scope Scope, id uuid.UUID) (Object, error)
	// GetMany returns objects for ids in the same order. Missing/out-of-scope ids are omitted.
	GetMany(ctx context.Context, scope Scope, ids []uuid.UUID) ([]Object, error)
	// ListChildren returns parts ordered by position.
	ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]Object, error)
}

// KindReader reads durable kind schema rows (schema fallback, LoadKindsFromStore).
type KindReader interface {
	GetKind(ctx context.Context, kind string) (ObjectKind, error)
	ListKinds(ctx context.Context) ([]ObjectKind, error)
}

// KindWriter upserts durable kind schema rows (ApplyKinds / PersistKinds).
// Not required for open-mode or process-only catalogs. Together with KindReader
// this is KindRegistry — implement both for a custom durable backend.
type KindWriter interface {
	PutKind(ctx context.Context, k ObjectKind) error
}

// ObjectWriter persists knowledge objects (Engine.Put / SoftDelete).
// PostgresStore and MemoryStore implement it. Custom backends implement for
// non-Postgres deployments. Not required for read-only engines.
type ObjectWriter interface {
	Put(ctx context.Context, obj Object) error
	SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error
}

// PartSearcher is the candidate retrieval port for hybrid / exact search.
type PartSearcher interface {
	SearchLexical(ctx context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error)
	SearchVector(ctx context.Context, scope Scope, embedding []float32, filters Filters, k int) ([]ScoredID, error)
	SearchTrigram(ctx context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error)
}

// Store is the full read + search surface required by Engine.
type Store interface {
	ObjectReader
	KindReader
	PartSearcher
}
