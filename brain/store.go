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

// Store is the combined read surface for the engine.
type Store interface {
	ObjectReader
	KindReader
}
