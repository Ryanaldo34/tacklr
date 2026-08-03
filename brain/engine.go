package brain

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Engine is the retrieval facade over a Store.
type Engine struct {
	store Store
}

// NewEngine builds an Engine over a Store. store must be non-nil.
func NewEngine(store Store) (*Engine, error) {
	if store == nil {
		return nil, fmt.Errorf("brain: store is required")
	}
	return &Engine{store: store}, nil
}

// Read returns the full rich object for id under scope.
func (e *Engine) Read(ctx context.Context, scope Scope, id uuid.UUID) (RichObject, error) {
	if id == uuid.Nil {
		return RichObject{}, fmt.Errorf("brain: object id is required")
	}
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return RichObject{}, err
	}
	return RichFromObject(obj, true), nil
}

// Schema returns kind documentation. Empty kind lists all registered kinds.
func (e *Engine) Schema(ctx context.Context, kind string) (SchemaResult, error) {
	kind = strings.TrimSpace(kind)
	if kind != "" {
		k, err := e.store.GetKind(ctx, kind)
		if err != nil {
			return SchemaResult{}, err
		}
		return SchemaResult{Kinds: []ObjectKindInfo{KindInfoFrom(k)}}, nil
	}
	kinds, err := e.store.ListKinds(ctx)
	if err != nil {
		return SchemaResult{}, err
	}
	out := make([]ObjectKindInfo, 0, len(kinds))
	for _, k := range kinds {
		out = append(out, KindInfoFrom(k))
	}
	return SchemaResult{Kinds: out}, nil
}

// ListChildren returns ordered children for a parent visible under scope.
func (e *Engine) ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]RichObject, error) {
	if parentID == uuid.Nil {
		return nil, fmt.Errorf("brain: parent id is required")
	}
	if _, err := e.store.Get(ctx, scope, parentID); err != nil {
		return nil, err
	}
	parts, err := e.store.ListChildren(ctx, scope, parentID)
	if err != nil {
		return nil, err
	}
	out := make([]RichObject, 0, len(parts))
	for _, p := range parts {
		out = append(out, RichFromObject(p, false))
	}
	return out, nil
}
