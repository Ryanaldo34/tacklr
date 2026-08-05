package brain

import (
	"context"
	"fmt"
	"maps"
	"slices"
)

// KindRegistry is the durable port for host-owned object kind schemas.
//
// PostgresStore is the default production backend. MemoryStore satisfies the same
// port for tests. Custom stores implement KindReader + KindWriter against their
// own persistence.
//
// The Engine type-asserts KindWriter when applying migrations; search still works
// with a read-only KindReader for LoadKindsFromStore / schema fallback.
type KindRegistry interface {
	KindReader
	KindWriter
}

// PersistKinds upserts validated kind specs into any KindWriter.
// Idempotent: re-applying is the migration path for add/modify. Specs not listed
// are left untouched in storage (additive upsert).
func PersistKinds(ctx context.Context, w KindWriter, specs ...KindSpec) error {
	if w == nil {
		return fmt.Errorf("brain: kind writer is required")
	}
	batch, err := normalizeKindBatch(specs)
	if err != nil {
		return err
	}
	return putKindBatch(ctx, w, batch)
}

// ApplyKinds is the host migration entry point for object kind schemas.
//
// Specs become the desired process catalog. When the store implements KindWriter,
// each kind is upserted. Empty ApplyKinds clears the process catalog (open mode)
// without deleting durable rows.
//
//	if err := eng.ApplyKinds(ctx, specs...); err != nil { return err }
//
// Call at startup before search freezes the catalog.
func (e *Engine) ApplyKinds(ctx context.Context, specs ...KindSpec) error {
	batch, err := normalizeKindBatch(specs)
	if err != nil {
		return err
	}
	if err := e.catalog.ensureNotFrozen(); err != nil {
		return err
	}
	if w, ok := e.store.(KindWriter); ok {
		if err := putKindBatch(ctx, w, batch); err != nil {
			return err
		}
	}
	return e.catalog.replaceNormalized(batch)
}

// SyncKindsToStore pushes the current process catalog to the store (upsert).
func (e *Engine) SyncKindsToStore(ctx context.Context) error {
	w, ok := e.store.(KindWriter)
	if !ok {
		return fmt.Errorf("brain: store does not support kind writes")
	}
	return PersistKinds(ctx, w, e.catalog.All()...)
}

// LoadKindsFromStore replaces the process catalog with kinds from the store.
func (e *Engine) LoadKindsFromStore(ctx context.Context) error {
	rows, err := e.store.ListKinds(ctx)
	if err != nil {
		return err
	}
	specs := make([]KindSpec, 0, len(rows))
	for _, row := range rows {
		spec, err := KindSpecFromObjectKind(row)
		if err != nil {
			return err
		}
		specs = append(specs, spec)
	}
	return e.catalog.replace(specs)
}

func putKindBatch(ctx context.Context, w KindWriter, batch map[string]KindSpec) error {
	for _, name := range slices.Sorted(maps.Keys(batch)) {
		if err := w.PutKind(ctx, objectKindFromNormalized(batch[name])); err != nil {
			return fmt.Errorf("brain: persist kind %q: %w", name, err)
		}
	}
	return nil
}
