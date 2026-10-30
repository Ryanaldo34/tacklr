package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/brain"
)

// stagingCols are the objects_staging columns StageParts copies, in order.
var stagingCols = []string{
	"run", "id", "reused", "parent_id", "position", "properties",
	"kind", "title", "summary", "content", "content_type",
	"embedding", "embedding_model", "namespace", "created_at", "updated_at",
}

// TreeState implements brain.ObjectWriter in one query. A part shares its
// parent's namespace and readers, so the parent's scope check covers both.
func (s *Store) TreeState(ctx context.Context, scope brain.Scope, parentID uuid.UUID) (brain.TreeState, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return brain.TreeState{}, err
	}
	var rev int64
	var ids []uuid.UUID
	err := s.db.QueryRow(ctx, `
		SELECT tree_rev, ARRAY(SELECT c.id FROM objects c WHERE c.parent_id = p.id AND c.deleted_at IS NULL)
		FROM objects p
		WHERE p.id = $1
		  AND p.namespace @> $2::jsonb AND (cardinality(p.readers) = 0 OR p.readers && $3::text[])`,
		parentID, scope.Namespace, scope.Principals).Scan(&rev, &ids)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return brain.TreeState{}, fmt.Errorf("postgres: tree state: %w", err)
	}
	st := brain.TreeState{Rev: rev, Live: make(map[uuid.UUID]struct{}, len(ids))}
	for _, id := range ids {
		st.Live[id] = struct{}{}
	}
	return st, nil
}

// StageParts implements brain.ObjectWriter with one COPY into the unlogged
// objects_staging table. Vectors go as binary real[]; CommitTree casts them.
func (s *Store) StageParts(ctx context.Context, scope brain.Scope, run uuid.UUID, parts []brain.TreePart) error {
	if err := scope.Namespace.Validate(); err != nil {
		return err
	}
	if run == uuid.Nil {
		return fmt.Errorf("%w: run id is required", brain.ErrInvalid)
	}
	for _, p := range parts {
		if err := brain.ValidateTreePart(p); err != nil {
			return err
		}
		if !scope.Namespace.Covers(p.Object.Namespace) {
			return fmt.Errorf("%w: part namespace is outside scope", brain.ErrInvalid)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	now := time.Now().UTC()
	src := pgx.CopyFromSlice(len(parts), func(i int) ([]any, error) {
		p := parts[i].Object
		props := p.Properties
		if props == nil {
			props = map[string]any{}
		}
		var emb []float32
		var model *string
		if len(p.Embedding) > 0 {
			emb, model = p.Embedding, &p.EmbeddingModel
		}
		return []any{
			run, p.ID, parts[i].Reused, p.ParentID, p.Position, props,
			p.Kind, p.Title, p.Summary, p.Content, p.ContentType,
			emb, model, p.Namespace, cmp.Or(p.CreatedAt, now), cmp.Or(p.UpdatedAt, now),
		}, nil
	})
	if _, err := s.db.CopyFrom(ctx, pgx.Identifier{"objects_staging"}, stagingCols, src); err != nil {
		return fmt.Errorf("postgres: stage parts: %w", err)
	}
	return nil
}

// CommitTree implements brain.ObjectWriter in one transaction. Rows of
// reused parts are only rewritten when their position, properties, namespace
// or readers changed.
func (s *Store) CommitTree(ctx context.Context, scope brain.Scope, run uuid.UUID, rev int64, parent brain.Object) error {
	if run == uuid.Nil {
		return fmt.Errorf("%w: run id is required", brain.ErrInvalid)
	}
	upsert, args, err := upsertObject(scope, parent)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		// The whole commit is one round trip. Its checks are read once the
		// batch has run; any failed check returns an error, which rolls the
		// transaction back.
		b := &pgx.Batch{}
		// One commit per tree at a time, so the revision check holds.
		b.Queue(`SELECT pg_advisory_xact_lock(hashtext('tacklr.tree:' || $1::text))`, parent.ID)
		b.Queue(upsert, args...)
		b.Queue(`UPDATE objects SET tree_rev = tree_rev + 1 WHERE id = $1 AND tree_rev = $2`, parent.ID, rev)
		b.Queue(`
			SELECT count(*) FILTER (WHERE NOT s.namespace @> $3::jsonb),
			       count(*) FILTER (WHERE s.reused AND NOT EXISTS (
			           SELECT 1 FROM objects o WHERE o.id = s.id AND o.parent_id = $2))
			FROM objects_staging s WHERE s.run = $1`, run, parent.ID, scope.Namespace)
		b.Queue(`
			DELETE FROM objects o WHERE o.parent_id = $2
			  AND NOT EXISTS (SELECT 1 FROM objects_staging s WHERE s.run = $1 AND s.id = o.id)`, run, parent.ID)
		b.Queue(`
			INSERT INTO objects (
				id, kind, title, summary, properties, content, content_type,
				parent_id, position, embedding, embedding_model, embedding_updated_at,
				namespace, readers, created_at, updated_at, deleted_at
			)
			SELECT s.id, s.kind, s.title, s.summary, s.properties, s.content, s.content_type,
			       p.id, s.position, s.embedding::vector, s.embedding_model,
			       CASE WHEN s.embedding IS NULL THEN NULL ELSE s.updated_at END,
			       p.namespace, p.readers, s.created_at, s.updated_at, NULL
			FROM objects_staging s JOIN objects p ON p.id = $2
			WHERE s.run = $1 AND NOT s.reused
			ON CONFLICT (id) DO UPDATE SET
				kind = EXCLUDED.kind,
				title = EXCLUDED.title,
				summary = EXCLUDED.summary,
				properties = EXCLUDED.properties,
				content = EXCLUDED.content,
				content_type = EXCLUDED.content_type,
				position = EXCLUDED.position,
				embedding = EXCLUDED.embedding,
				embedding_model = EXCLUDED.embedding_model,
				embedding_updated_at = EXCLUDED.embedding_updated_at,
				namespace = EXCLUDED.namespace,
				readers = EXCLUDED.readers,
				updated_at = EXCLUDED.updated_at,
				deleted_at = NULL
			WHERE objects.parent_id = EXCLUDED.parent_id`, run, parent.ID)
		b.Queue(`
			UPDATE objects o SET
				position = s.position, properties = s.properties,
				namespace = p.namespace, readers = p.readers, deleted_at = NULL
			FROM objects_staging s, objects p
			WHERE s.run = $1 AND s.reused AND o.id = s.id AND o.parent_id = $2 AND p.id = $2
			  AND (o.position IS DISTINCT FROM s.position OR o.properties IS DISTINCT FROM s.properties
			       OR o.namespace IS DISTINCT FROM p.namespace OR o.readers IS DISTINCT FROM p.readers
			       OR o.deleted_at IS NOT NULL)`, run, parent.ID)
		b.Queue(`DELETE FROM objects_staging WHERE run = $1`, run)
		br := tx.SendBatch(ctx, b)
		_, lockErr := br.Exec()
		put, putErr := br.Exec()
		bump, bumpErr := br.Exec()
		var foreign, gone int
		checkErr := br.QueryRow().Scan(&foreign, &gone)
		if err := cmp.Or(lockErr, putErr, bumpErr, checkErr, br.Close()); err != nil {
			return fmt.Errorf("postgres: commit tree: %w", err)
		}
		switch {
		case put.RowsAffected() == 0:
			return fmt.Errorf("%w: %s", brain.ErrNotFound, parent.ID)
		case bump.RowsAffected() == 0:
			return fmt.Errorf("%w: tree %s changed since revision %d", brain.ErrConflict, parent.ID, rev)
		case foreign > 0:
			return fmt.Errorf("%w: staged parts lie outside scope", brain.ErrInvalid)
		case gone > 0:
			return fmt.Errorf("%w: %d reused parts of %s are gone", brain.ErrConflict, gone, parent.ID)
		}
		return nil
	})
}
