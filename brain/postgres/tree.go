package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr/brain"
)

// stagingCols are the objects_staging columns stage copies, in order.
var stagingCols = []string{
	"run", "id", "reused", "parent_id", "position", "properties",
	"kind", "title", "summary", "content", "content_type",
	"embedding", "embedding_model", "namespace", "created_at", "updated_at",
}

// PutTree implements brain.Store. It reads the tree's revision and the parts
// it can reuse, then runs Embedding.EmbedBatches: each finished batch is
// copied into the unlogged objects_staging table, so memory holds a few
// batches whatever the number of parts. One short commit then makes the
// staged parts the parent's only parts. Parts staged by a write that fails
// stay until PurgeDeleted drops them.
func (s *Store) PutTree(ctx context.Context, scope brain.Scope, parent brain.Indexed, parts iter.Seq2[brain.Indexed, error]) (brain.Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return brain.Object{}, err
	}
	p, err := s.embedded(ctx, parent)
	if err != nil {
		return brain.Object{}, err
	}
	upsert, args, err := upsertObject(scope, p)
	if err != nil {
		return brain.Object{}, err
	}
	rev, live, err := s.treeState(ctx, scope, p.ID)
	if err != nil {
		return brain.Object{}, err
	}
	run := uuid.New()
	_, err = s.embedding.EmbedBatches(ctx, checkedParts(scope, parts), func(id uuid.UUID) bool {
		_, ok := live[id]
		return ok
	}, func(ctx context.Context, batch []brain.TreePart) error {
		return s.stage(ctx, run, batch)
	})
	if err != nil {
		return brain.Object{}, err
	}
	return s.commitTree(ctx, run, rev, p.ID, upsert, args)
}

// checkedParts yields parts and fails at the first one that is invalid, lies
// outside scope, or repeats an earlier id.
func checkedParts(scope brain.Scope, parts iter.Seq2[brain.Indexed, error]) iter.Seq2[brain.Indexed, error] {
	return func(yield func(brain.Indexed, error) bool) {
		seen := map[uuid.UUID]struct{}{}
		for p, err := range parts {
			if err == nil {
				err = brain.ValidateTreePart(brain.TreePart{Object: p.Object})
			}
			if err == nil && !scope.Namespace.Covers(p.Namespace) {
				err = fmt.Errorf("%w: part namespace is outside scope", brain.ErrInvalid)
			}
			if _, dup := seen[p.ID]; err == nil && dup {
				err = fmt.Errorf("%w: part %s appears twice", brain.ErrInvalid, p.ID)
			}
			seen[p.ID] = struct{}{}
			if !yield(p, err) || err != nil {
				return
			}
		}
	}
}

// treeState returns parentID's tree revision and its live parts that the
// store's model embedded, in one query. A part shares its parent's namespace
// and readers, so the parent's scope check covers both.
func (s *Store) treeState(ctx context.Context, scope brain.Scope, parentID uuid.UUID) (int64, map[uuid.UUID]struct{}, error) {
	var rev int64
	var ids []uuid.UUID
	err := s.db.QueryRow(ctx, `
		SELECT tree_rev, ARRAY(SELECT c.id FROM objects c
			WHERE c.parent_id = p.id AND c.deleted_at IS NULL AND coalesce(c.embedding_model, '') = $4)
		FROM objects p
		WHERE p.id = $1
		  AND p.tenant = ($2::jsonb)->0 AND p.namespace @> $2::jsonb AND (cardinality(p.readers) = 0 OR p.readers && $3::text[])`,
		parentID, scope.Namespace, scope.Principals, s.embedding.Model()).Scan(&rev, &ids)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, fmt.Errorf("postgres: tree state: %w", err)
	}
	live := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		live[id] = struct{}{}
	}
	return rev, live, nil
}

// stage copies one batch into objects_staging. Vectors go as binary real[];
// commitTree casts them.
func (s *Store) stage(ctx context.Context, run uuid.UUID, parts []brain.TreePart) error {
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

// commitTree runs the parent's upsert and makes run's staged parts its only
// parts in one transaction, and returns the stored parent. Rows of reused
// parts are only rewritten when their position, properties, namespace or
// readers changed.
func (s *Store) commitTree(ctx context.Context, run uuid.UUID, rev int64, parentID uuid.UUID, upsert string, args []any) (brain.Object, error) {
	var stored brain.Object
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		// The whole commit is one round trip. Its checks are read once the
		// batch has run; any failed check returns an error, which rolls the
		// transaction back.
		b := &pgx.Batch{}
		// One commit per tree at a time, so the revision check holds.
		b.Queue(`SELECT pg_advisory_xact_lock(hashtext('tacklr.tree:' || $1::text))`, parentID)
		b.Queue(upsert, args...)
		b.Queue(`UPDATE objects SET tree_rev = tree_rev + 1 WHERE id = $1 AND tree_rev = $2`, parentID, rev)
		b.Queue(`
			SELECT count(*) FROM objects_staging s
			WHERE s.run = $1 AND s.reused AND NOT EXISTS (SELECT 1 FROM objects o WHERE o.id = s.id AND o.parent_id = $2)`, run, parentID)
		b.Queue(`
			DELETE FROM objects o WHERE o.parent_id = $2
			  AND NOT EXISTS (SELECT 1 FROM objects_staging s WHERE s.run = $1 AND s.id = o.id)`, run, parentID)
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
			WHERE objects.parent_id = EXCLUDED.parent_id`, run, parentID)
		b.Queue(`
			UPDATE objects o SET
				position = s.position, properties = s.properties,
				namespace = p.namespace, readers = p.readers, deleted_at = NULL
			FROM objects_staging s, objects p
			WHERE s.run = $1 AND s.reused AND o.id = s.id AND o.parent_id = $2 AND p.id = $2
			  AND (o.position IS DISTINCT FROM s.position OR o.properties IS DISTINCT FROM s.properties
			       OR o.namespace IS DISTINCT FROM p.namespace OR o.readers IS DISTINCT FROM p.readers
			       OR o.deleted_at IS NOT NULL)`, run, parentID)
		b.Queue(`DELETE FROM objects_staging WHERE run = $1`, run)
		br := tx.SendBatch(ctx, b)
		_, lockErr := br.Exec()
		var putErr error
		stored, putErr = scanObject(br.QueryRow())
		bump, bumpErr := br.Exec()
		var gone int
		checkErr := br.QueryRow().Scan(&gone)
		if errors.Is(putErr, brain.ErrNotFound) {
			// No row: the id is held outside scope. The batch still ran, so
			// close it and roll back.
			return cmp.Or(br.Close(), fmt.Errorf("%w: %s", brain.ErrNotFound, parentID))
		}
		if err := cmp.Or(lockErr, putErr, bumpErr, checkErr, br.Close()); err != nil {
			return fmt.Errorf("postgres: commit tree: %w", err)
		}
		switch {
		case bump.RowsAffected() == 0:
			return fmt.Errorf("%w: tree %s changed since revision %d", brain.ErrConflict, parentID, rev)
		case gone > 0:
			return fmt.Errorf("%w: %d reused parts of %s are gone", brain.ErrConflict, gone, parentID)
		}
		return nil
	})
	if err != nil {
		return brain.Object{}, err
	}
	return stored, nil
}
