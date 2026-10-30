package postgres

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/brain"
)

// Store is the Postgres brain.Store. Hosts open it with Open and inject it
// into brain.NewEngine. Run Setup before serving (a deploy or migration
// step); it applies pending schema migrations and is safe to run from many
// processes at once.
type Store struct {
	db     *pgxpool.Pool
	schema string
	dim    int
}

// objectSelectCols are the columns scanObject reads, in order.
const objectSelectCols = `
	id, kind, coalesce(title, ''), coalesce(summary, ''), properties,
	coalesce(content, ''), coalesce(content_type, ''), parent_id, position,
	namespace, readers, coalesce(embedding_model, ''), created_at, updated_at, deleted_at
`

// Get implements ObjectReader.
func (s *Store) Get(ctx context.Context, scope brain.Scope, id uuid.UUID) (brain.Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return brain.Object{}, err
	}
	obj, err := scanObject(s.db.QueryRow(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE id = $1 AND deleted_at IS NULL
		  AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])`,
		id, scope.Namespace, scope.Principals))
	if !errors.Is(err, brain.ErrNotFound) || strings.TrimSpace(scope.SessionID) == "" {
		return obj, err
	}
	saved, err := s.readSessionMessages(ctx, scope, []uuid.UUID{id})
	if err != nil {
		return brain.Object{}, err
	}
	if len(saved) == 0 {
		return brain.Object{}, fmt.Errorf("%w: %s", brain.ErrNotFound, id)
	}
	return saved[0], nil
}

// GetMany implements ObjectReader.
func (s *Store) GetMany(ctx context.Context, scope brain.Scope, ids []uuid.UUID) ([]brain.Object, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	found, err := s.queryObjects(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE id = ANY($1) AND deleted_at IS NULL
		  AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])`,
		ids, scope.Namespace, scope.Principals)
	if err != nil {
		return nil, fmt.Errorf("postgres: get many: %w", err)
	}
	byID := make(map[uuid.UUID]brain.Object, len(found))
	for _, obj := range found {
		byID[obj.ID] = obj
	}
	if sid := strings.TrimSpace(scope.SessionID); sid != "" && len(byID) < len(ids) {
		missing := make([]uuid.UUID, 0, len(ids)-len(byID))
		for _, id := range ids {
			if _, ok := byID[id]; !ok && id != uuid.Nil {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			extra, err := s.readSessionMessages(ctx, scope, missing)
			if err != nil {
				return nil, err
			}
			for _, obj := range extra {
				byID[obj.ID] = obj
			}
		}
	}
	out := make([]brain.Object, 0, len(byID))
	for _, id := range ids {
		if o, ok := byID[id]; ok {
			out = append(out, o)
		}
	}
	return out, nil
}

// ListChildren implements ObjectReader.
func (s *Store) ListChildren(ctx context.Context, scope brain.Scope, parentID uuid.UUID) ([]brain.Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	kids, err := s.queryObjects(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE parent_id = $1 AND deleted_at IS NULL
		  AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])
		ORDER BY position ASC NULLS LAST, id ASC`,
		parentID, scope.Namespace, scope.Principals)
	if err != nil {
		return nil, fmt.Errorf("postgres: list children: %w", err)
	}
	return kids, nil
}

// Put implements ObjectWriter: a full-column upsert that clears a soft
// delete. The conflict update only applies to a row visible under scope, so
// an id held outside scope updates nothing (ErrNotFound).
func (s *Store) Put(ctx context.Context, scope brain.Scope, obj brain.Object) error {
	q, args, err := upsertObject(scope, obj)
	if err != nil {
		return err
	}
	tag, err := s.db.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("postgres: put object: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", brain.ErrNotFound, obj.ID)
	}
	return nil
}

// upsertObject is the statement Put and CommitTree run to write obj. It
// affects no row when obj.ID is held by a row outside scope.
func upsertObject(scope brain.Scope, obj brain.Object) (string, []any, error) {
	if err := brain.ValidateObjectIdentity(obj); err != nil {
		return "", nil, err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return "", nil, err
	}
	if !scope.Namespace.Covers(obj.Namespace) {
		return "", nil, fmt.Errorf("%w: object namespace is outside scope", brain.ErrInvalid)
	}
	var model *string
	if len(obj.Embedding) > 0 {
		model = &obj.EmbeddingModel
	}
	now := time.Now().UTC()
	return `
		INSERT INTO objects (
			id, kind, title, summary, properties, content, content_type,
			parent_id, position, embedding, embedding_model, embedding_updated_at,
			namespace, readers, created_at, updated_at, deleted_at
		) VALUES (
			$1, $2, $3, $4, coalesce($5::jsonb, '{}'), $6, $7,
			$8, $9, $10::real[]::vector, $11, CASE WHEN $11::text IS NULL THEN NULL ELSE $15::timestamptz END,
			$12::jsonb, COALESCE($13::text[], '{}'), $14, $15, NULL
		)
		ON CONFLICT (id) DO UPDATE SET
			kind = EXCLUDED.kind,
			title = EXCLUDED.title,
			summary = EXCLUDED.summary,
			properties = EXCLUDED.properties,
			content = EXCLUDED.content,
			content_type = EXCLUDED.content_type,
			parent_id = EXCLUDED.parent_id,
			position = EXCLUDED.position,
			embedding = EXCLUDED.embedding,
			embedding_model = EXCLUDED.embedding_model,
			embedding_updated_at = EXCLUDED.embedding_updated_at,
			namespace = EXCLUDED.namespace,
			readers = EXCLUDED.readers,
			updated_at = EXCLUDED.updated_at,
			deleted_at = NULL
		WHERE objects.namespace @> $16::jsonb
		  AND (cardinality(objects.readers) = 0 OR objects.readers && $17::text[])`, []any{
		obj.ID, obj.Kind, obj.Title, obj.Summary, obj.Properties, obj.Content, obj.ContentType,
		obj.ParentID, obj.Position, obj.Embedding, model, obj.Namespace, obj.Readers,
		cmp.Or(obj.CreatedAt, now), cmp.Or(obj.UpdatedAt, now), scope.Namespace, scope.Principals,
	}, nil
}

// SoftDelete implements ObjectWriter: id and its parts.
func (s *Store) SoftDelete(ctx context.Context, scope brain.Scope, id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: object id is required", brain.ErrInvalid)
	}
	if err := scope.Namespace.Validate(); err != nil {
		return err
	}
	tag, err := s.db.Exec(ctx, `UPDATE objects SET deleted_at = $1, updated_at = $1
		WHERE (id = $2 OR parent_id = $2) AND deleted_at IS NULL
		  AND namespace @> $3::jsonb AND (cardinality(readers) = 0 OR readers && $4::text[])`,
		time.Now().UTC(), id, scope.Namespace, scope.Principals)
	if err != nil {
		return fmt.Errorf("postgres: soft delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", brain.ErrNotFound, id)
	}
	return nil
}

// PurgeDeleted implements Purger. It also drops parts staged by tree writes
// that never committed.
func (s *Store) PurgeDeleted(ctx context.Context, scope brain.Scope, before time.Time) (int, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return 0, err
	}
	tag, err := s.db.Exec(ctx, `
		WITH staged AS (DELETE FROM objects_staging WHERE staged_at < $1 AND namespace @> $2::jsonb)
		DELETE FROM objects WHERE deleted_at < $1 AND namespace @> $2::jsonb`, before, scope.Namespace)
	if err != nil {
		return 0, fmt.Errorf("postgres: purge deleted: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Erase implements Purger in one statement.
func (s *Store) Erase(ctx context.Context, scope brain.Scope) ([]uuid.UUID, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	rows, _ := s.db.Query(ctx, `
		WITH gone AS (DELETE FROM objects WHERE namespace @> $1::jsonb RETURNING id, parent_id),
		     sessions AS (DELETE FROM session_messages WHERE namespace @> $1::jsonb),
		     staged AS (DELETE FROM objects_staging WHERE namespace @> $1::jsonb)
		SELECT id FROM gone WHERE parent_id IS NULL`, scope.Namespace)
	parents, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("postgres: erase: %w", err)
	}
	return parents, nil
}

// Record is a brain.AuditFunc that appends ev to audit_events.
func (s *Store) Record(ctx context.Context, ev brain.AuditEvent) error {
	if _, err := s.db.Exec(ctx, `
		INSERT INTO audit_events (at, op, namespace, principals, session_id, object_ids, count)
		VALUES ($1, $2, $3::jsonb, COALESCE($4::text[], '{}'), NULLIF($5, ''), COALESCE($6::uuid[], '{}'), $7)`,
		ev.Time, string(ev.Op), ev.Namespace, ev.Principals, ev.SessionID, ev.ObjectIDs, ev.Count,
	); err != nil {
		return fmt.Errorf("postgres: audit: %w", err)
	}
	return nil
}

// PutKind implements KindWriter.
func (s *Store) PutKind(ctx context.Context, k brain.ObjectKind) error {
	if strings.TrimSpace(k.Kind) == "" {
		return fmt.Errorf("postgres: kind is required")
	}
	fields := k.FilterableFields
	if len(fields) == 0 {
		fields = json.RawMessage("[]")
	}
	const q = `
		INSERT INTO object_kinds (kind, description, is_part, is_parent, filterable_fields)
		VALUES ($1, $2, $3, $4, $5::jsonb)
		ON CONFLICT (kind) DO UPDATE SET
			description = EXCLUDED.description,
			is_part = EXCLUDED.is_part,
			is_parent = EXCLUDED.is_parent,
			filterable_fields = EXCLUDED.filterable_fields`
	if _, err := s.db.Exec(ctx, q, k.Kind, k.Description, k.IsPart, k.IsParent, []byte(fields)); err != nil {
		return fmt.Errorf("postgres: put kind: %w", err)
	}
	return nil
}

// kindSelect reads object_kinds rows in brain.ObjectKind field order.
const kindSelect = `SELECT kind, coalesce(description, ''), is_part, is_parent, filterable_fields FROM object_kinds`

// GetKind implements KindReader.
func (s *Store) GetKind(ctx context.Context, kind string) (brain.ObjectKind, error) {
	rows, _ := s.db.Query(ctx, kindSelect+` WHERE kind = $1`, kind)
	k, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[brain.ObjectKind])
	if errors.Is(err, pgx.ErrNoRows) {
		return brain.ObjectKind{}, fmt.Errorf("%w: kind %q", brain.ErrNotFound, kind)
	}
	if err != nil {
		return brain.ObjectKind{}, fmt.Errorf("postgres: get kind: %w", err)
	}
	return k, nil
}

// ListKinds implements KindReader.
func (s *Store) ListKinds(ctx context.Context) ([]brain.ObjectKind, error) {
	rows, _ := s.db.Query(ctx, kindSelect+` ORDER BY kind ASC`)
	kinds, err := pgx.CollectRows(rows, pgx.RowToStructByPos[brain.ObjectKind])
	if err != nil {
		return nil, fmt.Errorf("postgres: list kinds: %w", err)
	}
	return kinds, nil
}

// ListByKind implements ObjectLister (first-class objects only).
func (s *Store) ListByKind(ctx context.Context, scope brain.Scope, kind string, limit int) ([]brain.Object, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" || limit <= 0 {
		return nil, nil
	}
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	objs, err := s.queryObjects(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE kind = $1 AND deleted_at IS NULL AND parent_id IS NULL
		  AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])
		ORDER BY title ASC NULLS LAST, id ASC
		LIMIT $4`,
		kind, scope.Namespace, scope.Principals, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list by kind: %w", err)
	}
	return objs, nil
}

// GetByProperty implements ObjectLister.
func (s *Store) GetByProperty(ctx context.Context, scope brain.Scope, key, value string) (brain.Object, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return brain.Object{}, fmt.Errorf("postgres: property key is required")
	}
	if err := scope.Namespace.Validate(); err != nil {
		return brain.Object{}, err
	}
	return scanObject(s.db.QueryRow(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE deleted_at IS NULL AND properties->>$1 = $2
		  AND namespace @> $3::jsonb AND (cardinality(readers) = 0 OR readers && $4::text[])
		LIMIT 1`,
		key, value, scope.Namespace, scope.Principals))
}

// KindsWithObjects implements ObjectLister.
func (s *Store) KindsWithObjects(ctx context.Context, scope brain.Scope) ([]string, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	rows, _ := s.db.Query(ctx, `SELECT DISTINCT kind FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NULL AND kind <> ''
		  AND namespace @> $1::jsonb AND (cardinality(readers) = 0 OR readers && $2::text[])
		ORDER BY kind ASC`,
		scope.Namespace, scope.Principals)
	kinds, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("postgres: kinds with objects: %w", err)
	}
	return kinds, nil
}

// queryObjects runs a query that selects objectSelectCols.
func (s *Store) queryObjects(ctx context.Context, q string, args ...any) ([]brain.Object, error) {
	rows, _ := s.db.Query(ctx, q, args...)
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (brain.Object, error) { return scanObject(row) })
}

// scanObject reads one objectSelectCols row. No row is brain.ErrNotFound.
func scanObject(row pgx.Row) (brain.Object, error) {
	var o brain.Object
	err := row.Scan(&o.ID, &o.Kind, &o.Title, &o.Summary, &o.Properties, &o.Content, &o.ContentType,
		&o.ParentID, &o.Position, &o.Namespace, &o.Readers, &o.EmbeddingModel, &o.CreatedAt, &o.UpdatedAt, &o.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return brain.Object{}, brain.ErrNotFound
	}
	if err != nil {
		return brain.Object{}, fmt.Errorf("postgres: scan object: %w", err)
	}
	if len(o.Readers) == 0 {
		o.Readers = nil
	}
	return o, nil
}

// SearchLexical implements PartSearcher over parts (parent_id set).
// Score is ts_rank_cd on search_tsv (title A, summary B, body C).
// websearch_to_tsquery runs first (every term). A short page then ORs those terms.
func (s *Store) SearchLexical(ctx context.Context, scope brain.Scope, query string, filters brain.Filter, k int) ([]brain.ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" {
		return nil, nil
	}
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	where, fargs, err := brain.FilterSQL(filters, 4)
	if err != nil {
		return nil, err
	}
	args := append([]any{query, scope.Namespace, scope.Principals}, fargs...)
	args = append(args, k)
	limitPos := len(args)
	hits, err := s.searchFTS(ctx, objectsLexicalSQL(where, limitPos, true), objectsLexicalSQL(where, limitPos, false), args, k)
	if err != nil || strings.TrimSpace(scope.SessionID) == "" {
		return hits, err
	}
	extra, err := s.sessionLexical(ctx, scope, query, k)
	if err != nil {
		return nil, err
	}
	return mergeRanked(hits, extra), nil
}

func objectsLexicalSQL(where string, limitPos int, matchAll bool) string {
	return fmt.Sprintf(`
		SELECT id, coalesce(title, ''), coalesce(content, ''), parent_id, position, updated_at,
		       ts_rank_cd(search_tsv, tacklr_web_terms($1, %t), %d) AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND tacklr_web_terms($1, %t) IS NOT NULL
		  AND search_tsv @@ tacklr_web_terms($1, %t)
		  AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])%s
		ORDER BY score DESC
		LIMIT $%d`, matchAll, ftsRankNorm, matchAll, matchAll, where, limitPos)
}

// searchFTS runs the every-term query, then the any-term query when that page is shorter than k.
func (s *Store) searchFTS(ctx context.Context, matchAll, matchAny string, args []any, k int) ([]brain.ScoredID, error) {
	hits, err := s.queryScored(ctx, matchAll, args)
	if err != nil || len(hits) >= k {
		return hits, err
	}
	extra, err := s.queryScored(ctx, matchAny, args)
	if err != nil {
		return nil, err
	}
	return mergeLex(hits, extra, k), nil
}

func mergeLex(primary, extra []brain.ScoredID, k int) []brain.ScoredID {
	if len(extra) == 0 {
		return primary
	}
	seen := make(map[uuid.UUID]struct{}, len(primary)+len(extra))
	out := make([]brain.ScoredID, 0, len(primary)+len(extra))
	for _, hit := range primary {
		seen[hit.ID] = struct{}{}
		out = append(out, hit)
	}
	for _, hit := range extra {
		if _, ok := seen[hit.ID]; ok {
			continue
		}
		seen[hit.ID] = struct{}{}
		out = append(out, hit)
	}
	slices.SortStableFunc(out, func(a, b brain.ScoredID) int {
		return cmp.Compare(b.Score, a.Score)
	})
	if k > 0 && len(out) > k {
		out = out[:k]
	}
	return out
}

// SearchVector implements PartSearcher using pgvector cosine distance over parts.
func (s *Store) SearchVector(ctx context.Context, scope brain.Scope, model string, embedding []float32, filters brain.Filter, k int) ([]brain.ScoredID, error) {
	if k <= 0 || len(embedding) == 0 {
		return nil, nil
	}
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	where, fargs, err := brain.FilterSQL(filters, 5)
	if err != nil {
		return nil, err
	}
	args := append([]any{embedding, model, scope.Namespace, scope.Principals}, fargs...)
	args = append(args, k)
	limitPos := len(args)
	q := fmt.Sprintf(`
		SELECT id, coalesce(title, ''), coalesce(content, ''), parent_id, position, updated_at,
		       1 - (embedding <=> $1::real[]::vector) AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND embedding IS NOT NULL AND embedding_model = $2
		  AND namespace @> $3::jsonb AND (cardinality(readers) = 0 OR readers && $4::text[])%s
		ORDER BY embedding <=> $1::real[]::vector
		LIMIT $%d`, where, limitPos)
	hits, err := s.queryScored(ctx, q, args)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(scope.SessionID) == "" {
		return brain.TopScores(hits, k), nil
	}
	extra, err := s.sessionVector(ctx, scope, model, embedding, k)
	if err != nil {
		return nil, err
	}
	return brain.TopScores(append(hits, extra...), k), nil
}

// SearchTrigram implements PartSearcher using pg_trgm similarity over parts.
func (s *Store) SearchTrigram(ctx context.Context, scope brain.Scope, query string, filters brain.Filter, k int) ([]brain.ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" {
		return nil, nil
	}
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	where, fargs, err := brain.FilterSQL(filters, 4)
	if err != nil {
		return nil, err
	}
	args := append([]any{query, scope.Namespace, scope.Principals}, fargs...)
	args = append(args, k)
	limitPos := len(args)
	q := fmt.Sprintf(`
		SELECT id, coalesce(title, ''), coalesce(content, ''), parent_id, position, updated_at,
		       GREATEST(similarity(coalesce(title,''), $1), word_similarity($1, coalesce(content,''))) AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND (
		    strpos(lower(coalesce(title,'')), lower($1)) > 0
		    OR strpos(lower(coalesce(content,'')), lower($1)) > 0
		    OR similarity(coalesce(title,''), $1) > 0.3
		    OR word_similarity($1, coalesce(content,'')) > 0.3
		  )
		  AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])%s
		ORDER BY score DESC
		LIMIT $%d`, where, limitPos)
	hits, err := s.queryScored(ctx, q, args)
	if err != nil || strings.TrimSpace(scope.SessionID) == "" {
		return hits, err
	}
	extra, err := s.sessionTrigram(ctx, scope, query, k)
	if err != nil {
		return nil, err
	}
	return mergeRanked(hits, extra), nil
}

func (s *Store) queryScored(ctx context.Context, q string, args []any) ([]brain.ScoredID, error) {
	rows, _ := s.db.Query(ctx, q, args...)
	hits, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (brain.ScoredID, error) {
		var h brain.ScoredID
		err := row.Scan(&h.ID, &h.Title, &h.Content, &h.ParentID, &h.Position, &h.UpdatedAt, &h.Score)
		return h, err
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: search: %w", err)
	}
	return hits, nil
}
