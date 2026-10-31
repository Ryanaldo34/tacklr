package postgres

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/brain"
)

var _ brain.Store = (*Store)(nil)

// Store is the Postgres brain.Store. Hosts open it with Open and inject it
// into brain.NewEngine. Run Setup before serving; it creates whatever schema
// is missing and is safe to run from many processes at once. It embeds with
// its own Options.Embedding and fuses its search channels with
// Options.Ranking.
type Store struct {
	db        *pgxpool.Pool
	schema    string
	dim       int
	embedding brain.Embedding
	ranking   brain.Ranking
}

// objectSelectCols are the columns scanObject reads, in order.
const objectSelectCols = `
	id, kind, coalesce(title, ''), coalesce(summary, ''), properties,
	coalesce(content, ''), coalesce(content_type, ''), parent_id, position,
	namespace, readers, coalesce(embedding_model, ''), created_at, updated_at, deleted_at
`

// Get implements brain.Store.
func (s *Store) Get(ctx context.Context, scope brain.Scope, id uuid.UUID) (brain.Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return brain.Object{}, err
	}
	obj, err := scanObject(s.db.QueryRow(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE id = $1 AND deleted_at IS NULL
		  AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])`,
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

// GetMany implements brain.Store.
func (s *Store) GetMany(ctx context.Context, scope brain.Scope, ids []uuid.UUID) ([]brain.Object, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	found, err := s.queryObjects(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE id = ANY($1) AND deleted_at IS NULL
		  AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])`,
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

// GetTree implements brain.Store in one query: the object, a part's
// record, and the record's parts. A saved session message (Scope.SessionID
// set) is read when no object has the id.
func (s *Store) GetTree(ctx context.Context, scope brain.Scope, id uuid.UUID, limit int) (brain.Tree, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return brain.Tree{}, err
	}
	var lim *int // LIMIT NULL is no limit
	if limit > 0 {
		lim = &limit
	}
	rows, err := s.queryObjects(ctx, treeSQL, id, scope.Namespace, scope.Principals, lim)
	if err != nil {
		return brain.Tree{}, fmt.Errorf("postgres: get tree: %w", err)
	}
	if len(rows) == 0 {
		obj, err := s.Get(ctx, scope, id)
		return brain.Tree{Object: obj}, err
	}
	var t brain.Tree
	for _, row := range rows {
		if row.ID == id {
			t.Object = row
		}
	}
	// The object is one row; a part is also one of its record's parts.
	self := false
	for _, row := range rows {
		switch {
		case row.ID == id && !self:
			self = true
		case t.Object.ParentID != nil && row.ID == *t.Object.ParentID:
			t.Parent = &row
		default:
			t.Parts = append(t.Parts, row)
		}
	}
	return t, nil
}

// treeSQL selects the object, a part's record, and the record's first $4
// parts, ordered by position, so the parts come back in order.
const treeSQL = `
	WITH t AS (
		SELECT id AS tid, parent_id AS tparent FROM objects
		WHERE id = $1 AND deleted_at IS NULL
		  AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])
	)
	(SELECT ` + objectSelectCols + ` FROM objects JOIN t ON id = tid)
	UNION ALL
	(SELECT ` + objectSelectCols + ` FROM objects JOIN t ON id = tparent
	 WHERE deleted_at IS NULL
	   AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[]))
	UNION ALL
	(SELECT ` + objectSelectCols + ` FROM objects JOIN t ON parent_id = coalesce(tparent, tid)
	 WHERE deleted_at IS NULL
	   AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb AND (cardinality(readers) = 0 OR readers && $3::text[])
	 ORDER BY position ASC NULLS LAST, id ASC
	 LIMIT $4)
	ORDER BY position ASC NULLS LAST, id ASC`

// Put implements brain.Store: it embeds in.Text, then runs a full-column
// upsert that clears a soft delete and returns the stored row. The conflict
// update only applies to a row visible under scope, so an id held outside
// scope updates nothing (ErrNotFound).
func (s *Store) Put(ctx context.Context, scope brain.Scope, in brain.Indexed) (brain.Object, error) {
	obj, err := s.embedded(ctx, in)
	if err != nil {
		return brain.Object{}, err
	}
	q, args, err := upsertObject(scope, obj)
	if err != nil {
		return brain.Object{}, err
	}
	stored, err := scanObject(s.db.QueryRow(ctx, q, args...))
	if errors.Is(err, brain.ErrNotFound) {
		return brain.Object{}, fmt.Errorf("%w: %s", brain.ErrNotFound, obj.ID)
	}
	return stored, err
}

// embedded returns in's object with the embedding of its text.
func (s *Store) embedded(ctx context.Context, in brain.Indexed) (brain.Object, error) {
	obj := in.Object
	vec, err := s.embedding.EmbedOne(ctx, in.Text)
	if err != nil {
		return brain.Object{}, fmt.Errorf("postgres: embed object: %w", err)
	}
	obj.Embedding, obj.EmbeddingModel = vec, ""
	if vec != nil {
		obj.EmbeddingModel = s.embedding.Model()
	}
	return obj, nil
}

// upsertObject is the statement Put and PutTree run to write obj; it
// returns the stored row. It affects no row when obj.ID is held by a row
// outside scope.
func upsertObject(scope brain.Scope, obj brain.Object) (string, []any, error) {
	if err := obj.ValidateIdentity(); err != nil {
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
		WHERE objects.tenant = ($16::jsonb)->0 AND objects.namespace @> $16::jsonb
		  AND (cardinality(objects.readers) = 0 OR objects.readers && $17::text[])
		RETURNING ` + objectSelectCols, []any{
		obj.ID, obj.Kind, obj.Title, obj.Summary, obj.Properties, obj.Content, obj.ContentType,
		obj.ParentID, obj.Position, obj.Embedding, model, obj.Namespace, obj.Readers,
		cmp.Or(obj.CreatedAt, now), cmp.Or(obj.UpdatedAt, now), scope.Namespace, scope.Principals,
	}, nil
}

// SoftDelete implements brain.Store: id and its parts.
func (s *Store) SoftDelete(ctx context.Context, scope brain.Scope, id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: object id is required", brain.ErrInvalid)
	}
	if err := scope.Namespace.Validate(); err != nil {
		return err
	}
	tag, err := s.db.Exec(ctx, `UPDATE objects SET deleted_at = $1, updated_at = $1
		WHERE (id = $2 OR parent_id = $2) AND deleted_at IS NULL
		  AND tenant = ($3::jsonb)->0 AND namespace @> $3::jsonb AND (cardinality(readers) = 0 OR readers && $4::text[])`,
		time.Now().UTC(), id, scope.Namespace, scope.Principals)
	if err != nil {
		return fmt.Errorf("postgres: soft delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", brain.ErrNotFound, id)
	}
	return nil
}

// PurgeDeleted implements brain.Store. It also drops parts staged by tree writes
// that never committed.
func (s *Store) PurgeDeleted(ctx context.Context, scope brain.Scope, before time.Time) (int, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return 0, err
	}
	tag, err := s.db.Exec(ctx, `
		WITH staged AS (DELETE FROM objects_staging WHERE staged_at < $1 AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb)
		DELETE FROM objects WHERE deleted_at < $1 AND tenant = ($2::jsonb)->0 AND namespace @> $2::jsonb`, before, scope.Namespace)
	if err != nil {
		return 0, fmt.Errorf("postgres: purge deleted: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Erase implements brain.Store in one statement.
func (s *Store) Erase(ctx context.Context, scope brain.Scope) ([]uuid.UUID, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	rows, _ := s.db.Query(ctx, `
		WITH gone AS (DELETE FROM objects WHERE tenant = ($1::jsonb)->0 AND namespace @> $1::jsonb RETURNING id, parent_id),
		     sessions AS (DELETE FROM session_messages WHERE tenant = ($1::jsonb)->0 AND namespace @> $1::jsonb),
		     staged AS (DELETE FROM objects_staging WHERE tenant = ($1::jsonb)->0 AND namespace @> $1::jsonb)
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

// PutKind implements brain.Store.
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

// ListKinds implements brain.Store.
func (s *Store) ListKinds(ctx context.Context) ([]brain.ObjectKind, error) {
	rows, _ := s.db.Query(ctx, kindSelect+` ORDER BY kind ASC`)
	kinds, err := pgx.CollectRows(rows, pgx.RowToStructByPos[brain.ObjectKind])
	if err != nil {
		return nil, fmt.Errorf("postgres: list kinds: %w", err)
	}
	return kinds, nil
}

// GetByProperty implements brain.Store.
func (s *Store) GetByProperty(ctx context.Context, scope brain.Scope, key, value string) (brain.Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return brain.Object{}, err
	}
	return scanObject(s.db.QueryRow(ctx, `SELECT `+objectSelectCols+` FROM objects
		WHERE deleted_at IS NULL AND properties->>$1 = $2
		  AND tenant = ($3::jsonb)->0 AND namespace @> $3::jsonb AND (cardinality(readers) = 0 OR readers && $4::text[])
		LIMIT 1`,
		key, value, scope.Namespace, scope.Principals))
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
