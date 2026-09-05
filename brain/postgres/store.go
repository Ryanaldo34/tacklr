package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ryanaldo34/tacklr/brain"
)

// PgxDB is satisfied by *pgx.Conn and *pgxpool.Pool.
type PgxDB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

var (
	_ brain.Store        = (*Store)(nil)
	_ brain.ObjectWriter = (*Store)(nil)
	_ brain.ObjectLister = (*Store)(nil)
	_ brain.KindWriter   = (*Store)(nil)
	_ PgxDB              = tracedDB{}
)

// Store is the optional Postgres brain.Store. Hosts inject it into NewEngine.
// Call Setup once per database. Setup does not migrate an existing vector column.
type Store struct {
	// EmbeddingDim is the pgvector size Setup uses. Zero means DefaultEmbeddingDim.
	EmbeddingDim int
	db           PgxDB
}

// New wraps a pgx pool or connection. Query/Exec emit otelpgx
// client spans as children of ctx (after telemetry.Init).
func New(db PgxDB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres: db is required")
	}
	return &Store{db: newTracedDB(db)}, nil
}

const objectSelectCols = `
	id, kind, title, summary, properties, content, content_type,
	parent_id, position, namespace, created_at, updated_at, deleted_at
`

// Get implements ObjectReader.
func (s *Store) Get(ctx context.Context, scope brain.Scope, id uuid.UUID) (brain.Object, error) {
	q := `SELECT ` + objectSelectCols + ` FROM objects WHERE id = $1 AND deleted_at IS NULL`
	args := []any{id}
	q, args = appendNamespaceSQL(q, args, scope)
	return s.scanObject(s.db.QueryRow(ctx, q, args...))
}

// GetMany implements ObjectReader.
func (s *Store) GetMany(ctx context.Context, scope brain.Scope, ids []uuid.UUID) ([]brain.Object, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := `SELECT ` + objectSelectCols + ` FROM objects WHERE id = ANY($1) AND deleted_at IS NULL`
	args := []any{ids}
	q, args = appendNamespaceSQL(q, args, scope)
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: get many: %w", err)
	}
	defer rows.Close()
	byID := make(map[uuid.UUID]brain.Object, len(ids))
	for rows.Next() {
		obj, err := s.scanObject(rows)
		if err != nil {
			return nil, err
		}
		byID[obj.ID] = obj
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: get many: %w", err)
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
	q := `SELECT ` + objectSelectCols + `
		FROM objects
		WHERE parent_id = $1 AND deleted_at IS NULL`
	args := []any{parentID}
	q, args = appendNamespaceSQL(q, args, scope)
	q += ` ORDER BY position ASC NULLS LAST, id ASC`
	return s.scanObjectRows(ctx, q, args, "list children", 0)
}

// Put implements ObjectWriter (full-column upsert; clears soft-delete on revive).
func (s *Store) Put(ctx context.Context, obj brain.Object) error {
	if err := brain.ValidateObjectIdentity(obj); err != nil {
		return err
	}
	if obj.Properties == nil {
		obj.Properties = map[string]any{}
	}
	propsJSON, err := json.Marshal(obj.Properties)
	if err != nil {
		return fmt.Errorf("postgres: marshal properties: %w", err)
	}
	var emb any
	if len(obj.Embedding) > 0 {
		emb = formatVectorLiteral(obj.Embedding)
	}
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	if obj.UpdatedAt.IsZero() {
		obj.UpdatedAt = now
	}
	const q = `
		INSERT INTO objects (
			id, kind, title, summary, properties, content, content_type,
			parent_id, position, embedding, namespace, created_at, updated_at, deleted_at
		) VALUES (
			$1, $2, $3, $4, $5::jsonb, $6, $7,
			$8, $9, $10::vector, $11::jsonb, $12, $13, NULL
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
			namespace = EXCLUDED.namespace,
			updated_at = EXCLUDED.updated_at,
			deleted_at = NULL`
	nsJSON, _ := obj.Namespace.Value()
	if _, err := s.db.Exec(ctx, q,
		obj.ID, obj.Kind, obj.Title, obj.Summary, propsJSON, obj.Content, obj.ContentType,
		obj.ParentID, obj.Position, emb, nsJSON, obj.CreatedAt, obj.UpdatedAt,
	); err != nil {
		return fmt.Errorf("postgres: put object: %w", err)
	}
	return nil
}

// SoftDelete implements ObjectWriter.
func (s *Store) SoftDelete(ctx context.Context, scope brain.Scope, id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: object id is required", brain.ErrInvalid)
	}
	q := `UPDATE objects SET deleted_at = $1, updated_at = $1 WHERE id = $2 AND deleted_at IS NULL`
	args := []any{time.Now().UTC(), id}
	q, args = appendNamespaceSQL(q, args, scope)
	tag, err := s.db.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("postgres: soft delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", brain.ErrNotFound, id)
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

// GetKind implements KindReader.
func (s *Store) GetKind(ctx context.Context, kind string) (brain.ObjectKind, error) {
	const q = `
		SELECT kind, description, is_part, is_parent, filterable_fields
		FROM object_kinds WHERE kind = $1`
	var k brain.ObjectKind
	var desc *string
	var fields []byte
	err := s.db.QueryRow(ctx, q, kind).Scan(&k.Kind, &desc, &k.IsPart, &k.IsParent, &fields)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return brain.ObjectKind{}, fmt.Errorf("%w: kind %q", brain.ErrNotFound, kind)
		}
		return brain.ObjectKind{}, fmt.Errorf("postgres: get kind: %w", err)
	}
	if desc != nil {
		k.Description = *desc
	}
	k.FilterableFields = json.RawMessage(fields)
	return k, nil
}

// ListKinds implements KindReader.
func (s *Store) ListKinds(ctx context.Context) ([]brain.ObjectKind, error) {
	const q = `
		SELECT kind, description, is_part, is_parent, filterable_fields
		FROM object_kinds ORDER BY kind ASC`
	rows, err := s.db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: list kinds: %w", err)
	}
	defer rows.Close()

	var out []brain.ObjectKind
	for rows.Next() {
		var k brain.ObjectKind
		var desc *string
		var fields []byte
		if err := rows.Scan(&k.Kind, &desc, &k.IsPart, &k.IsParent, &fields); err != nil {
			return nil, fmt.Errorf("postgres: list kinds: %w", err)
		}
		if desc != nil {
			k.Description = *desc
		}
		k.FilterableFields = json.RawMessage(fields)
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list kinds: %w", err)
	}
	return out, nil
}

// ListByKind implements ObjectLister (first-class objects only).
func (s *Store) ListByKind(ctx context.Context, scope brain.Scope, kind string, limit int) ([]brain.Object, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" || limit <= 0 {
		return nil, nil
	}
	q := `SELECT ` + objectSelectCols + `
		FROM objects
		WHERE kind = $1 AND deleted_at IS NULL AND parent_id IS NULL`
	args := []any{kind}
	q, args = appendNamespaceSQL(q, args, scope)
	q += fmt.Sprintf(` ORDER BY title ASC NULLS LAST, id ASC LIMIT $%d`, len(args)+1)
	args = append(args, limit)
	return s.scanObjectRows(ctx, q, args, "list by kind", limit)
}

// GetByProperty implements ObjectLister.
func (s *Store) GetByProperty(ctx context.Context, scope brain.Scope, key, value string) (brain.Object, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return brain.Object{}, fmt.Errorf("postgres: property key is required")
	}
	q := `SELECT ` + objectSelectCols + `
		FROM objects
		WHERE deleted_at IS NULL AND properties->>$1 = $2`
	args := []any{key, value}
	q, args = appendNamespaceSQL(q, args, scope)
	q += ` LIMIT 1`
	return s.scanObject(s.db.QueryRow(ctx, q, args...))
}

// KindsWithObjects implements ObjectLister.
func (s *Store) KindsWithObjects(ctx context.Context, scope brain.Scope) ([]string, error) {
	q := `SELECT DISTINCT kind FROM objects WHERE deleted_at IS NULL AND parent_id IS NULL`
	var args []any
	q, args = appendNamespaceSQL(q, args, scope)
	q += ` ORDER BY kind ASC`
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: kinds with objects: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("postgres: kinds with objects: %w", err)
		}
		if k != "" {
			out = append(out, k)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: kinds with objects: %w", err)
	}
	return out, nil
}

func (s *Store) scanObjectRows(ctx context.Context, q string, args []any, label string, capHint int) ([]brain.Object, error) {
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: %w", label, err)
	}
	defer rows.Close()
	out := make([]brain.Object, 0, capHint)
	for rows.Next() {
		obj, err := s.scanObject(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: %s: %w", label, err)
	}
	return out, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func (s *Store) scanObject(row scannable) (brain.Object, error) {
	var (
		o           brain.Object
		title       *string
		summary     *string
		propsRaw    []byte
		content     *string
		contentType *string
		parentID    *uuid.UUID
		position    *int32
		deletedAt   *time.Time
	)
	err := row.Scan(
		&o.ID,
		&o.Kind,
		&title,
		&summary,
		&propsRaw,
		&content,
		&contentType,
		&parentID,
		&position,
		&o.Namespace,
		&o.CreatedAt,
		&o.UpdatedAt,
		&deletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return brain.Object{}, brain.ErrNotFound
		}
		return brain.Object{}, fmt.Errorf("postgres: scan object: %w", err)
	}
	if title != nil {
		o.Title = *title
	}
	if summary != nil {
		o.Summary = *summary
	}
	if content != nil {
		o.Content = *content
	}
	if contentType != nil {
		o.ContentType = *contentType
	}
	o.ParentID = parentID
	if position != nil {
		p := int(*position)
		o.Position = &p
	}
	o.DeletedAt = deletedAt
	o.Properties = map[string]any{}
	if len(propsRaw) > 0 {
		if err := json.Unmarshal(propsRaw, &o.Properties); err != nil {
			return brain.Object{}, fmt.Errorf("postgres: properties json: %w", err)
		}
	}
	return o, nil
}

// SearchLexical implements PartSearcher using pg_textsearch BM25 over parts
// (parent_id set). search_text is title, summary, and content.
func (s *Store) SearchLexical(ctx context.Context, scope brain.Scope, query string, filters brain.Filter, k int) ([]brain.ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" {
		return nil, nil
	}
	where, fargs, err := brain.FilterSQL(scope, filters, 2)
	if err != nil {
		return nil, err
	}
	args := append([]any{query}, fargs...)
	limitPos := len(args) + 1
	args = append(args, k)
	q := fmt.Sprintf(`
		SELECT id, title, content, parent_id, position, updated_at,
		       search_text <@> to_bm25query($1, 'idx_objects_bm25') AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL%s
		ORDER BY search_text <@> to_bm25query($1, 'idx_objects_bm25')
		LIMIT $%d`, where, limitPos)
	return s.queryScored(ctx, q, args, true)
}

// SearchVector implements PartSearcher using pgvector cosine distance over parts.
func (s *Store) SearchVector(ctx context.Context, scope brain.Scope, embedding []float32, filters brain.Filter, k int) ([]brain.ScoredID, error) {
	if k <= 0 || len(embedding) == 0 {
		return nil, nil
	}
	where, fargs, err := brain.FilterSQL(scope, filters, 2)
	if err != nil {
		return nil, err
	}
	args := append([]any{formatVectorLiteral(embedding)}, fargs...)
	limitPos := len(args) + 1
	args = append(args, k)
	q := fmt.Sprintf(`
		SELECT id, title, content, parent_id, position, updated_at,
		       1 - (embedding <=> $1::vector) AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND embedding IS NOT NULL%s
		ORDER BY embedding <=> $1::vector
		LIMIT $%d`, where, limitPos)
	return s.queryScored(ctx, q, args, false)
}

// SearchTrigram implements PartSearcher using pg_trgm similarity over parts.
func (s *Store) SearchTrigram(ctx context.Context, scope brain.Scope, query string, filters brain.Filter, k int) ([]brain.ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" {
		return nil, nil
	}
	where, fargs, err := brain.FilterSQL(scope, filters, 2)
	if err != nil {
		return nil, err
	}
	args := append([]any{query}, fargs...)
	limitPos := len(args) + 1
	args = append(args, k)
	q := fmt.Sprintf(`
		SELECT id, title, content, parent_id, position, updated_at,
		       GREATEST(similarity(coalesce(title,''), $1), similarity(coalesce(content,''), $1)) AS score
		FROM objects
		WHERE deleted_at IS NULL AND parent_id IS NOT NULL
		  AND (
		    similarity(coalesce(title,''), $1) > 0.3
		    OR similarity(coalesce(content,''), $1) > 0.3
		  )%s
		ORDER BY score DESC
		LIMIT $%d`, where, limitPos)
	return s.queryScored(ctx, q, args, false)
}

func (s *Store) queryScored(ctx context.Context, q string, args []any, invertBM25 bool) ([]brain.ScoredID, error) {
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: search query: %w", err)
	}
	defer rows.Close()
	var out []brain.ScoredID
	for rows.Next() {
		var (
			id       uuid.UUID
			title    *string
			content  *string
			parentID *uuid.UUID
			position *int32
			updated  time.Time
			score    float64
		)
		if err := rows.Scan(&id, &title, &content, &parentID, &position, &updated, &score); err != nil {
			return nil, fmt.Errorf("postgres: search scan: %w", err)
		}
		if invertBM25 {
			score = -score
		}
		item := brain.ScoredID{ID: id, Score: score, UpdatedAt: updated, ParentID: parentID}
		if title != nil {
			item.Title = *title
		}
		if content != nil {
			item.Content = *content
		}
		if position != nil {
			p := int(*position)
			item.Position = &p
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: search rows: %w", err)
	}
	return out, nil
}

func appendNamespaceSQL(q string, args []any, scope brain.Scope) (string, []any) {
	if scope.Namespace.Empty() {
		return q, args
	}
	raw, _ := scope.Namespace.Value()
	q += fmt.Sprintf(` AND namespace @> $%d::jsonb`, len(args)+1)
	return q, append(args, raw)
}

func formatVectorLiteral(v []float32) string {
	var b strings.Builder
	b.Grow(2 + len(v)*8)
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}
