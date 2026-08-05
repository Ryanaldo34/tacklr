package brain

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
)

// PgxDB is satisfied by *pgx.Conn and *pgxpool.Pool.
type PgxDB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// PostgresStore implements Store against the objects / object_kinds schema.
type PostgresStore struct {
	db PgxDB
}

// NewPostgresStore wraps a pgx pool or connection.
func NewPostgresStore(db PgxDB) (*PostgresStore, error) {
	if db == nil {
		return nil, fmt.Errorf("brain: db is required")
	}
	return &PostgresStore{db: db}, nil
}

const objectSelectCols = `
	id, kind, title, summary, properties, content, content_type,
	parent_id, position, namespace_id, created_at, updated_at, deleted_at
`

// Get implements ObjectReader.
func (s *PostgresStore) Get(ctx context.Context, scope Scope, id uuid.UUID) (Object, error) {
	q := `SELECT ` + objectSelectCols + ` FROM objects WHERE id = $1 AND deleted_at IS NULL`
	args := []any{id}
	if scope.Namespace != nil {
		q += ` AND namespace_id = $2`
		args = append(args, *scope.Namespace)
	}
	return s.scanObject(s.db.QueryRow(ctx, q, args...))
}

// GetMany implements ObjectReader.
func (s *PostgresStore) GetMany(ctx context.Context, scope Scope, ids []uuid.UUID) ([]Object, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := `SELECT ` + objectSelectCols + ` FROM objects WHERE id = ANY($1) AND deleted_at IS NULL`
	args := []any{ids}
	if scope.Namespace != nil {
		q += ` AND namespace_id = $2`
		args = append(args, *scope.Namespace)
	}
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("brain: get many: %w", err)
	}
	defer rows.Close()
	byID := make(map[uuid.UUID]Object, len(ids))
	for rows.Next() {
		obj, err := s.scanObject(rows)
		if err != nil {
			return nil, err
		}
		byID[obj.ID] = obj
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("brain: get many: %w", err)
	}
	out := make([]Object, 0, len(byID))
	for _, id := range ids {
		if o, ok := byID[id]; ok {
			out = append(out, o)
		}
	}
	return out, nil
}

// ListChildren implements ObjectReader.
func (s *PostgresStore) ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]Object, error) {
	q := `SELECT ` + objectSelectCols + `
		FROM objects
		WHERE parent_id = $1 AND deleted_at IS NULL`
	args := []any{parentID}
	if scope.Namespace != nil {
		q += ` AND namespace_id = $2`
		args = append(args, *scope.Namespace)
	}
	q += ` ORDER BY position ASC NULLS LAST, id ASC`

	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("brain: list children: %w", err)
	}
	defer rows.Close()

	var out []Object
	for rows.Next() {
		obj, err := s.scanObject(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("brain: list children: %w", err)
	}
	return out, nil
}

// Put implements ObjectWriter (full-column upsert; clears soft-delete on revive).
func (s *PostgresStore) Put(ctx context.Context, obj Object) error {
	if err := requireObjectIdentity(obj); err != nil {
		return err
	}
	if obj.Properties == nil {
		obj.Properties = map[string]any{}
	}
	propsJSON, err := json.Marshal(obj.Properties)
	if err != nil {
		return fmt.Errorf("brain: marshal properties: %w", err)
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
			parent_id, position, embedding, namespace_id, created_at, updated_at, deleted_at
		) VALUES (
			$1, $2, $3, $4, $5::jsonb, $6, $7,
			$8, $9, $10::vector, $11, $12, $13, NULL
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
			namespace_id = EXCLUDED.namespace_id,
			updated_at = EXCLUDED.updated_at,
			deleted_at = NULL`
	if _, err := s.db.Exec(ctx, q,
		obj.ID, obj.Kind, obj.Title, obj.Summary, propsJSON, obj.Content, obj.ContentType,
		obj.ParentID, obj.Position, emb, obj.NamespaceID, obj.CreatedAt, obj.UpdatedAt,
	); err != nil {
		return fmt.Errorf("brain: put object: %w", err)
	}
	return nil
}

// SoftDelete implements ObjectWriter.
func (s *PostgresStore) SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("brain: object id is required")
	}
	q := `UPDATE objects SET deleted_at = $1, updated_at = $1 WHERE id = $2 AND deleted_at IS NULL`
	args := []any{time.Now().UTC(), id}
	if scope.Namespace != nil {
		q += ` AND namespace_id = $3`
		args = append(args, *scope.Namespace)
	}
	tag, err := s.db.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("brain: soft delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return nil
}

// PutKind implements KindWriter.
func (s *PostgresStore) PutKind(ctx context.Context, k ObjectKind) error {
	if strings.TrimSpace(k.Kind) == "" {
		return fmt.Errorf("brain: kind is required")
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
		return fmt.Errorf("brain: put kind: %w", err)
	}
	return nil
}

// GetKind implements KindReader.
func (s *PostgresStore) GetKind(ctx context.Context, kind string) (ObjectKind, error) {
	const q = `
		SELECT kind, description, is_part, is_parent, filterable_fields
		FROM object_kinds WHERE kind = $1`
	var k ObjectKind
	var desc *string
	var fields []byte
	err := s.db.QueryRow(ctx, q, kind).Scan(&k.Kind, &desc, &k.IsPart, &k.IsParent, &fields)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ObjectKind{}, fmt.Errorf("%w: kind %q", ErrNotFound, kind)
		}
		return ObjectKind{}, fmt.Errorf("brain: get kind: %w", err)
	}
	if desc != nil {
		k.Description = *desc
	}
	if len(fields) == 0 {
		k.FilterableFields = json.RawMessage("[]")
	} else {
		k.FilterableFields = json.RawMessage(fields)
	}
	return k, nil
}

// ListKinds implements KindReader.
func (s *PostgresStore) ListKinds(ctx context.Context) ([]ObjectKind, error) {
	const q = `
		SELECT kind, description, is_part, is_parent, filterable_fields
		FROM object_kinds ORDER BY kind ASC`
	rows, err := s.db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("brain: list kinds: %w", err)
	}
	defer rows.Close()

	var out []ObjectKind
	for rows.Next() {
		var k ObjectKind
		var desc *string
		var fields []byte
		if err := rows.Scan(&k.Kind, &desc, &k.IsPart, &k.IsParent, &fields); err != nil {
			return nil, fmt.Errorf("brain: list kinds: %w", err)
		}
		if desc != nil {
			k.Description = *desc
		}
		if len(fields) == 0 {
			k.FilterableFields = json.RawMessage("[]")
		} else {
			k.FilterableFields = json.RawMessage(fields)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("brain: list kinds: %w", err)
	}
	return out, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func (s *PostgresStore) scanObject(row scannable) (Object, error) {
	var (
		o           Object
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
		&o.NamespaceID,
		&o.CreatedAt,
		&o.UpdatedAt,
		&deletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Object{}, ErrNotFound
		}
		return Object{}, fmt.Errorf("brain: scan object: %w", err)
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
			return Object{}, fmt.Errorf("brain: properties json: %w", err)
		}
	}
	return o, nil
}

// SearchLexical implements PartSearcher using pg_textsearch BM25.
func (s *PostgresStore) SearchLexical(ctx context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" {
		return nil, nil
	}
	where, fargs, err := filterSQL(scope, filters, 2)
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

// SearchVector implements PartSearcher using pgvector cosine distance.
func (s *PostgresStore) SearchVector(ctx context.Context, scope Scope, embedding []float32, filters Filters, k int) ([]ScoredID, error) {
	if k <= 0 || len(embedding) == 0 {
		return nil, nil
	}
	where, fargs, err := filterSQL(scope, filters, 2)
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

// SearchTrigram implements PartSearcher using pg_trgm similarity.
func (s *PostgresStore) SearchTrigram(ctx context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error) {
	if k <= 0 || strings.TrimSpace(query) == "" {
		return nil, nil
	}
	where, fargs, err := filterSQL(scope, filters, 2)
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

func (s *PostgresStore) queryScored(ctx context.Context, q string, args []any, invertBM25 bool) ([]ScoredID, error) {
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("brain: search query: %w", err)
	}
	defer rows.Close()
	var out []ScoredID
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
			return nil, fmt.Errorf("brain: search scan: %w", err)
		}
		if invertBM25 {
			score = -score
		}
		item := ScoredID{ID: id, Score: score, UpdatedAt: updated, ParentID: parentID}
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
		return nil, fmt.Errorf("brain: search rows: %w", err)
	}
	return out, nil
}

func sanitizeJSONKey(k string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return -1
	}, k)
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
