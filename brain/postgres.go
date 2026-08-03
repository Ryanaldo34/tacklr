package brain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Row is satisfied by pgx.Row.
type Row interface {
	Scan(dest ...any) error
}

// Rows is a minimal subset of pgx.Rows for test fakes.
type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
}

// DBTX is the subset of pgx pool/conn used by PostgresStore.
type DBTX interface {
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// PostgresStore implements Store against the objects / object_kinds schema.
type PostgresStore struct {
	db DBTX
}

// NewPostgresStore wraps a pgx pool or connection.
func NewPostgresStore(db DBTX) (*PostgresStore, error) {
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
		// pgx may wrap; also handle empty
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return Object{}, fmt.Errorf("brain: scan object: %w", err)
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
