package brain_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ryanaldo34/tacklr/brain"
)

// stubRow implements brain.Row for PostgresStore without a database.
type stubRow struct {
	err  error
	vals []any
}

func (r stubRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.vals) {
		return errors.New("scan arity mismatch")
	}
	for i, d := range dest {
		if err := assignScan(d, r.vals[i]); err != nil {
			return err
		}
	}
	return nil
}

type stubRows struct {
	rows []stubRow
	i    int
	err  error
}

func (r *stubRows) Close()     {}
func (r *stubRows) Err() error { return r.err }
func (r *stubRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}
func (r *stubRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("scan before next")
	}
	return r.rows[r.i-1].Scan(dest...)
}

type stubDB struct {
	row      stubRow
	rows     *stubRows
	qErr     error
	lastSQL  string
	lastArgs []any
}

func (d *stubDB) QueryRow(_ context.Context, sql string, args ...any) brain.Row {
	d.lastSQL = sql
	d.lastArgs = args
	return d.row
}
func (d *stubDB) Query(_ context.Context, sql string, args ...any) (brain.Rows, error) {
	d.lastSQL = sql
	d.lastArgs = args
	if d.qErr != nil {
		return nil, d.qErr
	}
	if d.rows == nil {
		return &stubRows{}, nil
	}
	cp := *d.rows
	cp.i = 0
	return &cp, nil
}

func assignScan(dest, src any) error {
	switch d := dest.(type) {
	case *uuid.UUID:
		*d = src.(uuid.UUID)
	case **uuid.UUID:
		if src == nil {
			*d = nil
			return nil
		}
		switch v := src.(type) {
		case uuid.UUID:
			cp := v
			*d = &cp
		case *uuid.UUID:
			*d = v
		default:
			return errors.New("uuid type")
		}
	case *string:
		*d = src.(string)
	case **string:
		if src == nil {
			*d = nil
			return nil
		}
		switch v := src.(type) {
		case string:
			cp := v
			*d = &cp
		case *string:
			*d = v
		default:
			return errors.New("string type")
		}
	case *[]byte:
		if src == nil {
			*d = nil
			return nil
		}
		*d = src.([]byte)
	case *int32:
		*d = src.(int32)
	case **int32:
		if src == nil {
			*d = nil
			return nil
		}
		switch v := src.(type) {
		case int32:
			cp := v
			*d = &cp
		case *int32:
			*d = v
		default:
			return errors.New("int32 type")
		}
	case *time.Time:
		*d = src.(time.Time)
	case **time.Time:
		if src == nil {
			*d = nil
			return nil
		}
		switch v := src.(type) {
		case time.Time:
			cp := v
			*d = &cp
		case *time.Time:
			*d = v
		default:
			return errors.New("time type")
		}
	case *bool:
		*d = src.(bool)
	case *float64:
		switch v := src.(type) {
		case float64:
			*d = v
		case float32:
			*d = float64(v)
		case int:
			*d = float64(v)
		default:
			return errors.New("float64 type")
		}
	default:
		return errors.New("unsupported scan dest")
	}
	return nil
}

func objectScanVals(o brain.Object) []any {
	var title, summary, content, contentType *string
	if o.Title != "" {
		s := o.Title
		title = &s
	}
	if o.Summary != "" {
		s := o.Summary
		summary = &s
	}
	if o.Content != "" {
		s := o.Content
		content = &s
	}
	if o.ContentType != "" {
		s := o.ContentType
		contentType = &s
	}
	props, _ := json.Marshal(o.Properties)
	if o.Properties == nil {
		props = []byte("{}")
	}
	var pos *int32
	if o.Position != nil {
		p := int32(*o.Position)
		pos = &p
	}
	var parent any
	if o.ParentID != nil {
		parent = *o.ParentID
	}
	return []any{
		o.ID, o.Kind, title, summary, props, content, contentType,
		parent, pos, o.NamespaceID, o.CreatedAt, o.UpdatedAt, o.DeletedAt,
	}
}

// TestPostgresStore_GetAndChildren is the SQL adapter outcome against a stub DB:
// scoped Get and ListChildren map rows into Object values.
func TestPostgresStore_GetAndChildren(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	parentID := uuid.New()
	childID := uuid.New()
	now := time.Now().UTC()
	pos := 1
	parent := brain.Object{
		ID: parentID, Kind: "Document", Title: "P", Content: "body",
		NamespaceID: ns, CreatedAt: now, UpdatedAt: now,
		Properties: map[string]any{"k": "v"},
	}
	child := brain.Object{
		ID: childID, Kind: "Chunk", Title: "c1", NamespaceID: ns,
		ParentID: &parentID, Position: &pos, CreatedAt: now, UpdatedAt: now,
	}

	db := &stubDB{row: stubRow{vals: objectScanVals(parent)}}
	store, err := brain.NewPostgresStore(db)
	if err != nil {
		t.Fatal(err)
	}

	got, err := store.Get(ctx, brain.Scope{Namespace: &ns}, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != parentID || got.Title != "P" || got.Content != "body" || got.Properties["k"] != "v" {
		t.Fatalf("get: %+v", got)
	}

	db.rows = &stubRows{rows: []stubRow{{vals: objectScanVals(child)}}}
	kids, err := store.ListChildren(ctx, brain.Scope{Namespace: &ns}, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1 || kids[0].ID != childID || kids[0].Position == nil || *kids[0].Position != 1 {
		t.Fatalf("children: %+v", kids)
	}
}

// TestPostgresStore_Kinds is the object_kinds registry adapter outcome.
func TestPostgresStore_Kinds(t *testing.T) {
	ctx := context.Background()
	desc := "docs"
	fields := []byte(`[]`)
	db := &stubDB{
		row: stubRow{vals: []any{"Document", &desc, false, true, fields}},
		rows: &stubRows{rows: []stubRow{
			{vals: []any{"Chunk", (*string)(nil), true, false, []byte(nil)}},
			{vals: []any{"Document", &desc, false, true, fields}},
		}},
	}
	store, err := brain.NewPostgresStore(db)
	if err != nil {
		t.Fatal(err)
	}

	k, err := store.GetKind(ctx, "Document")
	if err != nil {
		t.Fatal(err)
	}
	if k.Kind != "Document" || !k.IsParent || k.Description != "docs" {
		t.Fatalf("get kind: %+v", k)
	}

	all, err := store.ListKinds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].Kind != "Chunk" || !all[0].IsPart {
		t.Fatalf("list kinds: %+v", all)
	}
}

// TestPostgresStore_GetNotFound maps pgx.ErrNoRows to brain.ErrNotFound.
func TestPostgresStore_GetNotFound(t *testing.T) {
	store, err := brain.NewPostgresStore(&stubDB{row: stubRow{err: pgx.ErrNoRows}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(context.Background(), brain.Scope{}, uuid.New())
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	_, err = store.GetKind(context.Background(), "nope")
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("kind: %v", err)
	}
}

// TestNewPostgresStore_requiresDB is the constructor guard outcome.
func TestNewPostgresStore_requiresDB(t *testing.T) {
	if _, err := brain.NewPostgresStore(nil); err == nil {
		t.Fatal("want error")
	}
}

// TestPostgresStore_queryFailures surface store errors to the engine boundary.
func TestPostgresStore_queryFailures(t *testing.T) {
	ctx := context.Background()
	store, err := brain.NewPostgresStore(&stubDB{
		row:  stubRow{err: errors.New("db down")},
		qErr: errors.New("query failed"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, brain.Scope{}, uuid.New()); err == nil {
		t.Fatal("get must surface scan error")
	}
	if _, err := store.ListChildren(ctx, brain.Scope{}, uuid.New()); err == nil {
		t.Fatal("list children must surface query error")
	}
	if _, err := store.GetKind(ctx, "X"); err == nil {
		t.Fatal("get kind must surface scan error")
	}
	if _, err := store.ListKinds(ctx); err == nil {
		t.Fatal("list kinds must surface query error")
	}
}

// TestPostgresStore_searchChannels builds BM25/vector/trigram SQL with filters
// and maps scored rows (stub DB; no real pg_textsearch required).
func TestPostgresStore_searchChannels(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	partID := uuid.New()
	parentID := uuid.New()
	title := "chunk"
	content := "oauth body"
	pos := int32(1)
	updated := time.Now().UTC()
	db := &stubDB{
		rows: &stubRows{rows: []stubRow{{
			vals: []any{partID, title, content, parentID, pos, updated, -1.5},
		}}},
	}
	store, err := brain.NewPostgresStore(db)
	if err != nil {
		t.Fatal(err)
	}
	filters := brain.Filters{"kind": "Chunk", "stage": "open"}
	scope := brain.Scope{Namespace: &ns}

	lex, err := store.SearchLexical(ctx, scope, "oauth", filters, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(lex) != 1 || lex[0].ID != partID || lex[0].Score != 1.5 {
		t.Fatalf("lexical invert bm25: %+v", lex)
	}
	if !strings.Contains(db.lastSQL, "to_bm25query") || !strings.Contains(db.lastSQL, "namespace_id") {
		t.Fatalf("lexical sql: %s", db.lastSQL)
	}
	if !strings.Contains(db.lastSQL, "properties->>'stage'") {
		t.Fatalf("property filter missing: %s", db.lastSQL)
	}

	db.rows = &stubRows{rows: []stubRow{{
		vals: []any{partID, title, content, parentID, pos, updated, 0.9},
	}}}
	vec, err := store.SearchVector(ctx, scope, []float32{0.1, 0.2}, filters, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) != 1 || vec[0].Score != 0.9 {
		t.Fatalf("vector: %+v", vec)
	}
	if !strings.Contains(db.lastSQL, "embedding") {
		t.Fatalf("vector sql: %s", db.lastSQL)
	}

	db.rows = &stubRows{rows: []stubRow{{
		vals: []any{partID, title, content, parentID, pos, updated, 0.5},
	}}}
	tri, err := store.SearchTrigram(ctx, scope, "oauth", nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(tri) != 1 {
		t.Fatalf("trigram: %+v", tri)
	}
	if !strings.Contains(db.lastSQL, "similarity") {
		t.Fatalf("trigram sql: %s", db.lastSQL)
	}

	// Date filters expand WHERE.
	db.rows = &stubRows{rows: []stubRow{}}
	_, err = store.SearchLexical(ctx, scope, "q", brain.Filters{
		"updated_after":  "2024-01-01T00:00:00Z",
		"updated_before": "2025-01-01T00:00:00Z",
		"title":          "T",
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(db.lastSQL, "updated_at >=") || !strings.Contains(db.lastSQL, "title =") {
		t.Fatalf("date/title filters: %s", db.lastSQL)
	}
}
