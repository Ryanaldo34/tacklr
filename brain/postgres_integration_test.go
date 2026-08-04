package brain_test

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ryanaldo34/tacklr/brain"
)

// Pre-built via Makefile/CI: docker build -f brain/testdata/Dockerfile.postgres -t tacklr-pg-brain:test
const brainPgImage = "tacklr-pg-brain:test"

var (
	pgOnce  sync.Once
	pgPool  *pgxpool.Pool
	pgStart error
	pgSkip  string
)

// TestPostgresStore_liveRetrievalChannels is the real-Postgres outcome for
// scoped reads, ordered children, BM25 lexical, dense vector, and trigram search.
func TestPostgresStore_liveRetrievalChannels(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}

	ctx := context.Background()
	pool := sharedPostgresPool(t)
	mustExec(t, pool, `TRUNCATE objects, object_kinds CASCADE`)
	store, err := brain.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	nsA := uuid.New()
	nsB := uuid.New()
	parentID := uuid.New()
	chunkOAuth := uuid.New()
	chunkOther := uuid.New()
	chunkDeleted := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)

	mustExec(t, pool, `
		INSERT INTO object_kinds (kind, description, is_part, is_parent, filterable_fields)
		VALUES
			('Document', 'parent docs', false, true, '[]'),
			('Chunk', 'parts', true, false, '[{"name":"stage"}]')
	`)
	mustExec(t, pool, `
		INSERT INTO objects (id, kind, title, summary, properties, content, parent_id, position, embedding, namespace_id, created_at, updated_at)
		VALUES
			($1, 'Document', 'OAuth Guide', 'parent', '{}', NULL, NULL, NULL, NULL, $2, $3, $3),
			($4, 'Chunk', 'pkce flow', '', '{"stage":"open"}', 'oauth pkce implementation details', $1, 1, '[1,0,0]'::vector, $2, $3, $3),
			($5, 'Chunk', 'unrelated', '', '{}', 'cooking recipes and pasta', $1, 2, '[0,1,0]'::vector, $2, $3, $3),
			($6, 'Chunk', 'gone', '', '{}', 'soft deleted oauth', $1, 3, '[1,0,0]'::vector, $2, $3, $3)
	`, parentID, nsA, now, chunkOAuth, chunkOther, chunkDeleted)
	mustExec(t, pool, `UPDATE objects SET deleted_at = $1 WHERE id = $2`, now, chunkDeleted)
	docB := uuid.New()
	mustExec(t, pool, `
		INSERT INTO objects (id, kind, title, namespace_id, created_at, updated_at)
		VALUES ($1, 'Document', 'other ns', $2, $3, $3)
	`, docB, nsB, now)

	scopeA := brain.Scope{Namespace: &nsA}

	got, err := store.Get(ctx, scopeA, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != parentID || got.Title != "OAuth Guide" || got.NamespaceID != nsA {
		t.Fatalf("get parent: %+v", got)
	}

	_, err = store.Get(ctx, scopeA, docB)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("docB must be invisible under nsA: %v", err)
	}
	_, err = store.Get(ctx, brain.Scope{Namespace: &nsB}, chunkOAuth)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("chunk must be invisible outside its namespace: %v", err)
	}
	_, err = store.Get(ctx, scopeA, chunkDeleted)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-deleted chunk must be ErrNotFound: %v", err)
	}

	kids, err := store.ListChildren(ctx, scopeA, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 2 || kids[0].ID != chunkOAuth || kids[1].ID != chunkOther {
		t.Fatalf("children ordered by position (deleted excluded): %+v", kids)
	}
	if kids[0].Position == nil || *kids[0].Position != 1 {
		t.Fatalf("position: %+v", kids[0])
	}

	kind, err := store.GetKind(ctx, "Document")
	if err != nil {
		t.Fatal(err)
	}
	if kind.Kind != "Document" || !kind.IsParent || kind.Description != "parent docs" {
		t.Fatalf("get kind: %+v", kind)
	}
	kinds, err := store.ListKinds(ctx)
	if err != nil || len(kinds) != 2 {
		t.Fatalf("list kinds: %+v err=%v", kinds, err)
	}

	// BM25 lexical: oauth terms should rank the oauth chunk over pasta.
	lex, err := store.SearchLexical(ctx, scopeA, "oauth pkce", brain.Filters{"kind": "Chunk"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lex) == 0 || lex[0].ID != chunkOAuth {
		t.Fatalf("lexical top hit: %+v", lex)
	}
	if lex[0].ParentID == nil || *lex[0].ParentID != parentID {
		t.Fatalf("lexical parent: %+v", lex[0])
	}
	// Score invert: pg_textsearch returns negative; store flips so higher is better.
	if lex[0].Score <= 0 {
		t.Fatalf("lexical score after invert should be > 0: %+v", lex[0])
	}

	// Property filter on lexical channel.
	lexOpen, err := store.SearchLexical(ctx, scopeA, "oauth", brain.Filters{"stage": "open"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lexOpen) != 1 || lexOpen[0].ID != chunkOAuth {
		t.Fatalf("lexical property filter: %+v", lexOpen)
	}

	// Vector: query near [1,0,0] should rank oauth chunk first.
	vec, err := store.SearchVector(ctx, scopeA, []float32{0.95, 0.05, 0}, brain.Filters{"kind": "Chunk"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) == 0 || vec[0].ID != chunkOAuth {
		t.Fatalf("vector top hit: %+v", vec)
	}
	if vec[0].ParentID == nil || *vec[0].ParentID != parentID {
		t.Fatalf("vector parent: %+v", vec[0])
	}

	vecOpen, err := store.SearchVector(ctx, scopeA, []float32{0.5, 0.5, 0}, brain.Filters{"stage": "open"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vecOpen) != 1 || vecOpen[0].ID != chunkOAuth {
		t.Fatalf("vector property filter: %+v", vecOpen)
	}

	tri, err := store.SearchTrigram(ctx, scopeA, "pkce flow", nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(tri) == 0 || tri[0].ID != chunkOAuth {
		t.Fatalf("trigram top hit: %+v", tri)
	}

	vecB, err := store.SearchVector(ctx, brain.Scope{Namespace: &nsB}, []float32{1, 0, 0}, nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vecB) != 0 {
		t.Fatalf("nsB must have no vector parts: %+v", vecB)
	}
	lexB, err := store.SearchLexical(ctx, brain.Scope{Namespace: &nsB}, "oauth", nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lexB) != 0 {
		t.Fatalf("nsB must have no lexical parts: %+v", lexB)
	}

	// Date / title / list filters on live SQL (same FilterPlan as Memory).
	after := now.Add(-time.Hour).Format(time.RFC3339)
	before := now.Add(time.Hour).Format(time.RFC3339)
	lexDate, err := store.SearchLexical(ctx, scopeA, "oauth", brain.Filters{
		"updated_after":  after,
		"updated_before": before,
		"created_after":  after,
		"created_before": before,
		"title":          "pkce flow",
	}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lexDate) != 1 || lexDate[0].ID != chunkOAuth {
		t.Fatalf("date/title filters: %+v", lexDate)
	}
	lexList, err := store.SearchLexical(ctx, scopeA, "oauth", brain.Filters{
		"stage": []any{"open", "closed"},
	}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lexList) != 1 || lexList[0].ID != chunkOAuth {
		t.Fatalf("list property filter: %+v", lexList)
	}
}

// TestPostgresStore_liveEmptyChannels is the zero-candidate outcome for
// empty query / empty embedding / k<=0 without hitting the database error path.
func TestPostgresStore_liveEmptyChannels(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}
	ctx := context.Background()
	pool := sharedPostgresPool(t)
	store, err := brain.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{}
	if got, err := store.SearchVector(ctx, scope, nil, nil, 10); err != nil || got != nil {
		t.Fatalf("empty embedding: got=%v err=%v", got, err)
	}
	if got, err := store.SearchTrigram(ctx, scope, "", nil, 10); err != nil || got != nil {
		t.Fatalf("empty query: got=%v err=%v", got, err)
	}
	if got, err := store.SearchLexical(ctx, scope, "x", nil, 0); err != nil || got != nil {
		t.Fatalf("k=0 lexical: got=%v err=%v", got, err)
	}
	if got, err := store.SearchLexical(ctx, scope, "", nil, 10); err != nil || got != nil {
		t.Fatalf("empty lexical query: got=%v err=%v", got, err)
	}
}

// sharedPostgresPool starts one container for the package process.
// Requires image tacklr-pg-brain:test (make brain-pg-image / CI prepare step).
// Tests that need a clean slate TRUNCATE their tables.
func sharedPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pgOnce.Do(func() {
		_, thisFile, _, ok := runtime.Caller(0)
		if !ok {
			pgStart = errors.New("runtime.Caller failed")
			return
		}
		schemaPath := filepath.Join(filepath.Dir(thisFile), "testdata", "schema_pgvector.sql")

		ctr, err := postgres.Run(ctx, brainPgImage,
			postgres.WithDatabase("brain"),
			postgres.WithUsername("brain"),
			postgres.WithPassword("brain"),
			postgres.WithInitScripts(schemaPath),
			postgres.BasicWaitStrategies(),
			postgres.WithSQLDriver("pgx"),
		)
		if err != nil {
			pgSkip = err.Error() + " (build image: make brain-pg-image)"
			return
		}
		// Keep container for process lifetime; Ryuk reaps on exit.
		connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			pgStart = err
			_ = ctr.Terminate(ctx)
			return
		}
		pool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			pgStart = err
			_ = ctr.Terminate(ctx)
			return
		}
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_extension WHERE extname IN ('vector', 'pg_trgm', 'pg_textsearch')
		`).Scan(&n); err != nil || n != 3 {
			pgStart = errors.New("expected vector, pg_trgm, and pg_textsearch extensions")
			pool.Close()
			_ = ctr.Terminate(ctx)
			return
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_objects_bm25'`).Scan(&n); err != nil || n != 1 {
			pgStart = errors.New("bm25 index idx_objects_bm25 missing")
			pool.Close()
			_ = ctr.Terminate(ctx)
			return
		}
		pgPool = pool
	})
	if pgSkip != "" {
		t.Skipf("postgres container unavailable (need Docker API runtime): %s", pgSkip)
	}
	if pgStart != nil {
		t.Fatal(pgStart)
	}
	if pgPool == nil {
		t.Fatal("postgres pool not initialized")
	}
	return pgPool
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec: %v\nsql: %s", err, sql)
	}
}
