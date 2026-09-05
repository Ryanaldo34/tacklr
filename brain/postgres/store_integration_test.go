package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
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
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}

	nsA := mustNS(t, "org", "a")
	nsB := mustNS(t, "org", "b")
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
		INSERT INTO objects (id, kind, title, summary, properties, content, parent_id, position, embedding, namespace, created_at, updated_at)
		VALUES
			($1, 'Document', 'OAuth Guide', 'parent', '{"vfs_path":"/engram/document/oauth.md"}', NULL, NULL, NULL, NULL, $2, $3, $3),
			($4, 'Chunk', 'pkce flow', '', '{"stage":"open"}', 'oauth pkce implementation details', $1, 1, '[1,0,0]'::vector, $2, $3, $3),
			($5, 'Chunk', 'unrelated', '', '{}', 'cooking recipes and pasta', $1, 2, '[0,1,0]'::vector, $2, $3, $3),
			($6, 'Chunk', 'gone', '', '{}', 'soft deleted oauth', $1, 3, '[1,0,0]'::vector, $2, $3, $3)
	`, parentID, nsA, now, chunkOAuth, chunkOther, chunkDeleted)
	mustExec(t, pool, `UPDATE objects SET deleted_at = $1 WHERE id = $2`, now, chunkDeleted)
	docB := uuid.New()
	mustExec(t, pool, `
		INSERT INTO objects (id, kind, title, namespace, created_at, updated_at)
		VALUES ($1, 'Document', 'other ns', $2, $3, $3)
	`, docB, nsB, now)

	scopeA := brain.Scope{Namespace: nsA}

	listed, err := store.ListByKind(ctx, scopeA, "Document", 10)
	if err != nil || len(listed) != 1 || listed[0].ID != parentID {
		t.Fatalf("list by kind: %+v err=%v", listed, err)
	}
	byProp, err := store.GetByProperty(ctx, scopeA, brain.PropVFSPath, "/engram/document/oauth.md")
	if err != nil || byProp.ID != parentID {
		t.Fatalf("get by vfs_path: %+v err=%v", byProp, err)
	}
	inUse, err := store.KindsWithObjects(ctx, scopeA)
	if err != nil || len(inUse) != 1 || inUse[0] != "Document" {
		t.Fatalf("kinds with objects: %v err=%v", inUse, err)
	}

	got, err := store.Get(ctx, scopeA, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != parentID || got.Title != "OAuth Guide" || !got.Namespace.Equal(nsA) {
		t.Fatalf("get parent: %+v", got)
	}

	_, err = store.Get(ctx, scopeA, docB)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("docB must be invisible under nsA: %v", err)
	}
	_, err = store.Get(ctx, brain.Scope{Namespace: nsB}, chunkOAuth)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("chunk must be invisible outside its namespace: %v", err)
	}
	nestedID := uuid.New()
	if err := store.Put(ctx, brain.Object{
		ID: nestedID, Kind: "Document", Title: "nested",
		Namespace: mustNS(t, "org", "a", "workspace", "west"),
	}); err != nil {
		t.Fatal(err)
	}
	gotNested, err := store.Get(ctx, scopeA, nestedID)
	if err != nil || gotNested.ID != nestedID {
		t.Fatalf("org scope should see workspace row: %v %+v", err, gotNested)
	}
	_, err = store.Get(ctx, brain.Scope{Namespace: mustNS(t, "org", "a", "workspace", "east")}, nestedID)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("other workspace: %v", err)
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
	lex, err := store.SearchLexical(ctx, scopeA, "oauth pkce", mustFilter(t, map[string]any{"kind": "Chunk"}), 5)
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
	lexOpen, err := store.SearchLexical(ctx, scopeA, "oauth", mustFilter(t, map[string]any{"stage": "open"}), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lexOpen) != 1 || lexOpen[0].ID != chunkOAuth {
		t.Fatalf("lexical property filter: %+v", lexOpen)
	}

	// Vector: query near [1,0,0] should rank oauth chunk first.
	vec, err := store.SearchVector(ctx, scopeA, []float32{0.95, 0.05, 0}, mustFilter(t, map[string]any{"kind": "Chunk"}), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) == 0 || vec[0].ID != chunkOAuth {
		t.Fatalf("vector top hit: %+v", vec)
	}
	if vec[0].ParentID == nil || *vec[0].ParentID != parentID {
		t.Fatalf("vector parent: %+v", vec[0])
	}

	vecOpen, err := store.SearchVector(ctx, scopeA, []float32{0.5, 0.5, 0}, mustFilter(t, map[string]any{"stage": "open"}), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vecOpen) != 1 || vecOpen[0].ID != chunkOAuth {
		t.Fatalf("vector property filter: %+v", vecOpen)
	}

	tri, err := store.SearchTrigram(ctx, scopeA, "pkce flow", brain.Filter{}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(tri) == 0 || tri[0].ID != chunkOAuth {
		t.Fatalf("trigram top hit: %+v", tri)
	}

	vecB, err := store.SearchVector(ctx, brain.Scope{Namespace: nsB}, []float32{1, 0, 0}, brain.Filter{}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vecB) != 0 {
		t.Fatalf("nsB must have no vector parts: %+v", vecB)
	}
	lexB, err := store.SearchLexical(ctx, brain.Scope{Namespace: nsB}, "oauth", brain.Filter{}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lexB) != 0 {
		t.Fatalf("nsB must have no lexical parts: %+v", lexB)
	}

	// Date / title / list filters on live SQL (same FilterPlan as Memory).
	after := now.Add(-time.Hour).Format(time.RFC3339)
	before := now.Add(time.Hour).Format(time.RFC3339)
	lexDate, err := store.SearchLexical(ctx, scopeA, "oauth", mustFilter(t, map[string]any{
		"updated_after":  after,
		"updated_before": before,
		"created_after":  after,
		"created_before": before,
		"title":          "pkce flow",
	}), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(lexDate) != 1 || lexDate[0].ID != chunkOAuth {
		t.Fatalf("date/title filters: %+v", lexDate)
	}
	lexList, err := store.SearchLexical(ctx, scopeA, "oauth", mustFilter(t, map[string]any{
		"stage": []any{"open", "closed"},
	}), 5)
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
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{}
	if got, err := store.SearchVector(ctx, scope, nil, brain.Filter{}, 10); err != nil || got != nil {
		t.Fatalf("empty embedding: got=%v err=%v", got, err)
	}
	if got, err := store.SearchTrigram(ctx, scope, "", brain.Filter{}, 10); err != nil || got != nil {
		t.Fatalf("empty query: got=%v err=%v", got, err)
	}
	if got, err := store.SearchLexical(ctx, scope, "x", brain.Filter{}, 0); err != nil || got != nil {
		t.Fatalf("k=0 lexical: got=%v err=%v", got, err)
	}
	if got, err := store.SearchLexical(ctx, scope, "", brain.Filter{}, 10); err != nil || got != nil {
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
		ctr, err := tcpostgres.Run(ctx, brainPgImage,
			tcpostgres.WithDatabase("brain"),
			tcpostgres.WithUsername("brain"),
			tcpostgres.WithPassword("brain"),
			tcpostgres.BasicWaitStrategies(),
			tcpostgres.WithSQLDriver("pgx"),
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
		store, err := postgres.New(pool)
		if err != nil {
			pgStart = err
			pool.Close()
			_ = ctr.Terminate(ctx)
			return
		}
		store.EmbeddingDim = 3
		if err := store.Setup(ctx); err != nil {
			pgStart = err
			pool.Close()
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

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func TestPostgresStore_notFoundAndCorruptProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}
	ctx := context.Background()
	pool := sharedPostgresPool(t)
	mustExec(t, pool, `TRUNCATE objects, object_kinds CASCADE`)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", "a")
	scope := brain.Scope{Namespace: ns}
	id := uuid.New()
	if err := store.Put(ctx, brain.Object{ID: id, Kind: "Document", Title: "doc", Namespace: ns}); err != nil {
		t.Fatal(err)
	}
	if err := store.SoftDelete(ctx, scope, uuid.New()); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft delete missing: %v", err)
	}
	if _, err := store.GetKind(ctx, "ghost"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("get kind missing: %v", err)
	}
	many, err := store.GetMany(ctx, scope, []uuid.UUID{id, uuid.New()})
	if err != nil || len(many) != 1 || many[0].ID != id {
		t.Fatalf("get many mixed: %+v err=%v", many, err)
	}
	if err := store.SoftDelete(ctx, scope, id); err != nil {
		t.Fatal(err)
	}
	revived := brain.Object{ID: id, Kind: "Document", Title: "revived", Namespace: ns}
	if err := store.Put(ctx, revived); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, scope, id)
	if err != nil || got.Title != "revived" {
		t.Fatalf("revive: %+v err=%v", got, err)
	}

	nsJSON, err := ns.Value()
	if err != nil {
		t.Fatal(err)
	}
	badID := uuid.New()
	mustExec(t, pool, `INSERT INTO objects (id, kind, title, properties, namespace) VALUES ($1, 'Document', 'bad', '"not-object"'::jsonb, $2::jsonb)`,
		badID, nsJSON)
	if _, err := store.Get(ctx, scope, badID); err == nil {
		t.Fatal("want corrupt properties error")
	}
}

func TestPostgresStore_canceledContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}
	pool := sharedPostgresPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	id := uuid.New()
	if _, err := store.GetMany(ctx, scope, []uuid.UUID{id}); err == nil {
		t.Fatal("get many")
	}
	if _, err := store.ListChildren(ctx, scope, id); err == nil {
		t.Fatal("list children")
	}
	if _, err := store.ListKinds(ctx); err == nil {
		t.Fatal("list kinds")
	}
	if _, err := store.KindsWithObjects(ctx, scope); err == nil {
		t.Fatal("kinds with objects")
	}
	if _, err := store.ListByKind(ctx, scope, "Document", 10); err == nil {
		t.Fatal("list by kind")
	}
	if _, err := store.GetKind(ctx, "Document"); err == nil {
		t.Fatal("get kind")
	}
	if _, err := store.Get(ctx, scope, id); err == nil {
		t.Fatal("get")
	}
	if err := store.Put(ctx, brain.Object{ID: id, Kind: "Document", Namespace: scope.Namespace}); err == nil {
		t.Fatal("put")
	}
	if err := store.PutKind(ctx, brain.ObjectKind{Kind: "Document"}); err == nil {
		t.Fatal("put kind")
	}
	if err := store.SoftDelete(ctx, scope, id); err == nil {
		t.Fatal("soft delete")
	}
	if _, err := store.SearchLexical(ctx, scope, "q", brain.Filter{}, 5); err == nil {
		t.Fatal("search")
	}
}

func mustFilter(t testing.TB, m map[string]any) brain.Filter {
	t.Helper()
	f, err := brain.DecodeFilter(m)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec: %v\nsql: %s", err, sql)
	}
}

// TestPut_livePostgresUpsertAndSoftDelete is the durable ObjectWriter outcome.
func TestPut_livePostgresUpsertAndSoftDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}
	ctx := context.Background()
	pool := sharedPostgresPool(t)
	mustExec(t, pool, `TRUNCATE objects, object_kinds CASCADE`)

	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	var _ brain.ObjectWriter = store

	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithKinds(
		brain.KindSpec{
			Kind: "Document", IsParent: true,
			Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}

	doc, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Document", Title: "live memo",
		Properties: map[string]any{"stage": "open"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, scope, doc.ID)
	if err != nil || got.Title != "live memo" {
		t.Fatalf("get after put: %+v err=%v", got, err)
	}

	// Upsert title
	doc.Title = "live memo v2"
	doc.Properties = map[string]any{"stage": "closed"}
	if _, err := eng.Put(ctx, scope, doc); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(ctx, scope, doc.ID)
	if err != nil || got.Title != "live memo v2" {
		t.Fatalf("upsert: %+v err=%v", got, err)
	}

	pos := 1
	chunk, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "part", Content: "oauth details",
		ParentID: &doc.ID, Position: &pos,
	})
	if err != nil {
		t.Fatal(err)
	}
	lex, err := store.SearchLexical(ctx, scope, "oauth", mustFilter(t, map[string]any{"kind": "Chunk"}), 5)
	if err != nil || len(lex) == 0 || lex[0].ID != chunk.ID {
		t.Fatalf("lexical after put: %+v err=%v", lex, err)
	}

	if err := eng.SoftDelete(ctx, scope, chunk.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, chunk.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-deleted chunk: %v", err)
	}
	lex, err = store.SearchLexical(ctx, scope, "oauth", mustFilter(t, map[string]any{"kind": "Chunk"}), 5)
	if err != nil || len(lex) != 0 {
		t.Fatalf("search excludes soft-deleted: %+v err=%v", lex, err)
	}
}

// TestEngine_livePostgresMultiTurnWriteSearch covers a multi-turn host path on
// real Postgres: ApplyKinds → Put (parent+parts with vectors) → hybrid search →
// continue → find_exact → expand children → soft-delete → revive Put → namespace isolation.
func TestEngine_livePostgresMultiTurnWriteSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}
	ctx := context.Background()
	pool := sharedPostgresPool(t)
	mustExec(t, pool, `TRUNCATE objects, object_kinds CASCADE`)

	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	eng, err := brain.NewEngine(store,
		brain.WithEmbedder(liveStubEmbedder{v: []float32{1, 0, 0}}),
		brain.WithConfig(brain.EngineConfig{
			DefaultLimit: 1, MaxLimit: 50, CandidateK: 20,
			Now: func() time.Time { return now },
		}),
		brain.WithKinds(
			brain.KindSpec{
				Kind: "Document", IsParent: true,
				Fields: []brain.FieldSpec{
					{Name: "stage", Type: brain.FieldTypeString, Required: true},
				},
			},
			brain.KindSpec{Kind: "Chunk", IsPart: true},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Persist kinds so store.GetKind reflects host migration.
	if err := eng.SyncKindsToStore(ctx); err != nil {
		t.Fatal(err)
	}

	ns := mustNS(t, "id", uuid.NewString())
	otherNS := mustNS(t, "org", "other")
	scope := brain.Scope{Namespace: ns}
	sc := brain.NewSearchContext()

	// Turn 1: create parent + two searchable parts.
	doc, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Document", Title: "OAuth guide",
		Properties: map[string]any{"stage": "open"},
	})
	if err != nil {
		t.Fatal(err)
	}
	pos1, pos2 := 1, 2
	c1, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "pkce", Content: "oauth pkce authorization code flow",
		ParentID: &doc.ID, Position: &pos1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(c1.Embedding) != 3 || c1.Embedding[0] != 1 {
		t.Fatalf("chunk embedding from put: %+v", c1.Embedding)
	}
	c2, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "pasta", Content: "cooking pasta recipes",
		ParentID: &doc.ID, Position: &pos2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c2

	// Turn 2: hybrid search ranks parent; result set + continue.
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "oauth pkce", Limit: 1}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != doc.ID {
		t.Fatalf("search page: %+v", page.Objects)
	}
	// Seed a second parent so continue has a next page of ranked parents.
	doc2, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Document", Title: "Second guide",
		Properties: map[string]any{"stage": "open"},
	})
	if err != nil {
		t.Fatal(err)
	}
	pos3 := 1
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "pkce-2", Content: "oauth pkce secondary material",
		ParentID: &doc2.ID, Position: &pos3,
	}); err != nil {
		t.Fatal(err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "oauth pkce", Limit: 1}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if page.ResultSetID == uuid.Nil || !page.HasMore {
		t.Fatalf("want has_more with limit 1: %+v", page)
	}
	page2, err := eng.Continue(ctx, scope, page.ResultSetID, 1, sc)
	if err != nil {
		t.Fatal(err)
	}
	if page2.ResultSetID != page.ResultSetID || len(page2.Objects) == 0 {
		t.Fatalf("continue: %+v", page2)
	}
	if page2.Objects[0].ID == page.Objects[0].ID {
		t.Fatalf("continue should advance: first=%s second=%s", page.Objects[0].ID, page2.Objects[0].ID)
	}

	// Turn 3: find_exact by parent UUID.
	exact, err := eng.FindExact(ctx, scope, brain.SearchRequest{Query: doc.ID.String()}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(exact.Objects) != 1 || exact.Objects[0].ID != doc.ID {
		t.Fatalf("find_exact: %+v", exact.Objects)
	}

	// Turn 4: expand containment children (ordered).
	kids, err := eng.Expand(ctx, scope, brain.ExpandRequest{ObjectID: doc.ID}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if kids.Mode != "children" || len(kids.Objects) != 2 {
		t.Fatalf("expand children: %+v", kids)
	}
	if kids.Objects[0].ID != c1.ID {
		t.Fatalf("child order: %+v", kids.Objects)
	}

	// Turn 5: soft-delete part → lexical no longer returns it; Get is ErrNotFound.
	if err := eng.SoftDelete(ctx, scope, c1.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, c1.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-deleted get: %v", err)
	}
	lex, err := store.SearchLexical(ctx, scope, "oauth", mustFilter(t, map[string]any{"kind": "Chunk"}), 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range lex {
		if hit.ID == c1.ID {
			t.Fatalf("soft-deleted chunk still in lexical: %+v", lex)
		}
	}

	// Turn 6: revive via Put (clears deleted_at); searchable again.
	c1.Title = "pkce-revived"
	c1.Content = "oauth pkce revived content"
	c1.DeletedAt = nil
	revived, err := eng.Put(ctx, scope, c1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, scope, revived.ID)
	if err != nil || got.Title != "pkce-revived" {
		t.Fatalf("revive get: %+v err=%v", got, err)
	}

	// Turn 7: catalog rejects bad put; namespace soft-delete isolation.
	if _, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "no stage"}); err == nil {
		t.Fatal("want required property failure")
	}
	if err := eng.SoftDelete(ctx, brain.Scope{Namespace: otherNS}, doc.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-delete other ns: %v", err)
	}
	if _, err := store.Get(ctx, scope, doc.ID); err != nil {
		t.Fatalf("doc still visible in own ns: %v", err)
	}

	// GetMany preserves order and omits missing.
	many, err := store.GetMany(ctx, scope, []uuid.UUID{doc.ID, uuid.New(), c1.ID})
	if err != nil || len(many) != 2 || many[0].ID != doc.ID || many[1].ID != c1.ID {
		t.Fatalf("get many: %+v err=%v", many, err)
	}
}

// liveStubEmbedder is a deterministic dense embedder for live Postgres hybrid search.
type liveStubEmbedder struct{ v []float32 }

func (s liveStubEmbedder) Embed(context.Context, string) ([]float32, error) { return s.v, nil }

// TestApplyKinds_livePostgresMigration is the durable KindRegistry outcome:
// host ApplyKinds upserts object_kinds, a new Engine LoadKindsFromStore adopts
// them, and schema() / filter validation use the catalog.
func TestApplyKinds_livePostgresMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}
	ctx := context.Background()
	pool := sharedPostgresPool(t)
	mustExec(t, pool, `TRUNCATE objects, object_kinds CASCADE`)

	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	// Compile-time / runtime: postgres.Store is a KindRegistry.
	var _ brain.KindRegistry = store

	eng, err := brain.NewEngine(store, brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx,
		brain.KindSpec{
			Kind: "Document", IsParent: true, Description: "parent docs",
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString},
			},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	); err != nil {
		t.Fatal(err)
	}

	// Modify Document and add Deal (migration v2).
	if err := eng.ApplyKinds(ctx,
		brain.KindSpec{
			Kind: "Document", IsParent: true, Description: "parent docs v2",
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString},
				{Name: "amount", Type: brain.FieldTypeNumber},
			},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
		brain.KindSpec{Kind: "Deal", IsParent: true},
	); err != nil {
		t.Fatal(err)
	}

	row, err := store.GetKind(ctx, "Document")
	if err != nil || row.Description != "parent docs v2" {
		t.Fatalf("upserted document: %+v err=%v", row, err)
	}
	if _, err := store.GetKind(ctx, "Deal"); err != nil {
		t.Fatal(err)
	}

	// Fresh process: load durable kinds (no hard-coded WithKinds).
	eng2, err := brain.NewEngine(store, brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.LoadKindsFromStore(ctx); err != nil {
		t.Fatal(err)
	}
	schema, err := eng2.Schema(ctx, "Document")
	if err != nil || len(schema.Kinds) != 1 || schema.Kinds[0].Description != "parent docs v2" {
		t.Fatalf("schema after load: %+v err=%v", schema, err)
	}
	if err := brain.ValidateFiltersAgainst(mustFilter(t, map[string]any{
		"kind": "Document", "stage": "open",
	}), eng2.Catalog()); err != nil {
		t.Fatal(err)
	}
	if err := brain.ValidateFiltersAgainst(mustFilter(t, map[string]any{
		"kind": "Document", "unknown": "x",
	}), eng2.Catalog()); err == nil {
		t.Fatal("want unknown property rejected after load")
	}
}

// TestPostgresStore_setupRegistersKinds is the host boot path: Setup is
// idempotent and upserts configured kinds into object_kinds.
func TestPostgresStore_setupRegistersKinds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in -short mode")
	}
	ctx := context.Background()
	pool := sharedPostgresPool(t)
	mustExec(t, pool, `TRUNCATE objects, object_kinds CASCADE`)

	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.EmbeddingDim = 3
	deal := brain.KindSpec{Kind: "Deal", IsParent: true, Description: "sales"}
	if err := store.Setup(ctx, deal); err != nil {
		t.Fatal(err)
	}
	if err := store.Setup(ctx, deal); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetKind(ctx, "Deal")
	if err != nil || got.Description != "sales" || !got.IsParent {
		t.Fatalf("kind: %+v err=%v", got, err)
	}
}
