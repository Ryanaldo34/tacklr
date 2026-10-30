package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

func TestOpen_rejectsABadSchemaOrConnectionString(t *testing.T) {
	t.Parallel()
	_, schemaErr := postgres.Open(t.Context(), "postgres://u:p@db.example.com:5432/postgres", postgres.Options{Schema: "public;drop"})
	_, connErr := postgres.Open(t.Context(), "://bad", postgres.Options{})

	if schemaErr == nil || connErr == nil {
		t.Fatalf("bad schema = %v, bad connection string = %v, want both rejected", schemaErr, connErr)
	}
}

func TestStore_aStatementStuckPastTheTimeoutIsCancelledByTheServer(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		timeout time.Duration
		want    func(error) bool
	}{
		"with a timeout": {100 * time.Millisecond, func(err error) bool {
			var pgErr *pgconn.PgError
			return errors.As(err, &pgErr) && pgErr.Code == "57014" // query_canceled
		}},
		"with the timeout off": {-1, func(err error) bool { return errors.Is(err, context.DeadlineExceeded) }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, pool := testDBWith(t, postgres.Options{StatementTimeout: tc.timeout})
			scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
			doc := brain.Object{ID: uuid.New(), Kind: "Document", Namespace: scope.Namespace}
			if err := store.Put(t.Context(), scope, doc); err != nil {
				t.Fatal(err)
			}
			// Another transaction holds the row; the write waits behind it.
			holder, err := pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = holder.Rollback(context.Background()) })
			if _, err := holder.Exec(t.Context(), `SELECT 1 FROM objects WHERE id = $1 FOR UPDATE`, doc.ID); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			err = store.Put(ctx, scope, doc)

			if !tc.want(err) {
				t.Fatalf("blocked write = %v", err)
			}
		})
	}
}

func TestStore_everyCallReportsADatabaseFailure(t *testing.T) {
	t.Parallel()
	store, _ := testDB(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	scope := brain.Scope{Namespace: mustNS(t, "org", "a"), SessionID: "s1"}
	ns, id := scope.Namespace, uuid.New()
	doc := brain.Object{ID: id, Kind: "Document", Namespace: ns}
	part := brain.TreePart{Object: brain.Object{ID: uuid.New(), Kind: "Chunk", ParentID: &id, Namespace: ns}}
	errs := map[string]error{
		"put":            store.Put(ctx, scope, doc),
		"soft delete":    store.SoftDelete(ctx, scope, id),
		"put kind":       store.PutKind(ctx, brain.ObjectKind{Kind: "Document"}),
		"record":         store.Record(ctx, brain.AuditEvent{Op: brain.AuditRead, Namespace: ns}),
		"stage parts":    store.StageParts(ctx, scope, uuid.New(), []brain.TreePart{part}),
		"commit tree":    store.CommitTree(ctx, scope, uuid.New(), 0, doc),
		"delete session": store.DeleteSessionMessages(ctx, ns, "s1"),
		"setup":          store.Setup(ctx),
	}
	_, errs["get"] = store.Get(ctx, scope, id)
	_, errs["get many"] = store.GetMany(ctx, scope, []uuid.UUID{id})
	_, errs["list children"] = store.ListChildren(ctx, scope, id)
	_, errs["purge deleted"] = store.PurgeDeleted(ctx, scope, time.Now())
	_, errs["erase"] = store.Erase(ctx, scope)
	_, errs["get kind"] = store.GetKind(ctx, "Document")
	_, errs["list kinds"] = store.ListKinds(ctx)
	_, errs["list by kind"] = store.ListByKind(ctx, scope, "Document", 5)
	_, errs["get by property"] = store.GetByProperty(ctx, scope, "path", "/a")
	_, errs["kinds with objects"] = store.KindsWithObjects(ctx, scope)
	_, errs["lexical"] = store.SearchLexical(ctx, scope, "q", brain.Filter{}, 5)
	_, errs["vector"] = store.SearchVector(ctx, scope, "m", []float32{1, 0, 0}, brain.Filter{}, 5)
	_, errs["trigram"] = store.SearchTrigram(ctx, scope, "q", brain.Filter{}, 5)
	_, errs["tree state"] = store.TreeState(ctx, scope, id)
	_, errs["save session"] = store.SaveSessionMessages(ctx, ns, "s1", "k", []brain.SessionMessage{{Role: "user"}})
	_, errs["purge session"] = store.PurgeSessionMessages(ctx, ns, time.Now())

	for name, err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context = %v, want context.Canceled", name, err)
		}
	}
}

func TestStore_rejectsInvalidArgumentsBeforeQuerying(t *testing.T) {
	t.Parallel()
	store, _ := testDB(t)
	ctx := context.Background()
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	id := uuid.New()

	if got, err := store.GetMany(ctx, scope, nil); err != nil || got != nil {
		t.Fatalf("empty ids: %v %v", got, err)
	}
	if err := store.SoftDelete(ctx, scope, uuid.Nil); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("nil id: %v", err)
	}
	if err := putFixture(store, brain.Object{}); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid put: %v", err)
	}
	if _, err := store.SearchLexical(ctx, scope, "q", brain.Filter{UpdatedAfter: "not-a-time"}, 5); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid lexical filter = %v, want ErrInvalid", err)
	}
	if _, err := store.SearchVector(ctx, scope, "", []float32{1}, brain.Filter{UpdatedAfter: "not-a-time"}, 5); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid vector filter = %v, want ErrInvalid", err)
	}
	if _, err := store.SearchTrigram(ctx, scope, "q", brain.Filter{UpdatedAfter: "not-a-time"}, 5); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid trigram filter = %v, want ErrInvalid", err)
	}
	if err := store.PutKind(ctx, brain.ObjectKind{}); err == nil {
		t.Fatal("want empty kind")
	}
	if _, err := store.GetByProperty(ctx, scope, "", "v"); err == nil {
		t.Fatal("want empty property key")
	}
	if got, err := store.ListByKind(ctx, scope, "", 10); err != nil || got != nil {
		t.Fatalf("empty kind list: %v %v", got, err)
	}
	if err := putFixture(store, brain.Object{
		ID: id, Kind: "Document", Namespace: mustNS(t, "org", "a"),
		Properties: map[string]any{"ch": make(chan int)},
	}); err == nil {
		t.Fatal("want properties marshal error")
	}
}

// Pre-built via Makefile/CI: docker build -f brain/testdata/Dockerfile.postgres -t tacklr-pg-brain:test
const brainPgImage = "tacklr-pg-brain:test"

var (
	pgOnce    sync.Once
	pgAdmin   *pgxpool.Pool // the container's default schema: creates and drops test schemas
	pgConn    string        // the container's connection string
	pgStart   error
	pgSkip    string
	pgSchemas atomic.Int64
)

// TestPostgresStore_liveRetrievalChannels is the real-Postgres outcome for
// scoped reads, ordered children, BM25 lexical, dense vector, and trigram search.
func TestPostgresStore_liveRetrievalChannels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := testDB(t)

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
			($1, 'Document', 'OAuth Guide', 'parent', '{"vfs_path":"/workspace/work/oauth.md"}', NULL, NULL, NULL, NULL, $2, $3, $3),
			($4, 'Chunk', 'pkce flow', '', '{"stage":"open"}', 'oauth pkce implementation details', $1, 1, '[1,0,0]'::vector, $2, $3, $3),
			($5, 'Chunk', 'unrelated', '', '{}', 'cooking recipes and pasta', $1, 2, '[0,1,0]'::vector, $2, $3, $3),
			($6, 'Chunk', 'gone', '', '{}', 'soft deleted oauth', $1, 3, '[1,0,0]'::vector, $2, $3, $3)
	`, parentID, nsA, now, chunkOAuth, chunkOther, chunkDeleted)
	mustExec(t, pool, `UPDATE objects SET deleted_at = $1 WHERE id = $2`, now, chunkDeleted)
	mustExec(t, pool, `UPDATE objects SET embedding_model = 'test-model' WHERE embedding IS NOT NULL`)
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
	byProp, err := store.GetByProperty(ctx, scopeA, brain.PropVFSPath, "/workspace/work/oauth.md")
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
	if err := putFixture(store, brain.Object{
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
	vec, err := store.SearchVector(ctx, scopeA, "test-model", []float32{0.95, 0.05, 0}, mustFilter(t, map[string]any{"kind": "Chunk"}), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) == 0 || vec[0].ID != chunkOAuth {
		t.Fatalf("vector top hit: %+v", vec)
	}
	if vec[0].ParentID == nil || *vec[0].ParentID != parentID {
		t.Fatalf("vector parent: %+v", vec[0])
	}

	vecOpen, err := store.SearchVector(ctx, scopeA, "test-model", []float32{0.5, 0.5, 0}, mustFilter(t, map[string]any{"stage": "open"}), 5)
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
	mustExec(t, pool, `UPDATE objects SET content = content || ' ' || repeat('padding word ', 80) || 'grace window' WHERE id = $1`, chunkOAuth)
	grace, err := store.SearchTrigram(ctx, scopeA, "grace window", brain.Filter{}, 5)
	if err != nil || len(grace) == 0 || grace[0].ID != chunkOAuth {
		t.Fatalf("trigram phrase inside long body: %+v err=%v", grace, err)
	}

	vecB, err := store.SearchVector(ctx, brain.Scope{Namespace: nsB}, "", []float32{1, 0, 0}, brain.Filter{}, 5)
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
	t.Parallel()
	ctx := context.Background()
	store, _ := testDB(t)
	scope := brain.Scope{}
	if got, err := store.SearchVector(ctx, scope, "", nil, brain.Filter{}, 10); err != nil || got != nil {
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

// startPostgres starts one container for the test binary and installs the
// extensions in public, where every test schema's Setup finds them.
func startPostgres(t *testing.T) {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
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
		// The container lives for the process; Ryuk reaps it on exit.
		if pgConn, err = ctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
			pgStart = err
			return
		}
		if pgAdmin, err = pgxpool.New(ctx, pgConn); err != nil {
			pgStart = err
			return
		}
		_, pgStart = pgAdmin.Exec(ctx, `
			CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA public;
			CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;
			CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;`)
	})
	if pgSkip != "" {
		t.Skipf("postgres container unavailable (need Docker API runtime): %s", pgSkip)
	}
	if pgStart != nil {
		t.Fatal(pgStart)
	}
}

// testSchema is a schema name no other test uses; it is dropped when t ends.
func testSchema(t *testing.T) string {
	t.Helper()
	startPostgres(t)
	name := fmt.Sprintf("test_%d", pgSchemas.Add(1))
	t.Cleanup(func() {
		if _, err := pgAdmin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+name+" CASCADE"); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	return name
}

// testDB is a set-up store over its own schema, so a test never sees or
// resets another test's rows. pool reaches the same schema.
func testDB(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	return testDBWith(t, postgres.Options{})
}

// testDBWith is testDB with opts; Schema and EmbeddingDim are the test's own.
func testDBWith(t *testing.T, opts postgres.Options) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	opts.Schema, opts.EmbeddingDim = testSchema(t), 3
	store, err := postgres.Open(t.Context(), pgConn, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), pgConn+"&search_path="+opts.Schema+",public")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return store, pool
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
	t.Parallel()
	ctx := context.Background()
	store, pool := testDB(t)
	ns := mustNS(t, "org", "a")
	scope := brain.Scope{Namespace: ns}
	id := uuid.New()
	if err := putFixture(store, brain.Object{ID: id, Kind: "Document", Title: "doc", Namespace: ns}); err != nil {
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
	if err := putFixture(store, revived); err != nil {
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

// TestEngine_livePostgresMultiTurnWriteSearch covers a multi-turn host path on
// real Postgres: ApplyKinds → Put (parent+parts with vectors) → hybrid search →
// continue → find_exact → expand children → soft-delete → revive Put → namespace isolation.
func TestEngine_livePostgresMultiTurnWriteSearch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := testDB(t)
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

func (s liveStubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	return repeatVector(s.v, len(texts)), nil
}

// TestApplyKinds_livePostgresMigration is the durable KindRegistry outcome:
// host ApplyKinds upserts object_kinds, a new Engine LoadKindsFromStore adopts
// them, and schema() / filter validation use the catalog.
func TestApplyKinds_livePostgresMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := testDB(t)
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

	if _, err := store.GetKind(ctx, "Ghost"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("missing kind: %v", err)
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
	t.Parallel()
	ctx := context.Background()
	store, _ := testDB(t)
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

// TestPostgresStore_lexicalRanksTitleAboveBody: a title hit outranks a body hit,
// a partial term match fills the page, and a stopword matches nothing.
func TestPostgresStore_lexicalRanksTitleAboveBody(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := testDB(t)
	ns := mustNS(t, "org", "fts")
	parent := uuid.New()
	titleHit := uuid.New()
	bodyHit := uuid.New()
	bothHit := uuid.New()
	onlyA := uuid.New()
	onlyB := uuid.New()
	now := time.Now().UTC()
	mustExec(t, pool, `
		INSERT INTO objects (id, kind, title, summary, content, parent_id, position, namespace, created_at, updated_at)
		VALUES
			($1, 'Document', 'Guide', '', NULL, NULL, NULL, $2, $3, $3),
			($4, 'Chunk', $5, '', 'shipping notes', $1, 1, $2, $3, $3),
			($6, 'Chunk', 'weekly', '', 'zymurgy', $1, 2, $2, $3, $3),
			($7, 'Chunk', 'alphaonlyterm betaonlyterm', '', 'paired', $1, 3, $2, $3, $3),
			($8, 'Chunk', 'notes', '', 'alphaonlyterm', $1, 4, $2, $3, $3),
			($9, 'Chunk', 'notes', '', 'betaonlyterm', $1, 5, $2, $3, $3)
	`, parent, ns, now, titleHit, "zymurgy "+strings.Repeat("filler ", 100), bodyHit, bothHit, onlyA, onlyB)

	scope := brain.Scope{Namespace: ns}
	top, err := store.SearchLexical(ctx, scope, "zymurgy", brain.Filter{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 1 || top[0].ID != titleHit || top[0].Score <= 0 {
		t.Fatalf("title page: %+v", top)
	}
	ranked, err := store.SearchLexical(ctx, scope, "zymurgy", brain.Filter{}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranked) < 2 || ranked[0].ID != titleHit || ranked[0].Score <= ranked[1].Score {
		t.Fatalf("title should outrank body: %+v", ranked)
	}
	filled, err := store.SearchLexical(ctx, scope, "alphaonlyterm betaonlyterm", brain.Filter{}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(filled) != 3 || filled[0].ID != bothHit {
		t.Fatalf("any-term fill: %+v", filled)
	}
	if got, err := store.SearchLexical(ctx, scope, "the", brain.Filter{}, 5); err != nil || len(got) != 0 {
		t.Fatalf("stopword: %+v err=%v", got, err)
	}
	if _, err := store.SaveSessionMessages(ctx, ns, "sess/1", "g1", []brain.SessionMessage{{
		Role: "user", Body: []byte(`{"role":"user","content":"quillwort"}`), SearchText: "quillwort grows in the margin",
	}}); err != nil {
		t.Fatal(err)
	}
	msgScope := brain.Scope{Namespace: ns, SessionID: "sess/1"}
	msgs, err := store.SearchLexical(ctx, msgScope, "quillwort", brain.Filter{}, 5)
	if err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, hit := range msgs {
		if strings.Contains(hit.Content, "quillwort") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("session message: %+v", msgs)
	}
}

func (liveStubEmbedder) Model() string { return "test-model" }

// putFixture writes obj straight to the store under its own namespace.
func putFixture(w brain.ObjectWriter, obj brain.Object) error {
	return w.Put(context.Background(), brain.Scope{Namespace: obj.Namespace}, obj)
}

func TestPostgresStore_isolationContract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) storetest.Store {
		store, _ := testDB(t)
		return store
	})
}

func TestStore_concurrentSetupMigratesOnceAndRejectsADimensionChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	schema := testSchema(t)
	newStore := func(dim int) *postgres.Store {
		store, err := postgres.Open(ctx, pgConn, postgres.Options{Schema: schema, EmbeddingDim: dim})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(store.Close)
		return store
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() { errs[i] = newStore(3).Setup(ctx) })
	}
	wg.Wait()
	resized := newStore(5).Setup(ctx)
	var applied, version int
	if err := pgAdmin.QueryRow(ctx, `SELECT count(*), max(version) FROM `+schema+`.schema_migrations`).Scan(&applied, &version); err != nil {
		t.Fatal(err)
	}

	if err := errors.Join(errs...); err != nil {
		t.Fatalf("concurrent setup: %v", err)
	}
	if applied == 0 || applied != version {
		t.Fatalf("migrations applied = %d, version = %d, want each version applied once", applied, version)
	}
	if resized == nil || !strings.Contains(resized.Error(), "EmbeddingDim") {
		t.Fatalf("setup with a new dimension = %v", resized)
	}
}

func TestStore_auditEventsArePersisted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := testDB(t)
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithAudit(store.Record))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", "a"), Principals: []string{"user:ann"}, SessionID: "s1"}

	obj, err := eng.Put(ctx, scope, brain.Object{Kind: "Note", Title: "n"})
	if err != nil {
		t.Fatal(err)
	}
	var (
		op         string
		principals []string
		session    string
		ids        []uuid.UUID
	)
	err = pool.QueryRow(ctx, `SELECT op, principals, session_id, object_ids FROM audit_events WHERE namespace @> '[{"name":"org","value":"a"}]'`).
		Scan(&op, &principals, &session, &ids)

	if err != nil || brain.AuditOp(op) != brain.AuditPut || principals[0] != "user:ann" || session != "s1" || len(ids) != 1 || ids[0] != obj.ID {
		t.Fatalf("audit row = %s %v %s %v err=%v", op, principals, session, ids, err)
	}
}

func TestStore_setupWaitsForAnotherSetupPastTheStatementTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	schema := testSchema(t)
	store, err := postgres.Open(ctx, pgConn, postgres.Options{Schema: schema, EmbeddingDim: 3, StatementTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	// Another deploy holds the Setup lock for longer than the statement timeout.
	holder, err := pgAdmin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('tacklr.setup:' || $1))`, schema); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)

	go func() { done <- store.Setup(ctx) }()
	time.Sleep(300 * time.Millisecond)
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := <-done; err != nil {
		t.Fatalf("setup after waiting for the lock: %v", err)
	}
}

// repeatVector returns n copies of v, one per embedded text.
func repeatVector(v []float32, n int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		out[i] = v
	}
	return out
}
