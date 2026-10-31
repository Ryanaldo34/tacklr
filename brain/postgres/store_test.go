package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
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
	"github.com/ryanaldo34/tacklr/internal/helixtest"
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
			if err := putFixture(store, doc); err != nil {
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

			_, err = store.Put(ctx, scope, brain.Indexed{Object: doc})

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
	doc := brain.Indexed{Object: brain.Object{ID: id, Kind: "Document", Namespace: ns}}
	errs := map[string]error{
		"soft delete":    store.SoftDelete(ctx, scope, id),
		"put kind":       store.PutKind(ctx, brain.ObjectKind{Kind: "Document"}),
		"record":         store.Record(ctx, brain.AuditEvent{Op: brain.AuditRead, Namespace: ns}),
		"delete session": store.DeleteSessionMessages(ctx, ns, "s1"),
		"setup":          store.Setup(ctx),
	}
	_, errs["put"] = store.Put(ctx, scope, doc)
	_, errs["put tree"] = store.PutTree(ctx, scope, doc, nil)
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
	_, errs["search"] = store.Search(ctx, scope, brain.PartSearch{Query: "q", Limit: 5})
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
	if _, err := store.PutTree(ctx, scope, brain.Indexed{}, nil); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid tree parent: %v", err)
	}
	embedding, _ := testDBWith(t, postgres.Options{Embedding: brain.Embedding{Embedder: &storetest.Embedder{}}})
	poisoned := brain.Indexed{Object: brain.Object{ID: id, Kind: "Document", Namespace: scope.Namespace}, Text: "poison"}
	if _, err := embedding.PutTree(ctx, scope, poisoned, nil); err == nil || !strings.Contains(err.Error(), "embedder down") {
		t.Fatalf("tree whose parent does not embed = %v, want the embedder's error", err)
	}
	if _, err := store.Search(ctx, scope, brain.PartSearch{Query: "q", Filters: brain.Filter{UpdatedAfter: "not-a-time"}, Limit: 5}); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid search filter = %v, want ErrInvalid", err)
	}
	if got, err := store.Search(ctx, scope, brain.PartSearch{Query: "q"}); err != nil || got != nil {
		t.Fatalf("search with no limit = %v %v, want nothing", got, err)
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
	store, _ := testDBWith(t, postgres.Options{Embedding: brain.Embedding{Embedder: &storetest.Embedder{}}})
	now := time.Now().UTC().Truncate(time.Microsecond)
	eng, err := brain.NewEngine(store,
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
	lex, err := store.Search(ctx, scope, brain.PartSearch{Query: "oauth", Filters: mustFilter(t, map[string]any{"kind": "Chunk"}), Limit: 10})
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

// TestApplyKinds_livePostgresMigration is the durable KindRegistry outcome:
// host ApplyKinds upserts object_kinds, a new Engine LoadKindsFromStore adopts
// them, and schema() / filter validation use the catalog.
func TestApplyKinds_livePostgresMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := testDB(t)
	eng, err := brain.NewEngine(store)
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
	eng2, err := brain.NewEngine(store)
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

// putFixture writes obj straight to the store under its own namespace.
func putFixture(s brain.Store, obj brain.Object) error {
	_, err := s.Put(context.Background(), brain.Scope{Namespace: obj.Namespace}, brain.Indexed{Object: obj})
	return err
}

func TestPostgresStore_isolationContract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T, emb brain.Embedder) brain.Store {
		store, _ := testDBWith(t, postgres.Options{Embedding: brain.Embedding{Embedder: emb}})
		return store
	})
}

func TestStore_concurrentSetupsSucceedAndADimensionChangeFails(t *testing.T) {
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
	store := newStore(3)
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	doc := brain.Object{ID: uuid.New(), Kind: "Document", Title: "after setup", Namespace: scope.Namespace}
	putErr := putFixture(store, doc)
	got, getErr := store.Get(ctx, scope, doc.ID)

	if err := errors.Join(errs...); err != nil {
		t.Fatalf("concurrent setup: %v", err)
	}
	if putErr != nil || getErr != nil || got.Title != "after setup" {
		t.Fatalf("store after concurrent setups: put %v, get %+v %v", putErr, got, getErr)
	}
	if resized == nil || !strings.Contains(resized.Error(), "EmbeddingDim") {
		t.Fatalf("setup with a new dimension = %v", resized)
	}
}

func TestStore_auditEventsArePersisted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := testDB(t)
	eng, err := brain.NewEngine(store, brain.WithAudit(store.Record))
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

func TestEngine_aClosedStoreFailsTheCallsThatFollowAGraphHit(t *testing.T) {
	ctx := t.Context()
	store, err := postgres.Open(ctx, pgConn, postgres.Options{Schema: testSchema(t), EmbeddingDim: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	from, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "from"})
	if err != nil {
		t.Fatal(err)
	}
	to, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "to"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kg.Link(ctx, scope, from.ID, to.ID, "references"); err != nil {
		t.Fatal(err)
	}
	store.Close()

	_, pathErr := kg.ShortestPath(ctx, scope, brain.PathRequest{From: from.ID, To: to.ID})
	_, findErr := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "from"}, brain.NewSearchContext())
	kindsErr := eng.ApplyKinds(ctx, brain.KindSpec{Kind: "Document", IsParent: true})

	for name, err := range map[string]error{"shortest path": pathErr, "find objects": findErr, "apply kinds": kindsErr} {
		if err == nil || !strings.Contains(err.Error(), "closed pool") {
			t.Errorf("%s on a closed store = %v, want its error", name, err)
		}
	}
}

func TestStore_searchRanksByTheWordsTheWayAReaderExpects(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, _ := testDB(t)
	ns := mustNS(t, "org", "fts")
	doc := brain.Object{ID: uuid.New(), Kind: "Document", Title: "Guide", Namespace: ns}
	titleHit := chunk(doc, "zymurgy", "shipping notes "+strings.Repeat("filler ", 100), 0)
	bodyHit := chunk(doc, "weekly", "zymurgy", 1)
	both := chunk(doc, "alphaonlyterm betaonlyterm", "paired", 2)
	onlyA, onlyB := chunk(doc, "notes", "alphaonlyterm", 3), chunk(doc, "notes", "betaonlyterm", 4)
	deep := chunk(doc, "long", strings.Repeat("padding word ", 80)+"grace window", 5)
	if _, err := store.PutTree(ctx, brain.Scope{Namespace: ns}, brain.Indexed{Object: doc}, indexedParts(titleHit, bodyHit, both, onlyA, onlyB, deep)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveSessionMessages(ctx, ns, "sess/1", "g1", []brain.SessionMessage{{
		Role: "user", Body: []byte(`{"role":"user","content":"quillwort"}`), SearchText: "quillwort grows in the margin",
	}}); err != nil {
		t.Fatal(err)
	}
	search := func(query string, exact bool) []brain.ScoredID {
		hits, err := store.Search(ctx, brain.Scope{Namespace: ns, SessionID: "sess/1"}, brain.PartSearch{Query: query, Exact: exact, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return hits
	}

	zymurgy := search("zymurgy", false)
	terms := search("alphaonlyterm betaonlyterm", false)
	phrase := search("grace window", true)
	session := search("quillwort", false)

	if len(zymurgy) != 2 || zymurgy[0].ID != titleHit.ID || zymurgy[1].ID != bodyHit.ID {
		t.Errorf("a title hit and a body hit = %v, want the title hit first", hitIDs(zymurgy))
	}
	if len(terms) != 3 || terms[0].ID != both.ID {
		t.Errorf("two terms = %v, want the part holding both first, then each part holding one", hitIDs(terms))
	}
	if len(phrase) == 0 || phrase[0].ID != deep.ID {
		t.Errorf("exact search for a phrase deep in a long body = %v, want that part", hitIDs(phrase))
	}
	if stop := search("the", false); len(stop) != 0 {
		t.Errorf("a stopword = %v, want nothing", hitIDs(stop))
	}
	if len(session) != 1 || !strings.Contains(session[0].Content, "quillwort") {
		t.Errorf("the session's message = %+v, want it found by its words", session)
	}
}

func TestStore_searchByMeaningKeepsFiltersAndNestedNamespaces(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, _ := testDBWith(t, postgres.Options{Embedding: brain.Embedding{Embedder: &storetest.Embedder{}}})
	org := mustNS(t, "org", uuid.NewString())
	west := append(org.Clone(), brain.Attr{Name: "workspace", Value: "west"})
	east := append(org.Clone(), brain.Attr{Name: "workspace", Value: "east"})
	doc := brain.Object{ID: uuid.New(), Kind: "Document", Title: "fleet", Namespace: west}
	manual, list := chunk(doc, "manual", "car manual", 0), chunk(doc, "list", "car parts list", 1)
	manual.Properties, list.Properties = map[string]any{"stage": "open"}, map[string]any{"stage": "closed"}
	if _, err := store.PutTree(ctx, brain.Scope{Namespace: west}, brain.Indexed{Object: doc}, indexedParts(manual, list)); err != nil {
		t.Fatal(err)
	}
	search := func(ns brain.Namespace, filter brain.Filter) []uuid.UUID {
		hits, err := store.Search(ctx, brain.Scope{Namespace: ns}, brain.PartSearch{Query: "automobile", Filters: filter, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return hitIDs(hits)
	}

	open := search(org, mustFilter(t, map[string]any{"stage": "open"}))
	all := search(org, brain.Filter{})
	sibling := search(east, brain.Filter{})

	if !slices.Equal(open, []uuid.UUID{manual.ID}) {
		t.Errorf("by meaning with a filter = %v, want the open part only", open)
	}
	if len(all) != 2 || len(sibling) != 0 {
		t.Errorf("by meaning from the org = %v, from a sibling workspace = %v, want both parts, then none", all, sibling)
	}
}

// chunk is a part of parent, indexed by its content, as Engine hands it to
// PutTree.
func chunk(parent brain.Object, title, content string, position int) brain.Indexed {
	return brain.Indexed{Object: brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: title, Content: content, Namespace: parent.Namespace,
		ParentID: &parent.ID, Position: new(position),
	}, Text: content}
}

func indexedParts(in ...brain.Indexed) iter.Seq2[brain.Indexed, error] {
	return func(yield func(brain.Indexed, error) bool) {
		for _, p := range in {
			if !yield(p, nil) {
				return
			}
		}
	}
}

func hitIDs(hits []brain.ScoredID) []uuid.UUID {
	out := make([]uuid.UUID, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}
