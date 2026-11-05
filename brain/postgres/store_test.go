package postgres_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
)

// TestConfigureAndOpen_rejectABadSchema: Configure and Open reject a schema
// name that is not a plain identifier.
func TestConfigureAndOpen_rejectABadSchema(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://u:p@db.example.com:5432/postgres")
	if err != nil {
		t.Fatal(err)
	}
	bad := postgres.Options{Schema: "public;drop"}
	configureErr := postgres.Configure(cfg, bad)
	_, openErr := postgres.Open(nil, bad)

	if configureErr == nil || openErr == nil {
		t.Fatalf("configure = %v, open = %v, want both to reject the schema", configureErr, openErr)
	}
}

// TestStore_aStatementStuckPastTheTimeoutIsCancelledByTheServer: the server
// cancels a statement that waits past StatementTimeout; with the timeout off,
// only the caller's deadline ends it.
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
			scope := brain.Scope{Namespace: storetest.Namespace(t, "org", "a")}
			doc := brain.Object{ID: uuid.New(), Kind: "Document", Namespace: scope.Namespace}
			if err := storetest.Put(store, doc); err != nil {
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

// TestStore_everyCallReportsADatabaseFailure: every store call returns the
// driver's error when the context has ended.
func TestStore_everyCallReportsADatabaseFailure(t *testing.T) {
	t.Parallel()
	store, _ := testDB(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	scope := brain.Scope{Namespace: storetest.Namespace(t, "org", "a"), SessionID: "s1"}
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
	_, errs["get tree"] = store.GetTree(ctx, scope, id, 0)
	_, errs["purge deleted"] = store.PurgeDeleted(ctx, scope, time.Now())
	_, errs["erase"] = store.Erase(ctx, scope)
	_, errs["list kinds"] = store.ListKinds(ctx)
	_, errs["get by property"] = store.GetByProperty(ctx, scope, "path", "/a")
	_, errs["search"] = store.Search(ctx, scope, brain.RecordSearch{Query: "q", Limit: 5})
	_, errs["save session"] = store.SaveSessionMessages(ctx, ns, "s1", "k", []brain.SessionMessage{{Role: "user"}})
	_, errs["purge session"] = store.PurgeSessionMessages(ctx, ns, time.Now())

	for name, err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context = %v, want context.Canceled", name, err)
		}
	}
}

// TestStore_rejectsInvalidArgumentsBeforeQuerying: each invalid argument
// fails, or returns nothing, before the store queries.
func TestStore_rejectsInvalidArgumentsBeforeQuerying(t *testing.T) {
	t.Parallel()
	store, _ := testDB(t)
	ctx := context.Background()
	scope := brain.Scope{Namespace: storetest.Namespace(t, "org", "a")}
	id := uuid.New()

	if got, err := store.GetMany(ctx, scope, nil); err != nil || got != nil {
		t.Fatalf("empty ids: %v %v", got, err)
	}
	if err := store.SoftDelete(ctx, scope, uuid.Nil); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("nil id: %v", err)
	}
	if err := storetest.Put(store, brain.Object{}); !errors.Is(err, brain.ErrInvalid) {
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
	if _, err := store.Search(ctx, scope, brain.RecordSearch{Query: "q", Filters: brain.Filter{UpdatedAfter: "not-a-time"}, Limit: 5}); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("invalid search filter = %v, want ErrInvalid", err)
	}
	if got, err := store.Search(ctx, scope, brain.RecordSearch{Query: "q"}); err != nil || got != nil {
		t.Fatalf("search with no limit = %v %v, want nothing", got, err)
	}
	if err := store.PutKind(ctx, brain.ObjectKind{}); err == nil {
		t.Fatal("want empty kind")
	}
	if _, err := store.GetByProperty(ctx, scope, "", "v"); err == nil {
		t.Fatal("want empty property key")
	}
	if err := storetest.Put(store, brain.Object{
		ID: id, Kind: "Document", Namespace: storetest.Namespace(t, "org", "a"),
		Properties: map[string]any{"ch": make(chan int)},
	}); err == nil {
		t.Fatal("want properties marshal error")
	}
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
	opts.Schema, opts.EmbeddingDim = pgtest.Schema(t), 3
	store, pool := testOpen(t, opts)
	if err := store.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return store, pool
}

// testOpen is a Store over a pool of the container that Configure set up
// with opts, as a host builds one. The pool reaches the Store's schema.
func testOpen(t *testing.T, opts postgres.Options) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.Configure(cfg, opts); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store, err := postgres.Open(pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	return store, pool
}

// TestPostgresStore_isolationContract: postgres.Store meets the store contract
// every brain store must meet.
func TestPostgresStore_isolationContract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T, emb brain.Embedder) brain.Store {
		store, _ := testDBWith(t, postgres.Options{Embedding: brain.Embedding{Embedder: emb}})
		return store
	})
}

// TestStore_setupWaitsForOtherSetupsAndRefusesANewDimension: setups that run
// at the same time wait for the one that holds the lock, even past the
// statement timeout, then all succeed and register their kinds; a setup with
// another embedding dimension fails.
func TestStore_setupWaitsForOtherSetupsAndRefusesANewDimension(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	schema := pgtest.Schema(t)
	newStore := func(dim int) *postgres.Store {
		store, _ := testOpen(t, postgres.Options{Schema: schema, EmbeddingDim: dim, StatementTimeout: 50 * time.Millisecond})
		return store
	}
	deal := brain.KindSpec{Kind: "Deal", IsParent: true, Description: "sales"}
	// Another deploy holds the setup lock for longer than the statement timeout.
	holder, err := pgtest.Admin(t).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('tacklr.setup:' || $1))`, schema); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() { errs[i] = newStore(3).Setup(ctx, deal) })
	}
	time.Sleep(300 * time.Millisecond)
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	resized := newStore(5).Setup(ctx)
	store := newStore(3)
	kind, kindErr := storetest.Kind(ctx, store, "Deal")

	if err := errors.Join(errs...); err != nil {
		t.Fatalf("setups that waited for the lock: %v", err)
	}
	if kindErr != nil || kind.Description != "sales" || !kind.IsParent {
		t.Errorf("kind after setup = %+v %v, want Deal", kind, kindErr)
	}
	if resized == nil || !strings.Contains(resized.Error(), "EmbeddingDim") {
		t.Errorf("setup with a new dimension = %v, want the EmbeddingDim error", resized)
	}
}

// TestStore_auditEventsArePersisted: Record writes each audit event as a row
// with its op, principals, session and object ids.
func TestStore_auditEventsArePersisted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := testDB(t)
	eng, err := brain.NewEngine(store, brain.WithAudit(store.Record))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: storetest.Namespace(t, "org", "a"), Principals: []string{"user:ann"}, SessionID: "s1"}

	obj, err := eng.Save(ctx, scope, brain.Object{Kind: "Note", Title: "n"})
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

// TestEngine_aClosedStoreFailsTheCallsThatReadIt: after the pool closes, every
// engine call that reads the store returns its error.
func TestEngine_aClosedStoreFailsTheCallsThatReadIt(t *testing.T) {
	ctx := t.Context()
	store, pool := testOpen(t, postgres.Options{Schema: pgtest.Schema(t), EmbeddingDim: 3})
	if err := store.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: storetest.Namespace(t, "org", uuid.NewString())}
	var ids []uuid.UUID
	for _, title := range []string{"from shared", "to shared"} {
		obj, err := eng.Save(ctx, scope, brain.Object{Kind: "Document", Title: title})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, obj.ID)
	}
	if err := eng.Link(ctx, scope, brain.LinkRequest{From: ids[0], To: ids[1], Relation: "references"}); err != nil {
		t.Fatal(err)
	}
	first, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "shared", Limit: 1})
	if err != nil || !first.More {
		t.Fatalf("first page = %+v %v, want more", first, err)
	}
	// The first open reads the kind registry; a fresh engine reads it after the close.
	if _, err := eng.Open(ctx, scope, ids[0]); err != nil {
		t.Fatal(err)
	}
	fresh, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	_, registryErr := fresh.Open(ctx, scope, ids[0])
	_, nextErr := eng.Next(ctx, scope, first.Rest, 1)
	_, searchErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "shared"})
	_, relatedErr := eng.Related(ctx, scope, brain.RelatedRequest{ID: ids[0]})
	_, openErr := eng.Open(ctx, scope, ids[0])
	_, listErr := eng.Search(ctx, scope, brain.SearchRequest{Kind: "Document"})
	_, withinErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "shared", Within: ids[0]})
	_, resolveErr := eng.Resolve(ctx, scope, brain.Ref{Path: "/workspace/work/doc.md"})
	kindsErr := eng.ApplyKinds(ctx, brain.KindSpec{Kind: "Document", IsParent: true})

	for name, err := range map[string]error{"next page": nextErr, "search": searchErr, "related": relatedErr, "open": openErr, "open's kind registry": registryErr, "list records": listErr, "search within": withinErr, "resolve a path": resolveErr, "apply kinds": kindsErr} {
		if err == nil || !strings.Contains(err.Error(), "closed pool") {
			t.Errorf("%s on a closed store = %v, want its error", name, err)
		}
	}
}

// TestStore_searchRanksByTheWordsTheWayAReaderExpects: full-text search ranks
// a title hit over a body hit and a part with both terms over a part with one,
// and an exact search finds a phrase deep in a long body.
func TestStore_searchRanksByTheWordsTheWayAReaderExpects(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, _ := testDB(t)
	ns := storetest.Namespace(t, "org", "fts")
	doc := brain.Object{ID: uuid.New(), Kind: "Document", Title: "Guide", Namespace: ns}
	titleHit := chunk(doc, "zymurgy", "shipping notes "+strings.Repeat("filler ", 100), 0)
	bodyHit := chunk(doc, "weekly", "zymurgy", 1)
	both := chunk(doc, "alphaonlyterm betaonlyterm", "paired", 2)
	onlyA, onlyB := chunk(doc, "notes", "alphaonlyterm", 3), chunk(doc, "notes", "betaonlyterm", 4)
	deep := chunk(doc, "long", strings.Repeat("padding word ", 80)+"grace window", 5)
	if _, err := store.PutTree(ctx, brain.Scope{Namespace: ns}, brain.Indexed{Object: doc}, indexedParts(titleHit, bodyHit, both, onlyA, onlyB, deep)); err != nil {
		t.Fatal(err)
	}
	search := func(query string, exact bool) []brain.ScoredID {
		hits, err := store.Search(ctx, brain.Scope{Namespace: ns}, brain.RecordSearch{Query: query, Exact: exact, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return hits
	}

	zymurgy := search("zymurgy", false)
	terms := search("alphaonlyterm betaonlyterm", false)
	phrase := search("grace window", true)

	if len(zymurgy) != 2 || zymurgy[0].ID != titleHit.ID || zymurgy[1].ID != bodyHit.ID {
		t.Errorf("a title hit and a body hit = %v, want the title hit first", hitIDs(zymurgy))
	}
	if len(terms) != 3 || terms[0].ID != both.ID {
		t.Errorf("two terms = %v, want the part holding both first, then each part holding one", hitIDs(terms))
	}
	if len(phrase) == 0 || phrase[0].ID != deep.ID {
		t.Errorf("exact search for a phrase deep in a long body = %v, want that part", hitIDs(phrase))
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
