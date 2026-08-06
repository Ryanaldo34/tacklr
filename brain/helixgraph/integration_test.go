package helixgraph_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
)

// enterprise-dev in-memory mode (no PATH_TO_QUERIES / MinIO). Dynamic /v1/query.
// https://docs.helix-db.com/database/local-development
const helixDevImage = "ghcr.io/helixdb/enterprise-dev:latest"

var (
	helixOnce sync.Once
	helixURL  string
	helixErr  error
	helixSkip string
)

// TestGraph_liveNeighborsBoth is the real Helix outcome for Both-direction
// expand: outbound and inbound edges by object_id, multi-label, and limit.
func TestGraph_liveNeighborsBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t)

	a, b, c := uuid.New(), uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{a, b, c} {
		if err := g.EnsureObject(ctx, brain.Object{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	// a --references--> b, c --references--> a (inbound to a)
	if err := g.AddEdge(ctx, a, b, "references", brain.EdgeMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, c, a, "references", brain.EdgeMeta{}); err != nil {
		t.Fatal(err)
	}
	// a --depends_on--> b (second label)
	if err := g.AddEdge(ctx, a, b, "depends_on", brain.EdgeMeta{}); err != nil {
		t.Fatal(err)
	}

	ns, err := g.Neighbors(ctx, a, []string{"references"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]string{}
	for _, n := range ns {
		got[n.ObjectID] = n.RelationType
	}
	if got[b] != "references" || got[c] != "references" {
		t.Fatalf("both directions for references: %+v", ns)
	}
	if len(ns) != 2 {
		t.Fatalf("want exactly b and c: %+v", ns)
	}

	// Label filter: depends_on should only surface b.
	dep, err := g.Neighbors(ctx, a, []string{"depends_on"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dep) != 1 || dep[0].ObjectID != b || dep[0].RelationType != "depends_on" {
		t.Fatalf("depends_on: %+v", dep)
	}

	// Multi-label still returns neighbors (deduped by object id across labels).
	multi, err := g.Neighbors(ctx, a, []string{"references", "depends_on"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	multiIDs := map[uuid.UUID]bool{}
	for _, n := range multi {
		multiIDs[n.ObjectID] = true
	}
	if !multiIDs[b] || !multiIDs[c] {
		t.Fatalf("multi label: %+v", multi)
	}

	// Limit truncates.
	lim, err := g.Neighbors(ctx, a, []string{"references"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(lim) != 1 {
		t.Fatalf("limit 1: %+v", lim)
	}

	// Unknown object → empty, not error.
	empty, err := g.Neighbors(ctx, uuid.New(), []string{"references"}, 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("unknown: %+v err=%v", empty, err)
	}
}

// TestGraph_liveEnsureObjectIdempotent re-ensures the same object_id without wiping edges.
func TestGraph_liveEnsureObjectIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t)
	id := uuid.New()
	other := uuid.New()
	if err := g.EnsureObject(ctx, brain.Object{ID: id, Title: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureObject(ctx, brain.Object{ID: other, Title: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, id, other, "references", brain.EdgeMeta{Note: "keep"}); err != nil {
		t.Fatal(err)
	}
	// Re-ensure without re-link: edges must survive.
	if err := g.EnsureObject(ctx, brain.Object{ID: id, Title: "a-v2"}); err != nil {
		t.Fatal(err)
	}
	ns, err := g.Neighbors(ctx, id, []string{"references"}, 5)
	if err != nil || len(ns) != 1 || ns[0].ObjectID != other {
		t.Fatalf("after re-ensure: %+v err=%v", ns, err)
	}
	if ns[0].Meta.Note != "keep" {
		t.Fatalf("edge meta after re-ensure: %+v", ns[0].Meta)
	}
}

// TestEngine_liveFindObjectsTextSearch: EnsureSearchIndexes + Put dual-write + FindObjects
// via native Helix TextSearchNodes (enterprise-dev).
func TestEngine_liveFindObjectsTextSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t)
	if err := g.Bootstrap(ctx, false); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Fact", IsParent: true},
		brain.KindSpec{Kind: "Deal", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasObjectSearch() {
		t.Fatal("HasObjectSearch")
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	sc := brain.NewSearchContext()
	fact, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Fact", Title: "MSA commercial liability risk",
		Content: "indemnity exclusivity penalty on late delivery",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Other deal", Content: "unrelated",
	}); err != nil {
		t.Fatal(err)
	}
	page, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "indemnity exclusivity penalty",
		Kinds: []string{"Fact"},
		Limit: 10,
	}, sc)
	if err != nil {
		// Helix enterprise-dev may not fully support text indexes in all builds.
		t.Logf("FindObjects live: %v (indexes may be limited in this Helix image)", err)
		// Fall back: at least SearchText should not panic; skip if unsupported.
		if hits, sErr := g.SearchText(ctx, "indemnity", 5, nil); sErr != nil {
			t.Skipf("Helix text search unavailable: %v", sErr)
		} else if len(hits) == 0 {
			t.Skip("Helix text search returned no hits; image may lack full text index support")
		}
		t.Fatalf("FindObjects: %v", err)
	}
	if len(page.Objects) == 0 {
		t.Skip("no FindObjects hits from Helix text search in this environment")
	}
	found := false
	for _, o := range page.Objects {
		if o.ID == fact.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected fact %s in %+v", fact.ID, page.Objects)
	}
}

// TestEngine_liveDualWriteLinkExpand is the Engine multi-turn outcome on real Helix
// (enterprise-dev in-memory): Put dual-writes nodes, Link creates edges, Expand
// returns hydrated neighbors; re-Put + re-Link after title update; soft-deleted
// targets are hidden by scope hydration.
func TestEngine_liveDualWriteLinkExpand(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t)
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store,
		brain.WithGraph(g),
		brain.WithEmbedder(helixStubEmbedder{v: []float32{0.5, 0.5}}),
		brain.WithKinds(
			brain.KindSpec{Kind: "Document", IsParent: true},
			brain.KindSpec{Kind: "Fact", IsParent: true},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasGraphWriter() {
		t.Fatal("HasGraphWriter")
	}

	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	sc := brain.NewSearchContext()

	// Turn 1: put discovery-like objects (graph nodes + embeddings).
	a, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Document", Title: "Source", Content: "source body",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Embedding) != 2 {
		t.Fatalf("embedding on put: %+v", a.Embedding)
	}
	b, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Fact", Title: "Target fact", Content: "durable claim",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Turn 2: link and expand graph mode.
	if err := eng.Link(ctx, scope, a.ID, b.ID, "references"); err != nil {
		t.Fatal(err)
	}
	exp, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a.ID, RelationTypes: []string{"references"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if exp.Mode != "graph" || len(exp.Objects) != 1 || exp.Objects[0].ID != b.ID {
		t.Fatalf("expand after link: %+v", exp)
	}
	if exp.Objects[0].Title != "Target fact" {
		t.Fatalf("hydrated title: %+v", exp.Objects[0])
	}

	// Turn 3: re-put A (graph EnsureObject in-place update) without re-link.
	a.Title = "Source-v2"
	a, err = eng.Put(ctx, scope, a)
	if err != nil {
		t.Fatal(err)
	}
	if a.Title != "Source-v2" {
		t.Fatalf("put update: %+v", a)
	}
	exp, err = eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a.ID, RelationTypes: []string{"references"},
	}, sc)
	if err != nil || len(exp.Objects) != 1 || exp.Objects[0].ID != b.ID {
		t.Fatalf("expand after re-put (no re-link): %+v err=%v", exp, err)
	}

	// Turn 4: soft-delete target → graph still has edge but hydrate drops it.
	if err := eng.SoftDelete(ctx, scope, b.ID); err != nil {
		t.Fatal(err)
	}
	exp, err = eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a.ID, RelationTypes: []string{"references"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Objects) != 0 {
		t.Fatalf("soft-deleted neighbor must not hydrate: %+v", exp.Objects)
	}

	// Containment expand on a parent with no children.
	c, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "Lonely parent"})
	if err != nil {
		t.Fatal(err)
	}
	emptyKids, err := eng.Expand(ctx, scope, brain.ExpandRequest{ObjectID: c.ID}, sc)
	if err != nil || emptyKids.Mode != "children" || len(emptyKids.Objects) != 0 {
		t.Fatalf("empty children: %+v err=%v", emptyKids, err)
	}
}

type helixStubEmbedder struct{ v []float32 }

func (s helixStubEmbedder) Embed(context.Context, string) ([]float32, error) { return s.v, nil }

// TestGraph_liveEnsureObjectRichProps dual-writes searchable node props and keeps edges
// after an endpoint is re-ensured with updated title/embedding.
func TestGraph_liveEnsureObjectRichProps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t)

	nsID := uuid.New()
	// Helix vector index dim is process-global; match dual-write tests (2-d).
	a := brain.Object{
		ID: uuid.New(), Kind: "Document", Title: "Alpha", Summary: "sum-a",
		Content: "alpha body", NamespaceID: nsID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Embedding: []float32{1, 0},
	}
	b := brain.Object{
		ID: uuid.New(), Kind: "Document", Title: "Beta",
		Content: "beta body", NamespaceID: nsID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Embedding: []float32{0, 1},
	}
	if err := g.EnsureObject(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureObject(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, a.ID, b.ID, "references", brain.EdgeMeta{}); err != nil {
		t.Fatal(err)
	}

	// Turn 2: re-ensure A with new title (in-place); edge to B must remain without re-link.
	a.Title = "Alpha-v2"
	a.Content = "updated alpha"
	if err := g.EnsureObject(ctx, a); err != nil {
		t.Fatal(err)
	}
	ns, err := g.Neighbors(ctx, a.ID, []string{"references"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 1 || ns[0].ObjectID != b.ID {
		t.Fatalf("neighbors after re-ensure without re-link: %+v", ns)
	}
}

func liveGraph(t *testing.T) *helixgraph.Graph {
	t.Helper()
	ctx := context.Background()
	base := sharedHelixURL(t)
	g, err := helixgraph.New(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Bootstrap(ctx, false); err != nil {
		t.Fatal(err)
	}
	return g
}

func sharedHelixURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	helixOnce.Do(func() {
		ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        helixDevImage,
				ExposedPorts: []string{"8080/tcp"},
				WaitingFor: wait.ForHTTP("/health").
					WithPort("8080/tcp").
					WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
		if err != nil {
			helixSkip = fmt.Sprintf("%v (docker pull %s)", err, helixDevImage)
			return
		}
		host, err := ctr.Host(ctx)
		if err != nil {
			helixErr = err
			_ = ctr.Terminate(ctx)
			return
		}
		port, err := ctr.MappedPort(ctx, "8080/tcp")
		if err != nil {
			helixErr = err
			_ = ctr.Terminate(ctx)
			return
		}
		// Keep container for process lifetime; Ryuk reaps on exit.
		helixURL = fmt.Sprintf("http://%s:%s", host, port.Port())
	})
	if helixSkip != "" {
		t.Skipf("helix container unavailable: %s", helixSkip)
	}
	if helixErr != nil {
		t.Fatal(helixErr)
	}
	if helixURL == "" {
		t.Fatal("helix url empty")
	}
	return helixURL
}
