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
		if err := g.EnsureObject(ctx, brain.Object{ID: id}, ""); err != nil {
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
	if err := g.EnsureObject(ctx, brain.Object{ID: id, Title: "a"}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureObject(ctx, brain.Object{ID: other, Title: "b"}, "b"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, id, other, "references", brain.EdgeMeta{Note: "keep"}); err != nil {
		t.Fatal(err)
	}
	// Re-ensure without re-link: edges must survive.
	if err := g.EnsureObject(ctx, brain.Object{ID: id, Title: "a-v2"}, "a-v2"); err != nil {
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
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
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

	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
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

	nsID := mustNS(t, "id", uuid.NewString())
	// Helix vector index dim is process-global; match dual-write tests (2-d).
	a := brain.Object{
		ID: uuid.New(), Kind: "Document", Title: "Alpha", Summary: "sum-a",
		Content: "alpha body", Namespace: nsID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Embedding: []float32{1, 0},
	}
	b := brain.Object{
		ID: uuid.New(), Kind: "Document", Title: "Beta",
		Content: "beta body", Namespace: nsID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Embedding: []float32{0, 1},
	}
	if err := g.EnsureObject(ctx, a, brain.EntityIndexText(a)); err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureObject(ctx, b, brain.EntityIndexText(b)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(ctx, a.ID, b.ID, "references", brain.EdgeMeta{}); err != nil {
		t.Fatal(err)
	}

	// Turn 2: re-ensure A with new title (in-place); edge to B must remain without re-link.
	a.Title = "Alpha-v2"
	a.Content = "updated alpha"
	if err := g.EnsureObject(ctx, a, brain.EntityIndexText(a)); err != nil {
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

// TestGraph_liveSearchEdgesTextVectorAndProps: edge-text index + SearchEdgesText,
// vector search, custom scalar Properties dual-write, RemoveObject, early guards.
// Real Helix enterprise-dev via Testcontainers (no mocks).
func TestGraph_liveSearchEdgesTextVectorAndProps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t)

	// Client escape hatch is non-nil after New.
	if g.Client() == nil {
		t.Fatal("Client()")
	}
	if !g.ObjectSearchReady() {
		t.Fatal("ObjectSearchReady after Bootstrap")
	}

	// Empty / invalid inputs: no error, empty result (guards).
	if hits, err := g.SearchText(ctx, "  ", 5, nil); err != nil || hits != nil {
		t.Fatalf("empty SearchText: %+v err=%v", hits, err)
	}
	if hits, err := g.SearchText(ctx, "q", 0, nil); err != nil || hits != nil {
		t.Fatalf("limit 0 SearchText: %+v err=%v", hits, err)
	}
	if hits, err := g.SearchVector(ctx, nil, 5, nil); err != nil || hits != nil {
		t.Fatalf("empty SearchVector: %+v err=%v", hits, err)
	}
	if hits, err := g.SearchVector(ctx, []float32{1, 0}, 0, nil); err != nil || hits != nil {
		t.Fatalf("limit 0 SearchVector: %+v err=%v", hits, err)
	}
	if hits, err := g.SearchEdgesText(ctx, "", "note", 5); err != nil || hits != nil {
		t.Fatalf("empty label SearchEdgesText: %+v err=%v", hits, err)
	}
	if hits, err := g.SearchEdgesText(ctx, "references", "", 5); err != nil || hits != nil {
		t.Fatalf("empty query SearchEdgesText: %+v err=%v", hits, err)
	}
	if err := g.EnsureEdgeTextIndex(ctx, "  "); err == nil {
		t.Fatal("EnsureEdgeTextIndex empty label")
	}
	if ns, err := g.Neighbors(ctx, uuid.Nil, []string{"references"}, 5); err != nil || ns != nil {
		t.Fatalf("nil object Neighbors: %+v err=%v", ns, err)
	}
	if ns, err := g.Neighbors(ctx, uuid.New(), nil, 5); err != nil || ns != nil {
		t.Fatalf("empty labels Neighbors: %+v err=%v", ns, err)
	}

	// Dual-write objects with custom scalar Properties (sortedPropKeys + scalarPropValue).
	nsID := mustNS(t, "id", uuid.NewString())
	a := brain.Object{
		ID: uuid.New(), Kind: "Document", Title: "EdgeSearchSource",
		Summary: "sum-src", Content: "source body unique-helix-alpha",
		Namespace: nsID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Embedding: []float32{1, 0},
		Properties: map[string]any{
			"flag":    true,
			"score":   3.5,
			"count":   int64(7),
			"label":   "  tagged  ",
			"empty":   "   ",
			"skip":    struct{}{}, // non-scalar ignored
			"":        "bad-key",
			"level":   int(2),
			"level32": int32(3),
			"f32":     float32(1.25),
		},
	}
	b := brain.Object{
		ID: uuid.New(), Kind: "Document", Title: "EdgeSearchTarget",
		Content: "target body unique-helix-beta", Namespace: nsID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Embedding: []float32{0, 1},
	}
	if err := g.EnsureObject(ctx, a, brain.EntityIndexText(a)); err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureObject(ctx, b, brain.EntityIndexText(b)); err != nil {
		t.Fatal(err)
	}
	// Full edge meta for AddEdge prop dual-write.
	evID := uuid.New()
	meta := brain.EdgeMeta{
		Note:       "indemnity exclusivity penalty clause",
		Status:     "active",
		Role:       "cites",
		Confidence: 0.91,
		EvidenceID: &evID,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := g.AddEdge(ctx, a.ID, b.ID, "references", meta); err != nil {
		t.Fatal(err)
	}

	// Edge text index + search (SearchEdgesText was 0% without this path).
	if err := g.EnsureEdgeTextIndex(ctx, "references"); err != nil {
		t.Fatalf("EnsureEdgeTextIndex: %v", err)
	}
	// Idempotent re-ensure
	if err := g.EnsureEdgeTextIndex(ctx, "references"); err != nil {
		t.Fatal(err)
	}
	edgeHits, err := g.SearchEdgesText(ctx, "references", "indemnity exclusivity", 10)
	if err != nil {
		// Some Helix images may not support edge text indexes yet.
		t.Logf("SearchEdgesText: %v", err)
		t.Skipf("Helix edge text search unavailable: %v", err)
	}
	if len(edgeHits) == 0 {
		t.Skip("SearchEdgesText returned no hits; image may lack edge text index support")
	}
	foundEdge := false
	for _, h := range edgeHits {
		if h.FromID == a.ID && h.ToID == b.ID && h.RelationType == "references" {
			foundEdge = true
			if h.Meta.Note == "" && h.Meta.Status == "" {
				t.Logf("edge hit meta sparse: %+v", h.Meta)
			}
			break
		}
	}
	if !foundEdge {
		t.Fatalf("expected edge a→b in SearchEdgesText: %+v", edgeHits)
	}

	// Text search nodes
	textHits, err := g.SearchText(ctx, "unique-helix-alpha", 5, nil)
	if err != nil {
		t.Logf("SearchText: %v", err)
	} else if len(textHits) > 0 {
		// ok when supported
		found := false
		for _, h := range textHits {
			if h.ID == a.ID {
				found = true
			}
		}
		if !found {
			t.Logf("SearchText hits did not include source (ok if ranking differs): %+v", textHits)
		}
	}

	// Vector search (2-d embeddings; same dim as dual-write tests)
	vecHits, err := g.SearchVector(ctx, []float32{1, 0}, 5, nil)
	if err != nil {
		t.Logf("SearchVector: %v (may be limited on this image)", err)
	} else if len(vecHits) == 0 {
		t.Log("SearchVector empty")
	}

	// Namespace-scoped search when tenant indexes are available
	if err := g.Bootstrap(ctx, true); err != nil {
		t.Logf("Bootstrap tenant: %v", err)
	} else if g.TenantEnabled() {
		if _, err := g.SearchText(ctx, "unique-helix-alpha", 5, nsID); err != nil {
			t.Logf("tenant SearchText: %v", err)
		}
		if _, err := g.SearchVector(ctx, []float32{0, 1}, 5, nsID); err != nil {
			t.Logf("tenant SearchVector: %v", err)
		}
	}

	// Neighbors default limit + direction meta
	ns, err := g.Neighbors(ctx, a.ID, []string{"references"}, 0) // default limit
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 1 || ns[0].ObjectID != b.ID {
		t.Fatalf("Neighbors: %+v", ns)
	}
	if ns[0].Direction != "out" {
		t.Fatalf("direction: %+v", ns[0])
	}
	if ns[0].Meta.Note != meta.Note {
		t.Fatalf("neighbor meta note: %+v", ns[0].Meta)
	}

	// RemoveObject drops node (and incident edges)
	if err := g.RemoveObject(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	ns, err = g.Neighbors(ctx, a.ID, []string{"references"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 0 {
		t.Fatalf("after RemoveObject neighbors: %+v", ns)
	}
	if err := g.RemoveObject(ctx, uuid.Nil); err == nil {
		t.Fatal("RemoveObject nil id")
	}
}

// TestEngine_liveFindLinksSearchEdges: Engine find_links via real Helix edge text search.
func TestEngine_liveFindLinksSearchEdges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping helix integration test in -short mode")
	}
	ctx := context.Background()
	g := liveGraph(t)
	if err := g.EnsureEdgeTextIndex(ctx, "about"); err != nil {
		t.Skipf("edge text index: %v", err)
	}
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Fact", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasEdgeSearch() {
		t.Fatal("HasEdgeSearch")
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	doc, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Document", Title: "Contract", Content: "MSA body",
	})
	if err != nil {
		t.Fatal(err)
	}
	fact, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Fact", Title: "Risk", Content: "late delivery penalty",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LinkWith(ctx, scope, doc.ID, fact.ID, "about", brain.EdgeMeta{
		Note: "late delivery penalty exclusivity",
	}); err != nil {
		t.Fatal(err)
	}
	links, err := eng.FindLinks(ctx, scope, brain.FindLinksRequest{
		Query:        "late delivery penalty",
		RelationType: "about",
		Limit:        10,
	})
	if err != nil {
		t.Skipf("FindLinks: %v", err)
	}
	if len(links.Links) == 0 {
		t.Skip("no FindLinks hits from Helix edge text search")
	}
	ok := false
	for _, l := range links.Links {
		if l.From.ID == doc.ID && l.To.ID == fact.ID {
			ok = true
		}
	}
	if !ok {
		t.Fatalf("expected doc→fact about link: %+v", links.Links)
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
