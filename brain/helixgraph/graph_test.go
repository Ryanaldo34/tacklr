package helixgraph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
)

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}
func TestNewFromClient_requiresClient(t *testing.T) {
	if _, err := helixgraph.NewFromClient(nil); err == nil {
		t.Fatal("want error")
	}
}

func TestGraph_everyCallReportsAnUnreachableHelix(t *testing.T) {
	ctx := t.Context()
	// Nothing listens on port 9 (discard), so every request fails to connect.
	g, err := helixgraph.New("http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", "a")
	a, b := uuid.New(), uuid.New()
	errs := map[string]error{
		"bootstrap":       g.Bootstrap(ctx),
		"edge text index": g.EnsureEdgeTextIndex(ctx, "cites"),
		"ensure":          g.EnsureObject(ctx, brain.Object{ID: a, Namespace: ns}, "text"),
		"remove":          g.RemoveObject(ctx, a),
		"link":            g.AddEdge(ctx, a, b, "references", brain.EdgeMeta{}),
		"unlink":          g.RemoveEdge(ctx, a, b, "references"),
	}
	_, errs["neighbors"] = g.Neighbors(ctx, a, []string{"references"}, 10)
	_, errs["text search"] = g.SearchText(ctx, "text", 10, ns)
	_, errs["edge search"] = g.SearchEdgesText(ctx, "cites", "text", 10)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	for name, err := range errs {
		if err == nil || errors.Is(err, brain.ErrInvalid) {
			t.Errorf("%s against an unreachable Helix = %v, want a request error", name, err)
		}
	}
	if err := g.Bootstrap(cancelled); !errors.Is(err, context.Canceled) || g.ObjectSearchReady() || g.Client() == nil {
		t.Errorf("cancelled bootstrap = %v, ready %v", err, g.ObjectSearchReady())
	}
}

// enterprise-dev in-memory mode (no PATH_TO_QUERIES / MinIO). Dynamic /v1/query.
// https://docs.helix-db.com/database/local-development
const helixDevImage = "ghcr.io/helixdb/enterprise-dev:latest"

var (
	helixOnce sync.Once
	helixURL  string
	helixErr  error
	helixSkip string
)

// TestEngine_liveFindObjectsTextSearch: Bootstrap + Put dual-write + FindObjects
// via native Helix TextSearchNodes (enterprise-dev).
func TestEngine_liveFindObjectsTextSearch(t *testing.T) {
	ctx := context.Background()
	g := liveGraph(t)
	if err := g.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(g), brain.WithKinds(
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
		t.Fatalf("FindObjects: %v", err)
	}
	if len(page.Objects) == 0 {
		t.Fatal("FindObjects returned no hits")
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

func (s helixStubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	return repeatVector(s.v, len(texts)), nil
}

// TestEngine_liveFindLinksSearchEdges: Engine find_links via real Helix edge text search.
func TestEngine_liveFindLinksSearchEdges(t *testing.T) {
	ctx := context.Background()
	g := liveGraph(t)
	if err := g.EnsureEdgeTextIndex(ctx, "about"); err != nil {
		t.Fatalf("EnsureEdgeTextIndex: %v", err)
	}
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(g), brain.WithKinds(
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
		t.Fatalf("FindLinks: %v", err)
	}
	if len(links.Links) == 0 {
		t.Fatal("FindLinks returned no hits")
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

func TestGraph_liveWritesObjectsWithMixedPropertyTypes(t *testing.T) {
	ctx := t.Context()
	g := liveGraph(t)
	ns := mustNS(t, "id", uuid.NewString())
	word := "props" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	obj := brain.Object{
		ID: uuid.New(), Kind: "Document", Title: "Mixed", Summary: "summary", Namespace: ns,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Embedding: []float32{1, 0},
		Properties: map[string]any{
			"flag": true, "score": 3.5, "count": int64(7), "level": 2, "level32": int32(3), "f32": float32(1.25),
			"label": "  tagged  ", "empty": "   ", "nested": struct{}{}, "": "no key",
		},
	}

	inserted := g.EnsureObject(ctx, obj, word)
	updated := g.EnsureObject(ctx, obj, word)
	hits, searchErr := g.SearchText(ctx, word, 5, ns)
	blank := g.EnsureEdgeTextIndex(ctx, " ")

	if inserted != nil || updated != nil || searchErr != nil || len(hits) != 1 || hits[0].ID != obj.ID {
		t.Fatalf("object with mixed properties: insert %v, update %v, search %v %v", inserted, updated, hits, searchErr)
	}
	if !errors.Is(blank, brain.ErrInvalid) {
		t.Fatalf("edge text index without a label = %v, want ErrInvalid", blank)
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
	if err := g.Bootstrap(ctx); err != nil {
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

func (helixStubEmbedder) Model() string { return "test-model" }

// repeatVector returns n copies of v, one per embedded text.
func repeatVector(v []float32, n int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		out[i] = v
	}
	return out
}
