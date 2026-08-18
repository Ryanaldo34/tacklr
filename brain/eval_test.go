package brain_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// --- Corpus ranking goldens -------------------------------------------------

func TestEval_goldenSearchQueries(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	now := time.Now().UTC()
	store := brain.NewMemoryStore()

	for _, d := range []struct {
		title string
		body  string
	}{
		{"OAuth PKCE Guide", "Implement OAuth 2.0 PKCE for mobile clients"},
		{"Billing Runbook", "Invoice reconciliation and chargeback flow"},
		{"Vector Search Notes", "HNSW cosine similarity embeddings for RAG"},
	} {
		pid := uuid.New()
		pos := 1
		_ = store.Put(ctx, brain.Object{ID: pid, Kind: "Document", Title: d.title, NamespaceID: ns, UpdatedAt: now})
		_ = store.Put(ctx, brain.Object{
			ID: uuid.New(), Kind: "Chunk", Content: d.body, ParentID: &pid, Position: &pos,
			NamespaceID: ns, UpdatedAt: now,
		})
	}

	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{
		Now: func() time.Time { return now },
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		query, wantTitle string
	}{
		{"oauth pkce mobile", "OAuth PKCE Guide"},
		{"invoice chargeback", "Billing Runbook"},
		{"hnsw embeddings rag", "Vector Search Notes"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			page, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
				Query: tc.query, Limit: 5,
			}, brain.NewSearchContext())
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range page.Objects {
				if o.Title == tc.wantTitle {
					return
				}
			}
			t.Fatalf("want title %q in %+v", tc.wantTitle, titlesOf(page.Objects))
		})
	}
}

// --- GraphRAG composition golden --------------------------------------------

// TestEval_graphRAGComposition locks the host recipe documented in package doc:
//
//	find_objects → LandingIDs → ExpandMany → search(scope_ids=…)
//
// Real MemoryStore + MemoryGraph (dual-write via Put/Link). No fake graph fakes.
func TestEval_graphRAGComposition(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString},
		}},
		brain.KindSpec{Kind: "Fact", IsParent: true},
		brain.KindSpec{Kind: "Buyer", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	sc := brain.NewSearchContext()

	// World: Acme deal linked to a risk fact and a buyer; noise deal elsewhere;
	// one corpus chunk under Acme for neighborhood search.
	deal, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Acme Enterprise Renewal", Summary: "enterprise renewal opportunity",
		Properties: map[string]any{"stage": "negotiation"},
	})
	if err != nil {
		t.Fatal(err)
	}
	fact, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Fact", Title: "Budget cut risk", Content: "customer signaled budget pressure on renewal",
	})
	if err != nil {
		t.Fatal(err)
	}
	buyer, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Buyer", Title: "Contoso LLC", Summary: "primary economic buyer",
	})
	if err != nil {
		t.Fatal(err)
	}
	noise, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Other Pipeline Deal", Summary: "unrelated opportunity",
		Properties: map[string]any{"stage": "prospect"},
	})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "pricing thread", Content: "pricing email about Acme renewal risk and budget",
		ParentID: &deal.ID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
	// Noise corpus that must not appear under scope_ids=[deal].
	noiseParent, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Noise Doc Parent", Summary: "holder for noise chunk",
	})
	if err != nil {
		t.Fatal(err)
	}
	pos2 := 1
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "noise", Content: "pricing email about Acme renewal risk and budget",
		ParentID: &noiseParent.ID, Position: &pos2,
	}); err != nil {
		t.Fatal(err)
	}

	if err := eng.Link(ctx, scope, fact.ID, deal.ID, "about"); err != nil {
		t.Fatal(err)
	}
	if err := eng.LinkWith(ctx, scope, deal.ID, buyer.ID, "has_buyer", brain.EdgeMeta{
		Note: "primary economic buyer for Acme renewal", Role: "primary",
	}); err != nil {
		t.Fatal(err)
	}
	// Noise edge so ExpandMany must not pull noise deal into Acme neighborhood.
	if err := eng.Link(ctx, scope, noise.ID, buyer.ID, "has_buyer"); err != nil {
		t.Fatal(err)
	}

	// 1) Entity land
	land, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "Acme Enterprise Renewal",
		Kinds: []string{"Deal"},
		Limit: 5,
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if !containsID(land.Objects, deal.ID) {
		t.Fatalf("find_objects must land on Acme deal, got %v", titlesOf(land.Objects))
	}

	// 2) Promote to expand-safe first-class ids
	seeds := brain.LandingIDsFromPage(land)
	if len(seeds) == 0 || seeds[0] != deal.ID {
		// Landing may include only Acme; if multiple Deals match, Acme must be present.
		if !containsUUID(seeds, deal.ID) {
			t.Fatalf("LandingIDs must include Acme deal: %v", seeds)
		}
	}
	// Parts from a corpus search promote to parent — fold LandingIDs outcome here.
	corpusHit, err := eng.Search(ctx, scope, brain.SearchRequest{
		Query: "pricing email Acme renewal", Limit: 10,
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	fromCorpus := brain.LandingIDsFromPage(corpusHit)
	if !containsUUID(fromCorpus, deal.ID) {
		t.Fatalf("corpus landings must promote to Acme parent: hits=%v landings=%v",
			titlesOf(corpusHit.Objects), fromCorpus)
	}

	// 3) ExpandMany from deal seed(s) — only Acme's neighborhood, tagged SourceID
	many, err := eng.ExpandMany(ctx, scope, brain.ExpandManyRequest{
		ObjectIDs:     []uuid.UUID{deal.ID},
		RelationTypes: []string{"about", "has_buyer"},
		MaxHops:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]brain.RichObject{}
	for _, o := range many.Objects {
		got[o.ID] = o
	}
	if _, ok := got[fact.ID]; !ok {
		t.Fatalf("ExpandMany missing fact: %v", titlesOf(many.Objects))
	}
	if _, ok := got[buyer.ID]; !ok {
		t.Fatalf("ExpandMany missing buyer: %v", titlesOf(many.Objects))
	}
	if _, ok := got[noise.ID]; ok {
		t.Fatalf("ExpandMany must not include noise deal: %v", titlesOf(many.Objects))
	}
	for _, id := range []uuid.UUID{fact.ID, buyer.ID} {
		o := got[id]
		if o.Relation == nil || o.Relation.SourceID == nil || *o.Relation.SourceID != deal.ID {
			t.Fatalf("neighbor %s SourceID want deal: %+v", o.Title, o.Relation)
		}
	}

	// 4) Neighborhood corpus — same tokens exist under noise parent; ScopeIDs isolates Acme.
	scoped, err := eng.Search(ctx, scope, brain.SearchRequest{
		Query:    "pricing email Acme renewal risk budget",
		ScopeIDs: []uuid.UUID{deal.ID},
		Limit:    10,
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped.Objects) != 1 || scoped.Objects[0].ID != deal.ID {
		t.Fatalf("scoped neighborhood search want only Acme parent: %v", titlesOf(scoped.Objects))
	}

	// 5) WantContainment mixes Postgres children with graph neighbors.
	mixed, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: deal.ID, RelationTypes: []string{"has_buyer"}, WantContainment: true,
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if mixed.Mode != "mixed" {
		t.Fatalf("want mixed mode, got %s", mixed.Mode)
	}
	if !containsID(mixed.Objects, buyer.ID) {
		t.Fatalf("mixed missing buyer: %v", titlesOf(mixed.Objects))
	}
	// Chunk under deal must appear via containment.
	var sawChunk bool
	for _, o := range mixed.Objects {
		if o.ParentID != nil && *o.ParentID == deal.ID {
			sawChunk = true
			break
		}
	}
	if !sawChunk {
		t.Fatalf("mixed missing containment chunk: %v", titlesOf(mixed.Objects))
	}

	// Recipe path: two-hop expand from fact through deal is host ExpandByRecipe.
	if err := eng.RegisterExpandRecipe(brain.ExpandRecipe{
		Name: "about_and_buyers", RelationTypes: []string{"about", "has_buyer"}, MaxHops: 2,
	}); err != nil {
		t.Fatal(err)
	}
	// From fact --about--> deal --has_buyer--> buyer (2 hops on mixed labels via multi-hop on union).
	// MemoryGraph walks each hop with the label set; hop1 from fact: deal; hop2 from deal: buyer (+ fact reverse).
	fromFact, err := eng.ExpandByRecipe(ctx, scope, fact.ID, "about_and_buyers", sc)
	if err != nil {
		t.Fatal(err)
	}
	if !containsID(fromFact.Objects, deal.ID) {
		t.Fatalf("recipe hop1 deal: %v", titlesOf(fromFact.Objects))
	}

	// Edge-text land: FindLinks matches edge note (MemoryGraph substring on note).
	links, err := eng.FindLinks(ctx, scope, brain.FindLinksRequest{
		RelationType: "has_buyer", Query: "economic buyer", Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(links.Links) != 1 || links.Links[0].From.ID != deal.ID || links.Links[0].To.ID != buyer.ID {
		t.Fatalf("find_links: %+v", links.Links)
	}
	if links.Links[0].Meta.Role != "primary" {
		t.Fatalf("link meta: %+v", links.Links[0].Meta)
	}

	if _, err := eng.FindLinks(ctx, scope, brain.FindLinksRequest{}); !errors.Is(err, brain.ErrLinkQueryRequired) {
		t.Fatalf("find_links required: %v", err)
	}

	empty, err := eng.FindLinks(ctx, scope, brain.FindLinksRequest{
		RelationType: "has_buyer", Query: "zzzz-no-such-note", Limit: 5,
	})
	if err != nil || len(empty.Links) != 0 {
		t.Fatalf("empty find_links: %+v err=%v", empty.Links, err)
	}

	ghost := uuid.New()
	if err := g.AddEdge(ctx, deal.ID, ghost, "has_buyer", brain.EdgeMeta{Note: "economic buyer ghost"}); err != nil {
		t.Fatal(err)
	}
	limited, err := eng.FindLinks(ctx, scope, brain.FindLinksRequest{
		RelationType: "has_buyer", Query: "economic buyer", Limit: 1,
	})
	if err != nil || len(limited.Links) != 1 {
		t.Fatalf("limit+ghost skip: %+v err=%v", limited.Links, err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := eng.FindLinks(cctx, scope, brain.FindLinksRequest{
		RelationType: "has_buyer", Query: "economic buyer",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("find_links cancel: %v", err)
	}
}

// TestEval_graphRAGScopeSafeMultiHop: multi-hop must not walk through out-of-scope nodes.
func TestEval_graphRAGScopeSafeMultiHop(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	nsA, nsB := uuid.New(), uuid.New()
	now := time.Now().UTC()
	// a --refs--> bridge(nsB) --refs--> c(nsA). Hop-2 must not reach c via bridge.
	a, bridge, c := uuid.New(), uuid.New(), uuid.New()
	_ = store.Put(ctx, brain.Object{ID: a, Kind: "Document", Title: "seed", NamespaceID: nsA, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{ID: bridge, Kind: "Document", Title: "bridge", NamespaceID: nsB, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{ID: c, Kind: "Document", Title: "target", NamespaceID: nsA, UpdatedAt: now})
	_ = g.AddEdge(ctx, a, bridge, "references", brain.EdgeMeta{})
	_ = g.AddEdge(ctx, bridge, c, "references", brain.EdgeMeta{})
	// In-scope path of length 2 for a positive outcome.
	mid, leaf := uuid.New(), uuid.New()
	_ = store.Put(ctx, brain.Object{ID: mid, Kind: "Document", Title: "mid", NamespaceID: nsA, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{ID: leaf, Kind: "Document", Title: "leaf", NamespaceID: nsA, UpdatedAt: now})
	_ = g.AddEdge(ctx, a, mid, "references", brain.EdgeMeta{})
	_ = g.AddEdge(ctx, mid, leaf, "references", brain.EdgeMeta{})

	eng, err := brain.NewEngine(store, brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: &nsA}
	res, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a, RelationTypes: []string{"references"}, MaxHops: 2,
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]int{}
	for _, o := range res.Objects {
		if o.Relation != nil {
			got[o.ID] = o.Relation.Depth
		} else {
			got[o.ID] = 0
		}
	}
	if _, ok := got[bridge]; ok {
		t.Fatalf("out-of-scope bridge must not hydrate: %v", got)
	}
	if _, ok := got[c]; ok {
		t.Fatalf("must not reach c through out-of-scope bridge: %v", got)
	}
	if got[mid] != 1 || got[leaf] != 2 {
		t.Fatalf("in-scope path want mid@1 leaf@2: %v", got)
	}
}

// --- Soft degrade goldens (minimal stubs, real store outcomes) --------------

func TestEval_degradeEmbedderKeepsLexical(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent, part := uuid.New(), uuid.New()
	pos := 1
	_ = store.Put(ctx, brain.Object{ID: parent, Kind: "Document", Title: "Widgets", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{
		ID: part, Kind: "Chunk", Content: "unique-widget-token alpha", ParentID: &parent, Position: &pos,
		NamespaceID: ns, UpdatedAt: now, Embedding: []float32{1, 0},
	})
	eng, err := brain.NewEngine(store,
		brain.WithEmbedder(failEmbedder{}),
		brain.WithConfig(brain.EngineConfig{Now: func() time.Time { return now }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: "unique-widget-token",
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != parent {
		t.Fatalf("lexical degrade: %+v", page.Objects)
	}
}

func TestEval_degradeGraphKeepsContainment(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent, child := uuid.New(), uuid.New()
	pos := 1
	_ = store.Put(ctx, brain.Object{ID: parent, Kind: "Document", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{
		ID: child, Kind: "Chunk", Content: "c", ParentID: &parent, Position: &pos,
		NamespaceID: ns, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store, brain.WithGraph(failGraph{}))
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{
		ObjectID: parent, RelationTypes: []string{"contains", "references"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "children" || len(res.Objects) != 1 || res.Objects[0].ID != child {
		t.Fatalf("containment-only degrade: %+v", res)
	}
	if _, err := eng.FindLinks(ctx, brain.Scope{Namespace: &ns}, brain.FindLinksRequest{
		RelationType: "about", Query: "anything",
	}); err == nil || !strings.Contains(err.Error(), "edge search down") {
		t.Fatalf("FindLinks graph error: %v", err)
	}
}

func TestConcurrent_searchAndExpand(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	_ = store.Put(ctx, brain.Object{ID: parent, Kind: "Document", Title: "Doc", NamespaceID: ns, UpdatedAt: now})
	for i := 1; i <= 8; i++ {
		id := uuid.New()
		pos := i
		_ = store.Put(ctx, brain.Object{
			ID: id, Kind: "Chunk", Content: "concurrent retrieval token",
			ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
		})
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{
		Now: func() time.Time { return now },
	}))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sc := brain.NewSearchContext()
			if _, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
				Query: "concurrent retrieval",
			}, sc); err != nil {
				errCh <- err
				return
			}
			if _, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{
				ObjectID: parent,
			}, sc); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// --- helpers ----------------------------------------------------------------

func titlesOf(objs []brain.RichObject) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Title
	}
	return out
}

func containsID(objs []brain.RichObject, id uuid.UUID) bool {
	for _, o := range objs {
		if o.ID == id {
			return true
		}
	}
	return false
}

func containsUUID(ids []uuid.UUID, want uuid.UUID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// failEmbedder is only used for soft-degrade goldens (embed channel down, lexical stays).
type failEmbedder struct{}

func (failEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, errString("embed down")
}

// failGraph only fails Neighbors so containment expand can still succeed.
type failGraph struct{}

func (failGraph) Neighbors(context.Context, uuid.UUID, []string, int) ([]brain.GraphNeighbor, error) {
	return nil, errString("graph down")
}

func (failGraph) SearchEdgesText(context.Context, string, string, int) ([]brain.EdgeSearchHit, error) {
	return nil, errString("edge search down")
}

type errString string

func (e errString) Error() string { return string(e) }
