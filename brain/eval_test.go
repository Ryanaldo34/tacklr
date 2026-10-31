package brain_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// --- GraphRAG composition golden --------------------------------------------

// TestEval_graphRAGComposition locks the host recipe documented in package doc:
//
//	find_objects → LandingIDs → ExpandMany → search(scope_ids=…)
//
// Real MemoryStore and live Helix (dual-write via Put/Link).
func TestEval_graphRAGComposition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := helixtest.Graph(t)
	eng, err := brain.NewEngine(store, brain.WithKinds(
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
	kg := brain.NewKnowledgeGraph(eng, g)
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
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

	if err := kg.Link(ctx, scope, fact.ID, deal.ID, "about"); err != nil {
		t.Fatal(err)
	}
	if err := kg.LinkWith(ctx, scope, deal.ID, buyer.ID, "has_buyer", brain.EdgeMeta{
		Note: "primary economic buyer for Acme renewal", Role: "primary",
	}); err != nil {
		t.Fatal(err)
	}
	// Noise edge so ExpandMany must not pull noise deal into Acme neighborhood.
	if err := kg.Link(ctx, scope, noise.ID, buyer.ID, "has_buyer"); err != nil {
		t.Fatal(err)
	}

	// 1) Entity land
	land, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
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
	seeds := brain.LandingIDs(land.Objects)
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
	fromCorpus := brain.LandingIDs(corpusHit.Objects)
	if !containsUUID(fromCorpus, deal.ID) {
		t.Fatalf("corpus landings must promote to Acme parent: hits=%v landings=%v",
			titlesOf(corpusHit.Objects), fromCorpus)
	}

	// 3) ExpandMany from deal seed(s) — only Acme's neighborhood, tagged SourceID
	many, err := kg.ExpandMany(ctx, scope, brain.ExpandManyRequest{
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
	mixed, err := kg.Expand(ctx, scope, brain.ExpandRequest{
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
	// The graph walks each hop with the label set; hop1 from fact: deal; hop2 from deal: buyer (+ fact reverse).
	fromFact, err := kg.ExpandByRecipe(ctx, scope, fact.ID, "about_and_buyers", sc)
	if err != nil {
		t.Fatal(err)
	}
	if !containsID(fromFact.Objects, deal.ID) {
		t.Fatalf("recipe hop1 deal: %v", titlesOf(fromFact.Objects))
	}

	// Edge-text land: FindLinks matches the edge note.
	links, err := kg.FindLinks(ctx, scope, brain.FindLinksRequest{
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

	if _, err := kg.FindLinks(ctx, scope, brain.FindLinksRequest{}); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("find_links required: %v", err)
	}

	empty, err := kg.FindLinks(ctx, scope, brain.FindLinksRequest{
		RelationType: "has_buyer", Query: "zzzz-no-such-note", Limit: 5,
	})
	if err != nil || len(empty.Links) != 0 {
		t.Fatalf("empty find_links: %+v err=%v", empty.Links, err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := kg.FindLinks(cctx, scope, brain.FindLinksRequest{
		RelationType: "has_buyer", Query: "economic buyer",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("find_links cancel: %v", err)
	}
}

// --- Soft degrade goldens (minimal stubs, real store outcomes) --------------

func TestConcurrent_searchAndExpand(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := mustNS(t, "id", uuid.NewString())
	now := time.Now().UTC()
	parent := uuid.New()
	_ = putFixture(store, brain.Object{ID: parent, Kind: "Document", Title: "Doc", Namespace: ns, UpdatedAt: now})
	for i := 1; i <= 8; i++ {
		id := uuid.New()
		pos := i
		_ = putFixture(store, brain.Object{
			ID: id, Kind: "Chunk", Content: "concurrent retrieval token",
			ParentID: &parent, Position: &pos, Namespace: ns, UpdatedAt: now,
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
			if _, err := eng.Search(ctx, brain.Scope{Namespace: ns}, brain.SearchRequest{
				Query: "concurrent retrieval",
			}, sc); err != nil {
				errCh <- err
				return
			}
			if _, err := eng.Expand(ctx, brain.Scope{Namespace: ns}, brain.ExpandRequest{
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

func (failEmbedder) Embed(context.Context, []string) ([][]float32, error) {
	return nil, errString("embed down")
}

type errString string

func (e errString) Error() string { return string(e) }

func (failEmbedder) Model() string { return "test-model" }
