package brain_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// TestEval_fusedSearchLandsEachKindOfResultOnItsQuery is the ranking golden
// for the engine's reciprocal rank fusion (k=60) of the store and the graph:
// an entity record wins its kind-filtered query, a chunk's words land on its
// record with the chunk as evidence, a note's words land on the link, and a
// query that matches both a record and a note returns both near the top. A
// record ranks in two sources (the store and the graph index its text), a
// link in one, so a link leads only on words no record holds.
func TestEval_fusedSearchLandsEachKindOfResultOnItsQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(helixtest.Graph(t)), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}}},
		brain.KindSpec{Kind: "Fact", IsParent: true},
		brain.KindSpec{Kind: "Buyer", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	// A deal with a chunk, a fact and a buyer linked to it, and noise: another
	// deal with the same buyer, and a record whose chunk repeats the deal's.
	const pricing = "pricing email about Acme renewal risk and budget"
	fact := save(t, eng, scope, brain.Object{Kind: "Fact", Title: "Budget cut risk", Content: "customer signaled budget pressure on renewal"})
	buyer := save(t, eng, scope, brain.Object{Kind: "Buyer", Title: "Contoso LLC", Summary: "primary economic buyer"})
	noiseDeal := save(t, eng, scope, brain.Object{Kind: "Deal", Title: "Other Pipeline Deal", Summary: "unrelated opportunity", Properties: map[string]any{"stage": "prospect"}})
	deal, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Deal", Title: "Acme Enterprise Renewal", Summary: "enterprise renewal opportunity", Properties: map[string]any{"stage": "negotiation"}},
		brain.Parts(brain.Object{Kind: "Chunk", Title: "pricing thread", Content: pricing}))
	if err != nil {
		t.Fatal(err)
	}
	noiseDoc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Deal", Title: "Noise Doc Parent", Summary: "holder for noise chunk"},
		brain.Parts(brain.Object{Kind: "Chunk", Title: "noise", Content: pricing}))
	if err != nil {
		t.Fatal(err)
	}
	link(t, eng, scope, fact.ID, deal.ID, "about", "")
	if err := eng.Link(ctx, scope, brain.LinkRequest{From: deal.ID, To: buyer.ID, Relation: "has_buyer",
		Meta: brain.EdgeMeta{Note: "primary economic buyer, signed off after the security review", Role: "primary"}}); err != nil {
		t.Fatal(err)
	}
	link(t, eng, scope, noiseDeal.ID, buyer.ID, "has_buyer", "")
	search := func(req brain.SearchRequest) []brain.Result {
		page, err := eng.Search(ctx, scope, req)
		if err != nil {
			t.Fatal(err)
		}
		return page.Results
	}

	entity := search(brain.SearchRequest{Query: "Acme Enterprise Renewal", Kind: "Deal"})
	chunk := search(brain.SearchRequest{Query: "pricing email Acme renewal risk budget"})
	byStage := search(brain.SearchRequest{Query: "renewal", Kind: "Deal", Filters: storetest.Filter(t, map[string]any{"stage": "negotiation"})})
	note := search(brain.SearchRequest{Query: "signed off after the security review"})
	both := search(brain.SearchRequest{Query: "economic buyer"})

	if len(entity) == 0 || entity[0].Object == nil || entity[0].Object.ID != deal.ID {
		t.Errorf("the entity query = %v, want the Acme deal first", describe(entity))
	}
	// The deal holds the chunk and also matches by its own words, so it
	// leads; the noise record holds the same chunk and stays near the top.
	if len(chunk) == 0 || chunk[0].Object == nil || chunk[0].Object.ID != deal.ID || len(chunk[0].Object.Evidence) == 0 ||
		!slices.Contains(describe(chunk[:min(3, len(chunk))]), noiseDoc.Title) {
		t.Errorf("the chunk query = %v, want the deal first with the chunk as evidence, and the noise record in the top 3", describe(chunk))
	}
	if len(byStage) != 1 || byStage[0].Object.ID != deal.ID {
		t.Errorf("the filtered query = %v, want the negotiating deal alone", describe(byStage))
	}
	if len(note) == 0 || note[0].Link == nil || note[0].Link.From.ID != deal.ID || note[0].Link.To.ID != buyer.ID || note[0].Link.Meta.Role != "primary" {
		t.Errorf("the note query = %v, want the deal→buyer link first, with its meta", describe(note))
	}
	if top := describe(both[:min(3, len(both))]); !slices.Contains(top, buyer.Title) || !slices.Contains(top, "has_buyer") {
		t.Errorf("the record-and-note query = %v, want the buyer and the link in the top 3", describe(both))
	}
}

// describe names each result: an object by its title, a link by its relation.
func describe(results []brain.Result) []string {
	out := make([]string, len(results))
	for i, r := range results {
		if r.Link != nil {
			out[i] = r.Link.Relation
		} else {
			out[i] = r.Object.Title
		}
	}
	return out
}

// TestEngine_searchAndRelatedAreSafeToRunConcurrently: many searches and
// related reads of one engine at the same time all succeed (run with -race).
func TestEngine_searchAndRelatedAreSafeToRunConcurrently(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	doc := docWithParts(t, eng, scope, "Doc", nil, "concurrent retrieval token", "concurrent retrieval token", "concurrent retrieval token")
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 16 {
		wg.Go(func() {
			if _, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "concurrent retrieval"}); err != nil {
				errs <- err
			}
			if _, err := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID}); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
