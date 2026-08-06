package brain_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// TestFindObjects_multiTurnMemoryGraph: Put dual-writes nodes; find_objects ranks entities;
// kind filter applies; expand remains structural.
func TestFindObjects_multiTurnMemoryGraph(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithEmbedder(stubEmbedder{v: []float32{1, 0, 0}}), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true},
		brain.KindSpec{Kind: "Fact", IsParent: true},
		brain.KindSpec{Kind: "Discovery", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasObjectSearch() {
		t.Fatal("MemoryGraph must support object search")
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	sc := brain.NewSearchContext()

	deal, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Acme Enterprise Renewal", Summary: "enterprise renewal opportunity",
	})
	if err != nil {
		t.Fatal(err)
	}
	fact, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Fact", Title: "MSA penalty risk", Content: "commercial liability indemnity exclusivity",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Discovery", Title: "Champion wants Q3", Content: "timeline pressure",
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Link(ctx, scope, fact.ID, deal.ID, "about"); err != nil {
		t.Fatal(err)
	}

	// Semantic-ish object find for risk themes (kinds Fact).
	page, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "penalty liability indemnity",
		Kinds: []string{"Fact"},
		Limit: 10,
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != fact.ID {
		t.Fatalf("find_objects fact: %+v", page.Objects)
	}
	if page.Objects[0].Kind != "Fact" {
		t.Fatalf("kind: %s", page.Objects[0].Kind)
	}

	// Deal resolve by title tokens.
	deals, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "Acme Enterprise Renewal",
		Kinds: []string{"Deal"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(deals.Objects) != 1 || deals.Objects[0].ID != deal.ID {
		t.Fatalf("find deal: %+v", deals.Objects)
	}

	// Expand structure from deal.
	exp, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: deal.ID, RelationTypes: []string{"about"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Objects) != 1 || exp.Objects[0].ID != fact.ID {
		t.Fatalf("expand about: %+v", exp.Objects)
	}

	// Empty query fails.
	if _, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "  "}, sc); err == nil {
		t.Fatal("want query required")
	}
	// Soft-deleted entity omitted (not hard fail).
	if err := eng.SoftDelete(ctx, scope, fact.ID); err != nil {
		t.Fatal(err)
	}
	afterDel, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "penalty liability indemnity", Kinds: []string{"Fact"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range afterDel.Objects {
		if o.ID == fact.ID {
			t.Fatal("soft-deleted fact must not appear")
		}
	}
	// Pagination + continue.
	for i := 0; i < 5; i++ {
		if _, err := eng.Put(ctx, scope, brain.Object{
			Kind: "Fact", Title: fmt.Sprintf("risk-%d", i), Content: "commercial risk item shared",
		}); err != nil {
			t.Fatal(err)
		}
	}
	page1, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "commercial risk", Kinds: []string{"Fact"}, Limit: 2,
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if !page1.HasMore || page1.ResultSetID == uuid.Nil || len(page1.Objects) != 2 {
		t.Fatalf("page1: %+v", page1)
	}
	page2, err := eng.Continue(ctx, scope, page1.ResultSetID, 2, sc)
	if err != nil {
		t.Fatal(err)
	}
	if page2.ResultSetID != page1.ResultSetID {
		t.Fatal("continue result set id")
	}
	// Namespace isolation.
	other := uuid.New()
	miss, err := eng.FindObjects(ctx, brain.Scope{Namespace: &other}, brain.FindObjectsRequest{
		Query: "Acme Enterprise", Kinds: []string{"Deal"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(miss.Objects) != 0 {
		t.Fatalf("other ns: %+v", miss.Objects)
	}
	// Without object searcher.
	engNo, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if engNo.HasObjectSearch() {
		t.Fatal("no graph should not have object search")
	}
	if _, err := engNo.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "x"}, sc); err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("want unavailable: %v", err)
	}
}
