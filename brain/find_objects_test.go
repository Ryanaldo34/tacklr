package brain_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// TestFindObjects_multiTurnOnAGraph: Put dual-writes nodes; find_objects ranks entities;
// kind filter applies; expand remains structural.
func TestFindObjects_multiTurnOnAGraph(t *testing.T) {
	t.Parallel()
	tenantAcme := uuid.NewString()
	tenantOther := uuid.NewString()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := helixtest.Graph(t)
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString},
		}},
		brain.KindSpec{Kind: "Fact", IsParent: true},
		brain.KindSpec{Kind: "Discovery", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, g)
	if _, err := kg.FindObjects(ctx, brain.Scope{}, brain.FindObjectsRequest{Query: "x"}, nil); err == nil {
		t.Fatal("FindObjects requires result set")
	}
	if _, err := kg.FindObjects(ctx, brain.Scope{}, brain.FindObjectsRequest{}, brain.NewSearchContext()); err == nil {
		t.Fatal("FindObjects requires query")
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	sc := brain.NewSearchContext()

	deal, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Acme Enterprise Renewal", Summary: "enterprise renewal opportunity",
		Properties: map[string]any{"stage": "negotiation"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Stage property filter (same keys as schema filterable_fields / search).
	byStage, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "Acme Renewal", Kinds: []string{"Deal"},
		Filters: mustFilter(t, map[string]any{"stage": "negotiation"}),
	}, sc)
	if err != nil || len(byStage.Objects) != 1 || byStage.Objects[0].ID != deal.ID {
		t.Fatalf("find_objects stage filter: %+v err=%v", byStage.Objects, err)
	}
	closed, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "Acme Renewal", Kinds: []string{"Deal"},
		Filters: mustFilter(t, map[string]any{"stage": "closed"}),
	}, sc)
	if err != nil || len(closed.Objects) != 0 {
		t.Fatalf("want empty for closed stage: %+v err=%v", closed.Objects, err)
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
	if err := kg.Link(ctx, scope, fact.ID, deal.ID, "about"); err != nil {
		t.Fatal(err)
	}

	// Semantic-ish object find for risk themes (kinds Fact).
	page, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
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
	notes, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "penalty timeline", Kinds: []string{"Fact", "Discovery"}}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	gotKinds := map[string]bool{}
	for _, o := range notes.Objects {
		gotKinds[o.Kind] = true
	}
	if len(gotKinds) != 2 || !gotKinds["Fact"] || !gotKinds["Discovery"] {
		t.Fatalf("find_objects over two kinds = %v, want Fact and Discovery only", gotKinds)
	}

	// Deal resolve by title tokens.
	deals, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
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
	exp, err := kg.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: deal.ID, RelationTypes: []string{"about"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Objects) != 1 || exp.Objects[0].ID != fact.ID {
		t.Fatalf("expand about: %+v", exp.Objects)
	}

	// Empty query fails.
	if _, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "  "}, sc); err == nil {
		t.Fatal("want query required")
	}
	// Soft-deleted entity omitted (not hard fail).
	if err := eng.SoftDelete(ctx, scope, fact.ID); err != nil {
		t.Fatal(err)
	}
	afterDel, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
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
	page1, err := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{
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
	other := mustNS(t, "org", tenantOther)
	miss, err := kg.FindObjects(ctx, brain.Scope{Namespace: other}, brain.FindObjectsRequest{
		Query: "Acme Enterprise", Kinds: []string{"Deal"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(miss.Objects) != 0 {
		t.Fatalf("other ns: %+v", miss.Objects)
	}
	nestedNS := mustNS(t, "org", tenantAcme, "workspace", "west")
	nested, err := eng.Put(ctx, brain.Scope{Namespace: nestedNS}, brain.Object{
		Kind: "Deal", Title: "Nested Acme Workspace Deal", Summary: "workspace scoped",
		Properties: map[string]any{"stage": "negotiation"},
	})
	if err != nil {
		t.Fatal(err)
	}
	pageOrg, err := kg.FindObjects(ctx, brain.Scope{Namespace: mustNS(t, "org", tenantAcme)}, brain.FindObjectsRequest{
		Query: "Nested Acme Workspace", Kinds: []string{"Deal"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if !containsID(pageOrg.Objects, nested.ID) {
		t.Fatalf("org scope should see workspace graph node: %+v", pageOrg.Objects)
	}
}

// Graph text hits carry no write time. Decay must read updated_at from the store
// before fusion, or the older title match stays ahead of the fresher body match.
func TestFindObjects_optInDecayUsesStoreTime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns := mustNS(t, "id", uuid.NewString())
	oldT := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newT := oldT.Add(90 * 24 * time.Hour)
	now := oldT
	g := helixtest.Open(t, func(o *helixgraph.Options) {
		o.Ranking = brain.Ranking{Decay: 0.02, Now: func() time.Time { return newT }}
	})
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithConfig(brain.EngineConfig{Now: func() time.Time { return now }}))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, g)
	if _, err := eng.Put(ctx, brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: "policy", Content: "policy"}); err != nil {
		t.Fatal(err)
	}
	now = newT
	fresh, err := eng.Put(ctx, brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: "note", Content: "policy detail"})
	if err != nil {
		t.Fatal(err)
	}
	newID := fresh.ID
	page, err := kg.FindObjects(ctx, brain.Scope{Namespace: ns}, brain.FindObjectsRequest{Query: "policy"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != newID {
		t.Fatalf("fresh fact should lead: %+v", page.Objects)
	}
}
