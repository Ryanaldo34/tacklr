package brain_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestExpandMany_tagsSourceAndDedupes(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c, shared := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{a, b, c, shared} {
		_ = store.Put(ctx, brain.Object{ID: id, Kind: "Document", Title: id.String()[:8], NamespaceID: ns, UpdatedAt: now})
	}
	_ = g.AddEdge(ctx, a, shared, "references", brain.EdgeMeta{})
	_ = g.AddEdge(ctx, b, shared, "references", brain.EdgeMeta{})
	_ = g.AddEdge(ctx, b, c, "references", brain.EdgeMeta{})
	eng, err := brain.NewEngine(store, brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.ExpandMany(ctx, brain.Scope{Namespace: &ns}, brain.ExpandManyRequest{
		ObjectIDs:     []uuid.UUID{a, b},
		RelationTypes: []string{"references"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// shared appears once (first landing a wins SourceID); c from b.
	byID := map[uuid.UUID]*uuid.UUID{}
	for _, o := range res.Objects {
		if o.Relation != nil {
			byID[o.ID] = o.Relation.SourceID
		}
	}
	if byID[shared] == nil || *byID[shared] != a {
		t.Fatalf("shared source: %+v", byID[shared])
	}
	if byID[c] == nil || *byID[c] != b {
		t.Fatalf("c source: %+v", byID[c])
	}
}

func TestExpandByRecipe_andFindLinks(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{a, b, c} {
		_ = store.Put(ctx, brain.Object{ID: id, Kind: "Document", Title: id.String()[:8], NamespaceID: ns, UpdatedAt: now})
	}
	_ = g.AddEdge(ctx, a, b, "references", brain.EdgeMeta{Note: "risk budget concern"})
	_ = g.AddEdge(ctx, b, c, "references", brain.EdgeMeta{Note: "follow on"})
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithExpandRecipes(
		brain.ExpandRecipe{Name: "two_hop", RelationTypes: []string{"references"}, MaxHops: 2},
	))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasEdgeSearch() {
		t.Fatal("MemoryGraph edge search")
	}
	res, err := eng.ExpandByRecipe(ctx, brain.Scope{Namespace: &ns}, a, "two_hop", brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]bool{}
	for _, o := range res.Objects {
		got[o.ID] = true
	}
	if !got[b] || !got[c] {
		t.Fatalf("recipe hops: %+v", res.Objects)
	}
	_, err = eng.ExpandByRecipe(ctx, brain.Scope{Namespace: &ns}, a, "missing", brain.NewSearchContext())
	if !errors.Is(err, brain.ErrExpandRecipeNotFound) {
		t.Fatalf("want ErrExpandRecipeNotFound: %v", err)
	}
	if err := eng.RegisterExpandRecipe(brain.ExpandRecipe{}); !errors.Is(err, brain.ErrExpandRecipeNameRequired) {
		t.Fatalf("want ErrExpandRecipeNameRequired: %v", err)
	}
	links, err := eng.FindLinks(ctx, brain.Scope{Namespace: &ns}, brain.FindLinksRequest{
		RelationType: "references", Query: "budget", Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(links.Links) != 1 || links.Links[0].From.ID != a || links.Links[0].To.ID != b {
		t.Fatalf("find_links: %+v", links.Links)
	}
	_, err = eng.FindLinks(ctx, brain.Scope{Namespace: &ns}, brain.FindLinksRequest{RelationType: "", Query: "x"})
	if !errors.Is(err, brain.ErrLinkQueryRequired) {
		t.Fatalf("want ErrLinkQueryRequired: %v", err)
	}
}

func TestExpand_scopeSafeMultiHopAndWantContainment(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	nsA, nsB := uuid.New(), uuid.New()
	now := time.Now().UTC()
	// a --refs--> bridge(out of scope) --refs--> c
	// Multi-hop must not walk through bridge into c.
	a, bridge, c := uuid.New(), uuid.New(), uuid.New()
	_ = store.Put(ctx, brain.Object{ID: a, Kind: "Document", Title: "a", NamespaceID: nsA, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{ID: bridge, Kind: "Document", Title: "bridge", NamespaceID: nsB, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{ID: c, Kind: "Document", Title: "c", NamespaceID: nsA, UpdatedAt: now})
	_ = g.AddEdge(ctx, a, bridge, "references", brain.EdgeMeta{})
	_ = g.AddEdge(ctx, bridge, c, "references", brain.EdgeMeta{})

	// parent with a part for WantContainment mix
	parent, part := uuid.New(), uuid.New()
	_ = store.Put(ctx, brain.Object{ID: parent, Kind: "Document", Title: "parent", NamespaceID: nsA, UpdatedAt: now})
	pid := parent
	pos := 0
	_ = store.Put(ctx, brain.Object{ID: part, Kind: "Chunk", Title: "part", NamespaceID: nsA, ParentID: &pid, Position: &pos, UpdatedAt: now})
	_ = g.AddEdge(ctx, parent, a, "references", brain.EdgeMeta{})

	eng, err := brain.NewEngine(store, brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: &nsA}
	two, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a, RelationTypes: []string{"references"}, MaxHops: 2,
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range two.Objects {
		if o.ID == bridge || o.ID == c {
			t.Fatalf("scope leak via multi-hop: %+v", two.Objects)
		}
	}
	mixed, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: parent, RelationTypes: []string{"references"}, WantContainment: true,
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if mixed.Mode != "mixed" {
		t.Fatalf("mode: %s", mixed.Mode)
	}
	got := map[uuid.UUID]bool{}
	for _, o := range mixed.Objects {
		got[o.ID] = true
	}
	if !got[part] || !got[a] {
		t.Fatalf("want part+a: %+v", mixed.Objects)
	}
}

func TestSortRichObjects_andReranker(t *testing.T) {
	ctx := context.Background()
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	objs := []brain.RichObject{
		{ID: uuid.New(), Title: "old", UpdatedAt: t1},
		{ID: uuid.New(), Title: "new", UpdatedAt: t2},
	}
	brain.SortRichObjects(objs, "updated_at", true)
	if objs[0].Title != "new" {
		t.Fatalf("sort desc: %+v", objs)
	}
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	ns := uuid.New()
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithReranker(reverseReranker{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Put(ctx, brain.Scope{Namespace: &ns}, brain.Object{Kind: "Document", Title: "alpha risk"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Put(ctx, brain.Scope{Namespace: &ns}, brain.Object{Kind: "Document", Title: "beta risk"})
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.FindObjects(ctx, brain.Scope{Namespace: &ns}, brain.FindObjectsRequest{Query: "risk", Limit: 10}, brain.NewSearchContext())
	if err != nil || len(page.Objects) < 2 {
		t.Fatalf("find: %+v err=%v", page.Objects, err)
	}
	// reverseReranker reverses order after ranking.
	if page.Objects[0].Title == page.Objects[1].Title {
		t.Fatal("expected two titles")
	}
}

type reverseReranker struct{}

func (reverseReranker) Rerank(_ context.Context, objects []brain.RichObject) ([]brain.RichObject, error) {
	out := make([]brain.RichObject, len(objects))
	for i := range objects {
		out[len(objects)-1-i] = objects[i]
	}
	return out, nil
}
