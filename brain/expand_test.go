package brain_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// TestExpand_cancelledContextFailsClosed before store work.
func TestExpand_cancelledContextFailsClosed(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = eng.Expand(ctx, brain.Scope{}, brain.ExpandRequest{ObjectID: uuid.New()}, brain.NewSearchContext())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestExpand_partNeighborhoodWindow(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := mustNS(t, "id", uuid.NewString())
	now := time.Now().UTC()
	parent := uuid.New()
	_ = putFixture(store, brain.Object{ID: parent, Kind: "Document", Title: "Doc", Namespace: ns, UpdatedAt: now})
	var parts []uuid.UUID
	for i := 1; i <= 7; i++ {
		id := uuid.New()
		parts = append(parts, id)
		pos := i
		_ = putFixture(store, brain.Object{
			ID: id, Kind: "Chunk", Title: "p", Content: "c",
			ParentID: &parent, Position: &pos, Namespace: ns, UpdatedAt: now,
		})
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{SiblingRadius: 1}))
	if err != nil {
		t.Fatal(err)
	}
	center := parts[3]
	res, err := eng.Expand(ctx, brain.Scope{Namespace: ns}, brain.ExpandRequest{ObjectID: center}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "neighborhood" {
		t.Fatalf("mode %s", res.Mode)
	}
	if len(res.Objects) != 4 || res.Objects[0].ID != parent {
		t.Fatalf("%+v", res.Objects)
	}
	got := map[uuid.UUID]bool{}
	for _, o := range res.Objects[1:] {
		got[o.ID] = true
	}
	for _, id := range []uuid.UUID{parts[2], parts[3], parts[4]} {
		if !got[id] {
			t.Fatalf("missing sibling %s in %+v", id, res.Objects)
		}
	}
}

func TestExpand_highCardinalityResultSetAndContinue(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := mustNS(t, "id", uuid.NewString())
	now := time.Now().UTC()
	parent := uuid.New()
	_ = putFixture(store, brain.Object{ID: parent, Kind: "Document", Namespace: ns, UpdatedAt: now})
	for i := 1; i <= 30; i++ {
		id := uuid.New()
		pos := i
		_ = putFixture(store, brain.Object{
			ID: id, Kind: "Chunk", Content: "c", ParentID: &parent, Position: &pos,
			Namespace: ns, UpdatedAt: now,
		})
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{
		ExpandInlineMax: 5, MaxResultSetSize: 10, DefaultLimit: 3, MaxLimit: 50,
		Now: func() time.Time { return now },
	}))
	if err != nil {
		t.Fatal(err)
	}
	sc := brain.NewSearchContext()
	res, err := eng.Expand(ctx, brain.Scope{Namespace: ns}, brain.ExpandRequest{ObjectID: parent, Limit: 3}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if res.ResultSetID == uuid.Nil || !res.HasMore || len(res.Objects) != 3 {
		t.Fatalf("%+v", res)
	}
	set, err := sc.Get(ctx, res.ResultSetID)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.ObjectIDs) > 10 {
		t.Fatalf("MaxResultSetSize: %d", len(set.ObjectIDs))
	}
	page2, err := eng.Continue(ctx, brain.Scope{Namespace: ns}, res.ResultSetID, 3, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Objects) == 0 {
		t.Fatal("continue empty")
	}
}

func TestExpand_followsTheDirectionAsked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithExpandRecipes(
		brain.ExpandRecipe{Name: "outward", RelationTypes: []string{"references"}, Direction: "out"}))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	scope := brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())}
	a, b, c := putDoc(t, eng, scope, "a"), putDoc(t, eng, scope, "b"), putDoc(t, eng, scope, "c")
	for _, l := range [][2]uuid.UUID{{a, b}, {b, c}} {
		if err := kg.Link(ctx, scope, l[0], l[1], "references"); err != nil {
			t.Fatal(err)
		}
	}
	expand := func(dir string) []string {
		res, err := kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: b, RelationTypes: []string{"references"}, Direction: dir}, brain.NewSearchContext())
		if err != nil {
			t.Fatal(err)
		}
		got := titlesOf(res.Objects)
		slices.Sort(got)
		return got
	}

	outward, outwardErr := kg.ExpandByRecipe(ctx, scope, b, "outward", brain.NewSearchContext())
	_, unknown := kg.ExpandByRecipe(ctx, scope, b, "nope", brain.NewSearchContext())

	for dir, want := range map[string][]string{"out": {"c"}, "in": {"a"}, "": {"a", "c"}} {
		if got := expand(dir); !slices.Equal(got, want) {
			t.Errorf("expand %q from b = %v, want %v", dir, got, want)
		}
	}
	if got := titlesOf(outward.Objects); outwardErr != nil || !slices.Equal(got, []string{"c"}) {
		t.Errorf("expand by the outward recipe = %v %v, want c", got, outwardErr)
	}
	if !errors.Is(unknown, brain.ErrNotFound) {
		t.Errorf("expand by an unknown recipe = %v, want ErrNotFound", unknown)
	}
}

// putDoc puts a Document titled title and returns its id.
func putDoc(t *testing.T, eng *brain.Engine, scope brain.Scope, title string) uuid.UUID {
	t.Helper()
	obj, err := eng.Put(context.Background(), scope, brain.Object{Kind: "Document", Title: title})
	if err != nil {
		t.Fatal(err)
	}
	return obj.ID
}

func TestExpand_continueKeepsEachNeighborsRelation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(),
		brain.WithConfig(brain.EngineConfig{ExpandInlineMax: 2, DefaultLimit: 2}))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	scope := brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())}
	root := putDoc(t, eng, scope, "root")
	notes := map[string]string{}
	for i := range 8 {
		title := fmt.Sprintf("n%d", i)
		notes[title] = "hop " + title
		if err := kg.LinkWith(ctx, scope, root, putDoc(t, eng, scope, title), "references", brain.EdgeMeta{Note: notes[title]}); err != nil {
			t.Fatal(err)
		}
	}
	sc := brain.NewSearchContext()

	first, err := kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: root, RelationTypes: []string{"references"}, Limit: 2}, sc)
	if err != nil {
		t.Fatal(err)
	}
	rest, err := eng.Continue(ctx, scope, first.ResultSetID, 10, sc)
	if err != nil {
		t.Fatal(err)
	}

	all := slices.Concat(first.Objects, rest.Objects)
	if !first.HasMore || len(all) != 8 {
		t.Fatalf("expand then continue = %d objects (more %v), want 8 over two pages", len(all), first.HasMore)
	}
	for _, o := range all {
		if o.Relation == nil || o.Relation.Type != "references" || o.Relation.Note != notes[o.Title] {
			t.Errorf("%s relation = %+v, want references with note %q", o.Title, o.Relation, notes[o.Title])
		}
	}
}

func TestExpandMany_spendsOneBudgetAcrossSeeds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	scope := brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())}
	tree := func(title string) uuid.UUID {
		doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: title}, brain.Parts(brain.Object{Kind: "Chunk", Content: title + " part"}))
		if err != nil {
			t.Fatal(err)
		}
		return doc.ID
	}
	p1, p2, neighbor := tree("p1"), tree("p2"), putDoc(t, eng, scope, "neighbor")
	if err := kg.Link(ctx, scope, p1, neighbor, "references"); err != nil {
		t.Fatal(err)
	}
	noGraph, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	request := func(budget int, ids ...uuid.UUID) brain.ExpandManyRequest {
		return brain.ExpandManyRequest{ObjectIDs: ids, WantContainment: true, RelationTypes: []string{"references"}, NeighborBudget: budget}
	}
	expand := func(ctx context.Context, budget int, ids ...uuid.UUID) ([]brain.RichObject, error) {
		res, err := kg.ExpandMany(ctx, scope, request(budget, ids...))
		return res.Objects, err
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	all, allErr := expand(ctx, 10, p1, uuid.New(), p2)
	budgeted, budgetErr := expand(ctx, 2, p1, p2)
	_, unsupported := noGraph.ExpandMany(ctx, scope, request(10, p1))
	_, stopped := expand(cancelled, 10, p1)

	var landings []string
	for _, o := range all {
		landings = append(landings, fmt.Sprintf("%s<-%s", o.Title, map[uuid.UUID]string{p1: "p1", p2: "p2"}[*o.Relation.SourceID]))
	}
	slices.Sort(landings)
	if allErr != nil || !slices.Equal(landings, []string{"<-p1", "<-p2", "neighbor<-p1"}) {
		t.Errorf("expand many over p1, an unknown id and p2 = %v %v, want each part and the neighbor by its landing", landings, allErr)
	}
	if budgetErr != nil || len(budgeted) != 2 {
		t.Errorf("expand many with a budget of 2 = %d objects %v, want 2", len(budgeted), budgetErr)
	}
	if !errors.Is(unsupported, brain.ErrUnsupported) || !errors.Is(stopped, context.Canceled) {
		t.Errorf("relations on an engine %v, cancelled %v, want ErrUnsupported and Canceled", unsupported, stopped)
	}
}

// TestSearch_withReranker exercises applyRerank path.
func TestSearch_withReranker(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := mustNS(t, "id", uuid.NewString())
	now := time.Now().UTC()
	strong, weak := uuid.New(), uuid.New()
	strongPart, weakPart := uuid.New(), uuid.New()
	pos := 1
	_ = putFixture(store, brain.Object{ID: strong, Kind: "Document", Title: "strong", Namespace: ns, UpdatedAt: now})
	_ = putFixture(store, brain.Object{
		ID: strongPart, Kind: "Chunk", Content: "alpha alpha alpha",
		ParentID: &strong, Position: &pos, Namespace: ns, UpdatedAt: now,
	})
	_ = putFixture(store, brain.Object{ID: weak, Kind: "Document", Title: "weak", Namespace: ns, UpdatedAt: now})
	_ = putFixture(store, brain.Object{
		ID: weakPart, Kind: "Chunk", Content: "alpha beta beta beta beta beta beta beta beta",
		ParentID: &weak, Position: &pos, Namespace: ns, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store,
		brain.WithReranker(reverseRerank{}),
		brain.WithConfig(brain.EngineConfig{DefaultLimit: 1, Now: func() time.Time { return now }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.Search(ctx, brain.Scope{Namespace: ns}, brain.SearchRequest{Query: "alpha"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != weak {
		t.Fatalf("rerank before page cut: %+v", page.Objects)
	}
}

func TestSearch_optInDecayReorders(t *testing.T) {
	ctx := context.Background()
	ns := mustNS(t, "id", uuid.NewString())
	oldT := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := oldT
	clock := func() time.Time { return now }
	store := brain.NewMemoryStore(brain.WithMemoryRanking(brain.Ranking{Decay: 0.02, Now: clock}))
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{Now: clock}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: ns}
	oldParent, newParent := uuid.New(), uuid.New()
	oldPart, newPart := uuid.New(), uuid.New()
	pos := 1
	put := func(parent, part uuid.UUID, content string) {
		t.Helper()
		if err := putFixture(store, brain.Object{ID: parent, Kind: "Document", Namespace: ns, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if err := putFixture(store, brain.Object{
			ID: part, Kind: "Chunk", Content: content, ParentID: &parent, Position: &pos,
			Namespace: ns, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	put(oldParent, oldPart, "alpha")
	now = oldT.Add(200 * 24 * time.Hour)
	put(newParent, newPart, "alpha beta beta beta beta beta beta beta beta")
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "alpha"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != newParent {
		t.Fatalf("decayed old hit should follow the fresh hit: %+v", page.Objects)
	}
}

type reverseRerank struct{}

func (reverseRerank) Rerank(_ context.Context, objects []brain.RichObject) ([]brain.RichObject, error) {
	out := append([]brain.RichObject(nil), objects...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func TestExpand_mixesChildrenAndLinks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	scope := brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())}
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "doc"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "part"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := kg.Link(ctx, scope, doc.ID, putDoc(t, eng, scope, "linked"), "references"); err != nil {
		t.Fatal(err)
	}

	res, err := kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: doc.ID, RelationTypes: []string{"contains", "references"}}, brain.NewSearchContext())

	var got []string
	for _, o := range res.Objects {
		got = append(got, o.Kind+" "+o.Title)
	}
	if err != nil || res.Mode != "mixed" || !slices.Equal(got, []string{"Chunk ", "Document linked"}) {
		t.Errorf("expand children and links = %s %v %v, want mixed: the part, then the link", res.Mode, got, err)
	}
}

func TestExpand_missingObject(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Expand(context.Background(), brain.Scope{Namespace: mustNS(t, "org", "a")}, brain.ExpandRequest{ObjectID: uuid.New()}, brain.NewSearchContext())
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestSplitRelationTypes(t *testing.T) {
	c, g := brain.SplitRelationTypes(nil)
	if !c || len(g) != 0 {
		t.Fatal(c, g)
	}
	c, g = brain.SplitRelationTypes([]string{"contains", "references", "REFERENCES"})
	if !c || len(g) != 1 || strings.ToLower(g[0]) != "references" {
		t.Fatal(c, g)
	}
	c, g = brain.SplitRelationTypes([]string{"similar_to"})
	if c || len(g) != 1 {
		t.Fatal(c, g)
	}
	c, g = brain.SplitRelationTypes([]string{"children"})
	if c || len(g) != 1 || g[0] != "children" {
		t.Fatal(c, g)
	}
}
