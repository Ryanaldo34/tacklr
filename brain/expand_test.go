package brain_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestExpand_parentChildrenOrdered(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	c1, c2 := uuid.New(), uuid.New()
	pos1, pos2 := 1, 2
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", Title: "Doc", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(context.Background(), brain.Object{ID: c2, Kind: "Chunk", Title: "second", Content: "body2", ParentID: &parent, Position: &pos2, NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(context.Background(), brain.Object{ID: c1, Kind: "Chunk", Title: "first", Content: "body1", ParentID: &parent, Position: &pos1, NamespaceID: ns, UpdatedAt: now})

	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{ObjectID: parent}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "children" || len(res.Objects) != 2 {
		t.Fatalf("%+v", res)
	}
	if res.Objects[0].ID != c1 || res.Objects[1].ID != c2 {
		t.Fatalf("order: %+v", res.Objects)
	}
	if res.Objects[0].Content != "" {
		t.Fatal("expand omits content")
	}
	if res.ResultSetID != uuid.Nil {
		t.Fatal("small expand is inline")
	}
}

func TestExpand_partNeighborhoodWindow(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", Title: "Doc", NamespaceID: ns, UpdatedAt: now})
	var parts []uuid.UUID
	for i := 1; i <= 7; i++ {
		id := uuid.New()
		parts = append(parts, id)
		pos := i
		_ = store.Put(context.Background(), brain.Object{
			ID: id, Kind: "Chunk", Title: "p", Content: "c",
			ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
		})
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{SiblingRadius: 1}))
	if err != nil {
		t.Fatal(err)
	}
	center := parts[3]
	res, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{ObjectID: center}, brain.NewSearchContext())
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
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", NamespaceID: ns, UpdatedAt: now})
	for i := 1; i <= 30; i++ {
		id := uuid.New()
		pos := i
		_ = store.Put(context.Background(), brain.Object{
			ID: id, Kind: "Chunk", Content: "c", ParentID: &parent, Position: &pos,
			NamespaceID: ns, UpdatedAt: now,
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
	res, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{ObjectID: parent, Limit: 3}, sc)
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
	page2, err := eng.Continue(ctx, brain.Scope{Namespace: &ns}, res.ResultSetID, 3, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Objects) == 0 {
		t.Fatal("continue empty")
	}
}

func TestExpand_graphNeighborsMemoryGraph(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	otherNS := uuid.New()
	for _, id := range []uuid.UUID{a, b, c} {
		_ = store.Put(context.Background(), brain.Object{ID: id, Kind: "Document", Title: id.String()[:8], NamespaceID: ns, UpdatedAt: now})
	}
	hidden := uuid.New()
	_ = store.Put(context.Background(), brain.Object{ID: hidden, Kind: "Document", NamespaceID: otherNS, UpdatedAt: now})

	g := brain.NewMemoryGraph()
	_ = g.AddEdge(context.Background(), a, b, "references")
	_ = g.AddEdge(context.Background(), c, a, "references")
	_ = g.AddEdge(context.Background(), a, hidden, "references")

	eng, err := brain.NewEngine(store, brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{
		ObjectID: a, RelationTypes: []string{"references"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "graph" {
		t.Fatalf("mode %s", res.Mode)
	}
	got := map[uuid.UUID]bool{}
	for _, o := range res.Objects {
		got[o.ID] = true
	}
	if !got[b] || !got[c] {
		t.Fatalf("want b and c, got %+v", res.Objects)
	}
	if got[hidden] {
		t.Fatal("wrong namespace must be filtered")
	}
}

func TestExpand_partMixedContainmentAndGraph(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	part := uuid.New()
	linked := uuid.New()
	pos := 1
	_ = store.Put(context.Background(), brain.Object{ID: parent, Kind: "Document", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(context.Background(), brain.Object{
		ID: part, Kind: "Chunk", Content: "c", ParentID: &parent, Position: &pos,
		NamespaceID: ns, UpdatedAt: now,
	})
	_ = store.Put(context.Background(), brain.Object{ID: linked, Kind: "Document", Title: "ref", NamespaceID: ns, UpdatedAt: now})

	g := brain.NewMemoryGraph()
	_ = g.AddEdge(context.Background(), part, linked, "references")

	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithConfig(brain.EngineConfig{SiblingRadius: 1}))
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{
		ObjectID: part, RelationTypes: []string{"contains", "references"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "mixed" {
		t.Fatalf("mode %s", res.Mode)
	}
	got := map[uuid.UUID]bool{}
	for _, o := range res.Objects {
		got[o.ID] = true
	}
	if !got[parent] || !got[part] || !got[linked] {
		t.Fatalf("want parent, part, linked: %+v", res.Objects)
	}
	if res.Objects[0].ID != parent {
		t.Fatalf("parent first: %+v", res.Objects)
	}
}

func TestExpand_graphStoreErrorSurfaces(t *testing.T) {
	store := &errAfterGetStore{
		ok:     brain.NewMemoryStore(),
		failID: uuid.New(),
	}
	ns := uuid.New()
	root := uuid.New()
	_ = store.ok.Put(context.Background(), brain.Object{ID: root, Kind: "Document", NamespaceID: ns})

	g := brain.NewMemoryGraph()
	_ = g.AddEdge(context.Background(), root, store.failID, "references")

	eng, err := brain.NewEngine(store, brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Expand(context.Background(), brain.Scope{Namespace: &ns}, brain.ExpandRequest{
		ObjectID: root, RelationTypes: []string{"references"},
	}, brain.NewSearchContext())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want store error, got %v", err)
	}
}

type errAfterGetStore struct {
	ok     *brain.MemoryStore
	failID uuid.UUID
}

func (s *errAfterGetStore) Get(ctx context.Context, scope brain.Scope, id uuid.UUID) (brain.Object, error) {
	if id == s.failID {
		return brain.Object{}, errors.New("boom")
	}
	return s.ok.Get(ctx, scope, id)
}
func (s *errAfterGetStore) ListChildren(ctx context.Context, scope brain.Scope, parentID uuid.UUID) ([]brain.Object, error) {
	return s.ok.ListChildren(ctx, scope, parentID)
}
func (s *errAfterGetStore) GetKind(ctx context.Context, kind string) (brain.ObjectKind, error) {
	return s.ok.GetKind(ctx, kind)
}
func (s *errAfterGetStore) ListKinds(ctx context.Context) ([]brain.ObjectKind, error) {
	return s.ok.ListKinds(ctx)
}
func (s *errAfterGetStore) SearchLexical(ctx context.Context, scope brain.Scope, query string, filters brain.Filters, k int) ([]brain.ScoredID, error) {
	return s.ok.SearchLexical(ctx, scope, query, filters, k)
}
func (s *errAfterGetStore) SearchVector(ctx context.Context, scope brain.Scope, emb []float32, filters brain.Filters, k int) ([]brain.ScoredID, error) {
	return s.ok.SearchVector(ctx, scope, emb, filters, k)
}
func (s *errAfterGetStore) SearchTrigram(ctx context.Context, scope brain.Scope, query string, filters brain.Filters, k int) ([]brain.ScoredID, error) {
	return s.ok.SearchTrigram(ctx, scope, query, filters, k)
}
func (s *errAfterGetStore) GetMany(ctx context.Context, scope brain.Scope, ids []uuid.UUID) ([]brain.Object, error) {
	var out []brain.Object
	for _, id := range ids {
		o, err := s.Get(ctx, scope, id)
		if err != nil {
			if errors.Is(err, brain.ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

func TestExpand_graphRequiresBackend(t *testing.T) {
	store := brain.NewMemoryStore()
	ns := uuid.New()
	id := uuid.New()
	_ = store.Put(context.Background(), brain.Object{ID: id, Kind: "Document", NamespaceID: ns})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Expand(context.Background(), brain.Scope{Namespace: &ns}, brain.ExpandRequest{
		ObjectID: id, RelationTypes: []string{"references"},
	}, brain.NewSearchContext())
	if err == nil || !strings.Contains(err.Error(), "graph backend") {
		t.Fatalf("got %v", err)
	}
}

func TestExpand_missingObject(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Expand(context.Background(), brain.Scope{}, brain.ExpandRequest{ObjectID: uuid.New()}, brain.NewSearchContext())
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
