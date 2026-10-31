package brain_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestEngine_ReadRejectsOutsideScope(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	nsA, nsB := mustNS(t, "org", "a"), mustNS(t, "org", "b")
	id := uuid.New()
	if err := putFixture(store, brain.Object{
		ID: id, Kind: "Document", Content: "secret", Namespace: nsA,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	_, err = eng.Read(ctx, brain.Scope{Namespace: nsB}, id)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("wrong namespace: %v", err)
	}

	deleted := time.Now().UTC()
	if err := putFixture(store, brain.Object{
		ID: id, Kind: "Document", Namespace: nsA, DeletedAt: &deleted,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = eng.Read(ctx, brain.Scope{Namespace: nsA}, id)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-deleted: %v", err)
	}

	objNS := mustNS(t, "org", "acme", "workspace", "west")
	hid := uuid.New()
	if err := putFixture(store, brain.Object{
		ID: hid, Kind: "Document", Content: "nested", Namespace: objNS,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := eng.Read(ctx, brain.Scope{Namespace: mustNS(t, "org", "acme")}, hid)
	if err != nil || got.Content != "nested" {
		t.Fatalf("org scope should see workspace object: %v %+v", err, got)
	}
	_, err = eng.Read(ctx, brain.Scope{Namespace: mustNS(t, "org", "acme", "workspace", "east")}, hid)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("other workspace: %v", err)
	}

	_, err = eng.Read(ctx, brain.Scope{}, uuid.Nil)
	if err == nil {
		t.Fatal("nil object id must fail")
	}
}

func TestEngine_SchemaAndOrderedChildren(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := mustNS(t, "id", uuid.NewString())
	parentID := uuid.New()
	c1, c2 := uuid.New(), uuid.New()
	pos1, pos2 := 1, 2

	if err := store.PutKind(ctx, brain.ObjectKind{
		Kind: "Chunk", IsPart: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutKind(ctx, brain.ObjectKind{
		Kind: "Document", Description: "A parent doc", IsParent: true,
		FilterableFields: json.RawMessage(`[{"name":"status","type":"string","operators":["eq"]}]`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := putFixture(store, brain.Object{
		ID: parentID, Kind: "Document", Title: "Parent", Namespace: ns,
	}); err != nil {
		t.Fatal(err)
	}
	if err := putFixture(store, brain.Object{
		ID: c2, Kind: "Chunk", Title: "second", Namespace: ns,
		ParentID: &parentID, Position: &pos2, Content: "c2-body",
	}); err != nil {
		t.Fatal(err)
	}
	if err := putFixture(store, brain.Object{
		ID: c1, Kind: "Chunk", Title: "first", Namespace: ns,
		ParentID: &parentID, Position: &pos1, Content: "c1-body",
	}); err != nil {
		t.Fatal(err)
	}
	deleted := time.Now().UTC()
	if err := putFixture(store, brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "gone", Namespace: ns,
		ParentID: &parentID, Position: &pos1, DeletedAt: &deleted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := putFixture(store, brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "other-ns", Namespace: mustNS(t, "id", uuid.NewString()),
		ParentID: &parentID, Position: &pos1,
	}); err != nil {
		t.Fatal(err)
	}

	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	all, err := eng.Schema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Kinds) != 2 || all.Kinds[0].Kind != "Chunk" || all.Kinds[1].Kind != "Document" {
		t.Fatalf("kinds sorted: %+v", all.Kinds)
	}

	one, err := eng.Schema(ctx, "Document")
	if err != nil {
		t.Fatal(err)
	}
	if len(one.Kinds) != 1 || one.Kinds[0].Description != "A parent doc" {
		t.Fatalf("single kind: %+v", one)
	}
	if string(one.Kinds[0].FilterableFields) == "" {
		t.Fatal("filterable fields required on Document")
	}

	children, err := eng.ListChildren(ctx, brain.Scope{Namespace: ns}, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 2 || children[0].ID != c1 || children[1].ID != c2 {
		t.Fatalf("ordered children: %+v", children)
	}
	if children[0].Content != "" || children[1].Content != "" {
		t.Fatalf("list payload omits content: %+v", children)
	}
	if children[0].Title != "first" {
		t.Fatalf("title: %q", children[0].Title)
	}
}

func TestEngine_SchemaUnknownKind(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	empty, err := eng.Schema(context.Background(), "")
	if err != nil || len(empty.Kinds) != 0 {
		t.Fatalf("empty registry: %+v err=%v", empty, err)
	}
	_, err = eng.Schema(context.Background(), "Missing")
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestEngine_ListChildrenRequiresVisibleParent(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := mustNS(t, "id", uuid.NewString())
	parentID := uuid.New()
	if err := putFixture(store, brain.Object{
		ID: parentID, Kind: "Document", Namespace: ns,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	other := mustNS(t, "org", "other")
	_, err = eng.ListChildren(ctx, brain.Scope{Namespace: other}, parentID)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}

	_, err = eng.ListChildren(ctx, brain.Scope{}, uuid.Nil)
	if err == nil {
		t.Fatal("nil parent id must fail")
	}
}

func TestNewEngine_requiresStore(t *testing.T) {
	if _, err := brain.NewEngine(nil); err == nil {
		t.Fatal("want error")
	}
}

func TestMemoryStore_PutRequiresIdentity(t *testing.T) {
	s := brain.NewMemoryStore()
	cases := []struct {
		name string
		fn   func() error
	}{
		{"empty object", func() error { return putFixture(s, brain.Object{}) }},
		{"missing kind", func() error { return putFixture(s, brain.Object{ID: uuid.New()}) }},
		{"missing namespace", func() error {
			return putFixture(s, brain.Object{ID: uuid.New(), Kind: "X"})
		}},
		{"empty kind registry", func() error { return s.PutKind(context.Background(), brain.ObjectKind{}) }},
	}
	for _, tc := range cases {
		if err := tc.fn(); err == nil {
			t.Fatalf("%s: want error", tc.name)
		}
	}
}

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func mustFilter(t testing.TB, m map[string]any) brain.Filter {
	t.Helper()
	f, err := brain.DecodeFilter(m)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type nopRerank struct{}

func (nopRerank) Rerank(context.Context, []brain.RichObject) ([]brain.RichObject, error) {
	return nil, nil
}

// TestNewEngine_options: WithReranker/ExpandRecipes/Kinds construct path.
func TestNewEngine_options(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	if _, err := brain.NewEngine(store, brain.WithExpandRecipes(brain.ExpandRecipe{Name: ""})); err == nil || !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("empty expand recipe name must fail construct: %v", err)
	}
	eng, err := brain.NewEngine(store,
		brain.WithReranker(nopRerank{}),
		brain.WithExpandRecipes(
			brain.ExpandRecipe{Name: "kids", MaxHops: 1, WantContainment: true},
		),
		brain.WithKinds(brain.KindSpec{
			Kind: "Note",
			Fields: []brain.FieldSpec{
				{Name: "tag", Type: brain.FieldTypeString},
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	// ExpandByRecipe missing name
	if _, err := eng.ExpandByRecipe(ctx, brain.Scope{}, uuid.Nil, "nope", brain.NewSearchContext()); err == nil {
		t.Fatal("missing recipe")
	}
	// Register + expand containment kids
	parent := uuid.New()
	child := uuid.New()
	ns := mustNS(t, "id", uuid.NewString())
	now := time.Now().UTC()
	pos := 1
	_ = putFixture(store, brain.Object{ID: parent, Kind: "Document", Title: "p", Namespace: ns, UpdatedAt: now})
	_ = putFixture(store, brain.Object{ID: child, Kind: "Chunk", Title: "c", ParentID: &parent, Position: &pos, Namespace: ns, UpdatedAt: now})
	scope := brain.Scope{Namespace: ns}
	res, err := eng.ExpandByRecipe(ctx, scope, parent, "kids", brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	many, err := eng.ExpandMany(ctx, scope, brain.ExpandManyRequest{ObjectIDs: []uuid.UUID{parent}, WantContainment: true})
	if err != nil || len(many.Objects) != 1 || many.Objects[0].ID != child {
		t.Fatalf("engine expand many over containment = %+v %v, want the child", many.Objects, err)
	}
	if len(res.Objects) < 1 {
		t.Fatalf("kids expand: %+v", res.Objects)
	}
	if err := eng.RegisterExpandRecipe(brain.ExpandRecipe{Name: ""}); err == nil {
		t.Fatal("empty recipe name")
	}
	if err := eng.RegisterExpandRecipe(brain.ExpandRecipe{Name: " extra ", WantContainment: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Expand(ctx, brain.Scope{Namespace: mustNS(t, "org", "a")}, brain.ExpandRequest{ObjectID: parent, RelationTypes: []string{"about"}}, brain.NewSearchContext()); !errors.Is(err, brain.ErrUnsupported) {
		t.Fatalf("expand over a relation on an engine = %v, want ErrUnsupported (relations need a KnowledgeGraph)", err)
	}
}

// putFixture writes obj straight to the store under its own namespace.
func putFixture(s brain.Store, obj brain.Object) error {
	_, err := s.Put(context.Background(), brain.Scope{Namespace: obj.Namespace}, brain.Indexed{Object: obj, Text: brain.IndexText(obj)})
	return err
}

func TestEngine_kindsWithObjectsListsTheKindsWritten(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", uuid.NewString())}
	for _, kind := range []string{"Note", "Deal", "Note"} {
		if _, err := eng.Put(ctx, scope, brain.Object{Kind: kind, Title: "x"}); err != nil {
			t.Fatal(err)
		}
	}

	kinds, err := eng.KindsWithObjects(ctx, scope)

	if err != nil || !slices.Equal(kinds, []string{"Deal", "Note"}) {
		t.Fatalf("kinds with objects = %v %v, want Deal and Note", kinds, err)
	}
}
