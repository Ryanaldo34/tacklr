package brain_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestEngine_ReadRichObjectInScope(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	id := uuid.New()
	if err := store.Put(brain.Object{
		ID: id, Kind: "Document", Title: "Deal memo", Summary: "Q3",
		Content: "full body", ContentType: "text/plain",
		NamespaceID: ns, Properties: map[string]any{"stage": "negotiation"},
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	got, err := eng.Read(ctx, brain.Scope{Namespace: &ns}, id)

	if err != nil {
		t.Fatal(err)
	}
	if got.ID != id || got.Kind != "Document" || got.Title != "Deal memo" {
		t.Fatalf("identity: %+v", got)
	}
	if got.Content != "full body" || got.ContentType != "text/plain" {
		t.Fatalf("content: %+v", got)
	}
	if got.Properties["stage"] != "negotiation" {
		t.Fatalf("properties: %+v", got.Properties)
	}
	if got.ParentID != nil {
		t.Fatalf("parent should be nil: %v", got.ParentID)
	}
}

func TestEngine_ReadRejectsOutsideScope(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	nsA, nsB := uuid.New(), uuid.New()
	id := uuid.New()
	if err := store.Put(brain.Object{
		ID: id, Kind: "Document", Content: "secret", NamespaceID: nsA,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	_, err = eng.Read(ctx, brain.Scope{Namespace: &nsB}, id)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("wrong namespace: %v", err)
	}

	deleted := time.Now().UTC()
	if err := store.Put(brain.Object{
		ID: id, Kind: "Document", NamespaceID: nsA, DeletedAt: &deleted,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = eng.Read(ctx, brain.Scope{}, id)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-deleted: %v", err)
	}

	_, err = eng.Read(ctx, brain.Scope{}, uuid.Nil)
	if err == nil {
		t.Fatal("nil object id must fail")
	}
}

func TestEngine_SchemaAndOrderedChildren(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	parentID := uuid.New()
	c1, c2 := uuid.New(), uuid.New()
	pos1, pos2 := 1, 2

	if err := store.PutKind(brain.ObjectKind{
		Kind: "Chunk", IsPart: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutKind(brain.ObjectKind{
		Kind: "Document", Description: "A parent doc", IsParent: true,
		FilterableFields: json.RawMessage(`[{"name":"status","type":"string","operators":["eq"]}]`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(brain.Object{
		ID: parentID, Kind: "Document", Title: "Parent", NamespaceID: ns,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(brain.Object{
		ID: c2, Kind: "Chunk", Title: "second", NamespaceID: ns,
		ParentID: &parentID, Position: &pos2, Content: "c2-body",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(brain.Object{
		ID: c1, Kind: "Chunk", Title: "first", NamespaceID: ns,
		ParentID: &parentID, Position: &pos1, Content: "c1-body",
	}); err != nil {
		t.Fatal(err)
	}
	deleted := time.Now().UTC()
	if err := store.Put(brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "gone", NamespaceID: ns,
		ParentID: &parentID, Position: &pos1, DeletedAt: &deleted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "other-ns", NamespaceID: uuid.New(),
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

	children, err := eng.ListChildren(ctx, brain.Scope{Namespace: &ns}, parentID)
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
	ns := uuid.New()
	parentID := uuid.New()
	if err := store.Put(brain.Object{
		ID: parentID, Kind: "Document", NamespaceID: ns,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	other := uuid.New()
	_, err = eng.ListChildren(ctx, brain.Scope{Namespace: &other}, parentID)
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
		{"empty object", func() error { return s.Put(brain.Object{}) }},
		{"missing kind", func() error { return s.Put(brain.Object{ID: uuid.New()}) }},
		{"missing namespace", func() error {
			return s.Put(brain.Object{ID: uuid.New(), Kind: "X"})
		}},
		{"empty kind registry", func() error { return s.PutKind(brain.ObjectKind{}) }},
	}
	for _, tc := range cases {
		if err := tc.fn(); err == nil {
			t.Fatalf("%s: want error", tc.name)
		}
	}
}
