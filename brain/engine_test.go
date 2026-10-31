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

func TestEngine_openReadsTheObjectItsKindAndItsFirstChildren(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := tenant(t)
	scope := brain.Scope{Namespace: ns}
	if err := store.PutKind(ctx, brain.ObjectKind{
		Kind: "Document", Description: "A parent doc", IsParent: true,
		FilterableFields: json.RawMessage(`[{"name":"status","type":"string","operators":["eq"]}]`),
	}); err != nil {
		t.Fatal(err)
	}
	parent := brain.Object{ID: uuid.New(), Kind: "Document", Title: "Parent", Content: "the body", Namespace: ns,
		Properties: map[string]any{brain.PropVFSPath: "/workspace/doc.md"}}
	if err := putFixture(store, parent); err != nil {
		t.Fatal(err)
	}
	var firstPart uuid.UUID
	for i := range 12 {
		pos := 11 - i // written in reverse; Open lists them by position
		part := brain.Object{ID: uuid.New(), Kind: "Chunk", Title: "part", Content: "c", Namespace: ns, ParentID: &parent.ID, Position: &pos}
		if pos == 0 {
			firstPart = part.ID
		}
		if err := putFixture(store, part); err != nil {
			t.Fatal(err)
		}
	}
	nested := brain.Object{ID: uuid.New(), Kind: "Note", Title: "nested", Namespace: append(ns.Clone(), brain.Attr{Name: "workspace", Value: "west"})}
	deleted := time.Now().UTC()
	gone := brain.Object{ID: uuid.New(), Kind: "Note", Namespace: ns, DeletedAt: &deleted}
	for _, o := range []brain.Object{nested, gone} {
		if err := putFixture(store, o); err != nil {
			t.Fatal(err)
		}
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	east := brain.Scope{Namespace: append(ns.Clone(), brain.Attr{Name: "workspace", Value: "east"})}

	opened, openErr := eng.Open(ctx, scope, parent.ID)
	part, partErr := eng.Open(ctx, scope, firstPart)
	fromOrg, nestedErr := eng.Open(ctx, scope, nested.ID)
	_, eastErr := eng.Open(ctx, east, nested.ID)
	_, goneErr := eng.Open(ctx, scope, gone.ID)
	_, otherErr := eng.Open(ctx, brain.Scope{Namespace: tenant(t)}, parent.ID)
	_, nilErr := eng.Open(ctx, scope, uuid.Nil)
	byPath, pathErr := eng.Resolve(ctx, scope, brain.Ref{Path: "/workspace/doc.md"})
	byID, idErr := eng.Resolve(ctx, scope, brain.Ref{ID: nested.ID})
	_, unindexedErr := eng.Resolve(ctx, scope, brain.Ref{Path: "/workspace/other.md"})
	_, emptyErr := eng.Resolve(ctx, scope, brain.Ref{})
	raw, rawErr := eng.Get(ctx, scope, parent.ID)
	_, rawNilErr := eng.Get(ctx, scope, uuid.Nil)

	if openErr != nil || opened.Object.Content != "the body" || opened.Kind == nil || opened.Kind.Description != "A parent doc" || len(opened.Kind.FilterableFields) == 0 {
		t.Errorf("open = %+v %v, want the content and the store's kind with its fields", opened, openErr)
	}
	if len(opened.Children) != 10 || !opened.MoreChildren || opened.Children[0].ID != firstPart || opened.Children[0].Content != "" {
		t.Errorf("open children = %d (more %v), want the first 10 by position, without content", len(opened.Children), opened.MoreChildren)
	}
	if partErr != nil || len(part.Children) != 0 || part.Kind != nil {
		t.Errorf("open a part = %+v %v, want no children and no registered kind", part, partErr)
	}
	if nestedErr != nil || fromOrg.Object.Title != "nested" || !errors.Is(eastErr, brain.ErrNotFound) {
		t.Errorf("a workspace object from its org = %v, from another workspace = %v, want visible and not found", nestedErr, eastErr)
	}
	for name, err := range map[string]error{"soft-deleted": goneErr, "another tenant": otherErr, "an unindexed path": unindexedErr} {
		if !errors.Is(err, brain.ErrNotFound) {
			t.Errorf("open %s = %v, want ErrNotFound", name, err)
		}
	}
	if !errors.Is(nilErr, brain.ErrInvalid) || !errors.Is(emptyErr, brain.ErrInvalid) || !errors.Is(rawNilErr, brain.ErrInvalid) {
		t.Errorf("no id: open %v, resolve %v, get %v, want ErrInvalid", nilErr, emptyErr, rawNilErr)
	}
	if pathErr != nil || byPath != parent.ID || idErr != nil || byID != nested.ID {
		t.Errorf("resolve by path = %s %v, by id = %s %v", byPath, pathErr, byID, idErr)
	}
	if rawErr != nil || raw.Properties[brain.PropVFSPath] != "/workspace/doc.md" {
		t.Errorf("get = %+v %v, want the stored object", raw, rawErr)
	}
}

func TestEngine_openTakesTheKindFromTheCatalogWhenKindsAreRegistered(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithKinds(brain.KindSpec{Kind: "Deal", Description: "A sale", IsParent: true,
		Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}}}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	deal := save(t, eng, scope, brain.Object{Kind: "Deal", Title: "Acme"})
	stray := brain.Object{ID: uuid.New(), Kind: "Unregistered", Namespace: scope.Namespace}
	if err := putFixture(store, stray); err != nil {
		t.Fatal(err)
	}

	opened, err := eng.Open(ctx, scope, deal.ID)
	strayOpened, strayErr := eng.Open(ctx, scope, stray.ID)

	if err != nil || opened.Kind == nil || opened.Kind.Description != "A sale" || !json.Valid(opened.Kind.FilterableFields) {
		t.Errorf("open a registered kind = %+v %v, want the catalog's kind", opened.Kind, err)
	}
	if strayErr != nil || strayOpened.Kind != nil {
		t.Errorf("open an unregistered kind = %+v %v, want no kind", strayOpened.Kind, strayErr)
	}
}

func TestNewEngine_requiresAStoreAndValidKinds(t *testing.T) {
	_, noStore := brain.NewEngine(nil)
	_, badKind := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{Kind: ""}))
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{Kind: "Note"}), brain.WithConfig(brain.EngineConfig{Now: time.Now}))

	if noStore == nil || badKind == nil {
		t.Errorf("no store %v, bad kind %v, want errors", noStore, badKind)
	}
	if err != nil || eng.HasGraph() || !slices.Equal(eng.Catalog().RecordKinds(), []string{"Note"}) {
		t.Errorf("engine = %v, graph %v, kinds %v", err, eng.HasGraph(), eng.Catalog().RecordKinds())
	}
}

func TestMemoryStore_PutRequiresIdentity(t *testing.T) {
	s := brain.NewMemoryStore()
	cases := map[string]func() error{
		"empty object":      func() error { return putFixture(s, brain.Object{}) },
		"missing kind":      func() error { return putFixture(s, brain.Object{ID: uuid.New()}) },
		"missing namespace": func() error { return putFixture(s, brain.Object{ID: uuid.New(), Kind: "X"}) },
		"empty kind":        func() error { return s.PutKind(context.Background(), brain.ObjectKind{}) },
	}
	for name, fn := range cases {
		if err := fn(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}
