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

func TestPut_openCatalogAndSoftDelete(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}

	got, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Note", Title: "hello", Content: "body",
		Properties: map[string]any{"tag": "a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == uuid.Nil || got.NamespaceID != ns {
		t.Fatalf("prepared: %+v", got)
	}

	rich, err := eng.Read(ctx, scope, got.ID)
	if err != nil || rich.Content != "body" {
		t.Fatalf("read: %+v err=%v", rich, err)
	}
	if err := eng.SoftDelete(ctx, scope, got.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Read(ctx, scope, got.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("after soft delete: %v", err)
	}
}

func TestPut_catalogValidation(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{
			Kind: "Document", IsParent: true,
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString, Required: true},
				{Name: "amount", Type: brain.FieldTypeNumber},
			},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	pid := uuid.New()

	cases := []struct {
		name    string
		obj     brain.Object
		wantErr string
	}{
		{"unknown kind", brain.Object{Kind: "Orphan"}, "not registered"},
		{"missing required", brain.Object{Kind: "Document"}, "required property"},
		{"unknown property", brain.Object{Kind: "Document", Properties: map[string]any{"stage": "open", "nope": 1}}, "not defined"},
		{"wrong type", brain.Object{Kind: "Document", Properties: map[string]any{"stage": 3}}, "want string"},
		{"parent with parent_id", brain.Object{Kind: "Document", ParentID: &pid, Properties: map[string]any{"stage": "open"}}, "must not have parent_id"},
		{"part without parent", brain.Object{Kind: "Chunk"}, "requires parent_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eng.Put(ctx, scope, tc.obj)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want containing %q", err, tc.wantErr)
			}
		})
	}

	doc, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Document", Title: "memo",
		Properties: map[string]any{"stage": "open", "amount": 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "c1", Content: "body",
		ParentID: &doc.ID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPut_embedsWhenConfigured(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	vec := []float32{1, 0, 0}
	eng, err := brain.NewEngine(store, brain.WithEmbedder(stubEmbedder{v: vec}))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	parent := uuid.New()
	// Need a part for vector search candidates (parts only).
	if _, err := eng.Put(ctx, scope, brain.Object{ID: parent, Kind: "Document", Title: "Doc"}); err != nil {
		t.Fatal(err)
	}
	pos := 1
	part, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "oauth", Content: "pkce flow",
		ParentID: &parent, Position: &pos,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(part.Embedding) != 3 || part.Embedding[0] != 1 {
		t.Fatalf("embedding on put: %+v", part.Embedding)
	}
	// Hybrid search should hit via vector channel.
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "anything"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != parent {
		t.Fatalf("search after embed put: %+v", page.Objects)
	}
}

func TestPut_embedErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithEmbedder(failEmbedder{}))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	_, err = eng.Put(ctx, brain.Scope{Namespace: &ns}, brain.Object{
		Kind: "Note", Title: "x", Content: "body",
	})
	if err == nil || !strings.Contains(err.Error(), "embed") {
		t.Fatalf("want embed error, got %v", err)
	}
}

func TestPut_noEmbedderSkipsVector(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	got, err := eng.Put(ctx, brain.Scope{Namespace: &ns}, brain.Object{
		Kind: "Note", Title: "x", Content: "body",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Embedding) != 0 {
		t.Fatalf("want no embedding without embedder: %+v", got.Embedding)
	}
}

func TestValidateObject_datetime(t *testing.T) {
	ns := uuid.New()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{
		Kind: "Event", IsParent: true,
		Fields: []brain.FieldSpec{{Name: "when", Type: brain.FieldTypeDateTime}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	obj := brain.Object{Kind: "Event", NamespaceID: ns, Properties: map[string]any{"when": "2024-06-01T00:00:00Z"}}
	if err := brain.ValidateObject(obj, eng.Catalog()); err != nil {
		t.Fatal(err)
	}
	obj.Properties["when"] = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	if err := brain.ValidateObject(obj, eng.Catalog()); err != nil {
		t.Fatal(err)
	}
	obj.Properties["when"] = "not-a-date"
	if err := brain.ValidateObject(obj, eng.Catalog()); err == nil {
		t.Fatal("want datetime error")
	}
}
