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

// TestPut_roundTripAndSoftDelete: open-catalog Put is readable, soft-delete hides the object.
func TestPut_roundTripAndSoftDelete(t *testing.T) {
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
	if got.ID == uuid.Nil || got.NamespaceID != ns || len(got.Embedding) != 0 {
		t.Fatalf("put: %+v", got)
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

// TestPut_catalogEnforced: each case is a distinct ValidateObject return path; success path writes parent+part.
func TestPut_catalogEnforced(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{
			Kind: "Document", IsParent: true,
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString, Required: true},
				{Name: "amount", Type: brain.FieldTypeNumber},
				{Name: "when", Type: brain.FieldTypeDateTime},
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
		{"bad datetime", brain.Object{Kind: "Document", Properties: map[string]any{"stage": "open", "when": "nope"}}, "RFC3339"},
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
		Properties: map[string]any{
			"stage":  "open",
			"amount": 10,
			"when":   time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		},
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

// TestPut_embedsAndSearchable: embedder on Put populates vectors; hybrid search finds the parent.
func TestPut_embedsAndSearchable(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithEmbedder(stubEmbedder{v: []float32{1, 0, 0}}))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	parent, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "Doc"})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	part, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "oauth", Summary: "auth", Content: "pkce flow",
		ParentID: &parent.ID, Position: &pos,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(part.Embedding) != 3 || part.Embedding[0] != 1 {
		t.Fatalf("embedding: %+v", part.Embedding)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "anything"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != parent.ID {
		t.Fatalf("search after embed put: %+v", page.Objects)
	}
}

// TestPut_embedderError: configured embedder failure fails the Put.
func TestPut_embedderError(t *testing.T) {
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

// TestIndexText_skipsEmptyParts: IndexText joins non-empty fields only.
func TestIndexText_skipsEmptyParts(t *testing.T) {
	got := brain.IndexText(brain.Object{Title: " t ", Summary: "", Content: "c"})
	if got != "t\nc" {
		t.Fatalf("%q", got)
	}
	if brain.IndexText(brain.Object{}) != "" {
		t.Fatal("empty object")
	}
	withParent := brain.IndexTextWithParent(brain.Object{Title: "chunk", Content: "body"}, "Parent Doc")
	if withParent != "Parent Doc\nchunk\nbody" {
		t.Fatalf("%q", withParent)
	}
}

// TestPut_partEmbedIncludesParentTitle: part embeddings / graph index text get parent context.
func TestPut_partEmbedIncludesParentTitle(t *testing.T) {
	ctx := context.Background()
	var gotEmbedText string
	emb := captureEmbedder{fn: func(text string) []float32 {
		gotEmbedText = text
		return []float32{1, 0}
	}}
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	parent, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "Acme Deal Memo"})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "risk note", Content: "penalty clause",
		ParentID: &parent.ID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotEmbedText, "Acme Deal Memo") || !strings.Contains(gotEmbedText, "penalty clause") {
		t.Fatalf("embed text missing parent context: %q", gotEmbedText)
	}
}

type captureEmbedder struct {
	fn func(string) []float32
}

func (c captureEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	return c.fn(text), nil
}

// TestPut_multiTurnMemoryGraph spans put dual-write, link, expand, soft-delete hydrate
// filtering, refuse soft-deleted Put, and SoftDelete not-found branches (MemoryStore).
func TestPut_multiTurnMemoryGraph(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	other := uuid.New()
	scope := brain.Scope{Namespace: &ns}
	sc := brain.NewSearchContext()

	a, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "A", Content: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "B", Content: "beta"})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Link(ctx, a.ID, b.ID, "about"); err != nil {
		t.Fatal(err)
	}
	pos := 1
	chunk, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "c", Content: "part body",
		ParentID: &a.ID, Position: &pos,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Mixed expand: children + graph neighbor.
	mixed, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a.ID, RelationTypes: []string{"contains", "about"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if mixed.Mode != "mixed" {
		t.Fatalf("mode: %s", mixed.Mode)
	}
	ids := map[uuid.UUID]bool{}
	for _, o := range mixed.Objects {
		ids[o.ID] = true
	}
	if !ids[chunk.ID] || !ids[b.ID] {
		t.Fatalf("mixed expand: %+v", mixed.Objects)
	}

	if err := eng.SoftDelete(ctx, scope, b.ID); err != nil {
		t.Fatal(err)
	}
	// Soft-deleted Put is refused.
	now := time.Now().UTC()
	b.DeletedAt = &now
	if _, err := eng.Put(ctx, scope, b); err == nil || !strings.Contains(err.Error(), "SoftDelete") {
		t.Fatalf("put soft-deleted: %v", err)
	}
	// SoftDelete wrong namespace / missing id.
	if err := eng.SoftDelete(ctx, brain.Scope{Namespace: &other}, a.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("wrong ns: %v", err)
	}
	if err := eng.SoftDelete(ctx, scope, uuid.New()); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if err := store.SoftDelete(ctx, scope, uuid.Nil); err == nil {
		t.Fatal("nil id soft-delete")
	}

	// Graph EnsureObject rejects nil id.
	if err := g.EnsureObject(ctx, brain.Object{}); err == nil {
		t.Fatal("nil ensure")
	}
}

// TestLink_expandFindsNeighbor: Put dual-writes graph nodes; Link + Expand returns the target.
func TestLink_expandFindsNeighbor(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithGraph(brain.NewMemoryGraph()))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasGraphWriter() {
		t.Fatal("WithGraph(MemoryGraph) must report HasGraphWriter")
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}

	a, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "A"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "B"})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Link(ctx, a.ID, b.ID, "references"); err != nil {
		t.Fatal(err)
	}

	res, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a.ID, RelationTypes: []string{"references"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "graph" {
		t.Fatalf("mode: %s", res.Mode)
	}
	if len(res.Objects) != 1 || res.Objects[0].ID != b.ID {
		t.Fatalf("expand: %+v", res.Objects)
	}

	if err := eng.Link(ctx, a.ID, b.ID, ""); err == nil {
		t.Fatal("want empty relation error")
	}
	engNoGraph, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if engNoGraph.HasGraphWriter() {
		t.Fatal("engine without WithGraph must not report HasGraphWriter")
	}
	if err := engNoGraph.Link(ctx, a.ID, b.ID, "references"); err == nil {
		t.Fatal("want graph writer required")
	}
}
