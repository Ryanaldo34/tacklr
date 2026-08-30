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
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}

	got, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Note", Title: "hello", Content: "body",
		Properties: map[string]any{"tag": "a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == uuid.Nil || !got.Namespace.Equal(ns) || len(got.Embedding) != 0 {
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
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly(), brain.WithKinds(
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
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
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
			"stage":    "open",
			"amount":   10,
			"when":     time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			"slug":     "memo",
			"vfs_path": "/engram/document/memo.md",
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
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
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
	ns := mustNS(t, "id", uuid.NewString())
	_, err = eng.Put(ctx, brain.Scope{Namespace: ns}, brain.Object{
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

// TestEntityIndexText_includesSummaryAndProperties: entity find text packs attributes.
func TestEntityIndexText_includesSummaryAndProperties(t *testing.T) {
	body := strings.Repeat("long body ", 300)
	got := brain.EntityIndexText(brain.Object{
		Title: "Acme renewal", Summary: "enterprise opportunity",
		Content: body,
		Properties: map[string]any{
			"stage":  "negotiation",
			"amount": 120000.0,
			"hot":    true,
		},
	})
	for _, want := range []string{"Acme renewal", "enterprise opportunity", "stage: negotiation", "amount: 120000", "hot: true", strings.TrimSpace(body)} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
	withSlug := brain.EntityIndexText(brain.Object{
		Title: "Acme", Properties: map[string]any{brain.PropSlug: "acme", "stage": "open"},
	})
	if strings.Contains(withSlug, "slug:") || !strings.Contains(withSlug, "stage: open") {
		t.Fatalf("slug must be omitted: %q", withSlug)
	}
}

// TestPut_skipIndexKeepsPropertyOffEntityText: SkipIndex fields stay on the
// object and remain filterable, but they are not in embed or find_objects text.
func TestPut_skipIndexKeepsPropertyOffEntityText(t *testing.T) {
	ctx := context.Background()
	var gotEmbed string
	emb := captureEmbedder{fn: func(text string) []float32 {
		gotEmbed = text
		return []float32{1, 0, 0}
	}}
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString},
			{Name: "message_id", Type: brain.FieldTypeString, SkipIndex: true},
		}},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	const msgID = "msg-skip-index-token"
	deal, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Acme renewal",
		Properties: map[string]any{"stage": "open", "message_id": msgID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gotEmbed, msgID) {
		t.Fatalf("skip-index field in embed text: %q", gotEmbed)
	}
	if !strings.Contains(gotEmbed, "stage: open") {
		t.Fatalf("indexed field missing from embed: %q", gotEmbed)
	}
	got, err := eng.GetByProperty(ctx, scope, "message_id", msgID)
	if err != nil || got.ID != deal.ID {
		t.Fatalf("GetByProperty: %+v err=%v", got, err)
	}
	rich, err := eng.Read(ctx, scope, deal.ID)
	if err != nil || rich.Properties["message_id"] != msgID {
		t.Fatalf("read still has property: %+v err=%v", rich, err)
	}
	miss, err := g.SearchText(ctx, msgID, 5, ns)
	if err != nil {
		t.Fatal(err)
	}
	if len(miss) != 0 {
		t.Fatalf("graph search text must not contain skip-index value: %+v", miss)
	}
	hit, err := g.SearchText(ctx, "Acme renewal", 5, ns)
	if err != nil || len(hit) != 1 || hit[0].ID != deal.ID {
		t.Fatalf("graph search title: %+v err=%v", hit, err)
	}
}

func TestPut_enumRejectsUnknownString(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly(), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString, Enum: []string{"open", "closed"}},
			{Name: "note", Type: brain.FieldTypeString, Examples: []string{"hello"}},
		}},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	_, err = eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "x", Properties: map[string]any{"stage": "Yellowish"},
	})
	if err == nil || !strings.Contains(err.Error(), "enum") {
		t.Fatalf("want enum error, got %v", err)
	}
	if _, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "x", Properties: map[string]any{"stage": "open", "note": "Yellowish"},
	}); err != nil {
		t.Fatalf("examples are not closed: %v", err)
	}
}

func TestPut_indexTextFuncShapesEmbedAndGraph(t *testing.T) {
	ctx := context.Background()
	var gotEmbed string
	emb := captureEmbedder{fn: func(text string) []float32 {
		gotEmbed = text
		return []float32{1, 0, 0}
	}}
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithGraph(g),
		brain.WithIndexText(func(obj brain.Object, defaultText string) string {
			return "related: neighbor record\n" + defaultText
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	obj, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Note", Title: "invoice", Content: "amount due Friday",
	})
	if err != nil {
		t.Fatal(err)
	}
	if obj.Content != "amount due Friday" {
		t.Fatalf("content must stay on the object: %q", obj.Content)
	}
	if !strings.Contains(gotEmbed, "related: neighbor record") || !strings.Contains(gotEmbed, "invoice") {
		t.Fatalf("embed text: %q", gotEmbed)
	}
	hit, err := g.SearchText(ctx, "neighbor record", 5, ns)
	if err != nil || len(hit) != 1 || hit[0].ID != obj.ID {
		t.Fatalf("graph search extra text: %+v err=%v", hit, err)
	}
}

func TestPut_indexTextFuncEmptySkipsEmbed(t *testing.T) {
	ctx := context.Background()
	emb := captureEmbedder{fn: func(string) []float32 {
		t.Fatal("embedder must not run on empty index text")
		return nil
	}}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithEmbedder(emb),
		brain.WithIndexText(func(brain.Object, string) string { return "  " }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	got, err := eng.Put(ctx, brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Note", Title: "x", Content: "body",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Embedding) != 0 {
		t.Fatalf("empty index text must not embed: %+v", got.Embedding)
	}
}

func TestApplyKinds_roundTripSkipIndexAndEnum(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, brain.KindSpec{
		Kind: "Deal", IsParent: true,
		Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString, Enum: []string{"open", "closed"}},
			{Name: "message_id", Type: brain.FieldTypeString, SkipIndex: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	eng2, err := brain.NewEngine(store, brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.LoadKindsFromStore(ctx); err != nil {
		t.Fatal(err)
	}
	spec, ok := eng2.Catalog().Get("Deal")
	if !ok {
		t.Fatal("missing Deal")
	}
	stage, _ := spec.Field("stage")
	if len(stage.Enum) != 2 || stage.Enum[0] != "open" {
		t.Fatalf("enum: %+v", stage)
	}
	mid, _ := spec.Field("message_id")
	if !mid.SkipIndex {
		t.Fatalf("skip_index: %+v", mid)
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
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
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
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	other := mustNS(t, "org", "other")
	scope := brain.Scope{Namespace: ns}
	sc := brain.NewSearchContext()

	a, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "A", Content: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "B", Content: "beta"})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Link(ctx, scope, a.ID, b.ID, "about"); err != nil {
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
	// Parts cannot be link endpoints.
	if err := eng.Link(ctx, scope, chunk.ID, b.ID, "about"); err == nil || !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("link part: %v", err)
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

	// SoftDelete removes store row visibility and graph node.
	if err := eng.SoftDelete(ctx, scope, b.ID); err != nil {
		t.Fatal(err)
	}
	resAfter, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a.ID, RelationTypes: []string{"about"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range resAfter.Objects {
		if o.ID == b.ID {
			t.Fatal("soft-deleted neighbor must not appear after graph remove")
		}
	}
	// Soft-deleted Put is refused.
	now := time.Now().UTC()
	b.DeletedAt = &now
	if _, err := eng.Put(ctx, scope, b); err == nil || !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("put soft-deleted: %v", err)
	}
	// SoftDelete wrong namespace / missing id.
	if err := eng.SoftDelete(ctx, brain.Scope{Namespace: other}, a.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("wrong ns: %v", err)
	}
	if err := eng.SoftDelete(ctx, scope, uuid.New()); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if err := store.SoftDelete(ctx, scope, uuid.Nil); err == nil {
		t.Fatal("nil id soft-delete")
	}

	// Graph EnsureObject rejects nil id.
	if err := g.EnsureObject(ctx, brain.Object{}, ""); err == nil {
		t.Fatal("nil ensure")
	}
}

// TestLink_expandFindsNeighbor: Put dual-writes graph nodes; Link + Expand returns the target.
func TestLink_expandFindsNeighbor(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(brain.NewMemoryGraph()))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.HasGraphWriter() {
		t.Fatal("WithGraph(MemoryGraph) must report HasGraphWriter")
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}

	a, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "A"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "B"})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Link(ctx, scope, a.ID, b.ID, "references"); err != nil {
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
	if res.Objects[0].Relation == nil || res.Objects[0].Relation.Type != "references" {
		t.Fatalf("expand should attach relation type: %+v", res.Objects[0].Relation)
	}

	if err := eng.Unlink(ctx, scope, a.ID, b.ID, "references"); err != nil {
		t.Fatal(err)
	}
	res2, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: a.ID, RelationTypes: []string{"references"},
	}, brain.NewSearchContext())
	if err != nil || len(res2.Objects) != 0 {
		t.Fatalf("after unlink: %+v err=%v", res2.Objects, err)
	}
	if err := eng.Link(ctx, scope, a.ID, b.ID, "references"); err != nil {
		t.Fatal(err)
	}

	if err := eng.Link(ctx, scope, a.ID, b.ID, ""); !errors.Is(err, brain.ErrInvalid) {
		t.Fatalf("want ErrInvalid for incomplete link: %v", err)
	}
	engNoGraph, err := brain.NewEngine(store, brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if engNoGraph.HasGraphWriter() {
		t.Fatal("engine without WithGraph must not report HasGraphWriter")
	}
	if err := engNoGraph.Link(ctx, scope, a.ID, b.ID, "references"); !errors.Is(err, brain.ErrUnsupported) {
		t.Fatalf("want ErrUnsupported: %v", err)
	}
	if err := engNoGraph.Unlink(ctx, scope, a.ID, b.ID, "references"); !errors.Is(err, brain.ErrUnsupported) {
		t.Fatalf("unlink want ErrUnsupported: %v", err)
	}
}

// TestLinkWith_missingEndpointAndCancelled: distinct LinkWith failure return paths.
func TestLinkWith_missingEndpointAndCancelled(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(brain.NewMemoryGraph()))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	a, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "A"})
	if err != nil {
		t.Fatal(err)
	}
	// Missing "to" under scope.
	if err := eng.LinkWith(ctx, scope, a.ID, uuid.New(), "about", brain.EdgeMeta{}); err == nil || !strings.Contains(err.Error(), "to") {
		t.Fatalf("missing to: %v", err)
	}
	// Cancelled before work.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := eng.LinkWith(cctx, scope, a.ID, a.ID, "about", brain.EdgeMeta{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	if err := eng.Unlink(cctx, scope, a.ID, a.ID, "about"); !errors.Is(err, context.Canceled) {
		t.Fatalf("unlink cancelled: %v", err)
	}
	if _, err := eng.Put(cctx, scope, brain.Object{Kind: "Document", Title: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("put cancelled: %v", err)
	}
	if err := eng.SoftDelete(cctx, scope, a.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("soft-delete cancelled: %v", err)
	}
}

// TestSoftDelete_graphRemoveErrorSurfaces: graph-first remove failure leaves store intact.
func TestSoftDelete_graphRemoveErrorSurfaces(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := &failRemoveGraph{MemoryGraph: brain.NewMemoryGraph()}
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	obj, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "doomed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.SoftDelete(ctx, scope, obj.ID); err == nil || !errors.Is(err, brain.ErrGraphRemove) {
		t.Fatalf("want ErrGraphRemove: %v", err)
	}
	// Store row must still be readable (graph failed before SoftDelete).
	if _, err := eng.Read(ctx, scope, obj.ID); err != nil {
		t.Fatalf("store must remain after graph remove failure: %v", err)
	}
}

type failRemoveGraph struct {
	*brain.MemoryGraph
}

func (f *failRemoveGraph) RemoveObject(context.Context, uuid.UUID) error {
	return errors.New("remove down")
}

// TestLinkWith_expandAttachesMeta: edge note/role/status surface on expand neighbors.
func TestLinkWith_expandAttachesMeta(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(g))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	sc := brain.NewSearchContext()

	email, err := eng.Put(ctx, scope, brain.Object{Kind: "Email", Title: "RE: pricing"})
	if err != nil {
		t.Fatal(err)
	}
	deal, err := eng.Put(ctx, scope, brain.Object{Kind: "Deal", Title: "Acme"})
	if err != nil {
		t.Fatal(err)
	}
	evid := email.ID
	meta := brain.EdgeMeta{
		Note: "security review thread supports this deal", Status: "active",
		Role: "source", Confidence: 0.85, EvidenceID: &evid,
	}
	if err := eng.LinkWith(ctx, scope, email.ID, deal.ID, "about", meta); err != nil {
		t.Fatal(err)
	}

	exp, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: email.ID, RelationTypes: []string{"about"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Objects) != 1 || exp.Objects[0].ID != deal.ID {
		t.Fatalf("expand: %+v", exp.Objects)
	}
	r := exp.Objects[0].Relation
	if r == nil || r.Type != "about" || r.Direction != "out" {
		t.Fatalf("relation hop: %+v", r)
	}
	if r.Note != meta.Note || r.Status != "active" || r.Role != "source" || r.Confidence != 0.85 {
		t.Fatalf("meta: %+v", r)
	}
	if r.EvidenceID == nil || *r.EvidenceID != evid {
		t.Fatalf("evidence: %+v", r.EvidenceID)
	}

	// Re-link updates meta (upsert); soft-delete of neighbor hides the hop.
	if err := eng.LinkWith(ctx, scope, email.ID, deal.ID, "about", brain.EdgeMeta{
		Note: "updated rationale", Status: "resolved",
	}); err != nil {
		t.Fatal(err)
	}
	exp2, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: email.ID, RelationTypes: []string{"about"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp2.Objects) != 1 || exp2.Objects[0].Relation == nil || exp2.Objects[0].Relation.Note != "updated rationale" {
		t.Fatalf("re-link meta: %+v", exp2.Objects)
	}
	if err := eng.SoftDelete(ctx, scope, deal.ID); err != nil {
		t.Fatal(err)
	}
	exp3, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: email.ID, RelationTypes: []string{"about"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp3.Objects) != 0 {
		t.Fatalf("soft-deleted neighbor: %+v", exp3.Objects)
	}
}

// TestLink_crossObjectEmailDealBuyer: first-class cross-object graph (not chunks).
func TestLink_crossObjectEmailDealBuyer(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	g := brain.NewMemoryGraph()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(g), brain.WithKinds(
		brain.KindSpec{Kind: "Email", IsParent: true},
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString},
		}},
		brain.KindSpec{Kind: "Person", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	sc := brain.NewSearchContext()

	email, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Email", Title: "RE: Acme pricing", Summary: "security review thread",
		Content: "please confirm FedRAMP timeline",
	})
	if err != nil {
		t.Fatal(err)
	}
	deal, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Deal", Title: "Acme Enterprise Renewal", Summary: "renewal opportunity",
		Properties: map[string]any{"stage": "negotiation"},
	})
	if err != nil {
		t.Fatal(err)
	}
	buyer, err := eng.Put(ctx, scope, brain.Object{
		Kind: "Person", Title: "Jordan Lee", Summary: "procurement buyer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LinkWith(ctx, scope, email.ID, deal.ID, "about", brain.EdgeMeta{
		Note: "FedRAMP timeline discussion",
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.LinkWith(ctx, scope, deal.ID, buyer.ID, "has_buyer", brain.EdgeMeta{
		Role: "primary", Status: "active",
	}); err != nil {
		t.Fatal(err)
	}
	// Property-aware entity find.
	page, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
		Query: "negotiation", Kinds: []string{"Deal"},
	}, sc)
	if err != nil || len(page.Objects) != 1 || page.Objects[0].ID != deal.ID {
		t.Fatalf("find deal by stage prop: %+v err=%v", page.Objects, err)
	}
	exp, err := eng.Expand(ctx, scope, brain.ExpandRequest{
		ObjectID: deal.ID, RelationTypes: []string{"about", "has_buyer"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]*brain.Relation{}
	for _, o := range exp.Objects {
		got[o.ID] = o.Relation
	}
	if got[email.ID] == nil || got[email.ID].Type != "about" || got[email.ID].Note != "FedRAMP timeline discussion" {
		t.Fatalf("email hop: %+v", got[email.ID])
	}
	if got[buyer.ID] == nil || got[buyer.ID].Type != "has_buyer" || got[buyer.ID].Role != "primary" {
		t.Fatalf("buyer hop: %+v", got[buyer.ID])
	}
	// Drill-down: email has no chunks yet; containment expand is empty.
	kids, err := eng.Expand(ctx, scope, brain.ExpandRequest{ObjectID: email.ID}, sc)
	if err != nil || kids.Mode != "children" {
		t.Fatalf("containment expand: %+v err=%v", kids, err)
	}
}
