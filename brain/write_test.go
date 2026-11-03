package brain_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

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
	ns := storetest.Namespace(t, "id", uuid.NewString())
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
		{"zero datetime", brain.Object{Kind: "Document", Properties: map[string]any{"stage": "open", "when": time.Time{}}}, "zero time"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eng.Save(ctx, scope, tc.obj)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want containing %q", err, tc.wantErr)
			}
		})
	}

	doc, err := eng.Save(ctx, scope, brain.Object{
		Kind: "Document", Title: "memo",
		Properties: map[string]any{
			"stage":    "open",
			"amount":   10,
			"when":     time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			"slug":     "memo",
			"vfs_path": "/workspace/work/memo.md",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	if _, err := eng.Save(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "c1", Content: "body",
		ParentID: &doc.ID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPut_embedderError: configured embedder failure fails the Put.
func TestPut_embedderError(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: failEmbedder{}})))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "id", uuid.NewString())
	_, err = eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Note", Title: "x", Content: "body",
	})
	if err == nil || !strings.Contains(err.Error(), "embed") {
		t.Fatalf("want embed error, got %v", err)
	}
}

// TestPut_skipIndexKeepsPropertyOffEntityText: SkipIndex fields stay on the
// object and remain filterable, but they are not in embed or graph search text.
func TestPut_skipIndexKeepsPropertyOffEntityText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var gotEmbed string
	emb := captureEmbedder{fn: func(text string) []float32 {
		gotEmbed = text
		return []float32{1, 0}
	}}
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
	g := helixtest.Graph(t)
	eng, err := brain.NewEngine(store, brain.WithIndex(g), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString},
			{Name: "message_id", Type: brain.FieldTypeString, SkipIndex: true},
		}},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	const msgID = "msg-skip-index-token"
	deal, err := eng.Save(ctx, scope, brain.Object{
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
	got, err := eng.Get(ctx, scope, deal.ID)
	if err != nil || got.Properties["message_id"] != msgID {
		t.Fatalf("the stored object still has the property: %+v err=%v", got, err)
	}
	miss, err := g.FindObjects(ctx, brain.ObjectSearch{Namespace: ns, Query: msgID, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(miss) != 0 {
		t.Fatalf("graph search text must not contain skip-index value: %+v", miss)
	}
	hit, err := g.FindObjects(ctx, brain.ObjectSearch{Namespace: ns, Query: "Acme renewal", Limit: 5})
	if err != nil || len(hit) != 1 || hit[0].ID != deal.ID {
		t.Fatalf("graph search title: %+v err=%v", hit, err)
	}
}

func TestPut_enumRejectsUnknownString(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString, Enum: []string{"open", "closed"}},
			{Name: "note", Type: brain.FieldTypeString, Examples: []string{"hello"}},
		}},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	_, err = eng.Save(ctx, scope, brain.Object{
		Kind: "Deal", Title: "x", Properties: map[string]any{"stage": "Yellowish"},
	})
	if err == nil || !strings.Contains(err.Error(), "enum") {
		t.Fatalf("want enum error, got %v", err)
	}
	if _, err := eng.Save(ctx, scope, brain.Object{
		Kind: "Deal", Title: "x", Properties: map[string]any{"stage": "open", "note": "Yellowish"},
	}); err != nil {
		t.Fatalf("examples are not closed: %v", err)
	}
}

func TestPut_indexTextFuncShapesEmbedAndGraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var gotEmbed string
	emb := captureEmbedder{fn: func(text string) []float32 {
		gotEmbed = text
		return []float32{1, 0}
	}}
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
	g := helixtest.Graph(t)
	eng, err := brain.NewEngine(store, brain.WithIndex(g),
		brain.WithIndexText(func(obj brain.Object, defaultText string) string {
			return "related: neighbor record\n" + defaultText
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	obj, err := eng.Save(ctx, scope, brain.Object{
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
	hit, err := g.FindObjects(ctx, brain.ObjectSearch{Namespace: ns, Query: "neighbor record", Limit: 5})
	if err != nil || len(hit) != 1 || hit[0].ID != obj.ID {
		t.Fatalf("graph search extra text: %+v err=%v", hit, err)
	}
}

type captureEmbedder struct {
	fn func(string) []float32
}

func (c captureEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = c.fn(t)
	}
	return out, nil
}

func TestLink_needsTwoVisibleRecordsARelationAndAGraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	storeOnly, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	doc := docWithParts(t, eng, scope, "A", nil, "a part")
	b := save(t, eng, scope, brain.Object{Kind: "Note", Title: "B"})
	partsTree, err := store.GetTree(ctx, scope, doc.ID, 0)
	parts := partsTree.Parts
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	try := func(eng *brain.Engine, ctx context.Context, req brain.LinkRequest) error {
		return eng.Link(ctx, scope, req)
	}

	for name, tc := range map[string]struct {
		err, want error
	}{
		"a part as an end":          {try(eng, ctx, brain.LinkRequest{From: parts[0].ID, To: b.ID, Relation: "about"}), brain.ErrInvalid},
		"a missing end":             {try(eng, ctx, brain.LinkRequest{From: doc.ID, To: uuid.New(), Relation: "about"}), brain.ErrNotFound},
		"no relation":               {try(eng, ctx, brain.LinkRequest{From: doc.ID, To: b.ID}), brain.ErrInvalid},
		"containment":               {try(eng, ctx, brain.LinkRequest{From: doc.ID, To: b.ID, Relation: "contains"}), brain.ErrInvalid},
		"a cancelled call":          {try(eng, cancelled, brain.LinkRequest{From: doc.ID, To: b.ID, Relation: "about"}), context.Canceled},
		"an engine without a graph": {try(storeOnly, ctx, brain.LinkRequest{From: doc.ID, To: b.ID, Relation: "about"}), brain.ErrUnsupported},
	} {
		if !errors.Is(tc.err, tc.want) {
			t.Errorf("link with %s = %v, want %v", name, tc.err, tc.want)
		}
	}
	// Soft-deleted objects are refused; SoftDelete needs a visible id.
	now := time.Now().UTC()
	gone := b
	gone.DeletedAt = &now
	if _, err := eng.Save(ctx, scope, gone); !errors.Is(err, brain.ErrInvalid) {
		t.Errorf("save a soft-deleted object = %v, want ErrInvalid", err)
	}
	if err := eng.SoftDelete(ctx, brain.Scope{Namespace: tenant(t)}, doc.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Errorf("soft delete from another tenant = %v, want ErrNotFound", err)
	}
}

func TestPutTree_rejectsPartParent(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	parent, err := eng.Save(ctx, scope, brain.Object{Kind: "Document", Title: "Memo"})
	if err != nil {
		t.Fatal(err)
	}
	pos := 0
	part, err := eng.Save(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "c", ParentID: &parent.ID, Position: &pos,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.PutTree(ctx, scope, part, nil)
	if err == nil || !strings.Contains(err.Error(), "first-class") {
		t.Fatalf("got %v", err)
	}
}

func (captureEmbedder) Model() string { return "test-model" }
