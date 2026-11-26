package brain_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestPut_catalogEnforced: under a catalog, Save rejects an object that breaks
// its kind (each case is one return path), and saves a record and its part
// that keep it. Examples do not close a field the way an enum does.
func TestPut_catalogEnforced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{
			Kind: "Document", IsParent: true,
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString, Required: true},
				{Name: "amount", Type: brain.FieldTypeNumber},
				{Name: "when", Type: brain.FieldTypeDateTime},
				{Name: "tier", Type: brain.FieldTypeString, Enum: []string{"gold", "silver"}},
				{Name: "note", Type: brain.FieldTypeString, Examples: []string{"hello"}},
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
		{"value outside the enum", brain.Object{Kind: "Document", Properties: map[string]any{"stage": "open", "tier": "Yellowish"}}, "enum"},
		{"no kind", brain.Object{Kind: " "}, "kind is required"},
		{"invalid namespace", brain.Object{Kind: "Document", Namespace: brain.Namespace{{Name: "", Value: "x"}}}, "name"},
		{"nil parent id", brain.Object{Kind: "Chunk", ParentID: &uuid.Nil}, "nil UUID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			"tier":     "gold",
			"note":     "not one of the examples",
			"when2":    nil,
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

// TestSave_failsWhenTheEmbedderFails: a Save whose text the store cannot
// embed returns the embedder's error.
func TestSave_failsWhenTheEmbedderFails(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: &storetest.Embedder{}})))
	if err != nil {
		t.Fatal(err)
	}

	_, err = eng.Save(t.Context(), brain.Scope{Namespace: tenant(t)}, brain.Object{Kind: "Note", Title: "x", Content: "poison"})

	if err == nil || !strings.Contains(err.Error(), "embedder down") {
		t.Fatalf("save = %v, want the embedder's error", err)
	}
}

// TestObject_entityIndexTextListsTheIndexedFieldsInKeyOrder: a record's
// index text is its title, summary, indexed properties in key order (each Go
// number type as its number) and content; a SkipIndex field, a blank, nil or
// nested value and the store's own properties stay out.
func TestObject_entityIndexTextListsTheIndexedFieldsInKeyOrder(t *testing.T) {
	t.Parallel()
	spec := brain.KindSpec{Kind: "Deal", Fields: []brain.FieldSpec{{Name: "message_id", Type: brain.FieldTypeString, SkipIndex: true}}}
	deal := brain.Object{
		Kind: "Deal", Title: " Acme renewal ", Summary: "enterprise opportunity", Content: "the renewal terms",
		Properties: map[string]any{
			"stage": "open", "amount": 120000.0, "hot": true, "message_id": "msg-1", brain.PropSlug: "acme", "count": 7,
			"share": float32(0.5), "rank": int32(3), "views": int64(4000), "seats": json.Number("12"), "owner": "  ", "lost": nil,
			"meta": map[string]any{"a": 1},
		},
	}

	got := deal.EntityIndexText(spec)

	want := "Acme renewal\nenterprise opportunity\namount: 120000\ncount: 7\nhot: true\nrank: 3\nseats: 12\nshare: 0.5\nstage: open\nviews: 4000\nthe renewal terms"
	if got != want {
		t.Errorf("index text = %q, want %q", got, want)
	}
}

// TestSave_indexesTheHostsRewriteOfTheIndexText: WithIndexText rewrites the
// text a record is indexed by, and the record is saved unchanged.
func TestSave_indexesTheHostsRewriteOfTheIndexText(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	g := memoryGraph()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithIndex(g),
		brain.WithIndexText(func(_ brain.Object, defaultText string) string {
			return "related: neighbor record\n" + defaultText
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}

	deal, err := eng.Save(ctx, scope, brain.Object{Kind: "Deal", Title: "Acme renewal", Content: "the renewal terms", Properties: map[string]any{"message_id": "msg-1"}})
	hits, findErr := g.FindObjects(ctx, brain.ObjectSearch{Namespace: scope.Namespace, Query: "neighbor record", Limit: 5})

	if err != nil || deal.Content != "the renewal terms" || deal.Properties["message_id"] != "msg-1" {
		t.Errorf("saved = %+v %v, want the content and every property kept", deal, err)
	}
	if findErr != nil || len(hits) != 1 || hits[0].ID != deal.ID {
		t.Errorf("graph search for the host's text = %+v %v, want the deal", hits, findErr)
	}
}

// TestLink_needsTwoVisibleRecordsARelationAndAGraph: a link needs two visible
// records, a relation that is not containment, a live context and a graph; a
// soft-deleted object cannot be saved, and another tenant cannot soft delete.
func TestLink_needsTwoVisibleRecordsARelationAndAGraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithGraph(memoryGraph()))
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
