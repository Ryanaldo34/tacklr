package brain_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// TestPut_roundTripAndSoftDelete: open-catalog Put is readable, soft-delete hides the object.
func TestPut_roundTripAndSoftDelete(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}

	got, err := eng.Save(ctx, scope, brain.Object{
		Kind: "Note", Title: "hello", Content: "body",
		Properties: map[string]any{"tag": "a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == uuid.Nil || !got.Namespace.Equal(ns) || len(got.Embedding) != 0 {
		t.Fatalf("put: %+v", got)
	}
	opened, err := eng.Open(ctx, scope, got.ID)
	if err != nil || opened.Object.Content != "body" {
		t.Fatalf("open: %+v err=%v", opened, err)
	}
	if err := eng.SoftDelete(ctx, scope, got.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Open(ctx, scope, got.ID); !errors.Is(err, brain.ErrNotFound) {
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

// TestPut_embedsAndSearchable: embedder on Put populates vectors; hybrid search finds the parent.
func TestPut_embedsAndSearchable(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: stubEmbedder{v: []float32{1, 0, 0}}}))
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	parent, err := eng.Save(ctx, scope, brain.Object{Kind: "Document", Title: "Doc"})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	part, err := eng.Save(ctx, scope, brain.Object{
		Kind: "Chunk", Title: "oauth", Summary: "auth", Content: "pkce flow",
		ParentID: &parent.ID, Position: &pos,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(part.Embedding) != 3 || part.Embedding[0] != 1 {
		t.Fatalf("embedding: %+v", part.Embedding)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "anything"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slices.Collect(page.Results.Objects())) == 0 || slices.Collect(page.Results.Objects())[0].ID != parent.ID {
		t.Fatalf("search after embed put: %+v", slices.Collect(page.Results.Objects()))
	}
}

// TestPut_embedderError: configured embedder failure fails the Put.
func TestPut_embedderError(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: failEmbedder{}})))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	_, err = eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Note", Title: "x", Content: "body",
	})
	if err == nil || !strings.Contains(err.Error(), "embed") {
		t.Fatalf("want embed error, got %v", err)
	}
}

// TestEntityIndexText_includesSummaryAndProperties: entity find text packs attributes.
func TestEntityIndexText_includesSummaryAndProperties(t *testing.T) {
	body := strings.Repeat("long body ", 300)
	got := brain.Object{
		Title: "Acme renewal", Summary: "enterprise opportunity",
		Content: body,
		Properties: map[string]any{
			"stage":  "negotiation",
			"amount": 120000.0,
			"hot":    true,
		},
	}.EntityIndexText()

	for _, want := range []string{"Acme renewal", "enterprise opportunity", "stage: negotiation", "amount: 120000", "hot: true", strings.TrimSpace(body)} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
	withSlug := brain.Object{
		Title: "Acme", Properties: map[string]any{brain.PropSlug: "acme", "stage": "open"},
	}.EntityIndexText()

	if strings.Contains(withSlug, "slug:") || !strings.Contains(withSlug, "stage: open") {
		t.Fatalf("slug must be omitted: %q", withSlug)
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
	ns := mustNS(t, "id", uuid.NewString())
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
	ns := mustNS(t, "id", uuid.NewString())
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
	ns := mustNS(t, "id", uuid.NewString())
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

func TestPut_indexTextFuncEmptySkipsEmbed(t *testing.T) {
	ctx := context.Background()
	emb := captureEmbedder{fn: func(string) []float32 {
		t.Fatal("embedder must not run on empty index text")
		return nil
	}}
	eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb})),
		brain.WithIndexText(func(brain.Object, string) string { return "  " }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	got, err := eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{
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
	eng, err := brain.NewEngine(store)
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
	eng2, err := brain.NewEngine(store)
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
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	parent, err := eng.Save(ctx, scope, brain.Object{Kind: "Document", Title: "Acme Deal Memo"})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	if _, err := eng.Save(ctx, scope, brain.Object{
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

func (c captureEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = c.fn(t)
	}
	return out, nil
}

func TestLink_createsUpdatesAndRemovesRelationsTheGraphWalks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	email := save(t, eng, scope, brain.Object{Kind: "Email", Title: "RE: pricing"})
	deal := save(t, eng, scope, brain.Object{Kind: "Deal", Title: "Acme"})
	evidence := email.ID
	meta := brain.EdgeMeta{Note: "security review thread supports this deal", Status: "active", Role: "source", Confidence: 0.85, EvidenceID: &evidence}
	relatedVia := func() *brain.Relation {
		page, err := eng.Related(ctx, scope, brain.RelatedRequest{ID: email.ID, Relation: "about"})
		if err != nil {
			t.Fatal(err)
		}
		if objs := slices.Collect(page.Results.Objects()); len(objs) == 1 && objs[0].ID == deal.ID {
			return objs[0].Relation
		}
		return nil
	}
	if err := eng.Link(ctx, scope, brain.LinkRequest{From: email.ID, To: deal.ID, Relation: "about", Meta: meta}); err != nil {
		t.Fatal(err)
	}

	linked := relatedVia()
	link(t, eng, scope, email.ID, deal.ID, "about", "  updated rationale  ")
	updated := relatedVia()
	removeErr := eng.Link(ctx, scope, brain.LinkRequest{From: email.ID, To: deal.ID, Relation: "about", Remove: true})
	removed := relatedVia()
	removeAgain := eng.Link(ctx, scope, brain.LinkRequest{From: email.ID, To: deal.ID, Relation: "about", Remove: true})
	link(t, eng, scope, email.ID, deal.ID, "about", "")
	softErr := eng.SoftDelete(ctx, scope, deal.ID)
	afterDelete := relatedVia()

	if linked == nil || linked.Type != "about" || linked.Direction != brain.DirectionOut || linked.Note != meta.Note || linked.Status != "active" ||
		linked.Role != "source" || linked.Confidence != 0.85 || linked.EvidenceID == nil || *linked.EvidenceID != evidence || linked.CreatedAt.IsZero() {
		t.Errorf("related through the link = %+v, want its relation and every meta field", linked)
	}
	if updated == nil || updated.Note != "updated rationale" || updated.Status != "" {
		t.Errorf("after linking again = %+v, want the new note alone", updated)
	}
	if removeErr != nil || removed != nil || removeAgain != nil {
		t.Errorf("remove = %v, then related %+v, remove again = %v, want gone and no error", removeErr, removed, removeAgain)
	}
	if softErr != nil || afterDelete != nil {
		t.Errorf("soft delete the target = %v, then related %+v, want it gone", softErr, afterDelete)
	}
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

func TestPutTree_searchHitsNewChunks(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "id", uuid.NewString())
	scope := brain.Scope{Namespace: ns}
	parent, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "Memo"}, brain.Parts(
		brain.Object{Kind: "Chunk", Title: "a", Content: "pkceflow"},
		brain.Object{Kind: "Chunk", Title: "b", Content: "neighbor"},
	))
	if err != nil {
		t.Fatal(err)
	}
	kidsTree, err := store.GetTree(ctx, scope, parent.ID, 0)
	kids := kidsTree.Parts
	if err != nil || len(kids) != 2 || kids[0].Title != "a" || kids[1].Title != "b" {
		t.Fatalf("children: %+v err=%v", kids, err)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "pkceflow"})
	if err != nil || len(slices.Collect(page.Results.Objects())) != 1 || slices.Collect(page.Results.Objects())[0].ID != parent.ID {
		t.Fatalf("search new chunk: %+v err=%v", slices.Collect(page.Results.Objects()), err)
	}
	if _, err := eng.PutTree(ctx, scope, parent, brain.Parts(
		brain.Object{Kind: "Chunk", Title: "c", Content: "indemnityclause"},
	)); err != nil {
		t.Fatal(err)
	}
	kidsTree, err = store.GetTree(ctx, scope, parent.ID, 0)
	kids = kidsTree.Parts
	if err != nil || len(kids) != 1 || kids[0].Title != "c" {
		t.Fatalf("replaced children: %+v err=%v", kids, err)
	}
	miss, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "pkceflow"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slices.Collect(miss.Results.Objects())) != 0 {
		t.Fatalf("old chunk must be gone: %+v", slices.Collect(miss.Results.Objects()))
	}
	hit, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "indemnityclause"})
	if err != nil || len(slices.Collect(hit.Results.Objects())) != 1 || slices.Collect(hit.Results.Objects())[0].ID != parent.ID {
		t.Fatalf("search replacement: %+v err=%v", slices.Collect(hit.Results.Objects()), err)
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
	ns := mustNS(t, "id", uuid.NewString())
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
