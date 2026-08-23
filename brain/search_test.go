package brain_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func seedDocWithParts(t *testing.T, store *brain.MemoryStore, ns uuid.UUID, title string, partBodies []string, updated time.Time) (parentID uuid.UUID) {
	t.Helper()
	parentID = uuid.New()
	if err := store.Put(context.Background(), brain.Object{
		ID: parentID, Kind: "Document", Title: title, Summary: title,
		NamespaceID: ns, UpdatedAt: updated, CreatedAt: updated,
	}); err != nil {
		t.Fatal(err)
	}
	for i, body := range partBodies {
		pid := uuid.New()
		pos := i + 1
		if err := store.Put(context.Background(), brain.Object{
			ID: pid, Kind: "Chunk", Title: title + " part", Content: body,
			ParentID: &parentID, Position: &pos,
			NamespaceID: ns, UpdatedAt: updated, CreatedAt: updated,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return parentID
}

func fixedNow(t time.Time) brain.EngineConfig {
	return brain.EngineConfig{Now: func() time.Time { return t }}
}

func TestSearch_promotesParentWithEvidenceAndNamespace(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	nsA, nsB := uuid.New(), uuid.New()
	now := time.Now().UTC()
	parent := seedDocWithParts(t, store, nsA, "OAuth guide", []string{
		"Implement OAuth PKCE for mobile clients with authorization code flow",
	}, now)
	seedDocWithParts(t, store, nsB, "Other ns", []string{"OAuth PKCE elsewhere"}, now)

	eng, err := brain.NewEngine(store, brain.WithObserver(telemetry.NewBrainObserver()), brain.WithConfig(fixedNow(now)))
	if err != nil {
		t.Fatal(err)
	}
	sc := brain.NewSearchContext()

	page, err := eng.Search(ctx, brain.Scope{Namespace: &nsA}, brain.SearchRequest{
		Query: "oauth pkce implementation",
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != parent {
		t.Fatalf("want promoted parent in nsA, got %+v", page.Objects)
	}
	if page.Objects[0].Content != "" {
		t.Fatal("search hits omit full content")
	}
	if len(page.Objects[0].Evidence) == 0 {
		t.Fatal("expected evidence")
	}
	if page.ResultSetID == uuid.Nil {
		t.Fatal("result_set_id required")
	}

	empty, err := eng.Search(ctx, brain.Scope{Namespace: &nsB}, brain.SearchRequest{
		Query: "oauth pkce implementation",
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	// nsB has its own hit; ensure nsA parent not returned under wrong scope was already covered.
	// Re-search nsA isolation: object only in nsA must not appear when scoped to empty other.
	_ = empty
	wrong, err := eng.Search(ctx, brain.Scope{Namespace: &nsB}, brain.SearchRequest{
		Query: "authorization code flow mobile",
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range wrong.Objects {
		if o.ID == parent {
			t.Fatal("parent from nsA must not appear under nsB")
		}
	}

	miss, err := eng.Search(ctx, brain.Scope{Namespace: &nsA}, brain.SearchRequest{
		Query: "quantum-chromodynamics-unrelated",
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(miss.Objects) != 0 {
		t.Fatalf("unrelated query must miss, got %+v", miss.Objects)
	}
}

func TestSearch_filtersProperty(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	seedDocWithParts(t, store, ns, "Deal A", []string{"pipeline revenue forecast"}, now)

	p2 := uuid.New()
	part := uuid.New()
	pos := 1
	if err := store.Put(context.Background(), brain.Object{
		ID: p2, Kind: "Document", Title: "Deal B", NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), brain.Object{
		ID: part, Kind: "Chunk", Title: "chunk", Content: "pipeline revenue forecast",
		ParentID: &p2, Position: &pos, NamespaceID: ns, UpdatedAt: now,
		Properties: map[string]any{"stage": "negotiation"},
	}); err != nil {
		t.Fatal(err)
	}

	eng, err := brain.NewEngine(store, brain.WithConfig(fixedNow(now)))
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query:   "pipeline revenue",
		Filters: brain.Filters{"stage": "negotiation"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != p2 {
		t.Fatalf("filter should keep p2 only, got %+v", page.Objects)
	}
}

func TestSearch_hybridWithStubEmbedder(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()

	lexParent, vecParent := uuid.New(), uuid.New()
	lexPart, vecPart := uuid.New(), uuid.New()
	pos := 1
	for _, o := range []brain.Object{
		{ID: lexParent, Kind: "Document", Title: "Lexical", NamespaceID: ns, UpdatedAt: now},
		{ID: vecParent, Kind: "Document", Title: "Vector", NamespaceID: ns, UpdatedAt: now},
		{
			ID: lexPart, Kind: "Chunk", Content: "banana banana banana unique-token-xyz",
			ParentID: &lexParent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
			Embedding: []float32{0, 1},
		},
		{
			ID: vecPart, Kind: "Chunk", Content: "unrelated body text",
			ParentID: &vecParent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
			Embedding: []float32{1, 0},
		},
	} {
		if err := store.Put(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}

	eng, err := brain.NewEngine(store,
		brain.WithEmbedder(stubEmbedder{v: []float32{1, 0}}),
		brain.WithConfig(fixedNow(now)),
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: "unique-token-xyz",
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	foundVec := false
	for _, o := range page.Objects {
		if o.ID == vecParent {
			foundVec = true
		}
	}
	if !foundVec {
		t.Fatalf("vector parent missing: %+v", page.Objects)
	}
}

type stubEmbedder struct{ v []float32 }

func (s stubEmbedder) Embed(context.Context, string) ([]float32, error) { return s.v, nil }

// TestSearch_scopeIDsRestrictsHits: corpus search can be limited to a parent neighborhood.
func TestSearch_scopeIDsRestrictsHits(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	p1, p2 := uuid.New(), uuid.New()
	c1, c2 := uuid.New(), uuid.New()
	pos := 1
	_ = store.Put(ctx, brain.Object{ID: p1, Kind: "Document", Title: "Deal A", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{ID: p2, Kind: "Document", Title: "Deal B", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(ctx, brain.Object{
		ID: c1, Kind: "Chunk", Title: "oauth risk", Content: "oauth risk material shared",
		ParentID: &p1, Position: &pos, NamespaceID: ns, UpdatedAt: now,
	})
	_ = store.Put(ctx, brain.Object{
		ID: c2, Kind: "Chunk", Title: "oauth other", Content: "oauth risk material shared",
		ParentID: &p2, Position: &pos, NamespaceID: ns, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{Now: func() time.Time { return now }}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: &ns}
	sc := brain.NewSearchContext()
	page, err := eng.Search(ctx, scope, brain.SearchRequest{
		Query: "oauth risk material", ScopeIDs: []uuid.UUID{p1},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != p1 {
		t.Fatalf("scoped search: %+v", page.Objects)
	}
}

func TestFindExact_uuidParentAndPart(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	part := uuid.New()
	pos := 1
	if err := store.Put(context.Background(), brain.Object{
		ID: parent, Kind: "Document", Title: "Direct", NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), brain.Object{
		ID: part, Kind: "Chunk", Title: "child", Content: "body",
		ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(fixedNow(now)))
	if err != nil {
		t.Fatal(err)
	}
	sc := brain.NewSearchContext()

	page, err := eng.FindExact(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: parent.String(),
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != parent || len(page.Objects[0].Evidence) != 0 {
		t.Fatalf("parent uuid: %+v", page.Objects)
	}

	page2, err := eng.FindExact(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: part.String(),
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Objects) != 1 || page2.Objects[0].ID != parent {
		t.Fatalf("part uuid promotes: %+v", page2.Objects)
	}
	if len(page2.Objects[0].Evidence) != 1 || page2.Objects[0].Evidence[0].PartID != part {
		t.Fatalf("evidence: %+v", page2.Objects[0].Evidence)
	}
}

func TestFindExact_titleMatch(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	part := uuid.New()
	pos := 1
	if err := store.Put(context.Background(), brain.Object{
		ID: parent, Kind: "Document", Title: "Parent", NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), brain.Object{
		ID: part, Kind: "Chunk", Title: "exact-filename.go", Content: "package main",
		ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(fixedNow(now)))
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.FindExact(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: "exact-filename.go",
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != parent {
		t.Fatalf("title exact: %+v", page.Objects)
	}
}

func TestContinue_andReplaceResultSet(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		seedDocWithParts(t, store, ns, "Topic shared", []string{
			"shared retrieval content alpha beta gamma " + string(rune('a'+i)),
		}, now)
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{
		DefaultLimit: 2, MaxLimit: 50,
		Now: func() time.Time { return now },
	}))
	if err != nil {
		t.Fatal(err)
	}
	sc := brain.NewSearchContext()
	page1, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: "shared retrieval content", Limit: 2,
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if !page1.HasMore || len(page1.Objects) != 2 {
		t.Fatalf("page1: has_more=%v n=%d", page1.HasMore, len(page1.Objects))
	}
	oldID := page1.ResultSetID

	page2, err := eng.Continue(ctx, brain.Scope{Namespace: &ns}, oldID, 2, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Objects) == 0 {
		t.Fatal("page2 empty")
	}
	seen := map[uuid.UUID]struct{}{}
	for _, o := range page1.Objects {
		seen[o.ID] = struct{}{}
	}
	for _, o := range page2.Objects {
		if _, ok := seen[o.ID]; ok {
			t.Fatalf("overlap on %s", o.ID)
		}
	}

	// Export/restore preserves offset for a further continue.
	raw, err := sc.Export()
	if err != nil {
		t.Fatal(err)
	}
	sc2 := brain.NewSearchContext()
	if err := sc2.Restore(raw); err != nil {
		t.Fatal(err)
	}
	page3, err := eng.Continue(ctx, brain.Scope{Namespace: &ns}, oldID, 2, sc2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3.Objects) == 0 && page2.HasMore {
		t.Fatal("continue after restore failed while has_more")
	}

	// New search invalidates prior result set.
	if _, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: "shared retrieval content",
	}, sc); err != nil {
		t.Fatal(err)
	}
	_, err = eng.Continue(ctx, brain.Scope{Namespace: &ns}, oldID, 2, sc)
	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("want not found after replace, got %v", err)
	}
}

func TestSearch_rejectsBadFiltersAndEmptyQuery(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	sc := brain.NewSearchContext()
	ctx := context.Background()

	_, err = eng.Search(ctx, brain.Scope{}, brain.SearchRequest{Query: "  "}, sc)
	if err == nil {
		t.Fatal("empty query must fail")
	}
	_, err = eng.Search(ctx, brain.Scope{}, brain.SearchRequest{
		Query: "q", Filters: brain.Filters{"updated_after": "not-a-date"},
	}, sc)
	if err == nil {
		t.Fatal("bad date filter must fail")
	}
	_, err = eng.Search(ctx, brain.Scope{}, brain.SearchRequest{
		Query: "q", Filters: brain.Filters{"": "x"},
	}, sc)
	if err == nil {
		t.Fatal("empty filter key must fail")
	}
}

func TestFindExact_trigramFuzzy(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	part := uuid.New()
	pos := 1
	if err := store.Put(context.Background(), brain.Object{
		ID: parent, Kind: "Document", Title: "P", NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	// No exact title match and no full substring of the query — only trigram overlap.
	if err := store.Put(context.Background(), brain.Object{
		ID: part, Kind: "Chunk", Title: "chunk-title",
		Content:  "abcdefghijklmnopqrstuvwxyz",
		ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(fixedNow(now)))
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.FindExact(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: "xyzabc", // shares xyz/abc trigrams; not a contiguous substring of content
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != parent {
		t.Fatalf("trigram fuzzy: %+v", page.Objects)
	}
}

func TestSearchContext_restoreInvalid(t *testing.T) {
	sc := brain.NewSearchContext()
	if err := sc.Restore([]byte(`{not json`)); err == nil {
		t.Fatal("want restore error")
	}
	if err := sc.Restore(nil); err != nil {
		t.Fatal(err)
	}
}
