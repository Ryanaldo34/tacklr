package brain_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestEval_goldenSearchQueries(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	now := time.Now().UTC()
	store := brain.NewMemoryStore()

	for _, d := range []struct {
		title string
		body  string
	}{
		{"OAuth PKCE Guide", "Implement OAuth 2.0 PKCE for mobile clients"},
		{"Billing Runbook", "Invoice reconciliation and chargeback flow"},
		{"Vector Search Notes", "HNSW cosine similarity embeddings for RAG"},
	} {
		pid := uuid.New()
		pos := 1
		_ = store.Put(brain.Object{ID: pid, Kind: "Document", Title: d.title, NamespaceID: ns, UpdatedAt: now})
		_ = store.Put(brain.Object{
			ID: uuid.New(), Kind: "Chunk", Content: d.body, ParentID: &pid, Position: &pos,
			NamespaceID: ns, UpdatedAt: now,
		})
	}

	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{
		Now: func() time.Time { return now },
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		query, wantTitle string
	}{
		{"oauth pkce mobile", "OAuth PKCE Guide"},
		{"invoice chargeback", "Billing Runbook"},
		{"hnsw embeddings rag", "Vector Search Notes"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			page, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
				Query: tc.query, Limit: 5,
			}, brain.NewSearchContext())
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range page.Objects {
				if o.Title == tc.wantTitle {
					return
				}
			}
			t.Fatalf("want title %q in %+v", tc.wantTitle, page.Objects)
		})
	}
}

func TestEval_degradeEmbedderKeepsLexical(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent, part := uuid.New(), uuid.New()
	pos := 1
	_ = store.Put(brain.Object{ID: parent, Kind: "Document", Title: "Widgets", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(brain.Object{
		ID: part, Kind: "Chunk", Content: "unique-widget-token alpha", ParentID: &parent, Position: &pos,
		NamespaceID: ns, UpdatedAt: now, Embedding: []float32{1, 0},
	})
	eng, err := brain.NewEngine(store,
		brain.WithEmbedder(failEmbedder{}),
		brain.WithConfig(brain.EngineConfig{Now: func() time.Time { return now }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
		Query: "unique-widget-token",
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != parent {
		t.Fatalf("%+v", page.Objects)
	}
}

func TestEval_degradeGraphKeepsContainment(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent, child := uuid.New(), uuid.New()
	pos := 1
	_ = store.Put(brain.Object{ID: parent, Kind: "Document", NamespaceID: ns, UpdatedAt: now})
	_ = store.Put(brain.Object{
		ID: child, Kind: "Chunk", Content: "c", ParentID: &parent, Position: &pos,
		NamespaceID: ns, UpdatedAt: now,
	})
	eng, err := brain.NewEngine(store, brain.WithGraph(failGraph{}))
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{
		ObjectID: parent, RelationTypes: []string{"contains", "references"},
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "children" || len(res.Objects) != 1 || res.Objects[0].ID != child {
		t.Fatalf("%+v", res)
	}
}

func TestConcurrent_searchAndExpand(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	parent := uuid.New()
	_ = store.Put(brain.Object{ID: parent, Kind: "Document", Title: "Doc", NamespaceID: ns, UpdatedAt: now})
	for i := 1; i <= 8; i++ {
		id := uuid.New()
		pos := i
		_ = store.Put(brain.Object{
			ID: id, Kind: "Chunk", Content: "concurrent retrieval token",
			ParentID: &parent, Position: &pos, NamespaceID: ns, UpdatedAt: now,
		})
	}
	eng, err := brain.NewEngine(store, brain.WithConfig(brain.EngineConfig{
		Now: func() time.Time { return now },
	}))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sc := brain.NewSearchContext()
			if _, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{
				Query: "concurrent retrieval",
			}, sc); err != nil {
				errCh <- err
				return
			}
			if _, err := eng.Expand(ctx, brain.Scope{Namespace: &ns}, brain.ExpandRequest{
				ObjectID: parent,
			}, sc); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

type failEmbedder struct{}

func (failEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, errString("embed down")
}

type failGraph struct{}

func (failGraph) Neighbors(context.Context, uuid.UUID, []string, int) ([]brain.GraphNeighbor, error) {
	return nil, errString("graph down")
}

type errString string

func (e errString) Error() string { return string(e) }
