package brain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestEngine_aWriteCancelledWhileEmbeddingStoresNothing: when the caller's
// context ends after the first embed, a session save and a tree write return
// Canceled and leave nothing to find.
func TestEngine_aWriteCancelledWhileEmbeddingStoresNothing(t *testing.T) {
	t.Parallel()
	scope := brain.Scope{Namespace: storetest.Namespace(t, "org", "a"), SessionID: "s1"}
	msgs := []brain.SessionMessage{{Role: "user", SearchText: "one"}, {Role: "user", SearchText: "two"}, {Role: "user", SearchText: "three"}}
	parts := []brain.Object{{Kind: "Chunk", Content: "one"}, {Kind: "Chunk", Content: "two"}, {Kind: "Chunk", Content: "three"}}
	run := func(write func(context.Context, *brain.Engine) error) (int, error) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		emb := embedFunc(func(_ context.Context, texts []string) ([][]float32, error) {
			cancel()
			return repeatVector([]float32{1, 0}, len(texts)), nil
		})
		eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb, BatchSize: 1})))
		if err != nil {
			t.Fatal(err)
		}
		err = write(ctx, eng)
		page, searchErr := eng.Search(context.Background(), scope, brain.SearchRequest{Query: "one two three"})
		if searchErr != nil {
			t.Fatal(searchErr)
		}
		return len(page.Results), err
	}

	saved, saveErr := run(func(ctx context.Context, eng *brain.Engine) error {
		_, err := eng.SaveSessionMessages(ctx, scope.Namespace, "s1", "k", msgs)
		return err
	})
	stored, treeErr := run(func(ctx context.Context, eng *brain.Engine) error {
		_, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "doc"}, brain.Parts(parts...))
		return err
	})

	if !errors.Is(saveErr, context.Canceled) || saved != 0 {
		t.Errorf("session save = %v leaving %d results, want Canceled and none", saveErr, saved)
	}
	if !errors.Is(treeErr, context.Canceled) || stored != 0 {
		t.Errorf("put tree = %v leaving %d results, want Canceled and none", treeErr, stored)
	}
}
