package brain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestEngine_cancelledWritesStopEmbeddingTheRest: when the caller's context
// ends during the first embed, a session save and a tree write return
// Canceled and embed nothing more.
func TestEngine_cancelledWritesStopEmbeddingTheRest(t *testing.T) {
	t.Parallel()
	scope := brain.Scope{Namespace: storetest.Namespace(t, "org", "a")}
	msgs := []brain.SessionMessage{{Role: "user", SearchText: "one"}, {Role: "user", SearchText: "two"}, {Role: "user", SearchText: "three"}}
	parts := []brain.Object{{Kind: "Chunk", Content: "one"}, {Kind: "Chunk", Content: "two"}, {Kind: "Chunk", Content: "three"}}
	run := func(write func(context.Context, *brain.Engine) error) (int, error) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		// The first embed succeeds and the caller gives up just after it.
		emb := embedFunc(func(_ context.Context, texts []string) ([][]float32, error) {
			calls++
			cancel()
			return repeatVector([]float32{1, 0}, len(texts)), nil
		})
		eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb, BatchSize: 1})))
		if err != nil {
			t.Fatal(err)
		}
		err = write(ctx, eng)
		return calls, err
	}

	saveCalls, saveErr := run(func(ctx context.Context, eng *brain.Engine) error {
		_, err := eng.SaveSessionMessages(ctx, scope.Namespace, "s1", "k", msgs)
		return err
	})
	treeCalls, treeErr := run(func(ctx context.Context, eng *brain.Engine) error {
		_, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "doc"}, brain.Parts(parts...))
		return err
	})

	if !errors.Is(saveErr, context.Canceled) || saveCalls != 1 {
		t.Errorf("session save = %v after %d embeds, want Canceled after 1", saveErr, saveCalls)
	}
	if !errors.Is(treeErr, context.Canceled) || treeCalls != 1 {
		t.Errorf("put tree = %v after %d embeds, want Canceled after 1", treeErr, treeCalls)
	}
}
