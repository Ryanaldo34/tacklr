package brain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

// cancelEmbedder cancels the caller's ctx on its first call and counts calls.
// The first call succeeds, as an embed that returns just before cancel does.
type cancelEmbedder struct {
	cancel context.CancelFunc
	calls  *int
}

func (e cancelEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	*e.calls++
	e.cancel()
	return repeatVector([]float32{1, 0}, len(texts)), nil
}

func (cancelEmbedder) Model() string { return "cancel-model" }

// repeatVector returns n copies of v, one per embedded text.
func repeatVector(v []float32, n int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func TestEngine_cancelledWritesStopEmbeddingTheRest(t *testing.T) {
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	msgs := []brain.SessionMessage{{Role: "user", SearchText: "one"}, {Role: "user", SearchText: "two"}, {Role: "user", SearchText: "three"}}
	parts := []brain.Object{{Kind: "Chunk", Content: "one"}, {Kind: "Chunk", Content: "two"}, {Kind: "Chunk", Content: "three"}}
	run := func(write func(context.Context, *brain.Engine) error) (int, error) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: cancelEmbedder{cancel: cancel, calls: &calls}, BatchSize: 1})))
		if err != nil {
			t.Fatal(err)
		}
		return calls, write(ctx, eng)
	}

	saveCalls, saveErr := run(func(ctx context.Context, eng *brain.Engine) error {
		_, err := eng.SaveSessionMessages(ctx, scope.Namespace, "s1", "k", msgs)
		return err
	})
	treeCalls, treeErr := run(func(ctx context.Context, eng *brain.Engine) error {
		_, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "doc"}, brain.Parts(parts...))
		return err
	})

	// The first embed cancels; nothing after it is embedded or saved.
	if !errors.Is(saveErr, context.Canceled) || saveCalls != 1 {
		t.Errorf("session save = %v after %d embeds, want Canceled after 1", saveErr, saveCalls)
	}
	if !errors.Is(treeErr, context.Canceled) || treeCalls != 1 {
		t.Errorf("put tree = %v after %d embeds, want Canceled after 1", treeErr, treeCalls)
	}
}
