package brain_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestMemoryStore_contract: MemoryStore meets the store contract every brain
// store must meet.
func TestMemoryStore_contract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(_ *testing.T, emb brain.Embedder) brain.Store {
		return brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
	})
}
