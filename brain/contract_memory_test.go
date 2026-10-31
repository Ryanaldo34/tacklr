package brain_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

func TestMemoryStore_contract(t *testing.T) {
	storetest.Run(t, func(_ *testing.T, emb brain.Embedder) brain.Store {
		return brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
	})
}
