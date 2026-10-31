package brain

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"slices"
	"sync"

	"github.com/google/uuid"
)

// Embedder turns text into vectors for a backend's semantic channel. Each
// backend that embeds owns its Embedder.
type Embedder interface {
	// Embed returns one vector per text, in order.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	// Model names the model and version (for example
	// "text-embedding-3-small"). Backends store it with every vector and
	// compare only vectors from the same model.
	Model() string
}

// Embedding is how a backend embeds: its Embedder and how many texts it
// sends at once. A nil Embedder embeds nothing, so the backend ranks text
// alone. Zero BatchSize is 128 and zero Concurrency is 2.
type Embedding struct {
	Embedder    Embedder
	BatchSize   int
	Concurrency int
}

// Enabled reports whether e has an Embedder.
func (e Embedding) Enabled() bool { return e.Embedder != nil }

// Model is the Embedder's model, or "" without one.
func (e Embedding) Model() string {
	if e.Embedder == nil {
		return ""
	}
	return e.Embedder.Model()
}

// Embed returns one vector per text, cloned so the Embedder cannot change
// stored vectors later. Without an Embedder it returns nil.
func (e Embedding) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if e.Embedder == nil || len(texts) == 0 {
		return nil, nil
	}
	vecs, err := e.Embedder.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(vecs) != len(texts) {
		return nil, fmt.Errorf("embedder returned %d vectors for %d texts", len(vecs), len(texts))
	}
	for i, v := range vecs {
		vecs[i] = slices.Clone(v)
	}
	return vecs, nil
}

// EmbedOne embeds text, or returns nil without an Embedder or text.
func (e Embedding) EmbedOne(ctx context.Context, text string) ([]float32, error) {
	if text == "" {
		return nil, nil
	}
	vecs, err := e.Embed(ctx, []string{text})
	if err != nil || vecs == nil {
		return nil, err
	}
	return vecs[0], nil
}

// TreePart is one part of a tree write as EmbedBatches hands it to a
// backend. A Reused part is live already: only its ID, ParentID, Namespace,
// Position and Properties are set. A new part is the whole object, with its
// embedding when the backend embeds.
type TreePart struct {
	Object Object
	Reused bool
}

// EmbedBatches is the tree-write pipeline a backend can use for
// Store.PutTree. It reads parts in order, keeps a part that reuse reports
// live as a reference, embeds the new parts' Text in batches of BatchSize
// with at most Concurrency batches at once, and passes each batch to stage
// as it finishes. Memory therefore holds a few batches, whatever the number
// of parts. It returns how many parts it read. The first error (from parts,
// embedding or stage) cancels the batches in flight and is returned once
// they stop.
func (e Embedding) EmbedBatches(ctx context.Context, parts iter.Seq2[Indexed, error], reuse func(uuid.UUID) bool, stage func(context.Context, []TreePart) error) (int, error) {
	size, slots := cmp.Or(e.BatchSize, 128), make(chan struct{}, cmp.Or(e.Concurrency, 2))
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	flush := func(batch []TreePart, texts []string) {
		// A full set of slots blocks the reader: backpressure. A cancelled
		// batch still returns its slot, so this never waits forever.
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			if ctx.Err() != nil { // an earlier batch failed
				return
			}
			if err := e.embedBatch(ctx, batch, texts); err != nil {
				cancel(fmt.Errorf("brain: embed parts: %w", err))
				return
			}
			if err := stage(ctx, batch); err != nil {
				cancel(err)
			}
		})
	}
	var batch []TreePart
	var texts []string
	n := 0
	for p, err := range parts {
		if err != nil {
			cancel(err)
			break
		}
		if ctx.Err() != nil {
			break
		}
		n++
		if reuse(p.ID) {
			batch = append(batch, TreePart{Reused: true, Object: Object{
				ID: p.ID, ParentID: p.ParentID, Namespace: p.Namespace,
				Position: p.Position, Properties: p.Properties,
			}})
			texts = append(texts, "")
		} else {
			batch = append(batch, TreePart{Object: p.Object})
			texts = append(texts, p.Text)
		}
		if len(batch) == size {
			flush(batch, texts)
			batch, texts = nil, nil
		}
	}
	if len(batch) > 0 && ctx.Err() == nil {
		flush(batch, texts)
	}
	wg.Wait()
	return n, context.Cause(ctx)
}

// embedBatch embeds the batch's new parts in one Embed call. texts[i] is
// part i's text; empty means the part is reused or has no text.
func (e Embedding) embedBatch(ctx context.Context, batch []TreePart, texts []string) error {
	var idx []int
	var in []string
	for i, t := range texts {
		if t != "" && e.Embedder != nil {
			idx = append(idx, i)
			in = append(in, t)
		}
	}
	vecs, err := e.Embed(ctx, in)
	if err != nil {
		return err
	}
	for j, i := range idx {
		batch[i].Object.Embedding, batch[i].Object.EmbeddingModel = vecs[j], e.Embedder.Model()
	}
	return nil
}
