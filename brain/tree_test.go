package brain_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// countingEmbedder records every text it embeds.
type countingEmbedder struct {
	mu     sync.Mutex
	texts  int
	short  bool   // return one vector too few
	failOn string // fail a batch with a text containing this
}

func (e *countingEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.texts += len(texts)
	if e.failOn != "" && slices.ContainsFunc(texts, func(s string) bool { return strings.Contains(s, e.failOn) }) {
		return nil, errors.New("embedding API down")
	}
	n := len(texts)
	if e.short {
		n--
	}
	return repeatVector([]float32{1, 0}, n), nil
}

func (*countingEmbedder) Model() string { return "counting-model" }

func (e *countingEmbedder) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.texts
}

func (e *countingEmbedder) take() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.texts
	e.texts = 0
	return n
}

func TestEngine_putTreeHoldsAFewBatchesWhateverTheTreeSizeAndReusesUnchangedParts(t *testing.T) {
	ctx := t.Context()
	emb := &countingEmbedder{}
	const batch, slots, total = 4, 2, 1000
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb, BatchSize: batch, Concurrency: slots}))
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	var held int
	parts := func(yield func(brain.Object, error) bool) {
		for i := range total {
			// Every part is new, so a part read but not yet embedded is held in memory.
			held = max(held, i-emb.count())
			if !yield(brain.Object{Kind: "Chunk", Content: fmt.Sprintf("part %d", i)}, nil) {
				return
			}
		}
	}

	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "big"}, parts)
	firstEmbeds, firstHeld := emb.take(), held
	if err != nil {
		t.Fatal(err)
	}
	_, againErr := eng.PutTree(ctx, scope, doc, parts)
	againEmbeds := emb.take()
	kidsTree, _ := store.GetTree(ctx, scope, doc.ID, 0)
	kids := kidsTree.Parts

	// The reader runs at most one batch per slot, plus the one it is filling, ahead of embedding.
	if firstHeld > batch*(slots+1) {
		t.Errorf("parts held before embedding = %d, want at most %d", firstHeld, batch*(slots+1))
	}
	if firstEmbeds != total+1 {
		t.Errorf("first write embedded %d texts, want every part and the parent (%d)", firstEmbeds, total+1)
	}
	if againErr != nil || againEmbeds != 1 {
		t.Errorf("unchanged rewrite = %v, embedded %d texts, want only the parent", againErr, againEmbeds)
	}
	if len(kids) != total || kids[0].Content != "part 0" || kids[total-1].Content != fmt.Sprintf("part %d", total-1) {
		t.Errorf("children = %d, want %d in order", len(kids), total)
	}
}

func TestEngine_putTreeFailsOnABadPartOrSourceAndKeepsThePreviousTree(t *testing.T) {
	ctx := t.Context()
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	failing := func(yield func(brain.Object, error) bool) {
		if yield(brain.Object{Kind: "Chunk", Content: "first"}, nil) {
			yield(brain.Object{}, errors.New("disk read failed"))
		}
	}
	catalog := brain.WithKinds(brain.KindSpec{Kind: "Document", IsParent: true}, brain.KindSpec{Kind: "Chunk", IsPart: true})

	for name, tc := range map[string]struct {
		fault func(*countingEmbedder) // breaks the embedder after the first tree
		opts  []brain.EngineOption
		parts iter.Seq2[brain.Object, error]
		want  string
	}{
		"a part with an id":            {nil, nil, brain.Parts(brain.Object{ID: uuid.New(), Kind: "Chunk", Content: "x"}), "part ids come from content"},
		"a part in another namespace":  {nil, nil, brain.Parts(brain.Object{Kind: "Chunk", Content: "x", Namespace: mustNS(t, "org", "b")}), "is not the parent's"},
		"a soft-deleted part":          {nil, nil, brain.Parts(brain.Object{Kind: "Chunk", Content: "x", DeletedAt: new(time.Now())}), "soft-deleted"},
		"a source read error":          {nil, nil, failing, "disk read failed"},
		"an embedder short a vector":   {func(e *countingEmbedder) { e.short = true }, nil, brain.Parts(brain.Object{Kind: "Chunk", Content: "x"}), "returned 0 vectors for 1 texts"},
		"a part the embedder fails on": {func(e *countingEmbedder) { e.failOn = "doomed" }, nil, brain.Parts(brain.Object{Kind: "Chunk", Content: "doomed"}), "embedding API down"},
		"a part of an unknown kind":    {nil, []brain.EngineOption{catalog}, brain.Parts(brain.Object{Kind: "Unknown", Content: "x"}), "Unknown"},
	} {
		emb := &countingEmbedder{}
		store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
		eng, err := brain.NewEngine(store, tc.opts...)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "v1"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "kept"}))
		if err != nil {
			t.Fatal(err)
		}
		if tc.fault != nil {
			emb.mu.Lock()
			tc.fault(emb)
			emb.mu.Unlock()
		}
		renamed := doc
		renamed.Title = "v2"

		_, err = eng.PutTree(ctx, scope, renamed, tc.parts)
		kidsTree, _ := store.GetTree(ctx, scope, doc.ID, 0)
		kids := kidsTree.Parts
		parent, _ := store.Get(ctx, scope, doc.ID)

		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: put tree = %v, want %q", name, err, tc.want)
		}
		if len(kids) != 1 || kids[0].Content != "kept" || parent.Title != "v1" {
			t.Errorf("%s: parent %q, parts %v, want the previous tree", name, parent.Title, kids)
		}
	}
}

func TestEngine_putTreeWithNoPartsEmptiesTheTree(t *testing.T) {
	ctx := t.Context()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "doc"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "old"}))
	if err != nil {
		t.Fatal(err)
	}

	_, err = eng.PutTree(ctx, scope, doc, nil)
	kidsTree, _ := store.GetTree(ctx, scope, doc.ID, 0)
	kids := kidsTree.Parts

	if err != nil || len(kids) != 0 {
		t.Errorf("put tree with no parts = %v, parts %v, want none", err, kids)
	}
}
