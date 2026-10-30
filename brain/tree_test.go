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
	store := brain.NewMemoryStore()
	emb := &countingEmbedder{}
	const batch, slots, total = 4, 2, 1000
	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithConfig(brain.EngineConfig{EmbedBatchSize: batch, EmbedConcurrency: slots}))
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
	kids, _ := store.ListChildren(ctx, scope, doc.ID)

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
	store := brain.NewMemoryStore()
	emb := &countingEmbedder{}
	eng, err := brain.NewEngine(store, brain.WithEmbedder(emb))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "v1"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "kept"}))
	if err != nil {
		t.Fatal(err)
	}
	failing := func(yield func(brain.Object, error) bool) {
		if yield(brain.Object{Kind: "Chunk", Content: "first"}, nil) {
			yield(brain.Object{}, errors.New("disk read failed"))
		}
	}
	catalogEng, err := brain.NewEngine(store, brain.WithEmbedder(emb), brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true}, brain.KindSpec{Kind: "Chunk", IsPart: true}))
	if err != nil {
		t.Fatal(err)
	}
	short := &countingEmbedder{short: true}
	down, err := brain.NewEngine(store, brain.WithEmbedder(&countingEmbedder{failOn: "doomed"}))
	if err != nil {
		t.Fatal(err)
	}
	shortEng, err := brain.NewEngine(store, brain.WithEmbedder(short))
	if err != nil {
		t.Fatal(err)
	}

	for name, tc := range map[string]struct {
		eng   *brain.Engine
		parts iter.Seq2[brain.Object, error]
		want  string
	}{
		"a part with an id":            {eng, brain.Parts(brain.Object{ID: uuid.New(), Kind: "Chunk", Content: "x"}), "part ids come from content"},
		"a part in another namespace":  {eng, brain.Parts(brain.Object{Kind: "Chunk", Content: "x", Namespace: mustNS(t, "org", "b")}), "is not the parent's"},
		"a soft-deleted part":          {eng, brain.Parts(brain.Object{Kind: "Chunk", Content: "x", DeletedAt: new(doc.UpdatedAt)}), "soft-deleted"},
		"a source read error":          {eng, failing, "disk read failed"},
		"an embedder short a vector":   {shortEng, brain.Parts(brain.Object{Kind: "Chunk", Content: "x"}), "returned 0 vectors for 1 texts"},
		"a part the embedder fails on": {down, brain.Parts(brain.Object{Kind: "Chunk", Content: "doomed"}), "embedding API down"},
		"a part of an unknown kind":    {catalogEng, brain.Parts(brain.Object{Kind: "Unknown", Content: "x"}), "Unknown"},
	} {
		renamed := doc
		renamed.Title = "v2"
		_, err := tc.eng.PutTree(ctx, scope, renamed, tc.parts)
		kids, _ := store.ListChildren(ctx, scope, doc.ID)
		parent, _ := store.Get(ctx, scope, doc.ID)

		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: put tree = %v, want %q", name, err, tc.want)
		}
		if len(kids) != 1 || kids[0].Content != "kept" || parent.Title != "v1" {
			t.Errorf("%s: parent %q, parts %v, want the previous tree", name, parent.Title, kids)
		}
	}
}

// failEnsure is a graph whose node writes fail.
type failEnsure struct{ *brain.MemoryGraph }

func (failEnsure) EnsureObject(context.Context, brain.Object, string) error {
	return errors.New("graph down")
}

func TestEngine_putTreeWithNoPartsEmptiesTheTreeAndReportsAGraphFailureAfterTheStoreWrite(t *testing.T) {
	ctx := t.Context()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithLexicalOnly(), brain.WithGraph(failEnsure{brain.NewMemoryGraph()}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", "a")}
	doc := brain.Object{ID: uuid.New(), Kind: "Document", Title: "doc"}
	_, first := eng.PutTree(ctx, scope, doc, brain.Parts(brain.Object{Kind: "Chunk", Content: "old"}))

	_, err = eng.PutTree(ctx, scope, doc, nil)
	kids, _ := store.ListChildren(ctx, scope, doc.ID)

	if !errors.Is(first, brain.ErrGraphEnsure) || !errors.Is(err, brain.ErrGraphEnsure) {
		t.Errorf("put tree with a failing graph = %v, %v, want ErrGraphEnsure", first, err)
	}
	// The store is the source of truth: the tree is written before the graph.
	if len(kids) != 0 {
		t.Errorf("parts after a put with no parts = %v, want none", kids)
	}
}
