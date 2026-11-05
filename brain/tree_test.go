package brain_test

import (
	"cmp"
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
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestEngine_putTreeHoldsAFewBatchesWhateverTheTreeSize: a tree of 1000 new
// parts is read at most one batch per embedding slot, plus the one it fills,
// ahead of the embedder, and every part is embedded and stored in order.
func TestEngine_putTreeHoldsAFewBatchesWhateverTheTreeSize(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	emb := &storetest.Embedder{}
	const batch, slots, total = 4, 2, 1000
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb, BatchSize: batch, Concurrency: slots}))
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	var held int
	parts := func(yield func(brain.Object, error) bool) {
		for i := range total {
			// Every part is new, so a part read but not yet embedded is held in memory.
			held = max(held, i-emb.Texts())
			if !yield(brain.Object{Kind: "Chunk", Content: fmt.Sprintf("part %d", i)}, nil) {
				return
			}
		}
	}

	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "big"}, parts)
	kidsTree, treeErr := store.GetTree(ctx, scope, doc.ID, 0)
	kids := kidsTree.Parts

	if err != nil || treeErr != nil {
		t.Fatalf("put tree = %v, get tree = %v", err, treeErr)
	}
	if held > batch*(slots+1) {
		t.Errorf("parts held before embedding = %d, want at most %d", held, batch*(slots+1))
	}
	if emb.Texts() != total+1 {
		t.Errorf("embedded %d texts, want every part and the parent (%d)", emb.Texts(), total+1)
	}
	if len(kids) != total || kids[0].Content != "part 0" || kids[total-1].Content != fmt.Sprintf("part %d", total-1) {
		t.Errorf("children = %d, want %d in order", len(kids), total)
	}
}

// TestEngine_putTreeFailsOnABadPartOrSourceAndKeepsThePreviousTree: a bad
// parent or part, a failed read of the parts, or an embedder that fails or
// answers short fails the tree write and leaves the previous tree as it was.
func TestEngine_putTreeFailsOnABadPartOrSourceAndKeepsThePreviousTree(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	scope := brain.Scope{Namespace: storetest.Namespace(t, "org", "a")}
	failing := func(yield func(brain.Object, error) bool) {
		if yield(brain.Object{Kind: "Chunk", Content: "first"}, nil) {
			yield(brain.Object{}, errors.New("disk read failed"))
		}
	}
	catalog := brain.WithKinds(brain.KindSpec{Kind: "Document", IsParent: true}, brain.KindSpec{Kind: "Chunk", IsPart: true})

	// The provider fails a batch with the word poison and answers a batch with
	// the word short one vector too few.
	emb := embedFunc(func(_ context.Context, texts []string) ([][]float32, error) {
		n := len(texts)
		for _, text := range texts {
			if strings.Contains(text, "poison") {
				return nil, errors.New("embedding API down")
			}
			if strings.Contains(text, "short") {
				n--
			}
		}
		return repeatVector([]float32{1, 0}, n), nil
	})

	for name, tc := range map[string]struct {
		opts  []brain.EngineOption
		title string // the new parent title
		parts iter.Seq2[brain.Object, error]
		want  string
		kind  string // the new parent kind; empty keeps it
	}{
		"a part with an id":                   {nil, "v2", brain.Parts(brain.Object{ID: uuid.New(), Kind: "Chunk", Content: "x"}), "part ids come from content", ""},
		"a part in another namespace":         {nil, "v2", brain.Parts(brain.Object{Kind: "Chunk", Content: "x", Namespace: storetest.Namespace(t, "org", "b")}), "is not the parent's", ""},
		"a soft-deleted part":                 {nil, "v2", brain.Parts(brain.Object{Kind: "Chunk", Content: "x", DeletedAt: new(time.Now())}), "soft-deleted", ""},
		"a source read error":                 {nil, "v2", failing, "disk read failed", ""},
		"a parent the embedder answers short": {nil, "short v2", brain.Parts(brain.Object{Kind: "Chunk", Content: "x"}), "returned 0 vectors for 1 texts", ""},
		"a part the embedder fails on":        {nil, "v2", brain.Parts(brain.Object{Kind: "Chunk", Content: "poison"}), "embedding API down", ""},
		"a part of an unknown kind":           {[]brain.EngineOption{catalog}, "v2", brain.Parts(brain.Object{Kind: "Unknown", Content: "x"}), "Unknown", ""},
		"a parent with no kind":               {nil, "v2", brain.Parts(brain.Object{Kind: "Chunk", Content: "x"}), "kind is required", " "},
	} {
		store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb}))
		eng, err := brain.NewEngine(store, tc.opts...)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "v1"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "kept"}))
		if err != nil {
			t.Fatal(err)
		}
		renamed := doc
		renamed.Title = tc.title
		renamed.Kind = cmp.Or(tc.kind, renamed.Kind)

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

// TestEngine_putTreeIndexesEachPartUnderItsRecordsTitle: a part is indexed as
// its record's title over its own text; a part with no text takes the title
// alone, and a part of an untitled record takes its own text alone.
func TestEngine_putTreeIndexesEachPartUnderItsRecordsTitle(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var mu sync.Mutex
	var embedded []string
	emb := embedFunc(func(_ context.Context, texts []string) ([][]float32, error) {
		mu.Lock()
		defer mu.Unlock()
		embedded = append(embedded, texts...)
		return repeatVector([]float32{1, 0}, len(texts)), nil
	})
	eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb})))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}

	_, titledErr := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "Manual"}, brain.Parts(
		brain.Object{Kind: "Chunk", Content: "setup steps"}, brain.Object{Kind: "Chunk"},
	))
	_, untitledErr := eng.PutTree(ctx, scope, brain.Object{Kind: "Document"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "loose notes"}))

	slices.Sort(embedded)
	if want := []string{"Manual", "Manual", "Manual\nsetup steps", "loose notes"}; titledErr != nil || untitledErr != nil || !slices.Equal(embedded, want) {
		t.Errorf("embedded %q (%v, %v), want %q", embedded, titledErr, untitledErr, want)
	}
}
