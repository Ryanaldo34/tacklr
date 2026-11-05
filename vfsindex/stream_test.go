package vfsindex_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// hookEmbedder is a host embedder that runs hook once, the first time it
// embeds a text that contains trigger. An empty trigger fires on the first
// call.
type hookEmbedder struct {
	storetest.Embedder
	trigger string
	hook    func()
	fired   atomic.Bool
}

func (e *hookEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if e.hook != nil && slices.ContainsFunc(texts, func(s string) bool { return strings.Contains(s, e.trigger) }) && e.fired.CompareAndSwap(false, true) {
		e.hook()
	}
	return e.Embedder.Embed(ctx, texts)
}

// harness indexes the local mount /workspace/work into a fresh engine.
type harness struct {
	dir   string
	ms    *vfs.MountSession
	idx   *vfsindex.MountIndexer
	eng   *brain.Engine
	store *brain.MemoryStore
	emb   *hookEmbedder
}

func newHarness(t *testing.T, opts ...brain.EngineOption) harness {
	t.Helper()
	dir := t.TempDir()
	ms := treeLocal(t, vfs.At("work", vfs.Local(dir)))
	emb := &hookEmbedder{}
	// One text a batch: chunks embed while the file is still read.
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: emb, BatchSize: 1}))
	eng, err := brain.NewEngine(store, opts...)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: storetest.Namespace(t, "id", uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	return harness{dir: dir, ms: ms, idx: idx, eng: eng, store: store, emb: emb}
}

// write stores body at /workspace/work/name.
func (h harness) write(t *testing.T, name, body string) {
	t.Helper()
	if err := h.ms.Route(t.Context(), "/workspace/work/"+name).WriteFile(t.Context(), []byte(body)); err != nil {
		t.Fatal(err)
	}
}

// stored is the indexed text of vpath's chunks in order.
func (h harness) stored(vpath string) string {
	tree, _ := h.store.GetTree(context.Background(), h.idx.Scope, h.idx.DocumentID(vpath), 0)
	return joinStored(tree.Parts)
}

func numberedLines(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "line %03d records a distinct fact about topic %d\n", i, i*7)
	}
	return b.String()
}

func TestMountIndexer_anEditReembedsOnlyTheChunksAroundIt(t *testing.T) {
	ctx := t.Context()
	h := newHarness(t)
	const vpath = "/workspace/work/big.txt"
	body := numberedLines(400)
	h.write(t, "big.txt", body)
	if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
		t.Fatal(err)
	}
	first := h.emb.Take()
	edited := "inserted-needle line at the top\n" + body
	h.write(t, "big.txt", edited)

	_, err := h.idx.IndexPath(ctx, vpath)
	second := h.emb.Take()
	kids, _ := h.store.GetTree(ctx, h.idx.Scope, h.idx.DocumentID(vpath), 0)
	page, _ := h.eng.Search(ctx, h.idx.Scope, brain.SearchRequest{Query: "inserted-needle"})
	hits := slices.Collect(page.Results.Objects())

	if err != nil || first < 8 {
		t.Fatalf("index = %v, first run embedded %d texts, want one per chunk", err, first)
	}
	// The parent, the chunk with the new line, and at most one neighbor.
	if second > 3 {
		t.Errorf("edit embedded %d texts of %d, want at most 3", second, first)
	}
	if joinStored(kids.Parts) != strings.TrimSuffix(edited, "\n") {
		t.Errorf("chunks no longer cover the file in order")
	}
	if end := kids.Parts[len(kids.Parts)-1].Properties[vfsindex.PropEndLine]; end != float64(401) {
		t.Errorf("last chunk ends at line %v, want 401 after the insert", end)
	}
	if len(hits) == 0 || hits[0].ID != h.idx.DocumentID(vpath) {
		t.Errorf("search for the inserted line = %v", hits)
	}
}

func TestMountIndexer_aReindexThatAnotherIndexerCommitsFirstIsRetried(t *testing.T) {
	ctx := t.Context()
	h := newHarness(t)
	const vpath = "/workspace/work/a.txt"
	h.write(t, "a.txt", numberedLines(100))
	latest := "rewritten while it was read\n" + numberedLines(50)
	// While the first index embeds its chunks, the file changes and another
	// index commits.
	h.emb.trigger = "records"
	h.emb.hook = func() {
		h.write(t, "a.txt", latest)
		if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
			t.Errorf("inner index: %v", err)
		}
	}

	_, err := h.idx.IndexPath(ctx, vpath)

	if err != nil {
		t.Fatalf("index after another indexer committed first = %v, want a retry", err)
	}
	if h.stored(vpath) != strings.TrimSuffix(latest, "\n") {
		t.Errorf("chunks after the retry do not hold the latest text")
	}
}

func TestMountIndexer_chunkSizesOutOfOrderFailTheIndex(t *testing.T) {
	h := newHarness(t)
	h.write(t, "a.txt", "text\n")
	h.idx.Chunking = vfsindex.ChunkSizes{Min: 64, Avg: 32}

	_, err := h.idx.IndexPath(t.Context(), "/workspace/work/a.txt")

	if err == nil || !strings.Contains(err.Error(), "Min < Avg <= Max") {
		t.Fatalf("index with Min > Avg = %v", err)
	}
}

func TestMountIndexer_aFileThatCannotBeReadOrChunkedFailsTheIndexAndKeepsThePreviousOne(t *testing.T) {
	const vpath = "/workspace/work/a.txt"
	for name, tc := range map[string]struct {
		arrange func(*testing.T, context.CancelFunc, harness)
		want    string
	}{
		"the file cannot be opened": {func(t *testing.T, _ context.CancelFunc, h harness) {
			if err := os.Chmod(filepath.Join(h.dir, "a.txt"), 0); err != nil {
				t.Fatal(err)
			}
		}, vfs.ErrPermission.Error()},
		// The parent embeds first, so the read finds the context ended.
		"the caller cancels before the read": {func(_ *testing.T, cancel context.CancelFunc, h harness) { h.emb.hook = cancel }, "context canceled"},
		"the caller cancels while chunks embed": {func(_ *testing.T, cancel context.CancelFunc, h harness) {
			h.emb.trigger, h.emb.hook = "records", cancel
		}, "context canceled"},
		"the chunk kind is unknown": {func(t *testing.T, _ context.CancelFunc, h harness) {
			if err := h.eng.ApplyKinds(t.Context(), vfsindex.MountIndexKinds()...); err != nil {
				t.Fatal(err)
			}
			h.idx.ChunkKind = "Unregistered"
		}, "Unregistered"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			h := newHarness(t)
			h.write(t, "a.txt", numberedLines(100))
			if _, err := h.idx.IndexPath(ctx, vpath); err != nil {
				t.Fatal(err)
			}
			h.write(t, "a.txt", numberedLines(3000))
			tc.arrange(t, cancel, h)

			_, err := h.idx.IndexPath(ctx, vpath)

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("index = %v, want %q", err, tc.want)
			}
			if h.stored(vpath) != strings.TrimSuffix(numberedLines(100), "\n") {
				t.Errorf("the previous chunks were replaced")
			}
		})
	}
}

func TestMountIndexer_anAuditFailureOnTheVersionCheckFailsTheIndex(t *testing.T) {
	ctx := t.Context()
	h := newHarness(t, brain.WithAudit(func(_ context.Context, ev brain.AuditEvent) error {
		if ev.Op == brain.AuditRead {
			return errors.New("audit sink down")
		}
		return nil
	}))
	h.write(t, "a.txt", "text\n")
	if _, err := h.idx.IndexPath(ctx, "/workspace/work/a.txt"); err != nil {
		t.Fatal(err)
	}

	_, err := h.idx.IndexPath(ctx, "/workspace/work/a.txt")

	if err == nil || !strings.Contains(err.Error(), "audit sink down") {
		t.Fatalf("re-index with a failing audit sink = %v", err)
	}
}

func joinStored(kids []brain.Object) string {
	parts := make([]string, len(kids))
	for i, k := range kids {
		parts[i] = k.Content
	}
	return strings.Join(parts, "\n")
}
