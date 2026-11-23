package vfsindex_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// TestMountIndexer_sourceReadersLimitSearchAndAnACLChangeReindexes: the
// Readers func's principals land on the Document and Chunks, search only
// shows the file to holders, and a sharing change re-indexes unchanged text.
func TestMountIndexer_sourceReadersLimitSearchAndAnACLChangeReindexes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", uuid.NewString())
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: ns, Principals: []string{"group:finance", "group:board"}})
	if err != nil {
		t.Fatal(err)
	}
	sharing := []string{"group:finance", "group:finance"}
	idx.Readers = func(context.Context, string, vfs.FileInfo) ([]string, error) { return sharing, nil }
	const file = "/workspace/work/budget.txt"
	if err := ms.Route(ctx, file).WriteFile(ctx, []byte("fy27 budget forecast\n")); err != nil {
		t.Fatal(err)
	}
	search := func(principals ...string) []brain.RichObject {
		page, err := eng.Search(ctx, brain.Scope{Namespace: ns, Principals: principals}, brain.SearchRequest{Query: "forecast"})
		if err != nil {
			t.Fatal(err)
		}
		return slices.Collect(page.Results.Objects())
	}

	first, firstErr := idx.IndexPath(ctx, file)
	finance, outsider := search("group:finance"), search("user:mallory")
	same, _ := idx.IndexPath(ctx, file)
	sharing = []string{"group:board"}
	reshared, _ := idx.IndexPath(ctx, file)
	financeAfter, board := search("group:finance"), search("group:board")
	doc, _ := eng.Get(ctx, brain.Scope{Namespace: ns, Principals: []string{"group:board"}}, idx.DocumentID(file))

	if firstErr != nil || first != vfsindex.PathIndexed || len(finance) != 1 || len(outsider) != 0 {
		t.Fatalf("first index = %s %v; finance hits %d, outsider hits %d", first, firstErr, len(finance), len(outsider))
	}
	if same != vfsindex.PathSkipped || reshared != vfsindex.PathIndexed {
		t.Fatalf("unchanged = %s, reshared = %s", same, reshared)
	}
	if len(financeAfter) != 0 || len(board) != 1 || !slices.Equal(doc.Readers, []string{"group:board"}) {
		t.Fatalf("after resharing: finance %d, board %d, readers %v", len(financeAfter), len(board), doc.Readers)
	}

	// No sharing at the source: every scope over the namespace sees the file.
	sharing = nil
	public, publicErr := idx.IndexPath(ctx, file)
	if everyone := search("user:mallory"); publicErr != nil || public != vfsindex.PathIndexed || len(everyone) != 1 {
		t.Fatalf("unshared = %s %v, outsider hits %d", public, publicErr, len(everyone))
	}
}

// TestMountIndexer_aReadersFuncThatFailsFailsTheIndex: a source whose
// sharing cannot be read fails the index with that error and the path.
func TestMountIndexer_aReadersFuncThatFailsFailsTheIndex(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write(t, "budget.txt", "fy27 budget forecast\n")
	refused := errors.New("sharing refused")
	h.idx.Readers = func(context.Context, string, vfs.FileInfo) ([]string, error) { return nil, refused }

	_, err := h.idx.IndexPath(t.Context(), "/workspace/work/budget.txt")

	if !errors.Is(err, refused) || !strings.Contains(err.Error(), "/workspace/work/budget.txt") {
		t.Fatalf("index = %v, want the readers error with the path", err)
	}
}

// TestMountIndexer_unindexRemovesAnIndexedFileOnce: unindexing an indexed
// file removes its mirror and leaves the file; unindexing it again does
// nothing.
func TestMountIndexer_unindexRemovesAnIndexedFileOnce(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := newHarness(t)
	h.write(t, "notes.txt", "release notes\n")
	const file = "/workspace/work/notes.txt"
	if _, err := h.idx.IndexPath(ctx, file); err != nil {
		t.Fatal(err)
	}

	removed, removeErr := h.idx.UnindexPath(ctx, file)
	again, againErr := h.idx.UnindexPath(ctx, file)

	if removeErr != nil || !removed || againErr != nil || again {
		t.Fatalf("unindex = %v %v, again = %v %v; want removed once", removed, removeErr, again, againErr)
	}
	if _, err := h.eng.Get(ctx, h.idx.Scope, h.idx.DocumentID(file)); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("document after unindex = %v, want ErrNotFound", err)
	}
	if _, err := h.ms.Route(ctx, file).Stat(ctx); err != nil {
		t.Fatalf("file after unindex = %v, want it kept", err)
	}
}
