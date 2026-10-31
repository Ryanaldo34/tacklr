package vfsindex_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// TestMountIndexer_sourceReadersLimitSearchAndAnACLChangeReindexes: the
// Readers func's principals land on the Document and Chunks, search only
// shows the file to holders, and a sharing change re-indexes unchanged text.
func TestMountIndexer_sourceReadersLimitSearchAndAnACLChangeReindexes(t *testing.T) {
	ctx := context.Background()
	ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", uuid.NewString())
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
		page, err := eng.Search(ctx, brain.Scope{Namespace: ns, Principals: principals}, brain.SearchRequest{Query: "forecast"}, brain.NewSearchContext())
		if err != nil {
			t.Fatal(err)
		}
		return page.Objects
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

func TestMountIndexer_aReadersErrorFailsTheIndex(t *testing.T) {
	ctx := context.Background()
	ms := treeLocal(t, vfs.At("work", vfs.Local(t.TempDir())))
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: mustNS(t, "org", "a")})
	if err != nil {
		t.Fatal(err)
	}
	idx.Readers = func(context.Context, string, vfs.FileInfo) ([]string, error) {
		return nil, errors.New("sharing API down")
	}
	if err := ms.Route(ctx, "/workspace/work/a.txt").WriteFile(ctx, []byte("a\n")); err != nil {
		t.Fatal(err)
	}

	_, err = idx.IndexPath(ctx, "/workspace/work/a.txt")

	if err == nil || !strings.Contains(err.Error(), "sharing API down") {
		t.Fatalf("index = %v", err)
	}
}
