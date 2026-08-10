package vfsindex_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// TestMountIndexer_indexSearchAndNotify: VFS file → brain Document/Chunks →
// search finds content; hash skip; re-index after write; missing path removes.
func TestMountIndexer_indexSearchAndNotify(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("idx", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}

	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	scope := brain.Scope{Namespace: &ns}

	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	idx.LinesPerChunk = 2

	body := "alpha line\nbeta TODO findme\ngamma line\ndelta line\n"
	if err := ms.WriteFile(ctx, "/work/note.txt", []byte(body)); err != nil {
		t.Fatal(err)
	}

	// IndexPrefix
	stats, err := idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 1 {
		t.Fatalf("stats: %+v", stats)
	}

	parentID := idx.DocumentID("/work/note.txt")
	doc, err := eng.Read(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Properties[vfsindex.PropVFSPath] != "/work/note.txt" {
		t.Fatalf("vfs_path: %+v", doc.Properties)
	}
	children, err := eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	// 4 lines, 2 per chunk → 2 chunks
	if len(children) != 2 {
		t.Fatalf("chunks=%d %#v", len(children), children)
	}
	// line anchors on first chunk
	c0, err := eng.Read(ctx, scope, children[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if c0.Properties[vfsindex.PropStartLine] != float64(1) || c0.Properties[vfsindex.PropEndLine] != float64(2) {
		t.Fatalf("chunk0 props: %+v content=%q", c0.Properties, c0.Content)
	}
	// Search finds the parent via chunk content
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "findme"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 || page.Objects[0].ID != parentID {
		t.Fatalf("search: %+v", page.Objects)
	}
	// Chunk 0 is lines 1–2 and includes TODO
	if !strings.Contains(c0.Content, "TODO") {
		t.Fatalf("chunk0 content: %q", c0.Content)
	}

	// Hash skip
	stats, err = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
	if err != nil || stats.Skipped != 1 || stats.Indexed != 0 {
		t.Fatalf("hash skip stats: %+v err=%v", stats, err)
	}

	// AfterPersist + SyncScheduler re-index on write
	sched := vfsindex.NewSyncScheduler(idx)
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		return sched.Notify(ctx, path, vfsindex.ReasonSync)
	})
	if err := ms.WriteFile(ctx, "/work/note.txt", []byte("rewritten uniquephrase\n")); err != nil {
		t.Fatal(err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "uniquephrase"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 {
		t.Fatal("expected re-index after WriteFile hook")
	}
	// old content should shrink to one chunk
	children, err = eng.ListChildren(ctx, scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("after rewrite chunks=%d", len(children))
	}

	// Remove file → soft-delete index
	if err := ms.Remove(ctx, "/work/note.txt"); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexPath(ctx, "/work/note.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Read(ctx, scope, parentID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("want not found after remove, got %v", err)
	}

	// Binary skip by extension
	if err := ms.WriteFile(ctx, "/work/pic.png", []byte{0x89, 'P', 'N', 'G', 0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	stats, err = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed != 0 {
		t.Fatalf("png skip: %+v err=%v", stats, err)
	}

	// MaxFiles
	if err := ms.WriteFile(ctx, "/work/c.txt", []byte("c\n")); err != nil {
		t.Fatal(err)
	}
	stats, err = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{MaxFiles: 1})
	if err != nil || stats.Indexed+stats.Skipped < 1 {
		t.Fatalf("max files: %+v err=%v", stats, err)
	}

	// IndexPath on directory is a no-op
	if err := idx.IndexPath(ctx, "/work"); err != nil {
		t.Fatal(err)
	}

	// Constructor guards
	if _, err := vfsindex.NewMountIndexer(nil, eng, scope); err == nil {
		t.Fatal("nil ms")
	}
	if _, err := vfsindex.NewMountIndexer(ms, nil, scope); err == nil {
		t.Fatal("nil eng")
	}
	if _, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{}); err == nil {
		t.Fatal("nil ns")
	}
	if err := vfsindex.NewSyncScheduler(nil).Notify(ctx, "/work/x", vfsindex.ReasonExplicit); err != nil {
		t.Fatal(err)
	}
}
