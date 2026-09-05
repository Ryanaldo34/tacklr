package vfsindex_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

func TestMountIndexer_unindexLeavesFileThenReindexRecovers(t *testing.T) {
	ctx := t.Context()
	ms := treeLocal(t, vfs.At("work", builtins.Local(t.TempDir())))
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())}
	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/a.txt", []byte("recoverable-phrase\n")); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexPath(ctx, "/workspace/work/a.txt"); err != nil {
		t.Fatal(err)
	}
	removed, err := idx.UnindexPath(ctx, "/workspace/work/a.txt")
	if err != nil || !removed {
		t.Fatalf("unindex: removed=%v err=%v", removed, err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/work/a.txt"); err != nil {
		t.Fatalf("vfs file must remain: %v", err)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "recoverable-phrase"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) != 0 {
		t.Fatalf("search after unindex: %+v err=%v", page, err)
	}
	if err := idx.IndexPath(ctx, "/workspace/work/a.txt"); err != nil {
		t.Fatal(err)
	}
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "recoverable-phrase"}, brain.NewSearchContext())
	if err != nil || len(page.Objects) == 0 {
		t.Fatalf("reindex recovery: %+v err=%v", page, err)
	}
}

func TestMountIndexer_nonePolicySkippedAndMaxFilesStops(t *testing.T) {
	ctx := t.Context()
	ms := treeLocal(t,
		vfs.At("work", builtins.Local(t.TempDir())),
		vfs.At("off", builtins.Local(t.TempDir())).Indexed("none"),
	)
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: mustNS(t, "id", uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/a.txt", []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/b.txt", []byte("two\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/off/x.txt", []byte("hidden\n")); err != nil {
		t.Fatal(err)
	}
	stats, err := idx.IndexPrefix(ctx, "/workspace/off", vfsindex.IndexOpts{})
	if err != nil || stats.Indexed != 0 {
		t.Fatalf("none prefix: %+v err=%v", stats, err)
	}
	stats, err = idx.IndexPrefix(ctx, "/workspace/work", vfsindex.IndexOpts{MaxFiles: 1})
	if err != nil || stats.Indexed != 1 {
		t.Fatalf("max files: %+v err=%v", stats, err)
	}
}
