package vfsindex_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// TestAsyncScheduler_notifyCoalesceAndEventualIndex: Notify returns without
// waiting on IndexPath, and content becomes searchable.
func TestAsyncScheduler_notifyCoalesceAndEventualIndex(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("async-sched", reg)
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
	idx, err := vfsindex.NewMountIndexer(ms, eng, brain.Scope{Namespace: &ns})
	if err != nil {
		t.Fatal(err)
	}

	body := "async uniquephrase alpha\n"
	if err := ms.WriteFile(ctx, "/work/a.txt", []byte(body)); err != nil {
		t.Fatal(err)
	}

	sched := vfsindex.NewAsyncScheduler(idx)
	defer func() { _ = sched.Close() }()

	// Notify returns without waiting on IndexPath (duplicates coalesce).
	if err := sched.Notify(ctx, "/work/a.txt", vfsindex.ReasonSync); err != nil {
		t.Fatal(err)
	}
	if err := sched.Notify(ctx, "/work/a.txt", vfsindex.ReasonExplicit); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		page, err := eng.Search(ctx, brain.Scope{Namespace: &ns}, brain.SearchRequest{Query: "uniquephrase"}, brain.NewSearchContext())
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Objects) > 0 {
			found = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found {
		t.Fatal("expected async IndexPath to make content searchable")
	}
}

// TestIndexPathResult_andUnindex: public helpers return compact outcomes.
func TestIndexPathResult_andUnindex(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("result-unindex", reg)
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

	if err := ms.WriteFile(ctx, "/work/x.txt", []byte("hello world\n")); err != nil {
		t.Fatal(err)
	}
	res, err := idx.IndexPathResult(ctx, "/work/x.txt")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("indexed: res=%q err=%v", res, err)
	}
	res, err = idx.IndexPathResult(ctx, "/work/x.txt")
	if err != nil || res != vfsindex.PathSkipped {
		t.Fatalf("skipped: res=%q err=%v", res, err)
	}
	res, err = idx.IndexPathResult(ctx, "/work")
	if err != nil || res != vfsindex.PathDirectory {
		t.Fatalf("directory: res=%q err=%v", res, err)
	}

	removed, err := idx.UnindexPath(ctx, "/work/x.txt")
	if err != nil || !removed {
		t.Fatalf("unindex: removed=%v err=%v", removed, err)
	}
	if _, err := ms.Stat(ctx, "/work/x.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Read(ctx, scope, idx.DocumentID("/work/x.txt")); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("expected soft-deleted parent, got %v", err)
	}
	removed, err = idx.UnindexPath(ctx, "/work/x.txt")
	if err != nil || removed {
		t.Fatalf("noop unindex: removed=%v err=%v", removed, err)
	}

	// Missing path IndexPathResult → removed (no mirror left)
	res, err = idx.IndexPathResult(ctx, "/work/missing.txt")
	if err != nil || res != vfsindex.PathRemoved {
		t.Fatalf("removed missing: res=%q err=%v", res, err)
	}

	// WriteDocument persists immediately; IndexPath sees the new body.
	doc := vfs.NewTextDocument("/work/dirty.txt", "text/plain", "utf-8", "dirtyphrase-only-in-cache\n")
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	res, err = idx.IndexPathResult(ctx, "/work/dirty.txt")
	if err != nil || res != vfsindex.PathIndexed {
		t.Fatalf("dirty index: res=%q err=%v", res, err)
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "dirtyphrase-only-in-cache"}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) == 0 {
		t.Fatal("IndexPath should use session-visible dirty body")
	}
	if page.Objects[0].Properties[vfsindex.PropVFSPath] != "/work/dirty.txt" {
		t.Fatalf("unexpected hit: %+v", page.Objects[0])
	}
}
