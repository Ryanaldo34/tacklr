package vfs_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestFS_mountUnmountListLookup(t *testing.T) {
	fs := vfs.New()
	ctx := t.Context()
	rootA := t.TempDir()
	rootB := t.TempDir()

	if mounts := fs.Mounts(); len(mounts) != 0 {
		t.Fatalf("Mounts on empty FS = %v, want empty", mounts)
	}
	_, _, err := fs.Lookup("/anything")
	if !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("Lookup with no mounts: %v, want ErrNotMounted", err)
	}

	if err := fs.Mount(ctx, "/data", vfs.LocalProvider{Root: rootA}, false); err != nil {
		t.Fatalf("Mount /data: %v", err)
	}
	if err := fs.Mount(ctx, "/data/nested", vfs.LocalProvider{Root: rootB}, true); err != nil {
		t.Fatalf("Mount /data/nested: %v", err)
	}
	if err := fs.Mount(ctx, "/artifacts", vfs.S3Provider{
		Client: struct{}{},
		Bucket: "bucket",
		Prefix: "runs/1/",
	}, false); err != nil {
		t.Fatalf("Mount /artifacts: %v", err)
	}

	mounts := fs.Mounts()
	if len(mounts) != 3 {
		t.Fatalf("Mounts len = %d, want 3: %v", len(mounts), mounts)
	}
	wantPoints := []string{"/artifacts", "/data", "/data/nested"}
	for i, p := range wantPoints {
		if mounts[i].Point != p {
			t.Errorf("Mounts[%d].Point = %q, want %q", i, mounts[i].Point, p)
		}
	}
	if mounts[0].ReadOnly || mounts[1].ReadOnly || !mounts[2].ReadOnly {
		t.Errorf("ReadOnly flags = %v/%v/%v, want false/false/true", mounts[0].ReadOnly, mounts[1].ReadOnly, mounts[2].ReadOnly)
	}

	info, rel, err := fs.Lookup("/data/nested/x/y")
	if err != nil {
		t.Fatalf("Lookup nested: %v", err)
	}
	if info.Point != "/data/nested" || !info.ReadOnly || rel != "x/y" {
		t.Fatalf("Lookup nested = %+v rel=%q", info, rel)
	}

	info, rel, err = fs.Lookup("/data/other")
	if err != nil {
		t.Fatalf("Lookup /data/other: %v", err)
	}
	if info.Point != "/data" || rel != "other" {
		t.Fatalf("Lookup /data/other = %+v rel=%q", info, rel)
	}

	info, rel, err = fs.Lookup("/artifacts")
	if err != nil {
		t.Fatalf("Lookup /artifacts: %v", err)
	}
	if info.Point != "/artifacts" || rel != "" {
		t.Fatalf("Lookup mount point = %+v rel=%q", info, rel)
	}

	// path.Clean on lookup
	info, rel, err = fs.Lookup("/data//foo/./bar")
	if err != nil || info.Point != "/data" || rel != "foo/bar" {
		t.Fatalf("Lookup cleaned: %+v rel=%q err=%v", info, rel, err)
	}

	err = fs.Mount(ctx, "/data", vfs.LocalProvider{Root: rootA}, false)
	if !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("double Mount: %v, want ErrAlreadyMounted", err)
	}

	if err := fs.Unmount("/data/nested"); err != nil {
		t.Fatalf("Unmount nested: %v", err)
	}
	info, rel, err = fs.Lookup("/data/nested/x")
	if err != nil || info.Point != "/data" || rel != "nested/x" {
		t.Fatalf("after unmount = %+v rel=%q err=%v", info, rel, err)
	}

	err = fs.Unmount("/data/nested")
	if !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("Unmount missing: %v, want ErrNotMounted", err)
	}
}

func TestFS_mountRootAndSegmentBoundary(t *testing.T) {
	fs := vfs.New()
	ctx := t.Context()
	dir := t.TempDir()

	if err := fs.Mount(ctx, "/", vfs.LocalProvider{Root: dir}, false); err != nil {
		t.Fatalf("Mount /: %v", err)
	}
	if err := fs.Mount(ctx, "/ab", vfs.S3Provider{Client: struct{}{}, Bucket: "b"}, false); err != nil {
		t.Fatalf("Mount /ab: %v", err)
	}

	info, rel, err := fs.Lookup("/a/file")
	if err != nil || info.Point != "/" || rel != "a/file" {
		t.Fatalf("segment boundary: %+v rel=%q err=%v", info, rel, err)
	}

	info, rel, err = fs.Lookup("/ab/x")
	if err != nil || info.Point != "/ab" || rel != "x" {
		t.Fatalf("/ab longest: %+v rel=%q err=%v", info, rel, err)
	}

	info, rel, err = fs.Lookup("/")
	if err != nil || info.Point != "/" || rel != "" {
		t.Fatalf("Lookup root: %+v rel=%q err=%v", info, rel, err)
	}
}

func TestFS_invalidPaths(t *testing.T) {
	fs := vfs.New()
	dir := t.TempDir()
	for _, p := range []string{"", "relative", "\\windows", "data", "/has\x00null"} {
		_, _, err := fs.Lookup(p)
		if !errors.Is(err, vfs.ErrInvalidPath) {
			t.Errorf("Lookup(%q) err = %v, want ErrInvalidPath", p, err)
		}
	}
	if err := fs.Mount(t.Context(), "relative", vfs.LocalProvider{Root: dir}, false); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("Mount relative: %v", err)
	}
	if err := fs.Unmount("relative"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("Unmount relative: %v", err)
	}
}

func TestFS_nilProviderAndNilFS(t *testing.T) {
	fs := vfs.New()
	if err := fs.Mount(t.Context(), "/x", nil, false); !errors.Is(err, vfs.ErrInvalidProvider) {
		t.Fatalf("nil provider: %v", err)
	}

	var nilFS *vfs.FS
	if err := nilFS.Mount(t.Context(), "/x", vfs.S3Provider{Client: struct{}{}, Bucket: "b"}, false); err == nil {
		t.Fatal("nil FS Mount: want error")
	}
	if err := nilFS.Unmount("/x"); err == nil {
		t.Fatal("nil FS Unmount: want error")
	}
	if mounts := nilFS.Mounts(); mounts != nil {
		t.Fatalf("nil FS Mounts = %v, want nil", mounts)
	}
	if _, _, err := nilFS.Lookup("/x"); err == nil {
		t.Fatal("nil FS Lookup: want error")
	}
}

func TestFS_canceledContext(t *testing.T) {
	fs := vfs.New()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := fs.Mount(ctx, "/data", vfs.LocalProvider{Root: t.TempDir()}, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Mount: %v", err)
	}
}

func TestFS_concurrentMountUnmount(t *testing.T) {
	fs := vfs.New()
	ctx := t.Context()
	dir := t.TempDir()

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			point := "/c/" + strconv.Itoa(i)
			_ = fs.Mount(ctx, point, vfs.LocalProvider{Root: dir}, false)
			_ = fs.Mounts()
			_, _, _ = fs.Lookup(point + "/f")
			_ = fs.Unmount(point)
		}(i)
	}
	wg.Wait()
}
