package vfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestFS_mountUnmountListLookupAndSpecs(t *testing.T) {
	fs := vfs.New()
	ctx := t.Context()
	rootA := t.TempDir()
	rootB := t.TempDir()

	if mounts := fs.Mounts(); len(mounts) != 0 {
		t.Fatalf("Mounts on empty FS = %v", mounts)
	}
	_, _, err := fs.Lookup("/anything")
	if !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("Lookup empty: %v", err)
	}

	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/data", Profile: "local", Params: map[string]string{"k": "v"}}, vfs.LocalProvider{Root: rootA}); err != nil {
		t.Fatalf("Mount /data: %v", err)
	}
	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/data/nested", Profile: "local", ReadOnly: true}, vfs.LocalProvider{Root: rootB}); err != nil {
		t.Fatalf("Mount nested: %v", err)
	}
	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/artifacts", Profile: "s3", Params: map[string]string{"bucket": "b"}}, vfs.S3Provider{Client: struct{}{}, Bucket: "b", Prefix: "runs/1/"}); err != nil {
		t.Fatalf("Mount artifacts: %v", err)
	}

	mounts := fs.Mounts()
	if len(mounts) != 3 || mounts[0].Point != "/artifacts" || mounts[2].ReadOnly != true {
		t.Fatalf("Mounts = %+v", mounts)
	}

	specs := fs.Specs()
	if len(specs) != 3 || specs[1].Profile != "local" || specs[1].Params["k"] != "v" {
		t.Fatalf("Specs = %+v", specs)
	}
	// Specs are checkpoint-safe copies
	specs[1].Params["k"] = "mutated"
	if fs.Specs()[1].Params["k"] != "v" {
		t.Fatal("Specs mutation leaked into live table")
	}

	raw, err := json.Marshal(specs)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []vfs.MountSpec
	if err := json.Unmarshal(raw, &decoded); err != nil || len(decoded) != 3 {
		t.Fatalf("JSON round-trip: %v %#v", err, decoded)
	}

	info, rel, err := fs.Lookup("/data/nested/x/y")
	if err != nil || info.Point != "/data/nested" || !info.ReadOnly || rel != "x/y" {
		t.Fatalf("Lookup nested: %+v rel=%q err=%v", info, rel, err)
	}

	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/data", Profile: "local"}, vfs.LocalProvider{Root: rootA}); !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("double mount: %v", err)
	}

	if err := fs.Unmount("/data/nested"); err != nil {
		t.Fatal(err)
	}
	if len(fs.Specs()) != 2 {
		t.Fatalf("after unmount Specs len = %d", len(fs.Specs()))
	}
	info, rel, err = fs.Lookup("/data/nested/x")
	if err != nil || info.Point != "/data" || rel != "nested/x" {
		t.Fatalf("after unmount lookup: %+v rel=%q err=%v", info, rel, err)
	}
	if err := fs.Unmount("/data/nested"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("unmount missing: %v", err)
	}
}

func TestFS_mountRootAndSegmentBoundary(t *testing.T) {
	fs := vfs.New()
	ctx := t.Context()
	dir := t.TempDir()
	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/", Profile: "local"}, vfs.LocalProvider{Root: dir}); err != nil {
		t.Fatal(err)
	}
	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/ab", Profile: "s3"}, vfs.S3Provider{Client: struct{}{}, Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	info, rel, err := fs.Lookup("/a/file")
	if err != nil || info.Point != "/" || rel != "a/file" {
		t.Fatalf("segment: %+v rel=%q err=%v", info, rel, err)
	}
	info, rel, err = fs.Lookup("/ab/x")
	if err != nil || info.Point != "/ab" || rel != "x" {
		t.Fatalf("/ab: %+v rel=%q err=%v", info, rel, err)
	}
}

func TestFS_invalidPathsAndNil(t *testing.T) {
	fs := vfs.New()
	dir := t.TempDir()
	for _, p := range []string{"", "relative", "\\windows", "data", "/has\x00null"} {
		if _, _, err := fs.Lookup(p); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Errorf("Lookup(%q): %v", p, err)
		}
	}
	if err := fs.Mount(t.Context(), vfs.MountSpec{Point: "relative", Profile: "local"}, vfs.LocalProvider{Root: dir}); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("Mount relative: %v", err)
	}
	if err := fs.Mount(t.Context(), vfs.MountSpec{Point: "/x", Profile: ""}, vfs.LocalProvider{Root: dir}); !errors.Is(err, vfs.ErrInvalidProvider) {
		t.Fatalf("empty profile: %v", err)
	}
	if err := fs.Mount(t.Context(), vfs.MountSpec{Point: "/x", Profile: "local"}, nil); !errors.Is(err, vfs.ErrInvalidProvider) {
		t.Fatalf("nil provider: %v", err)
	}

	var nilFS *vfs.FS
	if err := nilFS.Mount(t.Context(), vfs.MountSpec{Point: "/x", Profile: "p"}, vfs.S3Provider{Client: struct{}{}, Bucket: "b"}); err == nil {
		t.Fatal("nil FS Mount")
	}
	if err := nilFS.Unmount("/x"); err == nil {
		t.Fatal("nil FS Unmount")
	}
	if nilFS.Mounts() != nil || nilFS.Specs() != nil {
		t.Fatal("nil FS lists")
	}
}

func TestFS_canceledContext(t *testing.T) {
	fs := vfs.New()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := fs.Mount(ctx, vfs.MountSpec{Point: "/data", Profile: "local"}, vfs.LocalProvider{Root: t.TempDir()})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

func TestFS_unmountInvalidAndNilLookup(t *testing.T) {
	fs := vfs.New()
	if err := fs.Unmount("relative"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("unmount relative: %v", err)
	}
	var nilFS *vfs.FS
	if _, _, err := nilFS.Lookup("/x"); err == nil {
		t.Fatal("nil lookup")
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
			_ = fs.Mount(ctx, vfs.MountSpec{Point: point, Profile: "local"}, vfs.LocalProvider{Root: dir})
			_ = fs.Specs()
			_, _, _ = fs.Lookup(point + "/f")
			_ = fs.Unmount(point)
		}(i)
	}
	wg.Wait()
}
