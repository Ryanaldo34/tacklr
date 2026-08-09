package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestMountSession_localSession is the primary local-VFS outcome test:
// mounts, nested lookup, I/O, read-only, jail, rematerialize (restart shape).
func TestMountSession_localSession(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}

	ms := vfs.NewMountSession("sess-1", reg)
	if err := ms.Materialize(ctx, []vfs.MountSpec{
		{Point: "/work", Profile: "scratch"},
		{Point: "/work/nested", Profile: "scratch", ReadOnly: true, Params: map[string]string{"subpath": "nested"}},
		{Point: "/ab", Profile: "scratch", Params: map[string]string{"subpath": "ab"}},
	}); err != nil {
		t.Fatal(err)
	}
	if n := len(ms.Infos()); n != 3 {
		t.Fatalf("Infos len = %d", n)
	}

	// Nested longest-prefix + I/O
	if err := ms.WriteFile(ctx, "/work/hello.go", []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	f, err := ms.Open(ctx, "/work/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	_ = f.Close()
	if err != nil || fi.Name != "hello.go" {
		t.Fatalf("Open/Stat handle: %+v err=%v", fi, err)
	}
	// nested is ro and covers /work/nested/*
	if err := ms.WriteFile(ctx, "/work/nested/x.txt", []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro nested write: %v", err)
	}
	// parent still writable beside nested
	if err := ms.WriteFile(ctx, "/work/other.txt", []byte("ok")); err != nil {
		t.Fatal(err)
	}

	b, err := ms.ReadFile(ctx, "/work/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("ReadFile = %q err=%v", b, err)
	}
	info, err := ms.Stat(ctx, "/work/hello.go")
	if err != nil || info.IsDir || info.Name != "hello.go" {
		t.Fatalf("Stat = %+v err=%v", info, err)
	}

	if err := ms.MkdirAll(ctx, "/work/sub/dir"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/sub/dir/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	ents, err := ms.ReadDir(ctx, "/work/sub/dir")
	if err != nil || len(ents) != 1 || ents[0].Name != "a.txt" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}
	if err := ms.Remove(ctx, "/work/sub/dir/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Stat(ctx, "/work/sub/dir/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	// Segment boundary: /ab must not claim /a
	mi, rel, err := ms.Lookup("/a/file")
	if err != nil {
		if !errors.Is(err, vfs.ErrNotMounted) {
			t.Fatalf("lookup /a: %v", err)
		}
	} else if mi.Point == "/ab" {
		t.Fatalf("segment bleed: %+v rel=%q", mi, rel)
	}
	mi, rel, err = ms.Lookup("/ab/x")
	if err != nil || mi.Point != "/ab" || rel != "x" {
		t.Fatalf("lookup /ab: %+v rel=%q err=%v", mi, rel, err)
	}

	// Escape / missing
	if err := ms.WriteFile(ctx, "/work/foo/../../etc/passwd", []byte("x")); err == nil {
		t.Fatal("escape write: want error")
	}
	if _, err := ms.ReadFile(ctx, "/nosuch/x"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("no mount: %v", err)
	}

	// Lifecycle: detach, attach, rematerialize from Specs (restart)
	if err := ms.Unmount("/ab"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/keep", Profile: "scratch", Params: map[string]string{"subpath": "keep"}}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/keep/f.txt", []byte("k")); err != nil {
		t.Fatal(err)
	}
	// failed materialize must leave tree
	if err := ms.Materialize(ctx, []vfs.MountSpec{{Point: "/z", Profile: "missing"}}); err == nil {
		t.Fatal("want materialize failure")
	}
	if _, err := ms.ReadFile(ctx, "/keep/f.txt"); err != nil {
		t.Fatalf("tree should remain after failed materialize: %v", err)
	}

	specs := ms.Specs()
	// Specs must not expose host paths (only point/profile/params)
	for _, s := range specs {
		if s.Point == "" || s.Profile == "" {
			t.Fatalf("bad spec: %+v", s)
		}
	}
	ms2 := vfs.NewMountSession("sess-1", reg)
	if err := ms2.Materialize(ctx, specs); err != nil {
		t.Fatal(err)
	}
	b, err = ms2.ReadFile(ctx, "/keep/f.txt")
	if err != nil || string(b) != "k" {
		t.Fatalf("after rematerialize: %q err=%v", b, err)
	}

	// Empty materialize clears the table
	if err := ms.Materialize(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if len(ms.Specs()) != 0 {
		t.Fatalf("empty materialize left specs: %+v", ms.Specs())
	}

	// Concurrent mount churn (race detector)
	_ = ms.Materialize(ctx, []vfs.MountSpec{{Point: "/work", Profile: "scratch"}})
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			point := "/c" + strconv.Itoa(i)
			_ = ms.Mount(ctx, vfs.MountSpec{
				Point:   point,
				Profile: "scratch",
				Params:  map[string]string{"subpath": "c" + strconv.Itoa(i)},
			})
			_ = ms.Specs()
			_ = ms.Unmount(point)
		}(i)
	}
	wg.Wait()

	// Context cancel surfaces on ops
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.ReadFile(cctx, "/work/x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

// TestS3Factory_rejectsBadConfig covers factory validation without a live store.
func TestS3Factory_rejectsBadConfig(t *testing.T) {
	ctx := t.Context()
	if _, err := (vfs.S3Factory{ID: "s3"}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("nil client")
	}
	// Client is non-nil but bucket/prefix still validated before any API call.
	if _, err := (vfs.S3Factory{ID: "s3", Client: vfs.AWSS3{}}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("missing bucket")
	}
	if _, err := (vfs.S3Factory{ID: "s3", Client: vfs.AWSS3{}, DefaultBucket: "b"}).Open(ctx, "s", vfs.MountSpec{
		Params: map[string]string{"prefix": "a/../b"},
	}); err == nil {
		t.Fatal("bad prefix")
	}
}

// TestLocalFactory_rejectsUnsafeConfig covers factory construction failures that
// would otherwise put unsafe roots into the tree.
func TestLocalFactory_rejectsUnsafeConfig(t *testing.T) {
	ctx := t.Context()
	f := vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}
	if _, err := f.Open(ctx, "s", vfs.MountSpec{Params: map[string]string{"subpath": ".."}}); err == nil {
		t.Fatal("subpath ..")
	}
	if _, err := (vfs.LocalFactory{ID: "x", Base: "rel"}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("relative base")
	}
	file, err := os.CreateTemp(t.TempDir(), "notdir")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := (vfs.LocalFactory{ID: "f", Base: file.Name()}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("file as base")
	}
	if _, err := vfs.NewLocalProvider("relative"); err == nil {
		t.Fatal("NewLocalProvider relative")
	}
	// Symlink escape through provider (when OS allows symlinks)
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "leak")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	p, err := vfs.NewLocalProvider(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.OpenFile(ctx, "leak", os.O_RDONLY, 0); err == nil {
		t.Fatal("symlink escape: want error")
	}
	// Exclusive create already exists
	wf, err := p.OpenFile(ctx, "e", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_ = wf.Close()
	if _, err := p.OpenFile(ctx, "e", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, vfs.ErrExist) {
		t.Fatalf("excl: %v", err)
	}
}

// TestDetectMediaType covers extension map, byte sniff, and binary rejection.
func TestDetectMediaType(t *testing.T) {
	if mt := vfs.DetectMediaType("/a/main.go", nil); mt != "text/x-go" {
		t.Fatalf("extension: %s", mt)
	}
	if mt := vfs.DetectMediaType("/a/unknown", []byte("hello world\n")); mt != "text/plain" {
		t.Fatalf("utf8 sniff: %s", mt)
	}
	png := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}
	if mt := vfs.DetectMediaType("/a/x", png); mt != "image/png" {
		t.Fatalf("png sniff: %s", mt)
	}
	if mt := vfs.DetectMediaType("/a/x", []byte{0xff, 0xfe, 0xfd}); mt == "text/plain" {
		t.Fatalf("invalid utf8 as text: %s", mt)
	}
}

// TestMergeSpecs covers the host/harness helper used at session construct.
func TestMergeSpecs(t *testing.T) {
	merged, err := vfs.MergeSpecs(
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
		[]vfs.MountSpec{{Point: "/b", Profile: "p"}},
	)
	if err != nil || len(merged) != 2 {
		t.Fatalf("merge: %v %v", merged, err)
	}
	if _, err := vfs.MergeSpecs(
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
	); !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("duplicate: %v", err)
	}
}

// TestMountSession_configErrors covers unknown profile and invalid paths once each.
func TestMountSession_configErrors(t *testing.T) {
	ctx := t.Context()
	reg := vfs.NewBackendRegistry()
	_ = reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()})
	ms := vfs.NewMountSession("s", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/x", Profile: "nope"}); !errors.Is(err, vfs.ErrUnknownProfile) {
		t.Fatalf("unknown profile: %v", err)
	}
	if _, _, err := ms.Lookup(""); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("empty path: %v", err)
	}
	if _, _, err := ms.Lookup("/has\x00x"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("nul path: %v", err)
	}
	if err := reg.Register(nil); err == nil {
		t.Fatal("register nil factory")
	}
}
