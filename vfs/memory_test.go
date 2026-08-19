package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMemoryFactory_fileAndDirOps(t *testing.T) {
	ctx := context.Background()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(&vfs.MemoryFactory{ID: "mem"}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("mem-1", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/mem", Profile: "mem"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/mem/a/hello.txt", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/mem/a/hello.txt")
	if err != nil || string(got) != "hi" {
		t.Fatalf("read = %q err=%v", got, err)
	}
	ents, err := ms.ReadDir(ctx, "/mem/a")
	if err != nil || len(ents) != 1 || ents[0].Name != "hello.txt" {
		t.Fatalf("readdir = %+v err=%v", ents, err)
	}
	st, err := ms.Stat(ctx, "/mem/a")
	if err != nil || !st.IsDir {
		t.Fatalf("stat dir = %+v err=%v", st, err)
	}
	if err := ms.MkdirAll(ctx, "/mem/b/c"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Remove(ctx, "/mem/a/hello.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/mem/a/hello.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("removed file: %v", err)
	}
	if err := ms.Remove(ctx, "/mem/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}

	// Same factory reuses the session provider.
	p, err := (&vfs.MemoryFactory{ID: "x"}).Open(ctx, "s", vfs.MountSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.OpenFile(ctx, "dir", os.O_CREATE, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := p.MkdirAll(ctx, "dir", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := p.OpenFile(ctx, "dir", os.O_RDONLY, 0); !errors.Is(err, vfs.ErrIsDir) {
		t.Fatalf("open dir: %v", err)
	}
	if _, err := p.OpenFile(ctx, "nope", os.O_RDONLY, 0); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("open missing: %v", err)
	}
	wf, err := p.OpenFile(ctx, "w.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := wf.(io.Reader); ok {
		if n, err := wf.(io.Reader).Read(make([]byte, 1)); n != 0 && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("read on write: %d %v", n, err)
		}
	}
	w, ok := wf.(io.Writer)
	if !ok {
		t.Fatal("write handle")
	}
	if n, err := w.Write([]byte("ab")); err != nil || n != 2 {
		t.Fatalf("write: %d %v", n, err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}
	rf, err := p.OpenFile(ctx, "w.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := rf.(io.Writer); ok {
		if _, err := w.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("write on read: %v", err)
		}
	}
	r, ok := rf.(io.Reader)
	if !ok {
		t.Fatal("read handle")
	}
	buf, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(buf, []byte("ab")) {
		t.Fatalf("readback = %q err=%v", buf, err)
	}
	_ = rf.Close()
	if _, err := p.ReadDir(ctx, "w.txt"); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("readdir file: %v", err)
	}
	if err := p.Remove(ctx, "dir"); err != nil {
		t.Fatal(err)
	}
	f := &vfs.MemoryFactory{ID: "reuse"}
	p1, err := f.Open(ctx, "s", vfs.MountSpec{})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := f.Open(ctx, "s", vfs.MountSpec{})
	if err != nil || p1 != p2 {
		t.Fatalf("reuse: %v %p %p", err, p1, p2)
	}
	if err := p1.MkdirAll(ctx, "onlydir", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.ReadDir(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := p1.Remove(ctx, "onlydir"); err != nil {
		t.Fatal(err)
	}
}
