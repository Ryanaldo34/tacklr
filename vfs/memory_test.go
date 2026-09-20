package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMemoryFactory_fileAndDirOps(t *testing.T) {
	ctx := context.Background()
	ms, err := vfs.Tree(vfs.At("mem", builtins.Memory()))(ctx, "mem-1", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.WriteFile(ctx, "/workspace/mem/a/hello.txt", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/mem/a/hello.txt")
	if err != nil || string(got) != "hi" {
		t.Fatalf("read = %q err=%v", got, err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/mem/a")
	if err != nil || len(ents) != 1 || ents[0].Name != "hello.txt" {
		t.Fatalf("readdir = %+v err=%v", ents, err)
	}
	st, err := ms.Stat(ctx, "/workspace/mem/a")
	if err != nil || !st.IsDir {
		t.Fatalf("stat dir = %+v err=%v", st, err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/mem/b/c"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Remove(ctx, "/workspace/mem/a/hello.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/mem/a/hello.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("removed file: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/mem/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}

	// Same factory reuses the session provider.
	p, err := builtins.Memory()(ctx, "s", vfs.Binding{})
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
	if _, err := p.Stat(ctx, "nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("stat missing: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/mem/w.txt", []byte("ab")); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/mem/w.txt")
	if err != nil || !bytes.Equal(got, []byte("ab")) {
		t.Fatalf("readback = %q err=%v", got, err)
	}
	if _, err := p.ReadDir(ctx, "w.txt"); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("readdir file: %v", err)
	}
	if err := p.Remove(ctx, "dir"); err != nil {
		t.Fatal(err)
	}
	open := builtins.Memory()
	p1, err := open(ctx, "s", vfs.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := open(ctx, "s", vfs.Binding{})
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
