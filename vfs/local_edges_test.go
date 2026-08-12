package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestLocalProvider_edges: cancel, remove, mkdir, write/read via provider API.
func TestLocalProvider_edges(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	p, err := vfs.NewLocalProvider(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.Validate(canceled); err == nil {
		t.Fatal("Validate canceled")
	}
	if _, err := p.Stat(canceled, "x"); err == nil {
		t.Fatal("Stat canceled")
	}
	if _, err := p.OpenFile(canceled, "x", os.O_RDONLY, 0); err == nil {
		t.Fatal("OpenFile canceled")
	}
	if _, err := p.ReadDir(canceled, "."); err == nil {
		t.Fatal("ReadDir canceled")
	}
	if err := p.Remove(canceled, "x"); err == nil {
		t.Fatal("Remove canceled")
	}
	if err := p.MkdirAll(canceled, "d", 0o755); err == nil {
		t.Fatal("MkdirAll canceled")
	}

	if err := p.MkdirAll(ctx, "a/b", 0o755); err != nil {
		t.Fatal(err)
	}
	wf, err := p.OpenFile(ctx, "a/b/f.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.Write([]byte("hi\n")); err != nil {
		t.Fatal(err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := p.Stat(ctx, "a/b/f.txt")
	if err != nil || st.IsDir || st.Size != 3 {
		t.Fatalf("Stat: %+v err=%v", st, err)
	}
	ents, err := p.ReadDir(ctx, "a/b")
	if err != nil || len(ents) != 1 {
		t.Fatalf("ReadDir: %+v err=%v", ents, err)
	}
	rf, err := p.OpenFile(ctx, "a/b/f.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	n, _ := rf.Read(buf)
	_ = rf.Close()
	if string(buf[:n]) != "hi\n" {
		t.Fatalf("read %q", buf[:n])
	}
	if err := p.Remove(ctx, "a/b/f.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Stat(ctx, "a/b/f.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}
	// escape attempt
	if _, err := p.OpenFile(ctx, "../outside", os.O_RDONLY, 0); err == nil {
		t.Fatal("path escape")
	}
	// ReadDir on file fails
	if err := os.WriteFile(filepath.Join(root, "onlyfile"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ReadDir(ctx, "onlyfile"); err == nil {
		t.Fatal("ReadDir file")
	}
}
