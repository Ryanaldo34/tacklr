package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestLocalProvider_validateAndMount(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	p := vfs.LocalProvider{Root: dir}
	if err := p.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	fs := vfs.New()
	if err := fs.Mount(ctx, "/workspace", p, false); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	info, rel, err := fs.Lookup("/workspace/src/main.go")
	if err != nil || info.Point != "/workspace" || rel != "src/main.go" {
		t.Fatalf("Lookup = %+v rel=%q err=%v", info, rel, err)
	}
}

func TestLocalProvider_validateFailures(t *testing.T) {
	ctx := t.Context()

	for _, root := range []string{"", "relative/path"} {
		if err := (vfs.LocalProvider{Root: root}).Validate(ctx); err == nil {
			t.Fatalf("root %q: want error", root)
		}
	}

	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if err := (vfs.LocalProvider{Root: missing}).Validate(ctx); err == nil {
		t.Fatal("missing root: want error")
	}

	f, err := os.CreateTemp(t.TempDir(), "notdir")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	_ = f.Close()
	if err := (vfs.LocalProvider{Root: name}).Validate(ctx); err == nil {
		t.Fatal("file root: want error")
	}

	fs := vfs.New()
	if err := fs.Mount(ctx, "/x", vfs.LocalProvider{Root: missing}, false); !errors.Is(err, vfs.ErrInvalidProvider) {
		t.Fatalf("Mount bad local: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := (vfs.LocalProvider{Root: t.TempDir()}).Validate(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Validate canceled: %v", err)
	}
}
