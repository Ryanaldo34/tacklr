package vfs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestLocal_aSymlinkReportsItsTargetsVersion(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(dir)))(ctx, "sess-local-version", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/w/a.txt").WriteFile(ctx, []byte("target")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "a.txt"), filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}

	target, err := ms.Route(ctx, "/workspace/w/a.txt").Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	link, err := ms.Route(ctx, "/workspace/w/link.txt").Stat(ctx)

	if err != nil || link.Name != "link.txt" || link.Version != target.Version || link.Size != target.Size {
		t.Fatalf("symlink stat = %+v, target %+v, err=%v", link, target, err)
	}
}
