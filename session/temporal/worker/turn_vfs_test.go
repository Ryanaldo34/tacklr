package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/workspace"
)

// TestOpenWorkspace_showsTheTreeOnTheHostUntilClosed: with a FuseRoot the
// turn's tree shows under a host directory for the session, and closing the
// workspace unmounts and removes it. A FuseRoot that cannot hold a mount
// fails the open.
func TestOpenWorkspace_showsTheTreeOnTheHostUntilClosed(t *testing.T) {
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "note.md"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	ws := workspace.Config{Open: vfs.Tree(vfs.At("work", vfs.Local(work)))}
	root := t.TempDir()
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	_, srv, err := openWorkspace(t.Context(), "sess/fuse", ws, nil, root)
	if err != nil {
		t.Fatal(err)
	}
	dir := srv.Dir()
	seen, readErr := os.ReadFile(filepath.Join(dir, "workspace", "work", "note.md"))
	closeWorkspace(srv)
	_, statErr := os.Stat(dir)
	_, _, mountErr := openWorkspace(t.Context(), "s", ws, nil, notDir)

	if dir != filepath.Join(root, "sess_fuse") {
		t.Fatalf("host dir = %q, want the session's directory under %s", dir, root)
	}
	if readErr != nil || string(seen) != "hello" {
		t.Fatalf("host read %q %v, want the tree's file under %s", seen, readErr, dir)
	}
	if !os.IsNotExist(statErr) {
		t.Fatalf("after close stat = %v, want the host directory gone", statErr)
	}
	if mountErr == nil {
		t.Fatal("want the mount's error")
	}
}
