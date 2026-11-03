package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/vfs"
)

type failAttach struct{ err error }

func (f failAttach) Attach(*vfs.MountSession, string) error { return f.err }

// TestOpenTurnVFS_projectsTheTreeOnTheHostUntilClosed: the FUSE projection
// shows the turn's tree under a host directory, and closing the turn's VFS
// unmounts and removes it. A projection that fails fails the open.
func TestOpenTurnVFS_projectsTheTreeOnTheHostUntilClosed(t *testing.T) {
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "note.md"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	agent := tacklr.AgentOptions{OpenVFS: vfs.Tree(vfs.At("work", vfs.Local(work)))}

	ms, err := openTurnVFS(t.Context(), "sess/fuse", agent, nil, vfs.FuseProjection{})
	if err != nil {
		t.Fatal(err)
	}
	dir := ms.HostDir()
	seen, readErr := os.ReadFile(filepath.Join(dir, "workspace", "work", "note.md"))
	closeTurnVFS(ms)
	_, statErr := os.Stat(dir)
	_, attachErr := openTurnVFS(t.Context(), "s", agent, nil, failAttach{err: os.ErrPermission})

	if readErr != nil || string(seen) != "hello" {
		t.Fatalf("host read %q %v, want the tree's file under %s", seen, readErr, dir)
	}
	if !os.IsNotExist(statErr) {
		t.Fatalf("after close stat = %v, want the host directory gone", statErr)
	}
	if attachErr == nil {
		t.Fatal("want the projection's error")
	}
}
