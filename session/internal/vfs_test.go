package adapter

import (
	"os"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestOpenTurnVFS_nilOpenAndInProcessTree(t *testing.T) {
	ms, err := OpenTurnVFS(t.Context(), "s", tacklr.AgentOptions{}, nil, nil)
	if err != nil || ms != nil {
		t.Fatalf("no OpenVFS: %v %v", ms, err)
	}
	ms, err = OpenTurnVFS(t.Context(), "s", tacklr.AgentOptions{OpenVFS: vfs.Tree(vfs.At("scratch", vfs.Local(t.TempDir())))}, nil, nil)
	if err != nil || ms == nil {
		t.Fatalf("in-process tree: %v %v", ms, err)
	}
	CloseTurnVFS(ms)
	CloseTurnVFS(nil)
}

type failAttach struct{ err error }

func (f failAttach) Attach(*vfs.MountSession, string) error { return f.err }

func TestOpenTurnVFS_attachError(t *testing.T) {
	_, err := OpenTurnVFS(t.Context(), "s", tacklr.AgentOptions{
		OpenVFS: vfs.Tree(vfs.At("scratch", vfs.Local(t.TempDir()))),
	}, nil, failAttach{err: os.ErrPermission})
	if err == nil {
		t.Fatal("want attach error")
	}
}
