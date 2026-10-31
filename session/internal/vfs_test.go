package adapter

import (
	"os"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/vfs"
)

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
