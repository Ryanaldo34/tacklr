package vfs

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Projection publishes a MountSession to the host.
//
// Production uses FuseProjection (kernel tree). Tests use DirectProjection
// (in-process only; no /dev/fuse). If Available is false, Runtime does not
// inject a MountSession and VFS tools are not added.
type Projection interface {
	Available() bool
	Attach(ms *MountSession, sessionID string) error
}

// FuseProjection mounts the session via MountSession.FuseMount.
type FuseProjection struct{}

// Available reports whether this process can mount a FUSE tree.
func (FuseProjection) Available() bool { return FuseAvailable() }

// Attach projects ms under /tmp/tacklr-fuse/<sessionID>, retrying once with a -1 suffix.
func (FuseProjection) Attach(ms *MountSession, sessionID string) error {
	base := filepath.Join(os.TempDir(), "tacklr-fuse", sanitizeFuseSessionID(sessionID))
	attempted := make([]string, 0, 2)
	err := mountFuseAt(ms, base, &attempted)
	if err != nil {
		_ = syscall.Unmount(base, 0)
		err = mountFuseAt(ms, base+"-1", &attempted)
	}
	if err != nil {
		for _, d := range attempted {
			_ = os.Remove(d)
		}
		return err
	}
	return nil
}

// DirectProjection attaches VFS in-process with no kernel mount.
type DirectProjection struct{}

// Available is always true.
func (DirectProjection) Available() bool { return true }

// Attach is a no-op: the MountSession is already the agent-facing tree.
func (DirectProjection) Attach(*MountSession, string) error { return nil }

func mountFuseAt(ms *MountSession, dir string, attempted *[]string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	*attempted = append(*attempted, dir)
	_ = syscall.Unmount(dir, 0)
	return ms.FuseMount(dir)
}

func sanitizeFuseSessionID(id string) string {
	if id == "" || id == "." || id == ".." {
		return "session"
	}
	if !strings.ContainsAny(id, `/\`) {
		return id
	}
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' {
			return '_'
		}
		return r
	}, id)
}

var (
	_ Projection = FuseProjection{}
	_ Projection = DirectProjection{}
)
