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

// Attach projects ms under /tmp/tacklr-fuse/<sessionID>.
func (FuseProjection) Attach(ms *MountSession, sessionID string) error {
	dir := filepath.Join(os.TempDir(), "tacklr-fuse", sanitizeFuseSessionID(sessionID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	_ = syscall.Unmount(dir, 0)
	if err := ms.FuseMount(dir); err != nil {
		_ = os.Remove(dir)
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

func sanitizeFuseSessionID(id string) string {
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
