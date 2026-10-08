package vfs

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Projection publishes a MountSession on the host kernel.
// Nil means the MountSession is the tree and nothing is mounted.
type Projection interface {
	Attach(ms *MountSession, sessionID string) error
}

// FuseProjection mounts the session via MountSession.FuseMount.
type FuseProjection struct{}

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

func sanitizeFuseSessionID(id string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' {
			return '_'
		}
		return r
	}, id)
}

var _ Projection = FuseProjection{}
