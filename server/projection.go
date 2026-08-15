package server

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ryanaldo34/tacklr/vfs"
)

// VFSProjection publishes a MountSession to the host.
//
// Production injects FuseProjection (kernel tree). Tests inject
// DirectProjection (in-process only; no /dev/fuse). If Available is false,
// Registry does not create a MountSession and VFS tools are not injected.
type VFSProjection interface {
	Available() bool
	Attach(ms *vfs.MountSession, sessionID string) error
}

// FuseProjection mounts the session via vfs.MountSession.FuseMount.
type FuseProjection struct{}

// Available reports whether this process can mount a FUSE tree.
func (FuseProjection) Available() bool { return vfs.FuseAvailable() }

// Attach projects ms under /tmp/tacklr-fuse/<sessionID>, retrying once with a -1 suffix.
func (FuseProjection) Attach(ms *vfs.MountSession, sessionID string) error {
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
// Use from tests (WithVFSProjection) so tools work without /dev/fuse.
type DirectProjection struct{}

// Available is always true.
func (DirectProjection) Available() bool { return true }

// Attach is a no-op: the MountSession is already the agent-facing tree.
func (DirectProjection) Attach(*vfs.MountSession, string) error { return nil }

func mountFuseAt(ms *vfs.MountSession, dir string, attempted *[]string) error {
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
	_ VFSProjection = FuseProjection{}
	_ VFSProjection = DirectProjection{}
)

func (r *Registry) vfsProjection() VFSProjection {
	if r == nil || r.projection == nil {
		return FuseProjection{}
	}
	return r.projection
}
