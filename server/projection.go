package server

import "github.com/ryanaldo34/tacklr/vfs"

// VFSProjection publishes a MountSession to the host.
type VFSProjection = vfs.Projection

// FuseProjection mounts the session via vfs.MountSession.FuseMount.
type FuseProjection = vfs.FuseProjection

// DirectProjection attaches VFS in-process with no kernel mount.
type DirectProjection = vfs.DirectProjection
