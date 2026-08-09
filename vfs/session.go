package vfs

import (
	"context"
	"fmt"
	"sync"
)

// MountSession is the session-owned virtual filesystem mount table.
//
// Hosts manage attach/detach here — not on the agent harness. One MountSession
// maps to one durable session id. BackendRegistry is process-scoped (pools);
// this type only holds the live per-session tree.
//
// Specs() is what the harness checkpoints; restarts Materialize the same specs.
type MountSession struct {
	mu  sync.Mutex
	id  string
	reg *BackendRegistry
	fs  *FS
}

// NewMountSession binds a session id to a process registry.
func NewMountSession(sessionID string, reg *BackendRegistry) *MountSession {
	return &MountSession{id: sessionID, reg: reg, fs: New()}
}

// tree returns the live FS pointer under the session lock.
func (m *MountSession) tree() *FS {
	m.mu.Lock()
	fs := m.fs
	m.mu.Unlock()
	return fs
}

// Materialize replaces the live tree from specs. On error the previous tree is kept.
func (m *MountSession) Materialize(ctx context.Context, specs []MountSpec) error {
	if m == nil {
		return fmt.Errorf("vfs: nil mount session")
	}
	if len(specs) == 0 {
		m.mu.Lock()
		m.fs = New()
		m.mu.Unlock()
		return nil
	}
	if m.reg == nil {
		return fmt.Errorf("vfs: registry required")
	}
	fs, err := Materialize(ctx, m.reg, m.id, specs)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.fs = fs
	m.mu.Unlock()
	return nil
}

// Mount attaches a backend at spec.Point for the rest of the session life.
func (m *MountSession) Mount(ctx context.Context, spec MountSpec) error {
	if m == nil {
		return fmt.Errorf("vfs: nil mount session")
	}
	if m.reg == nil {
		return fmt.Errorf("vfs: registry required")
	}
	p, err := m.reg.Open(ctx, m.id, spec)
	if err != nil {
		return err
	}
	return m.tree().Mount(ctx, spec, p)
}

// Unmount detaches the mount at point for the rest of the session life.
func (m *MountSession) Unmount(point string) error {
	if m == nil {
		return fmt.Errorf("vfs: nil mount session")
	}
	return m.tree().Unmount(point)
}

// Specs returns the durable mount table (checkpoint-safe).
func (m *MountSession) Specs() []MountSpec {
	if m == nil {
		return nil
	}
	return m.tree().Specs()
}

// Infos returns agent-safe mount points.
func (m *MountSession) Infos() []MountInfo {
	if m == nil {
		return nil
	}
	return m.tree().Mounts()
}

// Lookup resolves a virtual path against the session mount table.
func (m *MountSession) Lookup(virtualPath string) (MountInfo, string, error) {
	if m == nil {
		return MountInfo{}, "", fmt.Errorf("vfs: nil mount session")
	}
	return m.tree().Lookup(virtualPath)
}
