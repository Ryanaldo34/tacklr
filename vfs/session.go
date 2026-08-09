package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// MaxReadFileBytes caps ReadFile to avoid unbounded memory use.
const MaxReadFileBytes = 32 << 20 // 32 MiB

// MountSession is the session-owned virtual filesystem: mount table + path I/O.
//
// Hosts attach/detach mounts here (not on the agent harness). BackendRegistry is
// process-scoped; this type holds the live per-session tree. Specs() is what the
// harness checkpoints.
//
// The underlying mount table is unexported — callers only see Specs, Infos, and path ops.
type MountSession struct {
	mu  sync.Mutex
	id  string
	reg *BackendRegistry
	tab *mountTable
}

// NewMountSession binds a session id to a process registry.
func NewMountSession(sessionID string, reg *BackendRegistry) *MountSession {
	return &MountSession{id: sessionID, reg: reg, tab: newMountTable()}
}

func (m *MountSession) table() *mountTable {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tab
}

// Materialize replaces the live tree from specs. On error the previous tree is kept.
func (m *MountSession) Materialize(ctx context.Context, specs []MountSpec) error {
	if m == nil {
		return errNilMountSession
	}
	if len(specs) == 0 {
		m.mu.Lock()
		m.tab = newMountTable()
		m.mu.Unlock()
		return nil
	}
	if m.reg == nil {
		return errRegistryRequired
	}
	next, err := materialize(ctx, m.reg, m.id, specs)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.tab = next
	m.mu.Unlock()
	return nil
}

// Mount attaches a backend at spec.Point for the rest of the session life.
func (m *MountSession) Mount(ctx context.Context, spec MountSpec) error {
	if m == nil {
		return errNilMountSession
	}
	if m.reg == nil {
		return errRegistryRequired
	}
	p, err := m.reg.open(ctx, m.id, spec)
	if err != nil {
		return err
	}
	return m.table().mount(ctx, spec, p)
}

// Unmount detaches the mount at point.
func (m *MountSession) Unmount(point string) error {
	if m == nil {
		return errNilMountSession
	}
	return m.table().unmount(point)
}

// Specs returns the durable mount table (checkpoint-safe; no host paths or secrets).
func (m *MountSession) Specs() []MountSpec {
	if m == nil {
		return nil
	}
	return m.table().specs()
}

// Infos returns agent-safe mount points (point + read-only only).
func (m *MountSession) Infos() []MountInfo {
	if m == nil {
		return nil
	}
	return m.table().infos()
}

// Lookup resolves a virtual path (no provider or host path exposure).
func (m *MountSession) Lookup(virtualPath string) (MountInfo, string, error) {
	if m == nil {
		return MountInfo{}, "", errNilMountSession
	}
	return m.table().lookup(virtualPath)
}

func (m *MountSession) at(ctx context.Context, virtualPath string, write bool) (Provider, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if m == nil {
		return nil, "", errNilMountSession
	}
	p, _, rel, ro, err := m.table().resolve(virtualPath)
	if err != nil {
		return nil, "", err
	}
	if write && ro {
		return nil, "", ErrReadOnly
	}
	return p, rel, nil
}

// Stat returns info for a virtual path.
func (m *MountSession) Stat(ctx context.Context, virtualPath string) (FileInfo, error) {
	p, rel, err := m.at(ctx, virtualPath, false)
	if err != nil {
		return FileInfo{}, err
	}
	return p.Stat(ctx, rel)
}

// Open opens a virtual path for reading.
func (m *MountSession) Open(ctx context.Context, virtualPath string) (File, error) {
	p, rel, err := m.at(ctx, virtualPath, false)
	if err != nil {
		return nil, err
	}
	return p.OpenFile(ctx, rel, os.O_RDONLY, 0)
}

// ReadFile reads an entire file (capped at MaxReadFileBytes).
func (m *MountSession) ReadFile(ctx context.Context, virtualPath string) ([]byte, error) {
	f, err := m.Open(ctx, virtualPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, int64(MaxReadFileBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, fmt.Errorf("vfs: file exceeds %d bytes", MaxReadFileBytes)
	}
	return data, nil
}

// WriteFile creates or truncates a file (fails on read-only mounts).
func (m *MountSession) WriteFile(ctx context.Context, virtualPath string, data []byte) error {
	p, rel, err := m.at(ctx, virtualPath, true)
	if err != nil {
		return err
	}
	f, err := p.OpenFile(ctx, rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}

// ReadDir lists a directory.
func (m *MountSession) ReadDir(ctx context.Context, virtualPath string) ([]DirEntry, error) {
	p, rel, err := m.at(ctx, virtualPath, false)
	if err != nil {
		return nil, err
	}
	return p.ReadDir(ctx, rel)
}

// Remove removes a file or empty directory.
func (m *MountSession) Remove(ctx context.Context, virtualPath string) error {
	p, rel, err := m.at(ctx, virtualPath, true)
	if err != nil {
		return err
	}
	return p.Remove(ctx, rel)
}

// MkdirAll creates a directory and parents.
func (m *MountSession) MkdirAll(ctx context.Context, virtualPath string) error {
	p, rel, err := m.at(ctx, virtualPath, true)
	if err != nil {
		return err
	}
	return p.MkdirAll(ctx, rel, 0o755)
}
