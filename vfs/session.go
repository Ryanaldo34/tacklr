package vfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
)

// MaxReadFileBytes caps full-file reads and writes.
const MaxReadFileBytes = 32 << 20 // 32 MiB

// MaxLineBytes caps a single line when streaming (ReadLines) or scanning.
const MaxLineBytes = 1 << 20 // 1 MiB

// See also MaxLineScanBytes and MaxLinesPerWindow in lines.go (streaming budgets,
// distinct from MaxReadFileBytes full-materialize cap).

// filePutter is implemented by providers that can write a full object in one shot
// (avoids S3 Open→buffer→Put double buffering).
type filePutter interface {
	PutFile(ctx context.Context, name string, r io.Reader, size int64) error
}

// AfterPersistFunc is called after content is successfully written to a backend
// (WriteFile or Sync). Used by optional bridges (e.g. vfsindex) without importing
// them. Errors from the hook are ignored so persist never rolls back.
type AfterPersistFunc func(ctx context.Context, virtualPath string) error

// MountSession is the session-owned virtual filesystem: mount table + path I/O
// plus a session-local textual IR cache (write-back until Sync).
//
// Hosts attach/detach mounts here (not on the agent harness). BackendRegistry is
// process-scoped; this type holds the live per-session tree. Specs() is what the
// harness checkpoints (mount table only — not file content).
//
// Dirty document edits flush via Sync / SyncAll (harness checkpoint calls SyncAll).
// Backend remains source of truth after a successful flush.
type MountSession struct {
	mu           sync.Mutex
	id           string
	reg          *BackendRegistry
	tab          *mountTable
	cache        *contentCache
	afterPersist AfterPersistFunc
}

// SetAfterPersist registers a hook after successful backend writes.
// Pass nil to clear. Safe to call at any time; concurrent with I/O.
func (m *MountSession) SetAfterPersist(fn AfterPersistFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.afterPersist = fn
}

func (m *MountSession) fireAfterPersist(ctx context.Context, virtualPath string) {
	m.mu.Lock()
	fn := m.afterPersist
	m.mu.Unlock()
	if fn != nil {
		_ = fn(ctx, virtualPath)
	}
}

// NewMountSession binds a session id to a process registry.
func NewMountSession(sessionID string, reg *BackendRegistry) *MountSession {
	return &MountSession{id: sessionID, reg: reg, tab: newMountTable(), cache: newContentCache()}
}

func (m *MountSession) table() *mountTable {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tab
}

// Materialize replaces the live tree from specs. On error the previous tree is kept.
// Clears the content cache (entries were bound to the previous mount set).
func (m *MountSession) Materialize(ctx context.Context, specs []MountSpec) error {
	if len(specs) == 0 {
		m.mu.Lock()
		m.tab = newMountTable()
		m.mu.Unlock()
		m.cache.clear()
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
	m.cache.clear()
	return nil
}

// Mount attaches a backend at spec.Point for the rest of the session life.
func (m *MountSession) Mount(ctx context.Context, spec MountSpec) error {
	if m.reg == nil {
		return errRegistryRequired
	}
	p, err := m.reg.open(ctx, m.id, spec)
	if err != nil {
		return err
	}
	return m.table().mount(ctx, spec, p)
}

// Unmount detaches the mount at point and drops cache entries under that point.
func (m *MountSession) Unmount(point string) error {
	if err := m.table().unmount(point); err != nil {
		return err
	}
	m.cache.removePrefix(point)
	return nil
}

// Specs returns the durable mount table (checkpoint-safe; no host paths or secrets).
func (m *MountSession) Specs() []MountSpec {
	return m.table().specs()
}

// Infos returns agent-safe mount points (point + read-only only).
func (m *MountSession) Infos() []MountInfo {
	return m.table().infos()
}

// Lookup resolves a virtual path (no provider or host path exposure).
func (m *MountSession) Lookup(virtualPath string) (MountInfo, string, error) {
	return m.table().lookup(virtualPath)
}

func (m *MountSession) at(ctx context.Context, virtualPath string, write bool) (Provider, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
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
//
// When File.Stat reports a size, the buffer is allocated once and oversize files
// are rejected without reading the body. Unknown sizes fall back to a limited
// streaming read.
func (m *MountSession) ReadFile(ctx context.Context, virtualPath string) ([]byte, error) {
	f, err := m.Open(ctx, virtualPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if fi, stErr := f.Stat(); stErr == nil && fi.Size >= 0 {
		if fi.Size > int64(MaxReadFileBytes) {
			return nil, errFileExceeds(MaxReadFileBytes)
		}
		if fi.Size == 0 {
			return []byte{}, nil
		}
		data := make([]byte, fi.Size)
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, err
		}
		return data, nil
	}

	data, err := io.ReadAll(io.LimitReader(f, int64(MaxReadFileBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	return data, nil
}

// WriteFile creates or truncates a file (fails on read-only mounts).
// Write-through to the backend, then drops any cached IR for the path.
func (m *MountSession) WriteFile(ctx context.Context, virtualPath string, data []byte) error {
	if err := m.writeContents(ctx, virtualPath, bytes.NewReader(data), int64(len(data))); err != nil {
		return err
	}
	m.cache.remove(virtualPath)
	m.fireAfterPersist(ctx, virtualPath)
	return nil
}

// writeContents writes exactly size bytes from r to virtualPath.
func (m *MountSession) writeContents(ctx context.Context, virtualPath string, r io.Reader, size int64) error {
	if size > int64(MaxReadFileBytes) {
		return errFileExceeds(MaxReadFileBytes)
	}
	p, rel, err := m.at(ctx, virtualPath, true)
	if err != nil {
		return err
	}
	if putter, ok := p.(filePutter); ok {
		return putter.PutFile(ctx, rel, r, size)
	}
	f, err := p.OpenFile(ctx, rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if size == 0 {
		return nil
	}
	n, err := io.Copy(f, io.LimitReader(r, size))
	if err != nil {
		return err
	}
	if n != size {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// ReadDir lists a directory.
func (m *MountSession) ReadDir(ctx context.Context, virtualPath string) ([]DirEntry, error) {
	p, rel, err := m.at(ctx, virtualPath, false)
	if err != nil {
		return nil, err
	}
	return p.ReadDir(ctx, rel)
}

// Remove removes a file or empty directory and drops any cache entry.
func (m *MountSession) Remove(ctx context.Context, virtualPath string) error {
	p, rel, err := m.at(ctx, virtualPath, true)
	if err != nil {
		return err
	}
	if err := p.Remove(ctx, rel); err != nil {
		return err
	}
	m.cache.remove(virtualPath)
	return nil
}

// MkdirAll creates a directory and parents.
func (m *MountSession) MkdirAll(ctx context.Context, virtualPath string) error {
	p, rel, err := m.at(ctx, virtualPath, true)
	if err != nil {
		return err
	}
	return p.MkdirAll(ctx, rel, 0o755)
}
