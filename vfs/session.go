package vfs

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"sync"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
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
// (WriteFile or WriteDocument). Used by optional bridges (e.g. vfsindex) without
// importing them. Errors from the hook are ignored so persist never rolls back.
type AfterPersistFunc func(ctx context.Context, virtualPath string) error

// MountSession is the session-owned virtual filesystem: mount table + path I/O.
// It routes document I/O to the provider; it does not encode IR or cache dirty
// documents. Hosts attach/detach mounts here (not on the agent harness).
// BackendRegistry is process-scoped; this type holds the live per-session tree.
type MountSession struct {
	mu           sync.Mutex
	id           string
	reg          *BackendRegistry
	tab          *mountTable
	afterPersist AfterPersistFunc
	fuse         *gofuse.Server
	hostDir      string
}

// SetAfterPersist registers a hook after successful backend writes.
// Pass nil to clear. Safe to call at any time; concurrent with I/O.
// Compose with GetAfterPersist when layering (e.g. host hook + vfsindex).
func (m *MountSession) SetAfterPersist(fn AfterPersistFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.afterPersist = fn
}

// GetAfterPersist returns the current AfterPersist hook, or nil.
func (m *MountSession) GetAfterPersist() AfterPersistFunc {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.afterPersist
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
// Hosts that want a kernel tree call FuseMount.
func NewMountSession(sessionID string, reg *BackendRegistry) (*MountSession, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("%w: session id is required", ErrInvalidPath)
	}
	if reg == nil {
		return nil, errRegistryRequired
	}
	return &MountSession{id: sessionID, reg: reg, tab: newMountTable()}, nil
}

// MustNewMountSession is for tests. It panics if NewMountSession fails.
func MustNewMountSession(sessionID string, reg *BackendRegistry) *MountSession {
	ms, err := NewMountSession(sessionID, reg)
	if err != nil {
		panic(err)
	}
	return ms
}

// HostDir is the directory last passed to FuseMount, or "".
// Hosts and run_command use this as cwd. Harness tool results, errors,
// Specs, and checkpoints must never print it. The child
// process can still observe it via pwd until a later jail.
func (m *MountSession) HostDir() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hostDir
}

func (m *MountSession) table() *mountTable {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tab
}

// Materialize replaces the live tree from specs. On error the previous tree is kept.
func (m *MountSession) Materialize(ctx context.Context, specs []MountSpec) error {
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
	return m.table().unmount(point)
}

// Specs returns the durable mount table (checkpoint-safe; no host paths or secrets).
func (m *MountSession) Specs() []MountSpec {
	return m.table().specs()
}

// SpecAt returns the durable MountSpec for the mount that owns virtualPath
// (longest matching point). Clone is safe to retain; no secrets.
func (m *MountSession) SpecAt(virtualPath string) (MountSpec, error) {
	e, _, _, err := m.table().resolveEntry(virtualPath)
	if err != nil {
		return MountSpec{}, err
	}
	return cloneSpec(e.spec), nil
}

// Classify returns the media type for virtualPath.
// Existing files use Stat.MediaType. New names use DetectMediaType.
func (m *MountSession) Classify(ctx context.Context, virtualPath string, sample []byte) (string, error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return "", err
	}
	if fi, err := m.Stat(ctx, cleaned); err == nil && !fi.IsDir && fi.MediaType != "" {
		return fi.MediaType, nil
	} else if err != nil && !errors.Is(err, ErrNotExist) {
		return "", err
	}
	// Stat NotExist already resolved a mount; classify the new name from bytes.
	return DetectMediaType(path.Base(cleaned), sample), nil
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
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return FileInfo{}, err
	}
	p, rel, err := m.at(ctx, cleaned, false)
	if err != nil {
		return FileInfo{}, err
	}
	return p.Stat(ctx, rel)
}

// Open opens a virtual path for reading.
func (m *MountSession) Open(ctx context.Context, virtualPath string) (File, error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return nil, err
	}
	p, rel, err := m.at(ctx, cleaned, false)
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
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return nil, err
	}

	f, err := m.Open(ctx, cleaned)
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
		r, ok := f.(io.Reader)
		if !ok {
			return nil, fmt.Errorf("vfs: file is not readable")
		}
		data := make([]byte, fi.Size)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		return data, nil
	}

	r, ok := f.(io.Reader)
	if !ok {
		return nil, fmt.Errorf("vfs: file is not readable")
	}
	data, err := io.ReadAll(io.LimitReader(r, int64(MaxReadFileBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	return data, nil
}

// WriteFile creates or truncates a file (fails on read-only mounts).
// Write-through to the backend.
func (m *MountSession) WriteFile(ctx context.Context, virtualPath string, data []byte) error {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return err
	}
	if err := m.writeContents(ctx, cleaned, bytes.NewReader(data), int64(len(data))); err != nil {
		return err
	}
	m.fireAfterPersist(ctx, cleaned)
	return nil
}

// writeContents writes exactly size bytes from r to virtualPath (must be cleaned).
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
	w, ok := f.(io.Writer)
	if !ok {
		return ErrReadOnly
	}
	if _, err := io.Copy(w, io.LimitReader(r, size)); err != nil {
		return err
	}
	return nil
}

// ReadDir lists a directory.
func (m *MountSession) ReadDir(ctx context.Context, virtualPath string) ([]DirEntry, error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return nil, err
	}
	p, rel, err := m.at(ctx, cleaned, false)
	if err != nil {
		return nil, err
	}
	return p.ReadDir(ctx, rel)
}

// Remove removes a file or empty directory.
func (m *MountSession) Remove(ctx context.Context, virtualPath string) error {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return err
	}
	p, rel, err := m.at(ctx, cleaned, true)
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

// mountTable is the session mount namespace. Hosts use MountSession only.
type mountTable struct {
	mu     sync.RWMutex
	mounts map[string]mountEntry // key: cleaned virtual mount point
}

type mountEntry struct {
	provider Provider
	spec     MountSpec
}

func newMountTable() *mountTable {
	return &mountTable{mounts: make(map[string]mountEntry)}
}

func (t *mountTable) mount(ctx context.Context, spec MountSpec, provider Provider) error {
	if provider == nil || strings.TrimSpace(spec.Profile) == "" {
		return ErrInvalidProvider
	}
	cleaned, err := cleanVirtualPath(spec.Point)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := provider.Validate(ctx); err != nil {
		return err
	}

	stored := cloneSpec(spec)
	stored.Point = cleaned

	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.mounts[cleaned]; exists {
		return ErrAlreadyMounted
	}
	t.mounts[cleaned] = mountEntry{provider: provider, spec: stored}
	return nil
}

func (t *mountTable) unmount(point string) error {
	cleaned, err := cleanVirtualPath(point)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.mounts[cleaned]; !exists {
		return ErrNotMounted
	}
	delete(t.mounts, cleaned)
	return nil
}

func (t *mountTable) specs() []MountSpec {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]MountSpec, 0, len(t.mounts))
	for _, e := range t.mounts {
		out = append(out, cloneSpec(e.spec))
	}
	slices.SortFunc(out, func(a, b MountSpec) int { return cmp.Compare(a.Point, b.Point) })
	return out
}

func (t *mountTable) resolveEntry(virtualPath string) (e mountEntry, point, rel string, err error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return mountEntry{}, "", "", err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	var bestPoint string
	var best mountEntry
	found := false
	for mp, ent := range t.mounts {
		if mp != "/" && cleaned != mp && !strings.HasPrefix(cleaned, mp+"/") {
			continue
		}
		if !found || len(mp) > len(bestPoint) {
			bestPoint, best, found = mp, ent, true
		}
	}
	if !found {
		return mountEntry{}, "", "", ErrNotMounted
	}
	rel = strings.TrimPrefix(strings.TrimPrefix(cleaned, bestPoint), "/")
	return best, bestPoint, rel, nil
}

func (t *mountTable) resolve(virtualPath string) (p Provider, point, rel string, readOnly bool, err error) {
	e, point, rel, err := t.resolveEntry(virtualPath)
	if err != nil {
		return nil, "", "", false, err
	}
	return e.provider, point, rel, e.spec.ReadOnly, nil
}

func cleanVirtualPath(s string) (string, error) {
	if s == "" || !path.IsAbs(s) || strings.ContainsAny(s, "\\\x00") {
		return "", ErrInvalidPath
	}
	return path.Clean(s), nil
}

func materialize(ctx context.Context, reg *BackendRegistry, sessionID string, specs []MountSpec) (*mountTable, error) {
	t := newMountTable()
	for _, spec := range specs {
		p, err := reg.open(ctx, sessionID, spec)
		if err != nil {
			return nil, err
		}
		if err := t.mount(ctx, spec, p); err != nil {
			return nil, err
		}
	}
	return t, nil
}
