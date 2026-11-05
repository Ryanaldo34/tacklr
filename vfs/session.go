package vfs

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/vfs/mount"
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

// MountSession groups providers for one session. Route returns the provider
// for a path. The provider reads, writes, and returns the error.
// Tree creates the session and mounts /workspace. The agent harness only
// borrows the pointer.
type MountSession struct {
	mu      sync.Mutex
	id      string
	mounts  map[string]mountEntry
	lastRev map[string]string
	// codecs read and write the session's documents; set before the
	// session is shared (Tree, OpenVFS.WithCodecs) and not changed after.
	codecs *ContentRegistry
}

// Codecs is the registry the session reads and writes documents with.
func (m *MountSession) Codecs() *ContentRegistry {
	return m.codecs
}

type mountEntry struct {
	provider Provider
	spec     MountSpec
}

func (m *MountSession) rememberRev(virtualPath, hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastRev == nil {
		m.lastRev = map[string]string{}
	}
	m.lastRev[virtualPath] = hash
}

func (m *MountSession) storedRev(virtualPath string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastRev[virtualPath]
}

// NewMountSession binds a session id. Hosts use Tree.
func NewMountSession(sessionID string) (*MountSession, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("%w: session id is required", ErrInvalidPath)
	}
	return &MountSession{id: sessionID, mounts: map[string]mountEntry{}, codecs: builtinCodecs}, nil
}

// mount puts p at spec.Point, which Tree builds clean from a checked member
// name.
func (m *MountSession) mount(ctx context.Context, spec MountSpec, p Provider) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.Validate(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mounts[spec.Point] = mountEntry{provider: p, spec: cloneSpec(spec)}
	return nil
}

// Specs returns the durable mount records (checkpoint-safe; no host paths or secrets).
func (m *MountSession) Specs() []MountSpec {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MountSpec, 0, len(m.mounts))
	for _, e := range m.mounts {
		out = append(out, cloneSpec(e.spec))
	}
	slices.SortFunc(out, func(a, b MountSpec) int { return cmp.Compare(a.Point, b.Point) })
	return out
}

// SpecAt returns the MountSpec for the backend that owns virtualPath.
// Clone is safe to retain; no secrets.
func (m *MountSession) SpecAt(virtualPath string) (MountSpec, error) {
	rt, err := m.lookup(virtualPath)
	if err != nil {
		return MountSpec{}, err
	}
	if rt.Provider == nil {
		return MountSpec{}, ErrNotMounted
	}
	return cloneSpec(rt.Spec), nil
}

// memberEntries lists the /workspace/<name> backends. Tree mounts every
// backend at exactly that depth.
func (m *MountSession) memberEntries() []DirEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := mount.WorkspacePoint + "/"
	out := make([]DirEntry, 0, len(m.mounts))
	for mp := range m.mounts {
		out = append(out, DirEntry{Name: strings.TrimPrefix(mp, prefix), IsDir: true, Type: fs.ModeDir})
	}
	slices.SortFunc(out, func(a, b DirEntry) int { return cmp.Compare(a.Name, b.Name) })
	return out
}

// Route is the provider for one virtual path. MountSession only chooses it.
// The provider does the file work and returns its own errors.
type Route struct {
	Provider Provider
	Spec     MountSpec
	Point    string
	Rel      string
	sess     *MountSession
	err      error
}

// Route returns the provider mounted at virtualPath.
// A failed lookup is stored on Route and returned by the file methods.
func (m *MountSession) Route(ctx context.Context, virtualPath string) Route {
	if err := ctx.Err(); err != nil {
		return Route{err: err}
	}
	rt, err := m.lookup(virtualPath)
	if err != nil {
		return Route{err: err}
	}
	rt.sess = m
	return rt
}

func (r Route) virtual() string {
	return path.Join(r.Point, r.Rel)
}

func (m *MountSession) lookup(virtualPath string) (Route, error) {
	cleaned, err := CleanPath(virtualPath)
	if err != nil {
		return Route{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var bestPoint string
	var best mountEntry
	found := false
	for mp, ent := range m.mounts {
		if mp != "/" && cleaned != mp && !strings.HasPrefix(cleaned, mp+"/") {
			continue
		}
		if !found || len(mp) > len(bestPoint) {
			bestPoint, best, found = mp, ent, true
		}
	}
	if found {
		return Route{
			Provider: best.provider,
			Spec:     best.spec,
			Point:    bestPoint,
			Rel:      strings.TrimPrefix(strings.TrimPrefix(cleaned, bestPoint), "/"),
		}, nil
	}
	if cleaned == "/" {
		return Route{Point: "/"}, nil
	}
	if !m.hasWorkspaceLocked() {
		return Route{}, ErrNotMounted
	}
	if cleaned == mount.WorkspacePoint {
		return Route{Point: mount.WorkspacePoint}, nil
	}
	if strings.HasPrefix(cleaned, mount.WorkspacePoint+"/") {
		return Route{
			Point: mount.WorkspacePoint,
			Rel:   strings.TrimPrefix(cleaned, mount.WorkspacePoint+"/"),
		}, nil
	}
	return Route{}, ErrNotMounted
}

func (m *MountSession) hasWorkspaceLocked() bool {
	prefix := mount.WorkspacePoint + "/"
	for mp := range m.mounts {
		if mp == mount.WorkspacePoint || strings.HasPrefix(mp, prefix) {
			return true
		}
	}
	return false
}
