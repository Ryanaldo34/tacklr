package vfs

import (
	"cmp"
	"context"
	"path"
	"slices"
	"strings"
	"sync"
)

// FS is an isolated virtual filesystem mount namespace.
// Mount/Unmount update the live table; Specs() is the durable checkpoint view.
type FS struct {
	mu     sync.RWMutex
	mounts map[string]mountEntry // key: cleaned virtual mount point
}

type mountEntry struct {
	provider Provider
	spec     MountSpec
}

// New returns an empty mount namespace.
func New() *FS {
	return &FS{mounts: make(map[string]mountEntry)}
}

// Mount attaches provider using the durable MountSpec.
func (fs *FS) Mount(ctx context.Context, spec MountSpec, provider Provider) error {
	if fs == nil {
		return ErrInvalidProvider
	}
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
	// Validate errors are already readable; do not wrap again.
	if err := provider.Validate(ctx); err != nil {
		return err
	}

	stored := cloneSpec(spec)
	stored.Point = cleaned

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, exists := fs.mounts[cleaned]; exists {
		return ErrAlreadyMounted
	}
	fs.mounts[cleaned] = mountEntry{provider: provider, spec: stored}
	return nil
}

// Unmount detaches the exact mount point. Nested mounts are not removed.
func (fs *FS) Unmount(point string) error {
	if fs == nil {
		return ErrNotMounted
	}
	cleaned, err := cleanVirtualPath(point)
	if err != nil {
		return err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, exists := fs.mounts[cleaned]; !exists {
		return ErrNotMounted
	}
	delete(fs.mounts, cleaned)
	return nil
}

// Mounts returns a snapshot sorted by point (agent-safe).
func (fs *FS) Mounts() []MountInfo {
	if fs == nil {
		return nil
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	out := make([]MountInfo, 0, len(fs.mounts))
	for point, e := range fs.mounts {
		out = append(out, MountInfo{Point: point, ReadOnly: e.spec.ReadOnly})
	}
	slices.SortFunc(out, func(a, b MountInfo) int { return cmp.Compare(a.Point, b.Point) })
	return out
}

// Specs returns a durable snapshot for checkpointing (params deep-copied).
func (fs *FS) Specs() []MountSpec {
	if fs == nil {
		return nil
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	out := make([]MountSpec, 0, len(fs.mounts))
	for _, e := range fs.mounts {
		out = append(out, cloneSpec(e.spec))
	}
	slices.SortFunc(out, func(a, b MountSpec) int { return cmp.Compare(a.Point, b.Point) })
	return out
}

// Lookup finds the longest covering mount and the path relative to it.
func (fs *FS) Lookup(virtualPath string) (MountInfo, string, error) {
	if fs == nil {
		return MountInfo{}, "", ErrNotMounted
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.lookup(virtualPath)
}

func (fs *FS) lookup(virtualPath string) (MountInfo, string, error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return MountInfo{}, "", err
	}
	var bestPoint string
	var best mountEntry
	found := false
	for point, e := range fs.mounts {
		if point != "/" && cleaned != point && !strings.HasPrefix(cleaned, point+"/") {
			continue
		}
		if !found || len(point) > len(bestPoint) {
			bestPoint, best, found = point, e, true
		}
	}
	if !found {
		return MountInfo{}, "", ErrNotMounted
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(cleaned, bestPoint), "/")
	return MountInfo{Point: bestPoint, ReadOnly: best.spec.ReadOnly}, rel, nil
}

func cleanVirtualPath(s string) (string, error) {
	if s == "" || !path.IsAbs(s) || strings.ContainsAny(s, "\\\x00") {
		return "", ErrInvalidPath
	}
	return path.Clean(s), nil
}
