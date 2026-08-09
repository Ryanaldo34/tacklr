package vfs

import (
	"cmp"
	"context"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"
)

// FS is an isolated virtual filesystem mount namespace.
// Create one with New. There is no process-global mount table.
type FS struct {
	mu     sync.RWMutex
	mounts map[string]mountEntry // key: cleaned virtual mount point
}

type mountEntry struct {
	provider Provider
	readOnly bool
}

// New returns an empty mount namespace with no mounts.
func New() *FS {
	return &FS{mounts: make(map[string]mountEntry)}
}

// Mount attaches provider at the absolute virtual path point.
// readOnly is like mount -o ro (enforced when file ops land).
func (fs *FS) Mount(ctx context.Context, point string, provider Provider, readOnly bool) error {
	if fs == nil {
		return fmt.Errorf("vfs: nil FS")
	}
	if provider == nil {
		return ErrInvalidProvider
	}
	cleaned, err := cleanVirtualPath(point)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := provider.Validate(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidProvider, err)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, exists := fs.mounts[cleaned]; exists {
		return ErrAlreadyMounted
	}
	fs.mounts[cleaned] = mountEntry{provider: provider, readOnly: readOnly}
	return nil
}

// Unmount detaches the mount at the exact virtual path point.
// Nested mounts under point are not removed. Phase 1 has no open-handle busy check.
func (fs *FS) Unmount(point string) error {
	if fs == nil {
		return fmt.Errorf("vfs: nil FS")
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

// Mounts returns a snapshot of current mounts sorted by point.
func (fs *FS) Mounts() []MountInfo {
	if fs == nil {
		return nil
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	out := make([]MountInfo, 0, len(fs.mounts))
	for point, e := range fs.mounts {
		out = append(out, MountInfo{Point: point, ReadOnly: e.readOnly})
	}
	slices.SortFunc(out, func(a, b MountInfo) int {
		return cmp.Compare(a.Point, b.Point)
	})
	return out
}

// Lookup finds the longest mount covering path and returns MountInfo plus the
// path relative to that mount (no leading slash; empty when path is the mount point).
func (fs *FS) Lookup(virtualPath string) (MountInfo, string, error) {
	if fs == nil {
		return MountInfo{}, "", fmt.Errorf("vfs: nil FS")
	}
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return MountInfo{}, "", err
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var bestPoint string
	var best mountEntry
	found := false
	for point, e := range fs.mounts {
		// Segment-boundary prefix: "/ab" must not cover "/a".
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
	return MountInfo{Point: bestPoint, ReadOnly: best.readOnly}, rel, nil
}

func cleanVirtualPath(s string) (string, error) {
	if s == "" || !path.IsAbs(s) || strings.ContainsAny(s, "\\\x00") {
		return "", ErrInvalidPath
	}
	return path.Clean(s), nil
}
