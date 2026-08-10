package vfs

import (
	"cmp"
	"context"
	"path"
	"slices"
	"strings"
	"sync"
)

// mountTable is the internal mount namespace. Hosts use MountSession only.
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

func (t *mountTable) infos() []MountInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]MountInfo, 0, len(t.mounts))
	for point, e := range t.mounts {
		out = append(out, MountInfo{Point: point, ReadOnly: e.spec.ReadOnly})
	}
	slices.SortFunc(out, func(a, b MountInfo) int { return cmp.Compare(a.Point, b.Point) })
	return out
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

func (t *mountTable) lookup(virtualPath string) (MountInfo, string, error) {
	_, point, rel, ro, err := t.resolve(virtualPath)
	if err != nil {
		return MountInfo{}, "", err
	}
	return MountInfo{Point: point, ReadOnly: ro}, rel, nil
}

func (t *mountTable) resolve(virtualPath string) (p Provider, point, rel string, readOnly bool, err error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return nil, "", "", false, err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	var bestPoint string
	var best mountEntry
	found := false
	for mp, e := range t.mounts {
		if mp != "/" && cleaned != mp && !strings.HasPrefix(cleaned, mp+"/") {
			continue
		}
		if !found || len(mp) > len(bestPoint) {
			bestPoint, best, found = mp, e, true
		}
	}
	if !found {
		return nil, "", "", false, ErrNotMounted
	}
	rel = strings.TrimPrefix(strings.TrimPrefix(cleaned, bestPoint), "/")
	return best.provider, bestPoint, rel, best.spec.ReadOnly, nil
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
