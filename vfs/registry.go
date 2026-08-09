package vfs

import (
	"context"
	"fmt"
	"sync"
)

// ProviderFactory opens Providers for a named backend profile.
// Factories are process-scoped and may hold shared connection pools.
type ProviderFactory interface {
	Profile() string
	Open(ctx context.Context, sessionID string, spec MountSpec) (Provider, error)
}

// BackendRegistry maps profile ids to factories (and thus to pooled clients).
type BackendRegistry struct {
	mu        sync.RWMutex
	factories map[string]ProviderFactory
}

// NewBackendRegistry returns an empty registry.
func NewBackendRegistry() *BackendRegistry {
	return &BackendRegistry{factories: make(map[string]ProviderFactory)}
}

// Register adds or replaces a factory under factory.Profile().
func (r *BackendRegistry) Register(factory ProviderFactory) error {
	if r == nil || factory == nil {
		return fmt.Errorf("vfs: register requires registry and factory")
	}
	id := factory.Profile()
	if id == "" {
		return fmt.Errorf("vfs: factory profile required")
	}
	r.mu.Lock()
	r.factories[id] = factory
	r.mu.Unlock()
	return nil
}

// Open resolves spec.Profile and opens a Provider.
func (r *BackendRegistry) Open(ctx context.Context, sessionID string, spec MountSpec) (Provider, error) {
	if r == nil {
		return nil, fmt.Errorf("vfs: registry required")
	}
	if spec.Profile == "" {
		return nil, ErrInvalidProvider
	}
	r.mu.RLock()
	f, ok := r.factories[spec.Profile]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownProfile, spec.Profile)
	}
	return f.Open(ctx, sessionID, spec)
}

// Materialize builds a new FS by opening each spec through the registry.
// Fail-closed: any error aborts (discard the returned FS).
func Materialize(ctx context.Context, reg *BackendRegistry, sessionID string, specs []MountSpec) (*FS, error) {
	if reg == nil {
		return nil, fmt.Errorf("vfs: registry required")
	}
	fs := New()
	for _, spec := range specs {
		p, err := reg.Open(ctx, sessionID, spec)
		if err != nil {
			return nil, err
		}
		if err := fs.Mount(ctx, spec, p); err != nil {
			return nil, err
		}
	}
	return fs, nil
}

// MergeSpecs concatenates bootstrap then durable specs.
// Duplicate points return ErrAlreadyMounted.
func MergeSpecs(bootstrap, durable []MountSpec) ([]MountSpec, error) {
	seen := make(map[string]struct{}, len(bootstrap)+len(durable))
	out := make([]MountSpec, 0, len(bootstrap)+len(durable))
	for _, list := range [][]MountSpec{bootstrap, durable} {
		for _, s := range list {
			cleaned, err := cleanVirtualPath(s.Point)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[cleaned]; ok {
				return nil, ErrAlreadyMounted
			}
			seen[cleaned] = struct{}{}
			s.Point = cleaned
			out = append(out, s)
		}
	}
	return out, nil
}
