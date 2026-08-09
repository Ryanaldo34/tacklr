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
// Hosts Register factories; MountSession opens them. Open is not part of the
// host-facing API.
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

func (r *BackendRegistry) open(ctx context.Context, sessionID string, spec MountSpec) (Provider, error) {
	if r == nil {
		return nil, errRegistryRequired
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

// MergeSpecs concatenates bootstrap then durable specs (host/harness helper).
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
