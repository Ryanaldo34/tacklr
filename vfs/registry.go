package vfs

import (
	"context"
	"fmt"
	"slices"
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
	if factory == nil {
		return fmt.Errorf("vfs: factory required")
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

// HasProfile reports whether a factory is registered under id.
func (r *BackendRegistry) HasProfile(id string) bool {
	if r == nil || id == "" {
		return false
	}
	r.mu.RLock()
	_, ok := r.factories[id]
	r.mu.RUnlock()
	return ok
}

// Profiles returns registered factory ids in sorted order.
func (r *BackendRegistry) Profiles() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	out := make([]string, 0, len(r.factories))
	for id := range r.factories {
		out = append(out, id)
	}
	r.mu.RUnlock()
	slices.Sort(out)
	return out
}

// CheckMount opens and validates a provider for spec without attaching it.
// Used to fail client binds before the mount is visible.
func CheckMount(ctx context.Context, reg *BackendRegistry, sessionID string, spec MountSpec) error {
	if reg == nil {
		return errRegistryRequired
	}
	p, err := reg.open(ctx, sessionID, spec)
	if err != nil {
		return err
	}
	return p.Validate(ctx)
}

func (r *BackendRegistry) open(ctx context.Context, sessionID string, spec MountSpec) (Provider, error) {
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
