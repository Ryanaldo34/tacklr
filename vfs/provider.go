package vfs

import "context"

// Provider is a backend that can be attached at a virtual mount point.
//
// Providers are opaque: the mount table routes by virtual path only. Callers
// must not rely on backend type tags. File operations (later) will be methods
// on this interface or composed ports implemented by each backend.
//
// Treat provider values as immutable after a successful FS.Mount.
type Provider interface {
	// Validate checks that the provider is configured well enough to mount.
	// Prefer offline checks so tests do not need network access.
	Validate(ctx context.Context) error
}
