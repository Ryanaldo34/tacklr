package vfs

import "errors"

// Sentinel errors for mount-table outcomes. Prefer returning these bare (or a
// single plain "vfs: …" message). Do not wrap sentinels in more sentinels.
var (
	ErrInvalidPath = errors.New("vfs: invalid path")

	ErrAlreadyMounted = errors.New("vfs: already mounted")

	// ErrNotMounted: unmount of a missing point, or lookup with no covering mount.
	ErrNotMounted = errors.New("vfs: not mounted")

	// ErrInvalidProvider: nil provider, missing profile, or failed Validate.
	ErrInvalidProvider = errors.New("vfs: invalid provider")

	// ErrUnknownProfile: registry has no factory for MountSpec.Profile.
	ErrUnknownProfile = errors.New("vfs: unknown profile")
)
