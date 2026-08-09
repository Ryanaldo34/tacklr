package vfs

import "errors"

var (
	// ErrInvalidPath is returned when a virtual path is empty, relative, or
	// otherwise not a safe absolute POSIX path under "/".
	ErrInvalidPath = errors.New("vfs: invalid path")

	// ErrAlreadyMounted is returned when Mount targets a point that already has a mount.
	ErrAlreadyMounted = errors.New("vfs: already mounted")

	// ErrNotMounted is returned when Unmount targets a point with no mount, or
	// Lookup finds no mount covering the path.
	ErrNotMounted = errors.New("vfs: not mounted")

	// ErrInvalidProvider is returned when the provider is nil or fails Validate.
	ErrInvalidProvider = errors.New("vfs: invalid provider")
)
