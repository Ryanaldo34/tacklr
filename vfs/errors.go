package vfs

import "errors"

// Sentinel errors for mount-table and path I/O outcomes.
// Prefer bare sentinels or a single plain "vfs: …" message — never wrap sentinels in sentinels.
var (
	ErrInvalidPath     = errors.New("vfs: invalid path")
	ErrAlreadyMounted  = errors.New("vfs: already mounted")
	ErrNotMounted      = errors.New("vfs: not mounted")
	ErrInvalidProvider = errors.New("vfs: invalid provider")
	ErrUnknownProfile  = errors.New("vfs: unknown profile")
	ErrNotSupported    = errors.New("vfs: not supported")
	ErrReadOnly        = errors.New("vfs: read-only mount")
	ErrNotExist        = errors.New("vfs: not found")
	ErrExist           = errors.New("vfs: already exists")

	errRegistryRequired = errors.New("vfs: registry required")
	errNilMountSession  = errors.New("vfs: nil mount session")
)
