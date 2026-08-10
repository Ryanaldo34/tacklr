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

	// Content IR
	ErrNoCodec        = errors.New("vfs: no codec for media type")
	ErrNotTextual     = errors.New("vfs: not a textual document")
	ErrLineOutOfRange = errors.New("vfs: line out of range")
	ErrInvalidUTF8    = errors.New("vfs: invalid utf-8")
	ErrInvalidLine    = errors.New("vfs: line contains newline")
	ErrLineTooLong    = errors.New("vfs: line too long")
	ErrTooLarge       = errors.New("vfs: file too large")
	// ErrStaleContent is for tool/host optimistic concurrency (expected hash ≠ current).
	// vfs lower-level APIs do not return this; tools wrap ContentRev checks.
	ErrStaleContent = errors.New("vfs: stale content revision")

	errRegistryRequired = errors.New("vfs: registry required")
)
