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
	ErrNotDir          = errors.New("vfs: not a directory")
	ErrIsDir           = errors.New("vfs: is a directory")
	ErrExist           = errors.New("vfs: already exists")
	ErrFuseNotMounted  = errors.New("vfs: fuse not mounted")
	ErrAuthExpired     = errors.New("vfs: auth expired")
	ErrAmbiguous       = errors.New("vfs: ambiguous path")
	ErrPermission      = errors.New("vfs: permission denied")

	// Content IR
	ErrNoCodec           = errors.New("vfs: no codec for media type")
	ErrAlreadyRegistered = errors.New("vfs: media type already registered")
	ErrNotTextual        = errors.New("vfs: not a textual document")
	ErrLineOutOfRange    = errors.New("vfs: line out of range")
	ErrInvalidUTF8       = errors.New("vfs: invalid utf-8")
	ErrInvalidLine       = errors.New("vfs: line contains newline")
	ErrLineTooLong       = errors.New("vfs: line too long")
	ErrTooLarge          = errors.New("vfs: file too large")
	// ErrProjected is returned when a line/HTML/SetText mutation is applied to a
	// projected document. Agents must use block IR instead.
	ErrProjected = errors.New("vfs: use block IR for this media type")
	// ErrConflict is a provider-level compare-and-swap failure (Docs requiredRevisionId).
	// Tools map it to ErrStaleContent. vfs path I/O does not return ErrStaleContent.
	ErrConflict = errors.New("vfs: remote content changed")
	// ErrStaleContent is for tool/host optimistic concurrency (expected hash ≠ current).
	// vfs lower-level APIs do not return this; tools wrap ContentRev checks.
	ErrStaleContent = errors.New("vfs: stale content revision")
)
