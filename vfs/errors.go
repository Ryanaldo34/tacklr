package vfs

import "errors"

// Sentinel errors for mount-table and path I/O outcomes.
// Prefer bare sentinels or a single plain "vfs: …" message — never wrap sentinels in sentinels.
var (
	ErrInvalidPath  = errors.New("vfs: invalid path")
	ErrNotMounted   = errors.New("vfs: not mounted")
	ErrNotSupported = errors.New("vfs: not supported")
	ErrReadOnly     = errors.New("vfs: read-only mount")
	ErrNotExist     = errors.New("vfs: not found")
	ErrNotDir       = errors.New("vfs: not a directory")
	ErrIsDir        = errors.New("vfs: is a directory")
	ErrExist        = errors.New("vfs: already exists")
	ErrAuthExpired  = errors.New("vfs: auth expired")
	ErrAmbiguous    = errors.New("vfs: ambiguous path")
	ErrPermission   = errors.New("vfs: permission denied")

	// Content IR
	ErrNoCodec = errors.New("vfs: no codec for media type")
	// ErrAlreadyRegistered is returned when Register is called for a media type
	// that already has a codec. First registration wins.
	ErrAlreadyRegistered = errors.New("vfs: media type already registered")
	ErrNotTextual        = errors.New("vfs: not a textual document")
	ErrLineOutOfRange    = errors.New("vfs: line out of range")
	// ErrInvalidA1 is a cell or range address that is not A1 notation.
	ErrInvalidA1   = errors.New("vfs: invalid A1 address")
	ErrInvalidUTF8 = errors.New("vfs: invalid utf-8")
	ErrTooLarge    = errors.New("vfs: file too large")
)
