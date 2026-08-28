package vfs

import "errors"

// Sentinel errors for mount-table and path I/O outcomes.
// Prefer bare sentinels or a single plain "vfs: …" message — never wrap sentinels in sentinels.
var (
	ErrInvalidPath     = errors.New("vfs: invalid path")
	ErrAlreadyMounted  = errors.New("vfs: already mounted")
	ErrNotMounted      = errors.New("vfs: not mounted")
	ErrInvalidProvider = errors.New("vfs: invalid provider")
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
	ErrNoCodec = errors.New("vfs: no codec for media type")
	// ErrAlreadyRegistered is returned when Register is called for a media type
	// that already has a codec. First registration wins.
	ErrAlreadyRegistered = errors.New("vfs: media type already registered")
	ErrNotTextual        = errors.New("vfs: not a textual document")
	ErrLineOutOfRange    = errors.New("vfs: line out of range")
	ErrInvalidUTF8       = errors.New("vfs: invalid utf-8")
	ErrInvalidLine       = errors.New("vfs: line contains newline")
	ErrLineTooLong       = errors.New("vfs: line too long")
	ErrTooLarge          = errors.New("vfs: file too large")
	// ErrProjected is returned when a line/HTML/SetText mutation is applied to a
	// projected spreadsheet. Docs/Word accept HTML line and full-content writes.
	ErrProjected = errors.New("that write is not supported on this file type")
	// ErrConflict is a provider-level compare-and-swap failure.
	// Apply retries Docs/Word persist once; leftover conflict becomes ErrInvalidWrite.
	ErrConflict = errors.New("the file changed on the server since it was last read")
	// ErrStaleContent is tool/host optimistic concurrency (expected hash ≠ current).
	ErrStaleContent = errors.New("the file changed since last read")
	// ErrInvalidWrite is a persist that cannot apply (bad insert location, etc.).
	// Never wrap provider SDK errors in this; the agent should retry the write.
	ErrInvalidWrite = errors.New("the document was not saved; read it and write the HTML again")
	// ErrUseHTML is a non-HTML full replace on an existing Docs/Word path.
	ErrUseHTML = errors.New("write HTML, not plain text")
	// ErrEmptyReplace is HTML or blocks that decoded to no headings, paragraphs, lists, or tables.
	ErrEmptyReplace = errors.New("that HTML had no headings, paragraphs, lists, or tables; put the text in <p> or <h1> tags")
	// ErrTabIDRequired is a replace on a multi-tab Doc/Word file without tab_id.
	ErrTabIDRequired = errors.New("this document has more than one tab; pass tab_id for the tab you want to replace")
)
