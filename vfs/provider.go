package vfs

import (
	"context"
	"io"
	"io/fs"
	"time"
)

// Provider is a backend attached at a virtual mount point.
//
// One interface covers mount validation and path I/O relative to the provider
// root. Paths use slash-separated relative names ("" = root). Backends that do
// not support I/O yet return ErrNotSupported from I/O methods.
//
// Treat provider values as immutable after a successful FS.Mount.
type Provider interface {
	Validate(ctx context.Context) error

	Stat(ctx context.Context, name string) (FileInfo, error)
	OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error)
	ReadDir(ctx context.Context, name string) ([]DirEntry, error)
	Remove(ctx context.Context, name string) error
	MkdirAll(ctx context.Context, name string, perm fs.FileMode) error
}

// Classifier types a provider-relative name (including files that do not exist
// yet). Stat.MediaType is the source of truth for existing files.
type Classifier interface {
	Classify(name string, sample []byte) string
}

// File is an open file handle for a provider-relative path.
type File interface {
	io.ReadWriteCloser
	Stat() (FileInfo, error)
}

// FileInfo describes a file or directory (agent-safe: no host paths).
//
// MediaType is the provider's classification of a file (never a host path).
// Empty on directories. On files, providers must set it: a concrete type
// (text/markdown, image/png, …) or application/octet-stream when unknown.
// OpenDocument does not sniff; it only looks up a codec for this value.
type FileInfo struct {
	Name      string
	Size      int64
	Mode      fs.FileMode
	ModTime   time.Time
	IsDir     bool
	MediaType string
}

// DirEntry is a single directory entry.
type DirEntry struct {
	Name  string
	IsDir bool
	// Type is the file mode bits (same as fs.DirEntry.Type).
	Type fs.FileMode
}
