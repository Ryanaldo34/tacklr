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

// File is an open file handle for a provider-relative path.
type File interface {
	io.ReadWriteCloser
	Stat() (FileInfo, error)
}

// FileInfo describes a file or directory (agent-safe: no host paths).
type FileInfo struct {
	Name    string
	Size    int64
	Mode    fs.FileMode
	ModTime time.Time
	IsDir   bool
}

// DirEntry is a single directory entry.
type DirEntry struct {
	Name  string
	IsDir bool
	// Type is the file mode bits (same as fs.DirEntry.Type).
	Type fs.FileMode
}
