package vfs

import (
	"context"
	"errors"
	"io"
	"path"
	"sync"
)

// Files is the POSIX view of a MountSession: the files that a shell sees.
// Plain text reads and writes as bytes. A document with a codec (Word, a
// Google Doc) reads as its projected text and refuses writes. A file with
// no text reads as its bytes. The root lists the mounts, and refuses new
// entries.
type Files struct {
	ms *MountSession
}

// NewFiles returns the POSIX view of ms.
func NewFiles(ms *MountSession) Files {
	return Files{ms: ms}
}

// OpenMode is how Files.Open opens a file.
type OpenMode struct {
	// Write opens the file for writing. A document refuses it.
	Write bool
	// Create makes the file if it does not exist.
	Create bool
	// Truncate empties the file. It implies Write.
	Truncate bool
	// Append makes every write go to the end of the file.
	Append bool
}

// Stat returns the file or directory at p. The size of a document is the
// size of its projected text.
func (f Files) Stat(ctx context.Context, p string) (FileInfo, error) {
	st, _, err := f.stat(ctx, p)
	return st, err
}

// stat is Stat, and the text of the file when it has one.
func (f Files) stat(ctx context.Context, p string) (FileInfo, []byte, error) {
	st, err := f.ms.Route(ctx, p).Stat(ctx)
	if err != nil || st.IsDir || (st.Size == 0 && !f.ms.Codecs().Identity(st.MediaType)) {
		return st, nil, err
	}
	text, err := f.ms.Route(ctx, p).ReadText(ctx)
	switch {
	case err == nil:
		body := []byte(text.Text())
		st.Size = int64(len(body))
		return st, body, nil
	case !errors.Is(err, ErrNoCodec) && !errors.Is(err, ErrNotTextual) && !errors.Is(err, ErrNotSupported):
		return FileInfo{}, nil, err
	}
	return st, nil, nil
}

// ReadDir lists the directory at p.
func (f Files) ReadDir(ctx context.Context, p string) ([]DirEntry, error) {
	return f.ms.Route(ctx, p).ReadDir(ctx)
}

// Mkdir makes the directory at p and its parents.
func (f Files) Mkdir(ctx context.Context, p string) error {
	if err := f.belowRoot(p); err != nil {
		return err
	}
	return f.ms.Route(ctx, p).MkdirAll(ctx)
}

// Remove removes the file or empty directory at p.
func (f Files) Remove(ctx context.Context, p string) error {
	if err := f.belowRoot(p); err != nil {
		return err
	}
	return f.ms.Route(ctx, p).Remove(ctx)
}

// Rename moves the file at from to to: a copy, then a remove, so it also
// moves a file from one mount to another.
func (f Files) Rename(ctx context.Context, from, to string) error {
	if err := errors.Join(f.belowRoot(from), f.belowRoot(to)); err != nil {
		return ErrPermission
	}
	data, err := f.ms.Route(ctx, from).ReadFile(ctx)
	if err != nil {
		return err
	}
	if err := f.ms.Route(ctx, to).WriteFile(ctx, data); err != nil {
		return err
	}
	return f.ms.Route(ctx, from).Remove(ctx)
}

// Open opens the file at p. Close the file to write its changes back.
func (f Files) Open(ctx context.Context, p string, mode OpenMode) (*OpenFile, error) {
	mode.Write = mode.Write || mode.Truncate || mode.Append
	st, body, err := f.stat(ctx, p)
	switch {
	case errors.Is(err, ErrNotExist) && mode.Create:
		if f.ms.Codecs().Projected(DetectMediaType(path.Base(p), nil)) {
			return nil, ErrReadOnly
		}
		if err := f.ms.Route(ctx, p).WriteFile(ctx, nil); err != nil {
			return nil, err
		}
		return &OpenFile{ms: f.ms, path: p, append: mode.Append}, nil
	case err != nil:
		return nil, err
	case st.IsDir:
		return nil, ErrIsDir
	case mode.Write && !f.ms.Codecs().Identity(st.MediaType):
		return nil, ErrReadOnly
	case mode.Truncate:
		return &OpenFile{ms: f.ms, path: p, append: mode.Append, dirty: true}, nil
	case body != nil || st.Size == 0:
		return &OpenFile{ms: f.ms, path: p, body: body, append: mode.Append}, nil
	}
	bin, err := f.ms.Route(ctx, p).Open(ctx)
	if err != nil {
		return nil, err
	}
	at, ok := bin.(io.ReaderAt)
	if !ok {
		return nil, errors.Join(ErrNotSupported, bin.Close())
	}
	return &OpenFile{ms: f.ms, path: p, bin: bin, at: at, size: st.Size}, nil
}

// belowRoot refuses p when it is the root or an entry of the root: the
// root holds the mounts, not files.
func (f Files) belowRoot(p string) error {
	if path.Dir(path.Clean("/"+p)) == "/" {
		return ErrPermission
	}
	return nil
}

// OpenFile is one open file of Files. Text is a snapshot of the file's
// bytes, written back on Sync and Close when it changed. A file with no
// text reads from the backend at each offset and refuses writes.
type OpenFile struct {
	mu     sync.Mutex
	ms     *MountSession
	path   string
	body   []byte
	bin    File
	at     io.ReaderAt
	size   int64
	dirty  bool
	append bool
}

// ReadAt reads len(p) bytes at off, and returns io.EOF after the end.
func (o *OpenFile) ReadAt(p []byte, off int64) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.at != nil {
		return o.at.ReadAt(p, off)
	}
	if off >= int64(len(o.body)) {
		return 0, io.EOF
	}
	n := copy(p, o.body[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt writes p at off, or at the end in append mode. The file may grow
// up to MaxReadFileBytes.
func (o *OpenFile) WriteAt(p []byte, off int64) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.at != nil {
		return 0, ErrReadOnly
	}
	if o.append {
		off = int64(len(o.body))
	}
	end := off + int64(len(p))
	if off < 0 || end > MaxReadFileBytes {
		return 0, ErrTooLarge
	}
	if end > int64(len(o.body)) {
		o.body = append(o.body, make([]byte, end-int64(len(o.body)))...)
	}
	copy(o.body[off:], p)
	o.dirty = true
	return len(p), nil
}

// Truncate sets the size of the file to size, up to MaxReadFileBytes.
func (o *OpenFile) Truncate(size int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch {
	case o.at != nil:
		return ErrReadOnly
	case size < 0 || size > MaxReadFileBytes:
		return ErrTooLarge
	case size <= int64(len(o.body)):
		o.body = o.body[:size]
	default:
		o.body = append(o.body, make([]byte, size-int64(len(o.body)))...)
	}
	o.dirty = true
	return nil
}

// Size is the size of the file as this OpenFile sees it.
func (o *OpenFile) Size() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.at != nil {
		return o.size
	}
	return int64(len(o.body))
}

// Sync writes the file's changes back.
func (o *OpenFile) Sync(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.dirty {
		return nil
	}
	if err := o.ms.Route(ctx, o.path).WriteFile(ctx, o.body); err != nil {
		return err
	}
	o.dirty = false
	return nil
}

// Close writes the file's changes back and releases it.
func (o *OpenFile) Close(ctx context.Context) error {
	err := o.Sync(ctx)
	if o.bin != nil {
		err = errors.Join(err, o.bin.Close())
	}
	return err
}
