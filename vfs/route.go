package vfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
)

func (r Route) Stat(ctx context.Context) (FileInfo, error) {
	if r.err != nil {
		return FileInfo{}, r.err
	}
	if r.Provider == nil {
		if r.Rel == "" {
			return FileInfo{Name: ".", Mode: fs.ModeDir | 0o755, IsDir: true}, nil
		}
		return FileInfo{}, ErrNotExist
	}
	return r.Provider.Stat(ctx, r.Rel)
}

func (r Route) Open(ctx context.Context) (File, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.Provider == nil {
		return nil, ErrNotExist
	}
	return r.Provider.OpenFile(ctx, r.Rel, os.O_RDONLY, 0)
}

func (r Route) ReadFile(ctx context.Context) ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	f, err := r.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if fi, stErr := f.Stat(); stErr == nil && fi.Size >= 0 {
		if fi.Size > int64(MaxReadFileBytes) {
			return nil, errFileExceeds(MaxReadFileBytes)
		}
		if fi.Size == 0 {
			return []byte{}, nil
		}
		rd, ok := f.(io.Reader)
		if !ok {
			return nil, fmt.Errorf("vfs: file is not readable")
		}
		data := make([]byte, fi.Size)
		if _, err := io.ReadFull(rd, data); err != nil {
			return nil, err
		}
		return data, nil
	}
	rd, ok := f.(io.Reader)
	if !ok {
		return nil, fmt.Errorf("vfs: file is not readable")
	}
	data, err := io.ReadAll(io.LimitReader(rd, int64(MaxReadFileBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	return data, nil
}

func (r Route) WriteFile(ctx context.Context, data []byte) error {
	if r.err != nil {
		return r.err
	}
	if r.Provider == nil {
		return ErrNotExist
	}
	if r.Spec.ReadOnly {
		return ErrReadOnly
	}
	if err := writeProvider(ctx, r.Provider, r.Rel, bytes.NewReader(data), int64(len(data))); err != nil {
		return err
	}
	if r.sess == nil {
		return nil
	}
	return r.sess.fireAfterPersist(ctx, r.virtual())
}

func writeProvider(ctx context.Context, p Provider, name string, rd io.Reader, size int64) error {
	if size > int64(MaxReadFileBytes) {
		return errFileExceeds(MaxReadFileBytes)
	}
	if putter, ok := p.(filePutter); ok {
		return putter.PutFile(ctx, name, rd, size)
	}
	f, err := p.OpenFile(ctx, name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if size == 0 {
		return nil
	}
	w, ok := f.(io.Writer)
	if !ok {
		return ErrReadOnly
	}
	_, err = io.Copy(w, io.LimitReader(rd, size))
	return err
}

func (r Route) ReadDir(ctx context.Context) ([]DirEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.Provider == nil {
		if r.Rel != "" {
			return nil, ErrNotExist
		}
		if r.Point == "/" {
			if r.sess != nil && len(r.sess.memberEntries()) > 0 {
				return []DirEntry{{Name: "workspace", IsDir: true, Type: fs.ModeDir}}, nil
			}
			return []DirEntry{}, nil
		}
		return r.sess.memberEntries(), nil
	}
	return r.Provider.ReadDir(ctx, r.Rel)
}

func (r Route) Remove(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	if r.Rel == "" {
		return ErrInvalidPath
	}
	if r.Provider == nil {
		return ErrNotExist
	}
	if r.Spec.ReadOnly {
		return ErrReadOnly
	}
	return r.Provider.Remove(ctx, r.Rel)
}

func (r Route) MkdirAll(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	if r.Rel == "" {
		return nil
	}
	if r.Provider == nil {
		return ErrNotSupported
	}
	if r.Spec.ReadOnly {
		return ErrReadOnly
	}
	return r.Provider.MkdirAll(ctx, r.Rel, 0o755)
}

func (r Route) OpenDocument(ctx context.Context, reg *ContentRegistry) (Document, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.Provider == nil {
		return nil, ErrNotExist
	}
	db, ok := r.Provider.(documentBackend)
	if !ok {
		return nil, ErrNotSupported
	}
	if reg == nil {
		reg = DefaultContentRegistry()
	}
	doc, err := db.OpenDocument(ctx, r.Rel, reg)
	if err != nil {
		return nil, err
	}
	return bindDocument(doc, r.virtual()), nil
}

func (r Route) ReadText(ctx context.Context) (Textual, error) {
	doc, err := r.OpenDocument(ctx, nil)
	if err != nil {
		return nil, err
	}
	t, ok := doc.(Textual)
	if !ok {
		return nil, ErrNotTextual
	}
	if r.sess != nil {
		r.sess.rememberRev(t.Path(), ContentToken(t))
	}
	return t, nil
}

func (r Route) WriteDocument(ctx context.Context, doc Document) error {
	if r.err != nil {
		return r.err
	}
	if r.Provider == nil {
		return ErrNotSupported
	}
	if r.Spec.ReadOnly {
		return ErrReadOnly
	}
	db, ok := r.Provider.(documentBackend)
	if !ok {
		return ErrNotSupported
	}
	if err := db.WriteDocument(ctx, r.Rel, doc); err != nil {
		return err
	}
	if r.sess == nil {
		return nil
	}
	return r.sess.fireAfterPersist(ctx, r.virtual())
}
