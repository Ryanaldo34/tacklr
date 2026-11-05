package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// S3API is the subset of an S3-compatible API used by the S3 provider.
// Hosts pass awss3.API{Client: c} (package vfs/awss3) or their own adapter.
type S3API interface {
	Head(ctx context.Context, bucket, key string) (S3Object, error)
	// Get opens n bytes of the object from off. n < 0 reads to the end; n is
	// never 0.
	Get(ctx context.Context, bucket, key string, off, n int64) (io.ReadCloser, error)
	Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error
	Delete(ctx context.Context, bucket, key string) error
	// List returns object keys and "directory" prefixes under prefix (delimiter "/").
	// prefix should end with "/" when listing a directory, or be the full key prefix.
	List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error)
}

// S3Object is the metadata Head returns for one object.
type S3Object struct {
	Size        int64
	ModTime     time.Time
	ContentType string // empty if unknown
	// ETag changes whenever the object's content changes.
	ETag string
}

// s3Provider mounts a bucket (+ optional key prefix). Unexported fields keep
// credentials and pool handles off the public surface.
type s3Provider struct {
	api    S3API
	bucket string
	prefix string // no leading or trailing slash; may be empty
}

var _ documentBackend = s3Provider{}

// Validate implements Provider.
func (p s3Provider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Factory Open rejects a missing client or bucket before this runs.
	return validateS3Prefix(p.prefix)
}

// objectKey is the key of name under the mount prefix. The mount root's key
// is the prefix itself.
func (p s3Provider) objectKey(name string) (string, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return "", err
	}
	return p.keyOf(rel), nil
}

// keyOf is the key of a clean relative name.
func (p s3Provider) keyOf(rel string) string {
	switch {
	case rel == "":
		return p.prefix
	case p.prefix == "":
		return rel
	}
	return p.prefix + "/" + rel
}

// isRoot reports whether key is the mount root.
func (p s3Provider) isRoot(key string) bool { return key == p.prefix }

// dirPrefix is the list prefix of the directory at key: key with a trailing
// slash, or empty for the root of a bucket.
func dirPrefix(key string) string {
	if key == "" {
		return ""
	}
	return key + "/"
}

// Stat implements Provider.
func (p s3Provider) Stat(ctx context.Context, name string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return FileInfo{}, err
	}
	return p.stat(ctx, key)
}

// stat is Stat of a key objectKey made.
func (p s3Provider) stat(ctx context.Context, key string) (FileInfo, error) {
	// Root of the mount is always a directory.
	if p.isRoot(key) {
		return FileInfo{Name: ".", Mode: fs.ModeDir | 0o755, IsDir: true, ModTime: time.Now().UTC()}, nil
	}

	// Try object first.
	obj, err := p.api.Head(ctx, p.bucket, key)
	if err == nil {
		base := path.Base(key)
		mt := s3KnownType(obj.ContentType)
		if mt == "" {
			mt = DetectMediaType(base, nil)
		}
		return FileInfo{
			Name:      base,
			Size:      obj.Size,
			Mode:      0o644,
			ModTime:   obj.ModTime,
			MediaType: mt,
			Version:   obj.ETag,
		}, nil
	}
	if !errors.Is(err, ErrNotExist) {
		return FileInfo{}, err
	}

	// Directory: marker key or any children under prefix.
	dirKey := dirPrefix(key)
	if obj, err := p.api.Head(ctx, p.bucket, dirKey); err == nil {
		return FileInfo{
			Name:    path.Base(key),
			Size:    obj.Size,
			Mode:    fs.ModeDir | 0o755,
			ModTime: obj.ModTime,
			IsDir:   true,
		}, nil
	} else if !errors.Is(err, ErrNotExist) {
		return FileInfo{}, err
	}
	keys, dirs, err := p.api.List(ctx, p.bucket, dirKey)
	if err != nil {
		return FileInfo{}, err
	}
	if len(keys) == 0 && len(dirs) == 0 {
		return FileInfo{}, ErrNotExist
	}
	return FileInfo{Name: path.Base(key), Mode: fs.ModeDir | 0o755, IsDir: true, ModTime: time.Now().UTC()}, nil
}

// PutFile writes a full object with one Put (no intermediate Open buffer).
func (p s3Provider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return err
	}
	if p.isRoot(key) {
		return fmt.Errorf("vfs: cannot write mount root as file")
	}
	return p.api.Put(ctx, p.bucket, key, r, size)
}

// OpenDocument translates an S3 object into Document IR.
func (p s3Provider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return nil, err
	}
	fi, err := p.stat(ctx, key)
	if err != nil {
		return nil, err
	}
	if fi.IsDir {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	if fi.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	body, err := p.api.Get(ctx, p.bucket, key, 0, -1)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(MaxReadFileBytes)+1))
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	return decodeProviderDocument(ctx, name, fi, data, reg)
}

// WriteDocument encodes IR and Puts the object immediately.
func (p s3Provider) WriteDocument(ctx context.Context, name string, doc Document, reg *ContentRegistry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := reg.Encode(ctx, doc)
	if err != nil {
		return err
	}
	return p.PutFile(ctx, name, bytes.NewReader(data), int64(len(data)))
}

// OpenFile implements Provider.
func (p s3Provider) OpenFile(ctx context.Context, name string, flag int, _ fs.FileMode) (File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return nil, err
	}
	if p.isRoot(key) {
		return nil, fmt.Errorf("vfs: cannot open mount root as file")
	}

	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND) != 0 || flag&os.O_CREATE != 0 || flag&os.O_TRUNC != 0

	if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
		if _, err := p.api.Head(ctx, p.bucket, key); err == nil {
			return nil, ErrExist
		} else if !errors.Is(err, ErrNotExist) {
			return nil, err
		}
	}

	if write && flag&os.O_TRUNC == 0 && flag&os.O_APPEND != 0 {
		// Append: load existing then allow write.
		body, err := p.api.Get(ctx, p.bucket, key, 0, -1)
		if err != nil && !errors.Is(err, ErrNotExist) {
			return nil, err
		}
		var buf bytes.Buffer
		if body != nil {
			_, _ = io.Copy(&buf, body)
			_ = body.Close()
		}
		return &s3WriteFile{buf: buf, ctx: ctx, p: p, key: key}, nil
	}

	if write {
		return &s3WriteFile{ctx: ctx, p: p, key: key}, nil
	}

	fi, err := p.stat(ctx, key)
	if err != nil {
		return nil, err
	}
	if fi.IsDir {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	return newRemoteFile(ctx, fi, func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
		return p.api.Get(ctx, p.bucket, key, off, n)
	}), nil
}

// ReadDir implements Provider.
func (p s3Provider) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return nil, err
	}
	// Ensure path is a directory (or empty root).
	st, err := p.stat(ctx, key)
	if err != nil {
		return nil, err
	}
	if !st.IsDir {
		return nil, ErrNotDir
	}
	prefix := dirPrefix(key)
	keys, dirs, err := p.api.List(ctx, p.bucket, prefix)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]DirEntry, len(keys)+len(dirs))
	for _, d := range dirs {
		// d is full key prefix ending in /
		name := strings.Trim(strings.TrimPrefix(d, prefix), "/")
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		seen[name] = DirEntry{Name: name, IsDir: true, Type: fs.ModeDir}
	}
	for _, k := range keys {
		k = strings.Trim(strings.TrimPrefix(k, prefix), "/")
		if k == "" || strings.Contains(k, "/") {
			continue // only immediate children
		}
		// Skip directory markers (empty name after trim of trailing slash keys already handled)
		if _, ok := seen[k]; !ok {
			seen[k] = DirEntry{Name: k, IsDir: false, Type: 0}
		}
	}
	return slices.Collect(maps.Values(seen)), nil
}

// Remove implements Provider (single object; directory only if empty).
func (p s3Provider) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return err
	}
	if p.isRoot(key) {
		return ErrInvalidPath
	}
	st, err := p.stat(ctx, key)
	if err != nil {
		return err
	}
	if !st.IsDir {
		return p.api.Delete(ctx, p.bucket, key)
	}
	prefix := dirPrefix(key)
	keys, dirs, err := p.api.List(ctx, p.bucket, prefix)
	if err != nil {
		return err
	}
	// Allow only the optional directory marker under this prefix.
	if len(dirs) > 0 || slices.ContainsFunc(keys, func(k string) bool { return k != prefix }) {
		return fmt.Errorf("vfs: directory not empty")
	}
	// Delete the marker if present.
	_ = p.api.Delete(ctx, p.bucket, prefix)
	return nil
}

// MkdirAll implements Provider using zero-byte directory marker objects.
func (p s3Provider) MkdirAll(ctx context.Context, name string, _ fs.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rel, err := cleanRel(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return nil
	}
	var cur string
	for part := range strings.SplitSeq(rel, "/") {
		cur = path.Join(cur, part)
		key := p.keyOf(cur)
		marker := key + "/"
		// Skip if already exists as object or marker.
		if _, err := p.api.Head(ctx, p.bucket, marker); err == nil {
			continue
		} else if !errors.Is(err, ErrNotExist) {
			return err
		}
		if _, err := p.api.Head(ctx, p.bucket, key); err == nil {
			// Exists as file — cannot mkdir through a file.
			return ErrNotDir
		} else if !errors.Is(err, ErrNotExist) {
			return err
		}
		if err := p.api.Put(ctx, p.bucket, marker, bytes.NewReader(nil), 0); err != nil {
			return err
		}
	}
	return nil
}

func validateS3Prefix(prefix string) error {
	prefix = strings.TrimLeft(prefix, "/")
	if prefix == "" {
		return nil
	}
	padded := "/" + strings.TrimSuffix(prefix, "/") + "/"
	if strings.Contains(padded, "/../") || strings.Contains(padded, "//") {
		return fmt.Errorf("vfs: invalid s3 prefix")
	}
	return nil
}

type s3WriteFile struct {
	buf bytes.Buffer
	ctx context.Context
	p   s3Provider
	key string
}

func (f *s3WriteFile) Write(p []byte) (int, error) { return f.buf.Write(p) }
func (f *s3WriteFile) Close() error {
	data := f.buf.Bytes()
	return f.p.api.Put(f.ctx, f.p.bucket, f.key, bytes.NewReader(data), int64(len(data)))
}
func (f *s3WriteFile) Stat() (FileInfo, error) {
	return FileInfo{Name: path.Base(f.key), Size: int64(f.buf.Len()), Mode: 0o644}, nil
}

// S3 opens providers that share one S3API client (HTTP pool).
// defaultBucket is used when the spec omits params["bucket"].
func S3(client S3API, defaultBucket string) Open {
	return func(ctx context.Context, _ string, b mount.Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if client == nil {
			return nil, fmt.Errorf("vfs: s3 client required")
		}
		bucket := b.Params["bucket"]
		if bucket == "" {
			bucket = defaultBucket
		}
		if bucket == "" {
			return nil, fmt.Errorf("vfs: s3 bucket required")
		}
		prefix := strings.Trim(b.Params["prefix"], "/")
		if err := validateS3Prefix(prefix); err != nil {
			return nil, err
		}
		return s3Provider{api: client, bucket: bucket, prefix: prefix}, nil
	}
}

// s3KnownType keeps Head Content-Type only when S3 actually declared a type.
// "application/octet-stream" is S3's default for "I don't know" — Stat then
// classifies from the object key (DetectMediaType) instead.
func s3KnownType(contentType string) string {
	t, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(contentType)), ";")
	t = strings.TrimSpace(t)
	if t == "" || t == "application/octet-stream" || t == "binary/octet-stream" {
		return ""
	}
	return t
}
