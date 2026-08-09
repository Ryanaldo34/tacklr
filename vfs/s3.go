package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"
)

// S3API is the subset of an S3-compatible API used by the S3 provider.
// Hosts typically pass *s3.Client via AWSS3 (or any adapter).
type S3API interface {
	Head(ctx context.Context, bucket, key string) (size int64, mod time.Time, err error)
	Get(ctx context.Context, bucket, key string) (body io.ReadCloser, size int64, mod time.Time, err error)
	Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error
	Delete(ctx context.Context, bucket, key string) error
	// List returns object keys and "directory" prefixes under prefix (delimiter "/").
	// prefix should end with "/" when listing a directory, or be the full key prefix.
	List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error)
}

// s3Provider mounts a bucket (+ optional key prefix). Unexported fields keep
// credentials and pool handles off the public surface.
type s3Provider struct {
	api    S3API
	bucket string
	prefix string // no leading slash; may be empty; trailing slash normalized off
}

// Validate implements Provider.
func (p s3Provider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.api == nil {
		return fmt.Errorf("vfs: s3 client required")
	}
	if strings.TrimSpace(p.bucket) == "" {
		return fmt.Errorf("vfs: s3 bucket required")
	}
	return validateS3Prefix(p.prefix)
}

func (p s3Provider) objectKey(name string) (string, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return "", err
	}
	base := strings.Trim(p.prefix, "/")
	if rel == "" {
		return base, nil
	}
	if base == "" {
		return rel, nil
	}
	return base + "/" + rel, nil
}

// dirPrefix returns the list prefix for a directory name (always trailing / when non-empty).
func (p s3Provider) dirPrefix(name string) (string, error) {
	key, err := p.objectKey(name)
	if err != nil {
		return "", err
	}
	if key == "" {
		return "", nil
	}
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	return key, nil
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
	// Root of the mount is always a directory.
	if key == strings.Trim(p.prefix, "/") && (name == "" || name == ".") {
		return FileInfo{Name: ".", Mode: fs.ModeDir | 0o755, IsDir: true, ModTime: time.Now().UTC()}, nil
	}

	// Try object first.
	if key != "" {
		size, mod, err := p.api.Head(ctx, p.bucket, key)
		if err == nil {
			return FileInfo{
				Name:    path.Base(key),
				Size:    size,
				Mode:    0o644,
				ModTime: mod,
				IsDir:   false,
			}, nil
		}
		if !errors.Is(err, ErrNotExist) {
			return FileInfo{}, err
		}
	}

	// Directory: marker key or any children under prefix.
	dirKey := key
	if dirKey != "" && !strings.HasSuffix(dirKey, "/") {
		dirKey += "/"
	}
	if dirKey != "" {
		if size, mod, err := p.api.Head(ctx, p.bucket, dirKey); err == nil {
			return FileInfo{
				Name:    path.Base(strings.TrimSuffix(dirKey, "/")),
				Size:    size,
				Mode:    fs.ModeDir | 0o755,
				ModTime: mod,
				IsDir:   true,
			}, nil
		} else if !errors.Is(err, ErrNotExist) {
			return FileInfo{}, err
		}
	}
	keys, dirs, err := p.api.List(ctx, p.bucket, dirKey)
	if err != nil {
		return FileInfo{}, err
	}
	if len(keys) == 0 && len(dirs) == 0 {
		return FileInfo{}, ErrNotExist
	}
	base := "."
	if key != "" {
		base = path.Base(strings.TrimSuffix(key, "/"))
	}
	return FileInfo{Name: base, Mode: fs.ModeDir | 0o755, IsDir: true, ModTime: time.Now().UTC()}, nil
}

// OpenFile implements Provider.
func (p s3Provider) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_ = perm
	key, err := p.objectKey(name)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, fmt.Errorf("vfs: cannot open mount root as file")
	}

	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND) != 0 || flag&os.O_CREATE != 0 || flag&os.O_TRUNC != 0
	read := flag&os.O_WRONLY == 0 // not write-only

	if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
		if _, _, err := p.api.Head(ctx, p.bucket, key); err == nil {
			return nil, ErrExist
		} else if !errors.Is(err, ErrNotExist) {
			return nil, err
		}
	}

	if write && flag&os.O_TRUNC == 0 && flag&os.O_APPEND != 0 {
		// Append: load existing then allow write.
		body, _, _, err := p.api.Get(ctx, p.bucket, key)
		if err != nil && !errors.Is(err, ErrNotExist) {
			return nil, err
		}
		var buf bytes.Buffer
		if body != nil {
			_, _ = io.Copy(&buf, body)
			_ = body.Close()
		}
		return &s3WriteFile{ctx: ctx, p: p, key: key, buf: buf}, nil
	}

	if write {
		return &s3WriteFile{ctx: ctx, p: p, key: key, buf: bytes.Buffer{}}, nil
	}

	if !read {
		return nil, fmt.Errorf("vfs: unsupported open flags")
	}
	body, size, mod, err := p.api.Get(ctx, p.bucket, key)
	if err != nil {
		return nil, err
	}
	return &s3ReadFile{
		body: body,
		info: FileInfo{Name: path.Base(key), Size: size, Mode: 0o644, ModTime: mod},
	}, nil
}

// ReadDir implements Provider.
func (p s3Provider) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Ensure path is a directory (or empty root).
	st, err := p.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	if !st.IsDir {
		return nil, fmt.Errorf("vfs: not a directory")
	}
	prefix, err := p.dirPrefix(name)
	if err != nil {
		return nil, err
	}
	keys, dirs, err := p.api.List(ctx, p.bucket, prefix)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]DirEntry, len(keys)+len(dirs))
	for _, d := range dirs {
		// d is full key prefix ending in /
		name := strings.TrimSuffix(d, "/")
		if prefix != "" {
			name = strings.TrimPrefix(name, prefix)
		}
		name = strings.Trim(name, "/")
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		seen[name] = DirEntry{Name: name, IsDir: true, Type: fs.ModeDir}
	}
	for _, k := range keys {
		if prefix != "" {
			k = strings.TrimPrefix(k, prefix)
		}
		k = strings.Trim(k, "/")
		if k == "" || strings.Contains(k, "/") {
			continue // only immediate children
		}
		// Skip directory markers (empty name after trim of trailing slash keys already handled)
		if _, ok := seen[k]; !ok {
			seen[k] = DirEntry{Name: k, IsDir: false, Type: 0}
		}
	}
	out := make([]DirEntry, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	return out, nil
}

// Remove implements Provider (single object; directory only if empty).
func (p s3Provider) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	st, err := p.Stat(ctx, name)
	if err != nil {
		return err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return err
	}
	if st.IsDir {
		prefix, err := p.dirPrefix(name)
		if err != nil {
			return err
		}
		keys, dirs, err := p.api.List(ctx, p.bucket, prefix)
		if err != nil {
			return err
		}
		// Allow only the optional directory marker under this prefix.
		for _, k := range keys {
			if k != prefix && k != strings.TrimSuffix(prefix, "/") {
				return fmt.Errorf("vfs: directory not empty")
			}
		}
		if len(dirs) > 0 {
			return fmt.Errorf("vfs: directory not empty")
		}
		// Delete marker if present.
		if key != "" {
			_ = p.api.Delete(ctx, p.bucket, key+"/")
			_ = p.api.Delete(ctx, p.bucket, key)
		}
		return nil
	}
	return p.api.Delete(ctx, p.bucket, key)
}

// MkdirAll implements Provider using zero-byte directory marker objects.
func (p s3Provider) MkdirAll(ctx context.Context, name string, perm fs.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = perm
	rel, err := cleanRel(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return nil
	}
	parts := strings.Split(rel, "/")
	var cur string
	for _, part := range parts {
		if cur == "" {
			cur = part
		} else {
			cur = cur + "/" + part
		}
		key, err := p.objectKey(cur)
		if err != nil {
			return err
		}
		marker := key + "/"
		// Skip if already exists as object or marker.
		if _, _, err := p.api.Head(ctx, p.bucket, marker); err == nil {
			continue
		} else if !errors.Is(err, ErrNotExist) {
			return err
		}
		if _, _, err := p.api.Head(ctx, p.bucket, key); err == nil {
			// Exists as file — cannot mkdir through a file.
			return fmt.Errorf("vfs: not a directory")
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

type s3ReadFile struct {
	body io.ReadCloser
	info FileInfo
}

func (f *s3ReadFile) Read(p []byte) (int, error) { return f.body.Read(p) }
func (f *s3ReadFile) Write([]byte) (int, error)  { return 0, fmt.Errorf("vfs: read-only file") }
func (f *s3ReadFile) Close() error {
	if f.body == nil {
		return nil
	}
	return f.body.Close()
}
func (f *s3ReadFile) Stat() (FileInfo, error) { return f.info, nil }

type s3WriteFile struct {
	ctx context.Context
	p   s3Provider
	key string
	buf bytes.Buffer
}

func (f *s3WriteFile) Read([]byte) (int, error) { return 0, fmt.Errorf("vfs: write-only file") }
func (f *s3WriteFile) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}
func (f *s3WriteFile) Close() error {
	data := f.buf.Bytes()
	return f.p.api.Put(f.ctx, f.p.bucket, f.key, bytes.NewReader(data), int64(len(data)))
}
func (f *s3WriteFile) Stat() (FileInfo, error) {
	return FileInfo{Name: path.Base(f.key), Size: int64(f.buf.Len()), Mode: 0o644}, nil
}

// S3Factory opens S3 providers that share one S3API client (HTTP pool).
// Client/DefaultBucket stay on the factory (process secrets/config).
type S3Factory struct {
	ID            string
	Client        S3API
	DefaultBucket string
}

// Profile implements ProviderFactory.
func (f S3Factory) Profile() string { return f.ID }

// Open implements ProviderFactory.
func (f S3Factory) Open(ctx context.Context, _ string, spec MountSpec) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.ID == "" || f.Client == nil {
		return nil, fmt.Errorf("vfs: s3 factory needs id and client")
	}
	bucket := spec.Params["bucket"]
	if bucket == "" {
		bucket = f.DefaultBucket
	}
	if bucket == "" {
		return nil, fmt.Errorf("vfs: s3 bucket required")
	}
	prefix := strings.Trim(spec.Params["prefix"], "/")
	if err := validateS3Prefix(prefix); err != nil {
		return nil, err
	}
	return s3Provider{api: f.Client, bucket: bucket, prefix: prefix}, nil
}
