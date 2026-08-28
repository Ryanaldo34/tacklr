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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3API is the subset of an S3-compatible API used by the S3 provider.
// Hosts typically pass *s3.Client via AWSS3 (or any adapter).
type S3API interface {
	// Head returns size, mtime, and optional Content-Type (empty if unknown).
	Head(ctx context.Context, bucket, key string) (size int64, mod time.Time, contentType string, err error)
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

var _ documentBackend = s3Provider{}

// Validate implements Provider.
func (p s3Provider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Factory Open rejects a missing client or bucket before this runs.
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
		size, mod, ct, err := p.api.Head(ctx, p.bucket, key)
		if err == nil {
			base := path.Base(key)
			mt := s3KnownType(ct)
			if mt == "" {
				mt = DetectMediaType(base, nil)
			}
			return FileInfo{
				Name:      base,
				Size:      size,
				Mode:      0o644,
				ModTime:   mod,
				IsDir:     false,
				MediaType: mt,
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
		if size, mod, _, err := p.api.Head(ctx, p.bucket, dirKey); err == nil {
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

// PutFile writes a full object with one Put (no intermediate Open buffer).
func (p s3Provider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := p.objectKey(name)
	if err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("vfs: cannot write mount root as file")
	}
	return p.api.Put(ctx, p.bucket, key, r, size)
}

// OpenDocument translates an S3 object into Document IR.
func (p s3Provider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fi, err := p.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	if fi.IsDir {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	if fi.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	key, err := p.objectKey(name)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, fmt.Errorf("vfs: cannot open mount root as file")
	}
	body, _, _, err := p.api.Get(ctx, p.bucket, key)
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
func (p s3Provider) WriteDocument(ctx context.Context, name string, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := EncodeDocument(ctx, doc)
	if err != nil {
		return err
	}
	return p.PutFile(ctx, name, bytes.NewReader(data), int64(len(data)))
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
		if _, _, _, err := p.api.Head(ctx, p.bucket, key); err == nil {
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
		return &s3WriteFile{buf: buf, ctx: ctx, p: p, key: key}, nil
	}

	if write {
		return &s3WriteFile{ctx: ctx, p: p, key: key}, nil
	}

	if !read {
		return nil, fmt.Errorf("vfs: unsupported open flags")
	}
	body, size, mod, err := p.api.Get(ctx, p.bucket, key)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	if size < 0 {
		size = int64(len(data))
	}
	return &s3ReadFile{
		Reader: bytes.NewReader(data),
		info:   FileInfo{Name: path.Base(key), Size: size, Mode: 0o644, ModTime: mod},
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
		return nil, ErrNotDir
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
		if _, _, _, err := p.api.Head(ctx, p.bucket, marker); err == nil {
			continue
		} else if !errors.Is(err, ErrNotExist) {
			return err
		}
		if _, _, _, err := p.api.Head(ctx, p.bucket, key); err == nil {
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

type s3ReadFile struct {
	*bytes.Reader
	info FileInfo
}

func (f *s3ReadFile) Close() error            { return nil }
func (f *s3ReadFile) Stat() (FileInfo, error) { return f.info, nil }

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
	return func(ctx context.Context, _ string, b Binding) (Provider, error) {
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

// AWSS3 implements S3API with the AWS SDK v2 client (MinIO, R2, and real S3).
type AWSS3 struct {
	Client *s3.Client
}

func (a AWSS3) require() error {
	if a.Client == nil {
		return fmt.Errorf("vfs: AWS S3 client required")
	}
	return nil
}

// Head implements S3API.
func (a AWSS3) Head(ctx context.Context, bucket, key string) (int64, time.Time, string, error) {
	if err := a.require(); err != nil {
		return 0, time.Time{}, "", err
	}
	out, err := a.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, time.Time{}, "", mapS3Error(err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	var mod time.Time
	if out.LastModified != nil {
		mod = *out.LastModified
	}
	ct := ""
	if out.ContentType != nil {
		ct = *out.ContentType
	}
	return size, mod, ct, nil
}

// Get implements S3API.
func (a AWSS3) Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, time.Time, error) {
	if err := a.require(); err != nil {
		return nil, 0, time.Time{}, err
	}
	out, err := a.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, time.Time{}, mapS3Error(err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	var mod time.Time
	if out.LastModified != nil {
		mod = *out.LastModified
	}
	return out.Body, size, mod, nil
}

// Put implements S3API.
func (a AWSS3) Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
	if err := a.require(); err != nil {
		return err
	}
	in := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if size >= 0 {
		in.ContentLength = aws.Int64(size)
	}
	_, err := a.Client.PutObject(ctx, in)
	return mapS3Error(err)
}

// Delete implements S3API.
func (a AWSS3) Delete(ctx context.Context, bucket, key string) error {
	if err := a.require(); err != nil {
		return err
	}
	_, err := a.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return mapS3Error(err)
}

// List implements S3API with delimiter "/" for virtual directories.
func (a AWSS3) List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error) {
	if err := a.require(); err != nil {
		return nil, nil, err
	}
	pager := s3.NewListObjectsV2Paginator(a.Client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, mapS3Error(err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		for _, p := range page.CommonPrefixes {
			if p.Prefix != nil {
				dirs = append(dirs, *p.Prefix)
			}
		}
	}
	return keys, dirs, nil
}

func mapS3Error(err error) error {
	if err == nil {
		return nil
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return ErrNotExist
	}
	var nsb *types.NotFound
	if errors.As(err, &nsb) {
		return ErrNotExist
	}
	// HeadObject 404 / NoSuchKey often wrap as generic API errors.
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "notfound") || strings.Contains(msg, "no such key") || strings.Contains(msg, "status code: 404") {
		return ErrNotExist
	}
	return err
}
