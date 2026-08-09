package vfs

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
)

// noIO embeds ErrNotSupported for Provider methods not yet implemented.
type noIO struct{}

func (noIO) Stat(context.Context, string) (FileInfo, error) {
	return FileInfo{}, ErrNotSupported
}
func (noIO) OpenFile(context.Context, string, int, fs.FileMode) (File, error) {
	return nil, ErrNotSupported
}
func (noIO) ReadDir(context.Context, string) ([]DirEntry, error) {
	return nil, ErrNotSupported
}
func (noIO) Remove(context.Context, string) error { return ErrNotSupported }
func (noIO) MkdirAll(context.Context, string, fs.FileMode) error {
	return ErrNotSupported
}

// s3Provider is unexported so hosts cannot read bucket/client fields.
// Obtain only via S3Factory (as Provider).
type s3Provider struct {
	noIO
	client any
	bucket string
	prefix string
}

// Validate implements Provider.
func (p s3Provider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.client == nil {
		return fmt.Errorf("vfs: s3 client required")
	}
	if strings.TrimSpace(p.bucket) == "" {
		return fmt.Errorf("vfs: s3 bucket required")
	}
	return validateS3Prefix(p.prefix)
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

// S3Factory opens S3 providers that share one Client (HTTP pool).
// Client/DefaultBucket stay on the factory (process secrets/config), not on mounts.
type S3Factory struct {
	ID            string
	Client        any
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
	prefix := spec.Params["prefix"]
	if err := validateS3Prefix(prefix); err != nil {
		return nil, err
	}
	return s3Provider{client: f.Client, bucket: bucket, prefix: prefix}, nil
}
