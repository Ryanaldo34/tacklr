package vfs

import (
	"context"
	"fmt"
	"strings"
)

// S3Provider mounts an S3-compatible bucket (optional key prefix) at a virtual path.
//
// Client is host-owned and must be non-nil at Mount. Object I/O is not part of
// the mount phase; wire a concrete S3 client when file ops land.
//
// Object keys use "/" as a logical delimiter; true directories are not native and
// will be emulated when file ops land. Prefix is treated as the source root for keys.
type S3Provider struct {
	// Client must be non-nil. Type is intentionally untyped until ops need a port.
	Client any
	// Bucket is the target bucket name.
	Bucket string
	// Prefix is an optional key prefix (source root). Leading slashes are stripped
	// for validation; ".." and empty segments are rejected.
	Prefix string
}

// Validate implements Provider. Checks are config-only (no network).
func (p S3Provider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Client == nil {
		return fmt.Errorf("vfs: s3 client is required")
	}
	if strings.TrimSpace(p.Bucket) == "" {
		return fmt.Errorf("vfs: s3 bucket is required")
	}
	prefix := strings.TrimLeft(p.Prefix, "/")
	if prefix == "" {
		return nil
	}
	// Guard with delimiters so "foo..bar" is allowed but ".." / "a/../b" are not.
	padded := "/" + strings.TrimSuffix(prefix, "/") + "/"
	if strings.Contains(padded, "/../") || strings.Contains(padded, "//") {
		return fmt.Errorf("vfs: s3 prefix is invalid")
	}
	return nil
}
