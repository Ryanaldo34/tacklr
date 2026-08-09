package vfs

import (
	"context"
	"fmt"
	"strings"
)

// S3Provider mounts an S3-compatible bucket (optional key prefix).
// Client is typically a long-lived shared client from S3Factory.
type S3Provider struct {
	Client any
	Bucket string
	Prefix string
}

// Validate implements Provider.
func (p S3Provider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Client == nil {
		return fmt.Errorf("vfs: s3 client required")
	}
	if strings.TrimSpace(p.Bucket) == "" {
		return fmt.Errorf("vfs: s3 bucket required")
	}
	return validateS3Prefix(p.Prefix)
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

// S3Factory opens S3Providers that share one Client (HTTP pool).
// Params: "bucket" (or DefaultBucket), "prefix" (optional).
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
	return S3Provider{Client: f.Client, Bucket: bucket, Prefix: prefix}, nil
}
