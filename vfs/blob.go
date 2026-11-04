package vfs

import (
	"cmp"
	"context"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// Blob opens Azure Blob providers that share one S3API client (HTTP pool).
// The object model matches S3: a container is the bucket, blob names are keys,
// and delimiter "/" listing gives virtual directories.
func Blob(client S3API, defaultContainer string) Open {
	return func(ctx context.Context, _ string, b mount.Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if client == nil {
			return nil, fmt.Errorf("vfs: blob client required")
		}
		name := cmp.Or(b.Params["container"], defaultContainer)
		if name == "" {
			return nil, fmt.Errorf("vfs: blob container required")
		}
		prefix := strings.Trim(b.Params["prefix"], "/")
		if err := validateS3Prefix(prefix); err != nil {
			return nil, err
		}
		return s3Provider{api: client, bucket: name, prefix: prefix}, nil
	}
}
