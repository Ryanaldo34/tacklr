package vfs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// Blob opens Azure Blob providers that share one S3API client (HTTP pool).
// The object model matches S3: a container is the bucket, blob names are keys,
// and delimiter "/" listing gives virtual directories.
func Blob(client S3API, defaultContainer string) Open {
	return func(ctx context.Context, _ string, b Binding) (Provider, error) {
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

// AzureBlob implements S3API with the Azure Blob SDK (Azurite and real Blob Storage).
type AzureBlob struct {
	Client *azblob.Client
}

func (a AzureBlob) require() error {
	if a.Client == nil {
		return fmt.Errorf("vfs: Azure Blob client required")
	}
	return nil
}

// Head implements S3API.
func (a AzureBlob) Head(ctx context.Context, bucket, key string) (int64, time.Time, string, error) {
	if err := a.require(); err != nil {
		return 0, time.Time{}, "", err
	}
	props, err := a.Client.ServiceClient().NewContainerClient(bucket).NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		return 0, time.Time{}, "", mapBlobError(err)
	}
	size, mod, ct := blobMeta(props.ContentLength, props.LastModified, props.ContentType)
	return size, mod, ct, nil
}

// Get implements S3API.
func (a AzureBlob) Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, time.Time, error) {
	if err := a.require(); err != nil {
		return nil, 0, time.Time{}, err
	}
	out, err := a.Client.DownloadStream(ctx, bucket, key, nil)
	if err != nil {
		return nil, 0, time.Time{}, mapBlobError(err)
	}
	size, mod, _ := blobMeta(out.ContentLength, out.LastModified, nil)
	return out.Body, size, mod, nil
}

// Put implements S3API. UploadStream reads body in blocks; size is unused.
func (a AzureBlob) Put(ctx context.Context, bucket, key string, body io.Reader, _ int64) error {
	if err := a.require(); err != nil {
		return err
	}
	_, err := a.Client.UploadStream(ctx, bucket, key, body, nil)
	return mapBlobError(err)
}

// Delete implements S3API.
func (a AzureBlob) Delete(ctx context.Context, bucket, key string) error {
	if err := a.require(); err != nil {
		return err
	}
	_, err := a.Client.DeleteBlob(ctx, bucket, key, nil)
	return mapBlobError(err)
}

// List implements S3API with delimiter "/" for virtual directories.
func (a AzureBlob) List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error) {
	if err := a.require(); err != nil {
		return nil, nil, err
	}
	opts := &container.ListBlobsHierarchyOptions{}
	if prefix != "" {
		opts.Prefix = &prefix
	}
	pager := a.Client.ServiceClient().NewContainerClient(bucket).NewListBlobsHierarchyPager("/", opts)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, mapBlobError(err)
		}
		if page.Segment == nil {
			continue
		}
		for _, obj := range page.Segment.BlobItems {
			if obj != nil && obj.Name != nil {
				keys = append(keys, *obj.Name)
			}
		}
		for _, p := range page.Segment.BlobPrefixes {
			if p != nil && p.Name != nil {
				dirs = append(dirs, *p.Name)
			}
		}
	}
	return keys, dirs, nil
}

func blobMeta(size *int64, mod *time.Time, contentType *string) (int64, time.Time, string) {
	var n int64
	var t time.Time
	var ct string
	if size != nil {
		n = *size
	}
	if mod != nil {
		t = *mod
	}
	if contentType != nil {
		ct = *contentType
	}
	return n, t, ct
}

func mapBlobError(err error) error {
	if err == nil {
		return nil
	}
	if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
		return ErrNotExist
	}
	var re *azcore.ResponseError
	if errors.As(err, &re) && re.StatusCode == http.StatusNotFound {
		return ErrNotExist
	}
	return err
}
