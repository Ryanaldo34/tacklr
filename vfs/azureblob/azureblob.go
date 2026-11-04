// Package azureblob connects the vfs Blob provider to the Azure Blob SDK:
//
//	vfs.Blob(azureblob.API{Client: client}, container)
package azureblob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/ryanaldo34/tacklr/vfs"
)

// API is a vfs.S3API over the Azure Blob SDK (Azure Blob Storage and
// Azurite).
type API struct {
	Client *azblob.Client
}

func (a API) require() error {
	if a.Client == nil {
		return fmt.Errorf("azureblob: client required")
	}
	return nil
}

// Head implements vfs.S3API.
func (a API) Head(ctx context.Context, bucket, key string) (vfs.S3Object, error) {
	if err := a.require(); err != nil {
		return vfs.S3Object{}, err
	}
	props, err := a.Client.ServiceClient().NewContainerClient(bucket).NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		return vfs.S3Object{}, mapBlobError(err)
	}
	obj := vfs.S3Object{
		Size:        deref(props.ContentLength),
		ModTime:     deref(props.LastModified),
		ContentType: deref(props.ContentType),
	}
	if props.ETag != nil {
		obj.ETag = string(*props.ETag)
	}
	return obj, nil
}

// Get implements vfs.S3API.
func (a API) Get(ctx context.Context, bucket, key string, off, n int64) (io.ReadCloser, error) {
	if err := a.require(); err != nil {
		return nil, err
	}
	out, err := a.Client.DownloadStream(ctx, bucket, key, &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: off, Count: max(n, 0)},
	})
	if err != nil {
		return nil, mapBlobError(err)
	}
	return out.Body, nil
}

// Put implements vfs.S3API. UploadStream reads body in blocks; size is unused.
func (a API) Put(ctx context.Context, bucket, key string, body io.Reader, _ int64) error {
	if err := a.require(); err != nil {
		return err
	}
	_, err := a.Client.UploadStream(ctx, bucket, key, body, nil)
	return mapBlobError(err)
}

// Delete implements vfs.S3API.
func (a API) Delete(ctx context.Context, bucket, key string) error {
	if err := a.require(); err != nil {
		return err
	}
	_, err := a.Client.DeleteBlob(ctx, bucket, key, nil)
	return mapBlobError(err)
}

// List implements vfs.S3API with delimiter "/" for virtual directories.
func (a API) List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error) {
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

func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

func mapBlobError(err error) error {
	if err == nil {
		return nil
	}
	if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
		return vfs.ErrNotExist
	}
	var re *azcore.ResponseError
	if errors.As(err, &re) && re.StatusCode == http.StatusNotFound {
		return vfs.ErrNotExist
	}
	return err
}
