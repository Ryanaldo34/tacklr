package vfs

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

func TestMapBlobError_notFoundAndPassthrough(t *testing.T) {
	if err := mapBlobError(nil); err != nil {
		t.Fatalf("nil: %v", err)
	}
	notFound := &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound), StatusCode: http.StatusNotFound}
	if err := mapBlobError(notFound); !errors.Is(err, ErrNotExist) {
		t.Fatalf("blob not found: %v", err)
	}
	statusOnly := &azcore.ResponseError{StatusCode: http.StatusNotFound}
	if err := mapBlobError(statusOnly); !errors.Is(err, ErrNotExist) {
		t.Fatalf("http 404: %v", err)
	}
	other := errors.New("throttle")
	if err := mapBlobError(other); !errors.Is(err, other) {
		t.Fatalf("passthrough: %v", err)
	}
}
