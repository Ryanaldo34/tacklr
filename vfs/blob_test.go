package vfs_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/azuritetest"
	"github.com/ryanaldo34/tacklr/internal/faultproxy"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/azureblob"
)

// TestBlob_serverAnswersReachTheCaller: when Azure Blob fails a request,
// answers 404 without an error code, lists a page without a segment, or
// omits a blob's properties, the operation returns what the answer means.
func TestBlob_serverAnswersReachTheCaller(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, container := azuritetest.Start(t)
	if _, err := client.UploadBuffer(ctx, container, "f.txt", []byte("some text"), nil); err != nil {
		t.Fatal(err)
	}
	proxy, proxied := azuritetest.Proxy(t)
	ms, err := vfs.Tree(vfs.At("data", vfs.Blob(azureblob.API{Client: proxied}, container)))(ctx, "s", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	route := func(name string) vfs.Route { return ms.Route(ctx, "/workspace/data/"+name) }
	method := func(m string) func(*http.Request) bool {
		return func(r *http.Request) bool { return r.Method == m && r.URL.Query().Get("comp") != "list" }
	}
	list := func(r *http.Request) bool { return r.URL.Query().Get("comp") == "list" }

	proxy.Set(faultproxy.Rule{Match: list, Fault: faultproxy.Answer(http.StatusInternalServerError, "")})
	_, listErr := route("").ReadDir(ctx)
	proxy.Set(faultproxy.Rule{Match: list, Fault: faultproxy.Answer(http.StatusOK, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="`+container+`"></EnumerationResults>`)})
	noSegment, noSegmentErr := route("").ReadDir(ctx)
	proxy.Set(faultproxy.Rule{Match: method(http.MethodGet), Fault: faultproxy.Answer(http.StatusNotFound, "")})
	_, uncodedErr := route("f.txt").ReadText(ctx)
	proxy.Set(faultproxy.Rule{Match: method(http.MethodHead), Fault: faultproxy.Answer(http.StatusOK, "")})
	bare, bareErr := route("f.txt").Stat(ctx)

	if listErr == nil || errors.Is(listErr, vfs.ErrNotExist) {
		t.Errorf("read dir whose list fails = %v, want the server error", listErr)
	}
	if noSegmentErr != nil || len(noSegment) != 0 {
		t.Errorf("read dir of a page without a segment = %+v %v, want an empty directory", noSegment, noSegmentErr)
	}
	if !errors.Is(uncodedErr, vfs.ErrNotExist) {
		t.Errorf("read of a blob the server answers 404 without a code = %v, want ErrNotExist", uncodedErr)
	}
	if bareErr != nil || bare.Size != 0 || !bare.ModTime.IsZero() || bare.Version != "" {
		t.Errorf("stat of a blob without properties = %+v %v, want a file with zero size, time and version", bare, bareErr)
	}
}
