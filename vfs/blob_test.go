package vfs_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/internal/faultproxy"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/azureblob"
)

const (
	azuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.33.0"
	azuriteKey   = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteAcct  = "devstoreaccount1"
)

// TestBlob_rejectsBadConfig: Blob does not open without a client, a
// container, a clean prefix or a live context, and the Azure API does not
// send a request without a client.
func TestBlob_rejectsBadConfig(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	if _, err := vfs.Blob(nil, "")(ctx, "s", mount.Binding{}); err == nil {
		t.Fatal("nil client")
	}
	if _, err := vfs.Blob(azureblob.API{}, "")(ctx, "s", mount.Binding{}); err == nil {
		t.Fatal("missing container")
	}
	if _, err := vfs.Blob(azureblob.API{}, "c")(ctx, "s", mount.Binding{
		Params: map[string]string{"prefix": "a/../b"},
	}); err == nil {
		t.Fatal("bad prefix")
	}
	var azure azureblob.API
	if _, err := azure.Head(ctx, "c", "k"); err == nil {
		t.Fatal("nil Azure Head")
	}
	if _, err := azure.Get(ctx, "c", "k", 0, -1); err == nil {
		t.Fatal("nil Azure Get")
	}
	if err := azure.Put(ctx, "c", "k", bytes.NewReader(nil), 0); err == nil {
		t.Fatal("nil Azure Put")
	}
	if err := azure.Delete(ctx, "c", "k"); err == nil {
		t.Fatal("nil Azure Delete")
	}
	if _, _, err := azure.List(ctx, "c", ""); err == nil {
		t.Fatal("nil Azure List")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := vfs.Blob(azureblob.API{}, "c")(canceled, "s", mount.Binding{}); !errors.Is(err, context.Canceled) {
		t.Fatal("Open canceled")
	}
}

// TestMountSession_azureBlobAzurite exercises real Blob path I/O against Azurite (no mocks).
func TestMountSession_azureBlobAzurite(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, container := startAzurite(t)

	ms, err := vfs.Tree(
		vfs.At("data", vfs.Blob(azureblob.API{Client: client}, container)),
		vfs.At("ro", vfs.Blob(azureblob.API{Client: client}, container)).ReadOnly(),
	)(ctx, "sess-blob", vfs.Request{Bindings: []mount.Binding{
		{Params: map[string]string{mount.ParamName: "data", "prefix": "runs/1"}},
		{Params: map[string]string{mount.ParamName: "ro", "prefix": "readonly"}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/data/hello.go").
		WriteFile(ctx, []byte("package main\n")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	b, err := ms.Route(ctx, "/workspace/data/hello.go").
		ReadFile(ctx)
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("ReadFile = %q err=%v", b, err)
	}

	st, err := ms.Route(ctx, "/workspace/data/hello.go").
		Stat(ctx)
	if err != nil || st.IsDir || st.Size != int64(len("package main\n")) {
		t.Fatalf("Stat file = %+v err=%v", st, err)
	}

	if err := ms.Route(ctx, "/workspace/data/sub/dir").
		MkdirAll(ctx); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/data/sub/dir/a.txt").
		WriteFile(ctx, []byte("a")); err != nil {
		t.Fatalf("WriteFile nested: %v", err)
	}

	ents, err := ms.Route(ctx, "/workspace/data/sub/dir").
		ReadDir(ctx)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 1 || ents[0].Name != "a.txt" || ents[0].IsDir {
		t.Fatalf("ReadDir = %+v", ents)
	}

	ents, err = ms.Route(ctx, "/workspace/data/sub").
		ReadDir(ctx)
	if err != nil {
		t.Fatalf("ReadDir sub: %v", err)
	}
	foundDir := false
	for _, e := range ents {
		if e.Name == "dir" && e.IsDir {
			foundDir = true
		}
	}
	if !foundDir {
		t.Fatalf("expected dir in /data/sub: %+v", ents)
	}

	if err := ms.Route(ctx, "/workspace/data/sub/dir/a.txt").
		Remove(ctx); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/data/sub/dir/a.txt").
		Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/ro/x.txt").
		WriteFile(ctx, []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/data/excl.txt").
		WriteFile(ctx, []byte("1")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/data/excl.txt").
		WriteFile(ctx, []byte("2")); err != nil {
		t.Fatal(err)
	}

	if b, err := ms.Route(ctx, "/workspace/data/excl.txt").
		ReadFile(ctx); err != nil || string(b) != "2" {
		t.Fatalf("overwrite: %q %v", b, err)
	}

	if st, err := ms.Route(ctx, "/workspace/data/sub/dir").
		Stat(ctx); err != nil || !st.IsDir {
		t.Fatalf("Stat dir = %+v err=%v", st, err)
	}
}

// TestBlob_serverAnswersReachTheCaller: when Azure Blob fails a request,
// answers 404 without an error code, lists a page without a segment, or
// omits a blob's properties, the operation returns what the answer means.
func TestBlob_serverAnswersReachTheCaller(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, container := startAzurite(t)
	if _, err := client.UploadBuffer(ctx, container, "f.txt", []byte("some text"), nil); err != nil {
		t.Fatal(err)
	}
	proxy := faultproxy.Start(t, azurite(t))
	ms, err := vfs.Tree(vfs.At("data", vfs.Blob(azureblob.API{Client: azuriteClient(t, proxy.URL(), -1)}, container)))(ctx, "s", vfs.Request{})
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

var (
	azuriteOnce sync.Once
	azuriteURL  *url.URL
	azuriteErr  error
)

// azurite returns the base URL of the Azurite server this test binary
// shares, starting it on first use. Ryuk removes it when the binary ends.
func azurite(t *testing.T) *url.URL {
	t.Helper()
	azuriteOnce.Do(func() {
		ctx := context.Background()
		ctr, err := testcontainers.Run(ctx, azuriteImage,
			testcontainers.WithCmd("azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--skipApiVersionCheck"),
			testcontainers.WithExposedPorts("10000/tcp"),
			testcontainers.WithWaitStrategy(wait.ForListeningPort("10000/tcp")),
		)
		if err != nil {
			azuriteErr = err
			return
		}
		var endpoint string
		if endpoint, azuriteErr = ctr.PortEndpoint(ctx, "10000/tcp", "http"); azuriteErr == nil {
			azuriteURL, azuriteErr = url.Parse(endpoint)
		}
	})
	if azuriteErr != nil {
		t.Fatalf("Azurite: %v", azuriteErr)
	}
	return azuriteURL
}

// azuriteClient is a client of the Azurite account at endpoint, the server
// or a proxy in front of it, that tries a request up to retries+1 times. A
// client through a proxy does not retry, so a fault the proxy injects
// reaches the caller once.
func azuriteClient(t *testing.T, endpoint string, retries int32) *azblob.Client {
	t.Helper()
	conn := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;",
		azuriteAcct, azuriteKey, endpoint, azuriteAcct)
	client, err := azblob.NewClientFromConnectionString(conn, &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: retries, RetryDelay: 100 * time.Millisecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// startAzurite returns a client of the shared Azurite and a new container
// on it that no other test uses.
func startAzurite(t *testing.T) (*azblob.Client, string) {
	t.Helper()
	client := azuriteClient(t, azurite(t).String(), 8)
	name := "t" + strings.ToLower(rand.Text())
	if _, err := client.CreateContainer(t.Context(), name, nil); err != nil {
		t.Fatal(err)
	}
	return client, name
}
