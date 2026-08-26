package vfs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/vfs"
)

const (
	azuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.33.0"
	azuriteKey   = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteAcct  = "devstoreaccount1"
)

// TestMountSession_azureBlobAzurite exercises real Blob path I/O against Azurite (no mocks).
func TestMountSession_azureBlobAzurite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Azurite integration test in -short mode")
	}
	ctx := t.Context()
	client, container := startAzurite(ctx, t)

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.BlobFactory{
		ID:               "blob",
		Client:           vfs.AzureBlob{Client: client},
		DefaultContainer: container,
	}); err != nil {
		t.Fatal(err)
	}

	ms, err := vfs.NewMountSession("sess-blob", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Materialize(ctx, []vfs.MountSpec{
		{Point: "/data", Profile: "blob", Params: map[string]string{"prefix": "runs/1"}},
		{Point: "/ro", Profile: "blob", ReadOnly: true, Params: map[string]string{"prefix": "readonly"}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := ms.WriteFile(ctx, "/data/hello.go", []byte("package main\n")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	b, err := ms.ReadFile(ctx, "/data/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("ReadFile = %q err=%v", b, err)
	}
	st, err := ms.Stat(ctx, "/data/hello.go")
	if err != nil || st.IsDir || st.Size != int64(len("package main\n")) {
		t.Fatalf("Stat file = %+v err=%v", st, err)
	}

	if err := ms.MkdirAll(ctx, "/data/sub/dir"); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := ms.WriteFile(ctx, "/data/sub/dir/a.txt", []byte("a")); err != nil {
		t.Fatalf("WriteFile nested: %v", err)
	}
	ents, err := ms.ReadDir(ctx, "/data/sub/dir")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 1 || ents[0].Name != "a.txt" || ents[0].IsDir {
		t.Fatalf("ReadDir = %+v", ents)
	}
	ents, err = ms.ReadDir(ctx, "/data/sub")
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

	if err := ms.Remove(ctx, "/data/sub/dir/a.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := ms.Stat(ctx, "/data/sub/dir/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	if err := ms.WriteFile(ctx, "/ro/x.txt", []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write: %v", err)
	}

	if err := ms.WriteFile(ctx, "/data/excl.txt", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/data/excl.txt", []byte("2")); err != nil {
		t.Fatal(err)
	}
	b, err = ms.ReadFile(ctx, "/data/excl.txt")
	if err != nil || string(b) != "2" {
		t.Fatalf("overwrite: %q %v", b, err)
	}

	st, err = ms.Stat(ctx, "/data/sub/dir")
	if err != nil || !st.IsDir {
		t.Fatalf("Stat dir = %+v err=%v", st, err)
	}
}

func startAzurite(ctx context.Context, t *testing.T) (*azblob.Client, string) {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        azuriteImage,
			Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--skipApiVersionCheck"},
			ExposedPorts: []string{"10000/tcp"},
			WaitingFor:   wait.ForListeningPort("10000/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("Azurite unavailable: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "10000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	conn := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://%s:%s/%s;",
		azuriteAcct, azuriteKey, host, port.Port(), azuriteAcct,
	)
	client, err := azblob.NewClientFromConnectionString(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	const name = "vfs-test"
	if _, err := client.CreateContainer(ctx, name, nil); err != nil {
		t.Fatal(err)
	}
	return client, name
}
