package vfs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/vfs"
)

const minioImage = "minio/minio:RELEASE.2024-06-13T22-53-53Z"

// TestMountSession_s3MinIO exercises real S3 path I/O against MinIO (no mocks).
func TestMountSession_s3MinIO(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MinIO integration test in -short mode")
	}
	ctx := context.Background()
	client, bucket := startMinIO(ctx, t)

	ms, err := vfs.Tree(
		vfs.At("data", builtins.S3(builtins.AWSS3{Client: client}, bucket)),
		vfs.At("ro", builtins.S3(builtins.AWSS3{Client: client}, bucket)).ReadOnly(),
	)(ctx, "sess-s3", vfs.Request{Bindings: []vfs.Binding{
		{Params: map[string]string{vfs.ParamName: "data", "prefix": "runs/1"}},
		{Params: map[string]string{vfs.ParamName: "ro", "prefix": "readonly"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	if err := ms.WriteFile(ctx, "/workspace/data/hello.go", []byte("package main\n")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	b, err := ms.ReadFile(ctx, "/workspace/data/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("ReadFile = %q err=%v", b, err)
	}
	st, err := ms.Stat(ctx, "/workspace/data/hello.go")
	if err != nil || st.IsDir || st.Size != int64(len("package main\n")) {
		t.Fatalf("Stat file = %+v err=%v", st, err)
	}

	if err := ms.MkdirAll(ctx, "/workspace/data/sub/dir"); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/data/sub/dir/a.txt", []byte("a")); err != nil {
		t.Fatalf("WriteFile nested: %v", err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/data/sub/dir")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 1 || ents[0].Name != "a.txt" || ents[0].IsDir {
		t.Fatalf("ReadDir = %+v", ents)
	}
	ents, err = ms.ReadDir(ctx, "/workspace/data/sub")
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

	if err := ms.Remove(ctx, "/workspace/data/sub/dir/a.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := ms.Stat(ctx, "/workspace/data/sub/dir/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	if err := ms.WriteFile(ctx, "/workspace/ro/x.txt", []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write: %v", err)
	}

	if err := ms.WriteFile(ctx, "/workspace/data/excl.txt", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/data/excl.txt", []byte("2")); err != nil {
		t.Fatal(err)
	}
	b, err = ms.ReadFile(ctx, "/workspace/data/excl.txt")
	if err != nil || string(b) != "2" {
		t.Fatalf("overwrite: %q %v", b, err)
	}

	// Directory stat
	st, err = ms.Stat(ctx, "/workspace/data/sub/dir")
	if err != nil || !st.IsDir {
		t.Fatalf("Stat dir = %+v err=%v", st, err)
	}
}

func startMinIO(ctx context.Context, t *testing.T) (*s3.Client, string) {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: minioImage,
			Env: map[string]string{
				"MINIO_ROOT_USER":     "minioadmin",
				"MINIO_ROOT_PASSWORD": "minioadmin",
			},
			Cmd:          []string{"server", "/data"},
			ExposedPorts: []string{"9000/tcp"},
			WaitingFor:   wait.ForHTTP("/minio/health/live").WithPort("9000/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	const bucket = "vfs-test"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	return client, bucket
}
