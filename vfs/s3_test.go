package vfs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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

	st, err = ms.Stat(ctx, "/workspace/data/sub/dir")
	if err != nil || !st.IsDir {
		t.Fatalf("Stat dir = %+v err=%v", st, err)
	}
	st, err = ms.Stat(ctx, "/workspace/data")
	if err != nil || !st.IsDir {
		t.Fatalf("stat mount root: %+v err=%v", st, err)
	}

	f, err := ms.Open(ctx, "/workspace/data/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(); err != nil {
		t.Fatal(err)
	}
	gotOpen, err := io.ReadAll(f.(io.Reader))
	_ = f.Close()
	if err != nil || !strings.Contains(string(gotOpen), "package main") {
		t.Fatalf("Open read: %q err=%v", gotOpen, err)
	}

	if err := ms.Remove(ctx, "/workspace/data/sub/dir"); err != nil {
		t.Fatalf("empty dir remove: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/data/sub/keep.txt", []byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := ms.Remove(ctx, "/workspace/data/sub"); err == nil {
		t.Fatal("non-empty dir remove")
	}
	if err := ms.Remove(ctx, "/workspace/data/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}

	if _, err := ms.ReadDir(ctx, "/workspace/data/hello.go"); err == nil {
		t.Fatal("ReadDir file")
	}
	if err := ms.MkdirAll(ctx, "/workspace/data/hello.go/nested"); err == nil {
		t.Fatal("MkdirAll through file")
	}
	if _, err := ms.ReadText(ctx, "/workspace/data/sub"); err == nil {
		t.Fatal("ReadText dir")
	}

	doc, err := ms.ReadText(ctx, "/workspace/data/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.ReplaceLines(1, 2, []string{"package main // edited"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	body, err := ms.ReadFile(ctx, "/workspace/data/hello.go")
	if err != nil || !strings.Contains(string(body), "edited") {
		t.Fatalf("ReadFile after WriteDocument: %q err=%v", body, err)
	}
	if mt, err := ms.Classify(ctx, "/workspace/data/hello.go", nil); err != nil || mt == "" {
		t.Fatalf("Classify: %q err=%v", mt, err)
	}
	if spec, err := ms.SpecAt("/workspace/data/hello.go"); err != nil || spec.Point != "/workspace/data" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}

	putTyped(t, ctx, client, bucket, "runs/1/notes", "# title\n\nbody\n", "text/markdown; charset=utf-8")
	putTyped(t, ctx, client, bucket, "runs/1/blob", "looks like utf8 text", "image/png")
	putTyped(t, ctx, client, bucket, "runs/1/main.go", "package main\n", "application/octet-stream")
	st, err = ms.Stat(ctx, "/workspace/data/notes")
	if err != nil || st.MediaType != "text/markdown" {
		t.Fatalf("Stat MediaType=%q err=%v", st.MediaType, err)
	}
	md, err := ms.ReadText(ctx, "/workspace/data/notes")
	if err != nil || md.MediaType() != "text/markdown" {
		t.Fatalf("hinted markdown: mt=%q err=%v", mediaOf(md), err)
	}
	if _, err := ms.OpenDocument(ctx, "/workspace/data/blob", nil); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("image/png hint should skip sniff: %v", err)
	}
	st, err = ms.Stat(ctx, "/workspace/data/main.go")
	if err != nil || st.MediaType != "text/x-go" {
		t.Fatalf("octet-stream + .go key: Stat MediaType=%q err=%v", st.MediaType, err)
	}

	open := builtins.S3(builtins.AWSS3{Client: client}, bucket)
	p, err := open(ctx, "sess", vfs.Binding{Params: map[string]string{"prefix": "direct"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	wf, err := p.OpenFile(ctx, "new.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.(io.Writer).Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if fi, err := wf.Stat(); err != nil || fi.Size != 5 {
		t.Fatalf("write Stat: %+v err=%v", fi, err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.OpenFile(ctx, "new.txt", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, vfs.ErrExist) {
		t.Fatalf("O_EXCL: %v", err)
	}
	af, err := p.OpenFile(ctx, "new.txt", os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := af.(io.Writer).Write([]byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := af.Close(); err != nil {
		t.Fatal(err)
	}
	rf, err := p.OpenFile(ctx, "new.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rf.(io.Reader))
	_ = rf.Close()
	if err != nil || string(got) != "hello!" {
		t.Fatalf("append result %q err=%v", got, err)
	}
	if _, err := p.OpenFile(ctx, ".", os.O_RDONLY, 0); err == nil {
		t.Fatal("open root as file")
	}

	blobMS, err := vfs.Tree(vfs.At("data", builtins.Blob(builtins.AWSS3{Client: client}, "")))(ctx, "blob-s3", vfs.Request{Bindings: []vfs.Binding{{
		Params: map[string]string{vfs.ParamName: "data", "container": bucket, "prefix": "blobrun"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobMS.Close() })
	if err := blobMS.WriteFile(ctx, "/workspace/data/hello.go", []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	b, err = blobMS.ReadFile(ctx, "/workspace/data/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("Blob container param: %q err=%v", b, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(canceled, "/workspace/data/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat cancel: %v", err)
	}
	if _, err := ms.Open(canceled, "/workspace/data/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancel: %v", err)
	}
	if _, err := ms.ReadDir(canceled, "/workspace/data/sub"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir cancel: %v", err)
	}
	if err := ms.Remove(canceled, "/workspace/data/excl.txt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Remove cancel: %v", err)
	}
	if err := ms.MkdirAll(canceled, "/workspace/data/newdir"); !errors.Is(err, context.Canceled) {
		t.Fatalf("MkdirAll cancel: %v", err)
	}
	if _, err := ms.ReadText(canceled, "/workspace/data/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadText cancel: %v", err)
	}
	if err := p.Validate(canceled); err == nil {
		t.Fatal("Validate canceled")
	}
}

func putTyped(t *testing.T, ctx context.Context, client *s3.Client, bucket, key, body, contentType string) {
	t.Helper()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          strings.NewReader(body),
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func mediaOf(d vfs.Textual) string {
	if d == nil {
		return ""
	}
	return d.MediaType()
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
