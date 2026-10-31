package vfs_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
)

// pattern is n deterministic bytes that differ from one offset to the next,
// so a read from the wrong offset never matches.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i ^ i>>8 ^ i>>16)
	}
	return b
}

func TestS3_anObjectLargerThanTheReadCapStreamsAndReadsAtOffsets(t *testing.T) {
	ctx := t.Context()
	client, bucket := startMinIO(ctx, t)
	ms, err := vfs.Tree(vfs.At("data", vfs.S3(vfs.AWSS3{Client: client}, bucket)))(ctx, "sess-s3-read", vfs.Request{
		Bindings: []vfs.Binding{{Params: map[string]string{vfs.ParamName: "data", "prefix": "reads"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	// Larger than ReadFile and OpenDocument allow: only a streaming handle reads it.
	want := pattern(vfs.MaxReadFileBytes + 1<<20 + 7)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("reads/big.bin"), Body: bytes.NewReader(want),
	}); err != nil {
		t.Fatal(err)
	}

	f, err := ms.Route(ctx, "/workspace/data/big.bin").Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 64)
	off := len(want) - 100
	n, atErr := f.(io.ReaderAt).ReadAt(buf, int64(off))
	got, readErr := io.ReadAll(f.(io.Reader))

	if atErr != nil || !bytes.Equal(buf[:n], want[off:off+64]) {
		t.Fatalf("ReadAt(%d) = %d bytes, err=%v", off, n, atErr)
	}
	if readErr != nil || !bytes.Equal(got, want) {
		t.Fatalf("streaming Read = %d bytes, err=%v", len(got), readErr)
	}
}

func TestGraph_anOffsetReadFailsWhenTheDownloadIsRefused(t *testing.T) {
	ctx := t.Context()
	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	fx.base = srv.URL
	ms, _ := mountGraphHTTP(t, srv, true)

	f, err := ms.Route(ctx, "/workspace/legal/exp.txt").Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.(io.ReaderAt).ReadAt(make([]byte, 1), 0); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("ReadAt = %v, want ErrAuthExpired", err)
	}
}

func TestLocal_aSymlinkReportsItsTargetsVersion(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(dir)))(ctx, "sess-local-version", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.Route(ctx, "/workspace/w/a.txt").WriteFile(ctx, []byte("target")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "a.txt"), filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}

	target, err := ms.Route(ctx, "/workspace/w/a.txt").Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	link, err := ms.Route(ctx, "/workspace/w/link.txt").Stat(ctx)

	if err != nil || link.Name != "link.txt" || link.Version != target.Version || link.Size != target.Size {
		t.Fatalf("symlink stat = %+v, target %+v, err=%v", link, target, err)
	}
}
