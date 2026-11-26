package vfs_test

import (
	"cmp"
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ryanaldo34/tacklr/internal/faultproxy"
	"github.com/ryanaldo34/tacklr/internal/s3test"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/awss3"
	"github.com/ryanaldo34/tacklr/vfs/azureblob"
)

// TestS3_opensOnlyWithAClientABucketAndACleanPrefix: S3 and Blob do not
// open without a client, a bucket (container) or a prefix that stays in
// it, and take the bucket or container of the binding over the default.
func TestS3_opensOnlyWithAClientABucketAndACleanPrefix(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	badPrefix := mount.Binding{Params: map[string]string{"prefix": "a/../b"}}
	opened := func(open vfs.Open, b mount.Binding) error {
		_, err := open(ctx, "s", b)
		return err
	}

	refused := map[string]error{
		"S3 without a client":          opened(vfs.S3(nil, "b"), mount.Binding{}),
		"S3 without a bucket":          opened(vfs.S3(awss3.API{}, ""), mount.Binding{}),
		"S3 with a prefix out of it":   opened(vfs.S3(awss3.API{}, "b"), badPrefix),
		"Blob without a client":        opened(vfs.Blob(nil, "c"), mount.Binding{}),
		"Blob without a container":     opened(vfs.Blob(azureblob.API{}, ""), mount.Binding{}),
		"Blob with a prefix out of it": opened(vfs.Blob(azureblob.API{}, "c"), badPrefix),
	}
	bound := map[string]error{
		"S3 with the binding's bucket":      opened(vfs.S3(awss3.API{}, ""), mount.Binding{Params: map[string]string{"bucket": "b"}}),
		"Blob with the binding's container": opened(vfs.Blob(azureblob.API{}, ""), mount.Binding{Params: map[string]string{"container": "c"}}),
	}

	for name, err := range refused {
		if err == nil {
			t.Errorf("%s opened, want an error", name)
		}
	}
	for name, err := range bound {
		if err != nil {
			t.Errorf("%s = %v", name, err)
		}
	}
}

// TestS3_failingServerFailsTheOperation: when the S3 server fails, cuts or
// answers "not found" to the request an operation sends, the operation
// returns that failure.
func TestS3_failingServerFailsTheOperation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, bucket := s3test.Start(t)
	putTyped(ctx, t, client, bucket, "f.txt", "some text", "text/plain")
	putTyped(ctx, t, client, bucket, "d/", "", "")
	putTyped(ctx, t, client, bucket, "d/a.txt", "a", "text/plain")
	putTyped(ctx, t, client, bucket, "i/a.txt", "a", "text/plain")
	request := func(method, key string) func(*http.Request) bool {
		return func(r *http.Request) bool {
			return r.Method == method && !r.URL.Query().Has("list-type") && strings.TrimPrefix(r.URL.Path, "/"+bucket+"/") == key
		}
	}
	list := func(r *http.Request) bool { return r.URL.Query().Has("list-type") }
	failed := faultproxy.Answer(http.StatusInternalServerError, "")
	notFound := faultproxy.Answer(http.StatusNotFound, "<Error><Code>NotFound</Code><Message>gone</Message></Error>")

	for _, tc := range []struct {
		name string
		rule faultproxy.Rule
		path string
		op   func(context.Context, vfs.Route) error
		want error // nil: any error
	}{
		{"open of a document whose head fails", faultproxy.Rule{Match: request(http.MethodHead, "f.txt"), Fault: failed}, "f.txt", routeOps["text"], nil},
		{"stat of a directory whose marker head fails", faultproxy.Rule{Match: request(http.MethodHead, "d/"), Fault: failed}, "d", routeOps["stat"], nil},
		{"stat of a directory without a marker whose list fails", faultproxy.Rule{Match: list, Fault: failed}, "i", routeOps["stat"], nil},
		{"open of a document whose get fails", faultproxy.Rule{Match: request(http.MethodGet, "f.txt"), Fault: failed}, "f.txt", routeOps["text"], nil},
		{"open of a document whose body is cut", faultproxy.Rule{Match: request(http.MethodGet, "f.txt"), Fault: faultproxy.Cut(2)}, "f.txt", routeOps["text"], io.ErrUnexpectedEOF},
		{"open of a document the server does not find", faultproxy.Rule{Match: request(http.MethodGet, "f.txt"), Fault: notFound}, "f.txt", routeOps["text"], vfs.ErrNotExist},
		{"read dir whose head fails", faultproxy.Rule{Match: request(http.MethodHead, "d"), Fault: failed}, "d", routeOps["list"], nil},
		{"read dir whose list fails", faultproxy.Rule{Match: list, Fault: failed}, "d", routeOps["list"], nil},
		{"remove of a directory whose list fails", faultproxy.Rule{Match: list, Fault: failed}, "d", routeOps["remove"], nil},
		{"mkdir whose marker head fails", faultproxy.Rule{Match: request(http.MethodHead, "m1/"), Fault: failed}, "m1", routeOps["mkdir"], nil},
		{"mkdir whose object head fails", faultproxy.Rule{Match: request(http.MethodHead, "m2"), Fault: failed}, "m2", routeOps["mkdir"], nil},
		{"mkdir whose marker put fails", faultproxy.Rule{Match: request(http.MethodPut, "m3/"), Fault: failed}, "m3", routeOps["mkdir"], nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proxy, proxied := s3test.Proxy(t)
			ms, err := vfs.Tree(vfs.At("data", vfs.S3(awss3.API{Client: proxied}, bucket)))(ctx, "s", vfs.Request{})
			if err != nil {
				t.Fatal(err)
			}
			proxy.Set(tc.rule)

			err = tc.op(ctx, ms.Route(ctx, "/workspace/data/"+tc.path))

			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, cmp.Or(tc.want, errors.New("an error")))
			}
		})
	}
}

// TestS3_readsKeysAHostWroteItself: a bucket mounted without a prefix
// lists its top-level prefixes, a prefix with no marker object is a
// directory, a key with an empty segment (which AWS keeps and this server
// refuses, so a proxy lists one) is left out of its listing, a directory
// that holds only a directory does not remove, and an object over the
// read cap does not read.
func TestS3_readsKeysAHostWroteItself(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, bucket := s3test.Start(t)
	putTyped(ctx, t, client, bucket, "runs/1/implicit/x.txt", "x", "text/plain")
	putTyped(ctx, t, client, bucket, "runs/1/outer/inner/", "", "")
	putTyped(ctx, t, client, bucket, "runs/1/big.txt", strings.Repeat("a", vfs.MaxReadFileBytes+1), "text/plain")
	proxy, proxied := s3test.Proxy(t)
	proxy.Set(faultproxy.Rule{
		Match: func(r *http.Request) bool { return r.URL.Query().Get("prefix") == "odd/" },
		Fault: faultproxy.Answer(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>`+bucket+`</Name><Prefix>odd/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>
<Contents><Key>odd/y.txt</Key><Size>1</Size></Contents><CommonPrefixes><Prefix>odd//</Prefix></CommonPrefixes></ListBucketResult>`),
	})
	ms, err := vfs.Tree(
		vfs.At("all", vfs.S3(awss3.API{Client: proxied}, bucket)),
		vfs.At("run", vfs.S3(awss3.API{Client: client}, bucket)),
	)(ctx, "s", vfs.Request{Bindings: []mount.Binding{{Params: map[string]string{mount.ParamName: "run", "prefix": "runs/1"}}}})
	if err != nil {
		t.Fatal(err)
	}

	top, topErr := ms.Route(ctx, "/workspace/all").ReadDir(ctx)
	odd, oddErr := ms.Route(ctx, "/workspace/all/odd").ReadDir(ctx)
	implicit, implicitErr := ms.Route(ctx, "/workspace/run/implicit").Stat(ctx)
	outerErr := ms.Route(ctx, "/workspace/run/outer").Remove(ctx)
	_, bigErr := ms.Route(ctx, "/workspace/run/big.txt").ReadText(ctx)

	if topErr != nil || !slices.ContainsFunc(top, func(e vfs.DirEntry) bool { return e.Name == "runs" && e.IsDir }) {
		t.Errorf("read dir of the bucket = %+v %v, want the runs directory", top, topErr)
	}
	if oddErr != nil || len(odd) != 1 || odd[0].Name != "y.txt" {
		t.Errorf("read dir over a key with an empty segment = %+v %v, want y.txt only", odd, oddErr)
	}
	if implicitErr != nil || !implicit.IsDir || implicit.Name != "implicit" {
		t.Errorf("stat of a directory without a marker = %+v %v, want a directory", implicit, implicitErr)
	}
	if outerErr == nil {
		t.Error("remove of a directory that holds only a directory succeeded, want not empty")
	}
	if !errors.Is(bigErr, vfs.ErrTooLarge) {
		t.Errorf("read text over the read limit = %v, want ErrTooLarge", bigErr)
	}
}

// TestS3_aStoredContentTypeChoosesTheCodec: the Content-Type an object was
// stored with is its media type, and decides its codec: a Markdown hint
// reads as Markdown, and an image hint over UTF-8 bytes has no codec.
func TestS3_aStoredContentTypeChoosesTheCodec(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, bucket := s3test.Start(t)
	putTyped(ctx, t, client, bucket, "notes", "# title\n\nbody\n", "text/markdown; charset=utf-8")
	putTyped(ctx, t, client, bucket, "blob", "looks like utf8 text", "image/png")
	ms, err := vfs.Tree(vfs.At("data", vfs.S3(awss3.API{Client: client}, bucket)))(ctx, "s", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	md, mdErr := ms.Route(ctx, "/workspace/data/notes").ReadText(ctx)
	_, pngErr := ms.Route(ctx, "/workspace/data/blob").OpenDocument(ctx)

	if mdErr != nil || md.MediaType() != "text/markdown" {
		t.Errorf("read text of a Markdown hint = %v %v, want text/markdown", md, mdErr)
	}
	if !errors.Is(pngErr, vfs.ErrNoCodec) {
		t.Errorf("document of an image hint = %v, want ErrNoCodec", pngErr)
	}
}

func putTyped(ctx context.Context, t *testing.T, client *s3.Client, bucket, key, body, contentType string) {
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
