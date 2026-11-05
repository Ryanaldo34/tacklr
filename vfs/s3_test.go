package vfs_test

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
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
)

// TestS3_rejectsBadConfigBadNamesAndAnEndedContext: S3 does not open without
// a client, a bucket or a clean prefix. Its provider rejects a name outside
// the mount, a write or remove of the mount root, and an operation whose
// context ended, before it sends a request.
func TestS3_rejectsBadConfigBadNamesAndAnEndedContext(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	// awss3.API{} has no client: an operation that sends a request fails
	// with "client required", not with the error a case wants.
	var noClient awss3.API
	p, err := vfs.S3(noClient, "b")(ctx, "s", mount.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(vfs.At("data", vfs.S3(noClient, "b")))(ctx, "s", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	route := func(name string) vfs.Route { return ms.Route(ctx, "/workspace/data/"+name) }
	type putter interface {
		PutFile(context.Context, string, io.Reader, int64) error
	}
	type documents interface {
		OpenDocument(context.Context, string, *vfs.ContentRegistry) (vfs.Document, error)
	}
	_, nilClient := vfs.S3(nil, "")(ctx, "s", mount.Binding{})
	_, noBucket := vfs.S3(noClient, "")(ctx, "s", mount.Binding{})
	_, badPrefix := vfs.S3(noClient, "b")(ctx, "s", mount.Binding{Params: map[string]string{"prefix": "a/../b"}})
	_, headErr := noClient.Head(ctx, "b", "k")
	_, getErr := noClient.Get(ctx, "b", "k", 0, -1)
	_, _, listErr := noClient.List(ctx, "b", "")
	_, statOutside := p.Stat(ctx, "../x")
	_, openOutside := p.OpenFile(ctx, "../x", os.O_RDONLY, 0)
	_, readDirOutside := p.ReadDir(ctx, "../x")
	_, documentOutside := p.(documents).OpenDocument(ctx, "../x", nil)
	_, openedCanceled := vfs.S3(noClient, "b")(canceled, "s", mount.Binding{})
	_, statCanceled := route("f").Stat(canceled)
	_, openCanceled := route("f").Open(canceled)
	_, readDirCanceled := route("d").ReadDir(canceled)
	_, documentCanceled := route("f").OpenDocument(canceled)

	for name, got := range map[string]error{
		"open without a client": nilClient, "open without a bucket": noBucket, "open with a prefix outside the bucket": badPrefix,
		"head without a client": headErr, "get without a client": getErr, "list without a client": listErr,
		"put without a client": noClient.Put(ctx, "b", "k", bytes.NewReader(nil), 0), "delete without a client": noClient.Delete(ctx, "b", "k"),
		"write of the mount root": p.(putter).PutFile(ctx, ".", bytes.NewReader(nil), 0),
	} {
		if got == nil {
			t.Errorf("%s succeeded, want an error", name)
		}
	}
	for name, got := range map[string]error{
		"stat": statOutside, "open": openOutside, "read dir": readDirOutside, "open document": documentOutside,
		"put": p.(putter).PutFile(ctx, "../x", bytes.NewReader(nil), 0), "remove": p.Remove(ctx, "../x"),
		"mkdir": p.MkdirAll(ctx, "../x", 0o755), "remove of the mount root": p.Remove(ctx, "."),
	} {
		if !errors.Is(got, vfs.ErrInvalidPath) {
			t.Errorf("%s outside the mount = %v, want ErrInvalidPath", name, got)
		}
	}
	if err := p.MkdirAll(ctx, ".", 0o755); err != nil {
		t.Errorf("mkdir of the mount root = %v, want success: it exists", err)
	}
	for name, got := range map[string]error{
		"open": openedCanceled, "validate": p.Validate(canceled), "stat": statCanceled, "open file": openCanceled,
		"read dir": readDirCanceled, "open document": documentCanceled, "write": route("f").WriteFile(canceled, nil),
		"write document": route("f").WriteDocument(canceled, nil), "remove": route("f").Remove(canceled),
		"mkdir": route("d").MkdirAll(canceled),
	} {
		if !errors.Is(got, context.Canceled) {
			t.Errorf("%s with an ended context = %v, want context.Canceled", name, got)
		}
	}
}

// TestS3_failingServerFailsTheOperation: when the S3 server fails, cuts or
// answers "not found" to the request an operation sends, the operation
// returns that failure.
func TestS3_failingServerFailsTheOperation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, bucket := startS3(t)
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
	stat := func(name string) func(*vfs.MountSession, vfs.Provider) error {
		return func(ms *vfs.MountSession, _ vfs.Provider) error {
			_, err := ms.Route(ctx, "/workspace/data/"+name).Stat(ctx)
			return err
		}
	}
	readText := func(ms *vfs.MountSession, _ vfs.Provider) error {
		_, err := ms.Route(ctx, "/workspace/data/f.txt").ReadText(ctx)
		return err
	}
	openFile := func(name string, flag int) func(*vfs.MountSession, vfs.Provider) error {
		return func(_ *vfs.MountSession, p vfs.Provider) error {
			_, err := p.OpenFile(ctx, name, flag, 0o644)
			return err
		}
	}
	readDir := func(ms *vfs.MountSession, _ vfs.Provider) error {
		_, err := ms.Route(ctx, "/workspace/data/d").ReadDir(ctx)
		return err
	}
	mkdir := func(name string) func(*vfs.MountSession, vfs.Provider) error {
		return func(ms *vfs.MountSession, _ vfs.Provider) error {
			return ms.Route(ctx, "/workspace/data/"+name).MkdirAll(ctx)
		}
	}

	for _, tc := range []struct {
		name string
		rule faultproxy.Rule
		act  func(*vfs.MountSession, vfs.Provider) error
		want error // nil: any error
	}{
		{name: "open of a document whose head fails", rule: faultproxy.Rule{Match: request(http.MethodHead, "f.txt"), Fault: failed}, act: readText},
		{name: "stat of a directory whose marker head fails", rule: faultproxy.Rule{Match: request(http.MethodHead, "d/"), Fault: failed}, act: stat("d")},
		{name: "stat of a directory without a marker whose list fails", rule: faultproxy.Rule{Match: list, Fault: failed}, act: stat("i")},
		{name: "open of a document whose get fails", rule: faultproxy.Rule{Match: request(http.MethodGet, "f.txt"), Fault: failed}, act: readText},
		{name: "open of a document whose body is cut", rule: faultproxy.Rule{Match: request(http.MethodGet, "f.txt"), Fault: faultproxy.Cut(2)}, act: readText, want: io.ErrUnexpectedEOF},
		{name: "open of a document the server does not find", rule: faultproxy.Rule{Match: request(http.MethodGet, "f.txt"), Fault: notFound}, act: readText, want: vfs.ErrNotExist},
		{name: "exclusive create whose head fails", rule: faultproxy.Rule{Match: request(http.MethodHead, "n.txt"), Fault: failed}, act: openFile("n.txt", os.O_CREATE|os.O_EXCL|os.O_WRONLY)},
		{name: "append whose get fails", rule: faultproxy.Rule{Match: request(http.MethodGet, "f.txt"), Fault: failed}, act: openFile("f.txt", os.O_WRONLY|os.O_APPEND)},
		{name: "read dir whose head fails", rule: faultproxy.Rule{Match: request(http.MethodHead, "d"), Fault: failed}, act: readDir},
		{name: "read dir whose list fails", rule: faultproxy.Rule{Match: list, Fault: failed}, act: readDir},
		{name: "remove of a directory whose list fails", rule: faultproxy.Rule{Match: list, Fault: failed}, act: func(ms *vfs.MountSession, _ vfs.Provider) error {
			return ms.Route(ctx, "/workspace/data/d").Remove(ctx)
		}},
		{name: "mkdir whose marker head fails", rule: faultproxy.Rule{Match: request(http.MethodHead, "m1/"), Fault: failed}, act: mkdir("m1")},
		{name: "mkdir whose object head fails", rule: faultproxy.Rule{Match: request(http.MethodHead, "m2"), Fault: failed}, act: mkdir("m2")},
		{name: "mkdir whose marker put fails", rule: faultproxy.Rule{Match: request(http.MethodPut, "m3/"), Fault: failed}, act: mkdir("m3")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proxy, proxied := s3test.Proxy(t)
			open := vfs.S3(awss3.API{Client: proxied}, bucket)
			ms, err := vfs.Tree(vfs.At("data", open))(ctx, "s", vfs.Request{})
			if err != nil {
				t.Fatal(err)
			}
			p, err := open(ctx, "s", mount.Binding{})
			if err != nil {
				t.Fatal(err)
			}
			proxy.Set(tc.rule)

			err = tc.act(ms, p)

			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, cmp.Or(tc.want, errors.New("an error")))
			}
		})
	}
}

// TestMountSession_s3 exercises real S3 path I/O against an S3 server (no mocks).
func TestMountSession_s3(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, bucket := startS3(t)

	ms, err := vfs.Tree(
		vfs.At("data", vfs.S3(awss3.API{Client: client}, bucket)),
		vfs.At("ro", vfs.S3(awss3.API{Client: client}, bucket)).ReadOnly(),
	)(ctx, "sess-s3", vfs.Request{Bindings: []mount.Binding{
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

	b, err = ms.Route(ctx, "/workspace/data/excl.txt").
		ReadFile(ctx)
	if err != nil || string(b) != "2" {
		t.Fatalf("overwrite: %q %v", b, err)
	}

	st, err = ms.Route(ctx, "/workspace/data/sub/dir").
		Stat(ctx)
	if err != nil || !st.IsDir {
		t.Fatalf("Stat dir = %+v err=%v", st, err)
	}

	st, err = ms.Route(ctx, "/workspace/data").
		Stat(ctx)
	if err != nil || !st.IsDir {
		t.Fatalf("stat mount root: %+v err=%v", st, err)
	}

	f, err := ms.Route(ctx, "/workspace/data/hello.go").
		Open(ctx)
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

	if err := ms.Route(ctx, "/workspace/data/sub/dir").
		Remove(ctx); err != nil {
		t.Fatalf("empty dir remove: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/data/sub/keep.txt").
		WriteFile(ctx, []byte("k")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/data/sub").
		Remove(ctx); err == nil {
		t.Fatal("non-empty dir remove")
	}

	if err := ms.Route(ctx, "/workspace/data/missing").
		Remove(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/data/hello.go").
		ReadDir(ctx); err == nil {
		t.Fatal("ReadDir file")
	}

	if err := ms.Route(ctx, "/workspace/data/hello.go/nested").
		MkdirAll(ctx); err == nil {
		t.Fatal("MkdirAll through file")
	}

	if _, err := ms.Route(ctx, "/workspace/data/sub").
		ReadText(ctx); err == nil {
		t.Fatal("ReadText dir")
	}

	doc, err := ms.Route(ctx, "/workspace/data/hello.go").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.ReplaceLines(1, 2, []string{"package main // edited"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}

	if body, err := ms.Route(ctx, "/workspace/data/hello.go").
		ReadFile(ctx); err != nil || !strings.Contains(string(body), "edited") {
		t.Fatalf("ReadFile after WriteDocument: %q err=%v", body, err)
	}
	if spec, err := ms.SpecAt("/workspace/data/hello.go"); err != nil || spec.Point != "/workspace/data" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}

	putTyped(ctx, t, client, bucket, "runs/1/notes", "# title\n\nbody\n", "text/markdown; charset=utf-8")
	putTyped(ctx, t, client, bucket, "runs/1/blob", "looks like utf8 text", "image/png")
	putTyped(ctx, t, client, bucket, "runs/1/main.go", "package main\n", "application/octet-stream")

	st, err = ms.Route(ctx, "/workspace/data/notes").
		Stat(ctx)
	if err != nil || st.MediaType != "text/markdown" {
		t.Fatalf("Stat MediaType=%q err=%v", st.MediaType, err)
	}

	if md, err := ms.Route(ctx, "/workspace/data/notes").
		ReadText(ctx); err != nil || md.MediaType() != "text/markdown" {
		t.Fatalf("hinted markdown: mt=%q err=%v", mediaOf(md), err)
	}

	if _, err := ms.Route(ctx, "/workspace/data/blob").
		OpenDocument(ctx); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("image/png hint should skip sniff: %v", err)
	}

	if st, err := ms.Route(ctx, "/workspace/data/main.go").
		Stat(ctx); err != nil || st.MediaType != "text/x-go" {
		t.Fatalf("octet-stream + .go key: Stat MediaType=%q err=%v", st.MediaType, err)
	}

	open := vfs.S3(awss3.API{Client: client}, bucket)
	p, err := open(ctx, "sess", mount.Binding{Params: map[string]string{"prefix": "direct"}})
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

	blobMS, err := vfs.Tree(vfs.At("data", vfs.Blob(awss3.API{Client: client}, "")))(ctx, "blob-s3", vfs.Request{Bindings: []mount.Binding{{
		Params: map[string]string{mount.ParamName: "data", "container": bucket, "prefix": "blobrun"},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	if err := blobMS.Route(ctx, "/workspace/data/hello.go").
		WriteFile(ctx, []byte("package main\n")); err != nil {
		t.Fatal(err)
	}

	if b, err := blobMS.Route(ctx, "/workspace/data/hello.go").
		ReadFile(ctx); err != nil || string(b) != "package main\n" {
		t.Fatalf("Blob container param: %q err=%v", b, err)
	}

	// Keys a host wrote itself: a directory with no marker and an object
	// over the read limit. AWS S3 also keeps a key with an empty segment
	// (odd//x.txt), which this server refuses, so a proxy lists one.
	putTyped(ctx, t, client, bucket, "runs/1/implicit/x.txt", "x", "text/plain")
	putTyped(ctx, t, client, bucket, "runs/1/big.txt", strings.Repeat("a", vfs.MaxReadFileBytes+1), "text/plain")
	proxy, proxied := s3test.Proxy(t)
	proxy.Set(faultproxy.Rule{
		Match: func(r *http.Request) bool { return r.URL.Query().Get("prefix") == "odd/" },
		Fault: faultproxy.Answer(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>`+bucket+`</Name><Prefix>odd/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>
<Contents><Key>odd/y.txt</Key><Size>1</Size></Contents><CommonPrefixes><Prefix>odd//</Prefix></CommonPrefixes></ListBucketResult>`),
	})
	whole, err := vfs.Tree(vfs.At("all", vfs.S3(awss3.API{Client: proxied}, bucket)))(ctx, "sess-all", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	bucketEnts, bucketErr := whole.Route(ctx, "/workspace/all").ReadDir(ctx)
	odd, oddErr := whole.Route(ctx, "/workspace/all/odd").ReadDir(ctx)
	implicit, implicitErr := ms.Route(ctx, "/workspace/data/implicit").Stat(ctx)
	againErr := ms.Route(ctx, "/workspace/data/sub").MkdirAll(ctx)
	if err := ms.Route(ctx, "/workspace/data/outer/inner").MkdirAll(ctx); err != nil {
		t.Fatal(err)
	}
	outerErr := ms.Route(ctx, "/workspace/data/outer").Remove(ctx)
	_, bigErr := ms.Route(ctx, "/workspace/data/big.txt").ReadText(ctx)
	noDocErr := ms.Route(ctx, "/workspace/data/x.md").WriteDocument(ctx, nil)

	if bucketErr != nil || !slices.ContainsFunc(bucketEnts, func(e vfs.DirEntry) bool { return e.Name == "runs" && e.IsDir }) {
		t.Errorf("read dir of a bucket mounted without a prefix = %+v %v, want the runs directory", bucketEnts, bucketErr)
	}
	if implicitErr != nil || !implicit.IsDir || implicit.Name != "implicit" {
		t.Errorf("stat of a directory without a marker = %+v %v, want a directory", implicit, implicitErr)
	}
	if oddErr != nil || len(odd) != 1 || odd[0].Name != "y.txt" {
		t.Errorf("read dir over a key with an empty segment = %+v %v, want y.txt only", odd, oddErr)
	}
	if againErr != nil {
		t.Errorf("mkdir of a directory that exists = %v, want success", againErr)
	}
	if outerErr == nil {
		t.Error("remove of a directory that holds only a directory succeeded, want not empty")
	}
	if !errors.Is(bigErr, vfs.ErrTooLarge) {
		t.Errorf("read text over the read limit = %v, want ErrTooLarge", bigErr)
	}
	if !errors.Is(noDocErr, vfs.ErrNotTextual) {
		t.Errorf("write of no document = %v, want ErrNotTextual", noDocErr)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := ms.Route(canceled, "/workspace/data/hello.go").
		Stat(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat cancel: %v", err)
	}

	if _, err := ms.Route(canceled, "/workspace/data/hello.go").
		Open(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancel: %v", err)
	}

	if _, err := ms.Route(canceled, "/workspace/data/sub").
		ReadDir(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir cancel: %v", err)
	}

	if err := ms.Route(canceled, "/workspace/data/excl.txt").
		Remove(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Remove cancel: %v", err)
	}

	if err := ms.Route(canceled, "/workspace/data/newdir").
		MkdirAll(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("MkdirAll cancel: %v", err)
	}

	if _, err := ms.Route(canceled, "/workspace/data/hello.go").
		ReadText(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadText cancel: %v", err)
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

func mediaOf(d vfs.Textual) string {
	if d == nil {
		return ""
	}
	return d.MediaType()
}

func startS3(t *testing.T) (*s3.Client, string) {
	t.Helper()
	return s3test.Start(t)
}
