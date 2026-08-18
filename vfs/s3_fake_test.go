package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

// memS3 is an in-process S3API for unit tests (no MinIO/network).
type memS3 struct {
	mu sync.Mutex
	// objects: full key → data (directory markers are keys ending in /)
	objects map[string][]byte
	types   map[string]string // optional Content-Type per key
	fail    map[string]error  // optional method→error injection
}

func newMemS3() *memS3 {
	return &memS3{objects: make(map[string][]byte), types: make(map[string]string), fail: make(map[string]error)}
}

func (m *memS3) Head(ctx context.Context, bucket, key string) (int64, time.Time, string, error) {
	if err := ctx.Err(); err != nil {
		return 0, time.Time{}, "", err
	}
	if err := m.fail["Head"]; err != nil {
		return 0, time.Time{}, "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return 0, time.Time{}, "", vfs.ErrNotExist
	}
	return int64(len(data)), time.Now().UTC(), m.types[key], nil
}

func (m *memS3) Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, time.Time{}, err
	}
	if err := m.fail["Get"]; err != nil {
		return nil, 0, time.Time{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, 0, time.Time{}, vfs.ErrNotExist
	}
	cp := append([]byte(nil), data...)
	return io.NopCloser(bytes.NewReader(cp)), int64(len(cp)), time.Now().UTC(), nil
}

func (m *memS3) Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.fail["Put"]; err != nil {
		return err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if size >= 0 && int64(len(data)) != size && size != 0 {
		// tolerate zero-size markers
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = data
	return nil
}

func (m *memS3) Delete(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.fail["Delete"]; err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memS3) List(ctx context.Context, bucket, prefix string) (keys []string, dirs []string, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := m.fail["List"]; err != nil {
		return nil, nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seenDirs := map[string]struct{}{}
	for k := range m.objects {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k
		if prefix != "" {
			rest = strings.TrimPrefix(k, prefix)
		}
		if rest == "" {
			continue
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			// immediate child dir under prefix
			d := prefix + rest[:i+1]
			seenDirs[d] = struct{}{}
			continue
		}
		// object at this level (not a pure trailing-slash marker consumed as dir)
		if strings.HasSuffix(k, "/") {
			seenDirs[k] = struct{}{}
			continue
		}
		keys = append(keys, k)
	}
	for d := range seenDirs {
		dirs = append(dirs, d)
	}
	return keys, dirs, nil
}

// TestMountSession_s3MemAPI: full S3 provider path via fake API (no containers).
func TestMountSession_s3MemAPI(t *testing.T) {
	ctx := context.Background()
	api := newMemS3()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.S3Factory{
		ID: "s3", Client: api, DefaultBucket: "bkt",
	}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("s3-mem", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{
		Point: "/data", Profile: "s3",
		Params: map[string]string{"prefix": "runs/1"},
	}); err != nil {
		t.Fatal(err)
	}
	// read-only mount for ErrReadOnly
	if err := ms.Mount(ctx, vfs.MountSpec{
		Point: "/ro", Profile: "s3", ReadOnly: true,
		Params: map[string]string{"prefix": "readonly"},
	}); err != nil {
		t.Fatal(err)
	}

	// Write + read + stat
	if err := ms.WriteFile(ctx, "/data/hello.go", []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	b, err := ms.ReadFile(ctx, "/data/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("ReadFile=%q err=%v", b, err)
	}
	st, err := ms.Stat(ctx, "/data/hello.go")
	if err != nil || st.IsDir || st.Size != int64(len("package main\n")) {
		t.Fatalf("Stat=%+v err=%v", st, err)
	}

	// Nested mkdir + list
	if err := ms.MkdirAll(ctx, "/data/sub/dir"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/data/sub/dir/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	ents, err := ms.ReadDir(ctx, "/data/sub/dir")
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 || ents[0].Name != "a.txt" || ents[0].IsDir {
		t.Fatalf("ReadDir dir=%+v", ents)
	}
	ents, err = ms.ReadDir(ctx, "/data/sub")
	if err != nil {
		t.Fatal(err)
	}
	foundDir := false
	for _, e := range ents {
		if e.Name == "dir" && e.IsDir {
			foundDir = true
		}
	}
	if !foundDir {
		t.Fatalf("expected dir child: %+v", ents)
	}
	// Stat directory
	dst, err := ms.Stat(ctx, "/data/sub/dir")
	if err != nil || !dst.IsDir {
		t.Fatalf("Stat dir=%+v err=%v", dst, err)
	}

	// Open read path — readable, not writable
	f, err := ms.Open(ctx, "/data/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	r, ok := f.(io.Reader)
	if !ok {
		t.Fatal("read file should be io.Reader")
	}
	buf := make([]byte, 64)
	n, err := r.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("expected read bytes")
	}
	if _, ok := f.(io.Writer); ok {
		t.Fatal("read file should not be io.Writer")
	}
	if _, err := f.Stat(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Remove file then missing
	if err := ms.Remove(ctx, "/data/sub/dir/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Stat(ctx, "/data/sub/dir/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("want not exist after remove: %v", err)
	}
	// empty dir remove
	if err := ms.Remove(ctx, "/data/sub/dir"); err != nil {
		t.Fatal(err)
	}

	// non-empty dir remove fails
	if err := ms.WriteFile(ctx, "/data/sub/keep.txt", []byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := ms.Remove(ctx, "/data/sub"); err == nil {
		t.Fatal("non-empty dir remove")
	}

	// read-only write
	if err := ms.WriteFile(ctx, "/ro/x.txt", []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write: %v", err)
	}

	// AfterPersist on WriteFile
	var saw string
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		saw = path
		return nil
	})
	if ms.GetAfterPersist() == nil {
		t.Fatal("GetAfterPersist")
	}
	if err := ms.WriteFile(ctx, "/data/hook.txt", []byte("h\n")); err != nil {
		t.Fatal(err)
	}
	if saw != "/data/hook.txt" {
		t.Fatalf("AfterPersist saw %q", saw)
	}
	hookErr := errors.New("index unavailable")
	ms.SetAfterPersist(func(context.Context, string) error { return hookErr })
	if err := ms.WriteFile(ctx, "/data/hook-error.txt", []byte("h\n")); !errors.Is(err, hookErr) {
		t.Fatalf("AfterPersist error = %v", err)
	}
	ms.SetAfterPersist(nil)

	// Document IR over S3 + Sync
	doc, err := ms.ReadText(ctx, "/data/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.ReplaceLines(1, 2, []string{"package main // edited"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	df, err := ms.Open(ctx, "/data/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := df.Stat(); err != nil || fi.Size == 0 {
		t.Fatalf("Stat after write: %+v err=%v", fi, err)
	}
	_ = df.Close()
	body, err := ms.ReadFile(ctx, "/data/hello.go")
	if err != nil || !strings.Contains(string(body), "edited") {
		t.Fatalf("ReadFile after WriteDocument: %q err=%v", body, err)
	}
	if mt, err := ms.Classify(ctx, "/data/hello.go", nil); err != nil || mt == "" {
		t.Fatalf("Classify: %q err=%v", mt, err)
	}
	if spec, err := ms.SpecAt("/data/hello.go"); err != nil || spec.Point != "/data" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}

	// Unmount drops cache under point
	if err := ms.Unmount("/ro"); err != nil {
		t.Fatal(err)
	}

	// Backend API failures surface as session I/O errors (not silent success).
	api.fail["Head"] = errors.New("s3 head down")
	if _, err := ms.Stat(ctx, "/data/hello.go"); err == nil || !strings.Contains(err.Error(), "head down") {
		t.Fatalf("Head fail: %v", err)
	}
	api.fail["Head"] = nil
	api.mu.Lock()
	api.objects["runs/1/onlykids/file.txt"] = []byte("x")
	api.mu.Unlock()
	api.fail["List"] = errors.New("s3 list down")
	if _, err := ms.Stat(ctx, "/data/onlykids"); err == nil || !strings.Contains(err.Error(), "list down") {
		t.Fatalf("List fail Stat dir: %v", err)
	}
	if _, err := ms.ReadDir(ctx, "/data/sub"); err == nil || !strings.Contains(err.Error(), "list down") {
		t.Fatalf("List fail ReadDir: %v", err)
	}
	api.fail["List"] = nil
	if _, err := ms.ReadDir(ctx, "/data/hello.go"); err == nil {
		t.Fatal("ReadDir file")
	}
	if err := ms.MkdirAll(ctx, "/data/hello.go/nested"); err == nil {
		t.Fatal("MkdirAll through file")
	}
	if _, err := ms.ReadText(ctx, "/data/sub"); err == nil {
		t.Fatal("OpenDocument dir")
	}
	api.fail["Get"] = errors.New("s3 get down")
	if _, err := ms.Open(ctx, "/data/hello.go"); err == nil || !strings.Contains(err.Error(), "get down") {
		t.Fatalf("Get fail: %v", err)
	}
	if _, err := ms.ReadText(ctx, "/data/hello.go"); err == nil || !strings.Contains(err.Error(), "get down") {
		t.Fatalf("OpenDocument Get fail: %v", err)
	}
	api.fail["Get"] = nil
	api.fail["Put"] = errors.New("s3 put down")
	if err := ms.WriteFile(ctx, "/data/fail.txt", []byte("x")); err == nil || !strings.Contains(err.Error(), "put down") {
		t.Fatalf("Put fail: %v", err)
	}
	api.fail["Put"] = nil

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(cctx, "/data/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat cancel: %v", err)
	}
	if _, err := ms.Open(cctx, "/data/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancel: %v", err)
	}
	if _, err := ms.ReadDir(cctx, "/data/sub"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir cancel: %v", err)
	}
	if err := ms.Remove(cctx, "/data/hook.txt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Remove cancel: %v", err)
	}
	if err := ms.MkdirAll(cctx, "/data/newdir"); !errors.Is(err, context.Canceled) {
		t.Fatalf("MkdirAll cancel: %v", err)
	}
	if _, err := ms.ReadText(cctx, "/data/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenDocument cancel: %v", err)
	}
}

// TestS3Provider_openWriteClose: WriteFile overwrite + provider OpenFile write/append/excl
// (s3WriteFile path is not used by PutFile WriteFile).
func TestS3Provider_openWriteClose(t *testing.T) {
	ctx := context.Background()
	api := newMemS3()
	factory := vfs.S3Factory{ID: "s3w", Client: api, DefaultBucket: "b"}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(factory); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("s3w", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/w", Profile: "s3w"}); err != nil {
		t.Fatal(err)
	}
	// Multiple writes / overwrite
	if err := ms.WriteFile(ctx, "/w/a.txt", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/w/a.txt", []byte("two")); err != nil {
		t.Fatal(err)
	}
	b, err := ms.ReadFile(ctx, "/w/a.txt")
	if err != nil || string(b) != "two" {
		t.Fatalf("got %q err=%v", b, err)
	}
	// mkdir through file fails
	if err := ms.WriteFile(ctx, "/w/file", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := ms.MkdirAll(ctx, "/w/file/child"); err == nil {
		t.Fatal("mkdir through file")
	}
	// missing path remove
	if err := ms.Remove(ctx, "/w/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}
	// List root
	ents, err := ms.ReadDir(ctx, "/w")
	if err != nil || len(ents) < 1 {
		t.Fatalf("ReadDir root: %+v err=%v", ents, err)
	}
	// Stat mount root
	st, err := ms.Stat(ctx, "/w")
	if err != nil || !st.IsDir {
		t.Fatalf("stat root: %+v err=%v", st, err)
	}

	// Direct provider OpenFile write/append/excl (covers s3WriteFile).
	p, err := factory.Open(ctx, "sess", vfs.MountSpec{Params: map[string]string{"prefix": "p"}})
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
	if _, ok := wf.(io.Reader); ok {
		t.Fatal("write file should not be io.Reader")
	}
	w, ok := wf.(io.Writer)
	if !ok {
		t.Fatal("write file should be io.Writer")
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if fi, err := wf.Stat(); err != nil || fi.Size != 5 {
		t.Fatalf("write Stat: %+v err=%v", fi, err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}
	// O_EXCL on existing
	if _, err := p.OpenFile(ctx, "new.txt", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, vfs.ErrExist) {
		t.Fatalf("O_EXCL: %v", err)
	}
	// Append loads existing then Close puts
	af, err := p.OpenFile(ctx, "new.txt", os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	aw, ok := af.(io.Writer)
	if !ok {
		t.Fatal("append file should be io.Writer")
	}
	if _, err := aw.Write([]byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := af.Close(); err != nil {
		t.Fatal(err)
	}
	rf, err := p.OpenFile(ctx, "new.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	rr, ok := rf.(io.Reader)
	if !ok {
		t.Fatal("s3 read file should be io.Reader")
	}
	data, _ := io.ReadAll(rr)
	_ = rf.Close()
	if string(data) != "hello!" {
		t.Fatalf("append result %q", data)
	}
	// Root open as file fails
	if _, err := p.OpenFile(ctx, ".", os.O_RDONLY, 0); err == nil {
		t.Fatal("open root as file")
	}
	// Canceled context on provider methods
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.Validate(canceled); err == nil {
		t.Fatal("Validate canceled")
	}
	if _, err := p.OpenFile(canceled, "x.txt", os.O_RDONLY, 0); err == nil {
		t.Fatal("OpenFile canceled")
	}
	if _, err := p.Stat(canceled, "x.txt"); err == nil {
		t.Fatal("Stat canceled")
	}
	if _, err := p.ReadDir(canceled, "."); err == nil {
		t.Fatal("ReadDir canceled")
	}
	if err := p.Remove(canceled, "x.txt"); err == nil {
		t.Fatal("Remove canceled")
	}
	if err := p.MkdirAll(canceled, "d", 0o755); err == nil {
		t.Fatal("MkdirAll canceled")
	}

	// Injected API failures
	api.fail["Head"] = errors.New("head down")
	if _, err := p.Stat(ctx, "new.txt"); err == nil {
		t.Fatal("Head failure should surface")
	}
	delete(api.fail, "Head")
	api.fail["List"] = errors.New("list down")
	if _, err := p.ReadDir(ctx, "."); err == nil {
		t.Fatal("List failure")
	}
	delete(api.fail, "List")
	api.fail["Get"] = errors.New("get down")
	if _, err := p.OpenFile(ctx, "new.txt", os.O_RDONLY, 0); err == nil {
		t.Fatal("Get failure")
	}
	delete(api.fail, "Get")
	api.fail["Delete"] = errors.New("del down")
	// Remove file path will Stat then Delete
	_ = p.Remove(ctx, "new.txt") // may fail on delete
	delete(api.fail, "Delete")
	api.fail["Put"] = errors.New("put down")
	wf2, err := p.OpenFile(ctx, "fail.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if w2, ok := wf2.(io.Writer); ok {
		_, _ = w2.Write([]byte("x"))
	}
	if err := wf2.Close(); err == nil {
		t.Fatal("Put failure on Close")
	}
	delete(api.fail, "Put")

	// Factory Validate empty client already covered; empty id
	if _, err := (vfs.S3Factory{Client: api, DefaultBucket: "b"}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("empty factory id")
	}
	// bucket from params
	p2, err := factory.Open(ctx, "s", vfs.MountSpec{Params: map[string]string{"bucket": "other", "prefix": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = p2
}

// TestOpenDocument_providerMediaType: specific S3 Content-Type wins; octet-stream does not.
func TestOpenDocument_providerMediaType(t *testing.T) {
	ctx := context.Background()
	api := newMemS3()
	api.objects["notes"] = []byte("# title\n\nbody\n")
	api.types["notes"] = "text/markdown; charset=utf-8"
	api.objects["blob"] = []byte("looks like utf8 text")
	api.types["blob"] = "image/png"
	api.objects["main.go"] = []byte("package main\n")
	api.types["main.go"] = "application/octet-stream"

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.S3Factory{ID: "s3", Client: api, DefaultBucket: "bkt"}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("s3-ctype", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/data", Profile: "s3"}); err != nil {
		t.Fatal(err)
	}

	st, err := ms.Stat(ctx, "/data/notes")
	if err != nil || st.MediaType != "text/markdown" {
		t.Fatalf("Stat MediaType=%q err=%v", st.MediaType, err)
	}
	doc, err := ms.ReadText(ctx, "/data/notes")
	if err != nil || doc.MediaType() != "text/markdown" {
		t.Fatalf("hinted markdown: mt=%q err=%v", mediaOf(doc), err)
	}
	if _, err := ms.OpenDocument(ctx, "/data/blob", nil); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("image/png hint should skip sniff: %v", err)
	}
	st, err = ms.Stat(ctx, "/data/main.go")
	if err != nil || st.MediaType != "text/x-go" {
		t.Fatalf("octet-stream + .go key: Stat MediaType=%q err=%v", st.MediaType, err)
	}
	goDoc, err := ms.ReadText(ctx, "/data/main.go")
	if err != nil || goDoc.MediaType() != "text/x-go" {
		t.Fatalf("provider-classified go: mt=%q err=%v", mediaOf(goDoc), err)
	}
}

func mediaOf(d vfs.Textual) string {
	if d == nil {
		return ""
	}
	return d.MediaType()
}
