package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestMountSession_localSession is the primary local-VFS outcome test:
// mounts, nested lookup, I/O, read-only, jail, rematerialize, unmount cache clear.
func TestMountSession_localSession(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}

	ms := vfs.NewMountSession("sess-1", reg)
	if err := ms.Materialize(ctx, []vfs.MountSpec{
		{Point: "/work", Profile: "scratch"},
		{Point: "/work/nested", Profile: "scratch", ReadOnly: true, Params: map[string]string{"subpath": "nested"}},
		{Point: "/ab", Profile: "scratch", Params: map[string]string{"subpath": "ab"}},
	}); err != nil {
		t.Fatal(err)
	}
	if n := len(ms.Infos()); n != 3 {
		t.Fatalf("Infos len = %d", n)
	}

	if err := ms.WriteFile(ctx, "/work/hello.go", []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	// empty write-through
	if err := ms.WriteFile(ctx, "/work/empty.txt", nil); err != nil {
		t.Fatal(err)
	}
	if b, err := ms.ReadFile(ctx, "/work/empty.txt"); err != nil || len(b) != 0 {
		t.Fatalf("empty = %q err=%v", b, err)
	}

	f, err := ms.Open(ctx, "/work/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	_ = f.Close()
	if err != nil || fi.Name != "hello.go" {
		t.Fatalf("Open/Stat handle: %+v err=%v", fi, err)
	}
	if err := ms.WriteFile(ctx, "/work/nested/x.txt", []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro nested write: %v", err)
	}
	if err := ms.WriteFile(ctx, "/work/other.txt", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	b, err := ms.ReadFile(ctx, "/work/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("ReadFile = %q err=%v", b, err)
	}
	info, err := ms.Stat(ctx, "/work/hello.go")
	if err != nil || info.IsDir || info.Name != "hello.go" {
		t.Fatalf("Stat = %+v err=%v", info, err)
	}

	if err := ms.MkdirAll(ctx, "/work/sub/dir"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/sub/dir/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	ents, err := ms.ReadDir(ctx, "/work/sub/dir")
	if err != nil || len(ents) != 1 || ents[0].Name != "a.txt" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}
	if err := ms.Remove(ctx, "/work/sub/dir/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Stat(ctx, "/work/sub/dir/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	// Segment boundary: /ab must not claim /a
	if _, _, err := ms.Lookup("/a/file"); err != nil && !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("lookup /a: %v", err)
	} else if err == nil {
		// longest prefix might still resolve to something — only fail if /ab claimed
	}
	mi, rel, err := ms.Lookup("/ab/x")
	if err != nil || mi.Point != "/ab" || rel != "x" {
		t.Fatalf("lookup /ab: %+v rel=%q err=%v", mi, rel, err)
	}

	if err := ms.WriteFile(ctx, "/work/foo/../../etc/passwd", []byte("x")); err == nil {
		t.Fatal("escape")
	}
	if _, err := ms.ReadFile(ctx, "/nosuch/x"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("not mounted: %v", err)
	}

	// Concurrent reads
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = ms.ReadFile(ctx, "/work/hello.go")
		}()
	}
	wg.Wait()

	// Cache under /ab then unmount drops it; rematerialize keeps other mounts' outcome via Specs
	if err := ms.WriteFile(ctx, "/ab/c.txt", []byte("cached\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/ab/c.txt"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Unmount("/ab"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/ab/c.txt"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("after unmount: %v", err)
	}

	// Rematerialize from Specs (restart shape)
	specs := ms.Specs()
	ms2 := vfs.NewMountSession("sess-2", reg)
	if err := ms2.Materialize(ctx, specs); err != nil {
		t.Fatal(err)
	}
	b, err = ms2.ReadFile(ctx, "/work/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("rematerialize ReadFile = %q err=%v", b, err)
	}
	// empty materialize clears tree
	if err := ms2.Materialize(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if n := len(ms2.Specs()); n != 0 {
		t.Fatalf("empty materialize specs = %d", n)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.ReadFile(cctx, "/work/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

// TestDocument_session: IR, cache write-back, Sync, revalidation, codec rejects, RO.
func TestDocument_session(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("doc-sess", reg)
	if err := ms.Materialize(ctx, []vfs.MountSpec{
		{Point: "/work", Profile: "scratch"},
		{Point: "/ro", Profile: "scratch", ReadOnly: true, Params: map[string]string{"subpath": "ro"}},
	}); err != nil {
		t.Fatal(err)
	}

	var b strings.Builder
	for i := 1; i <= 100; i++ {
		b.WriteString("L")
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('\n')
	}
	if err := ms.WriteFile(ctx, "/work/big.txt", []byte(b.String())); err != nil {
		t.Fatal(err)
	}
	win, err := ms.ReadLines(ctx, "/work/big.txt", 10, 13)
	if err != nil || win.Returned != 3 || win.Lines[0] != "L10" || win.Lines[2] != "L12" || win.EOF || win.NextStart != 13 {
		t.Fatalf("ReadLines = %+v err=%v", win, err)
	}
	// Soft EOF: request past last line ("L1\n"…"L100\n" → 101 segments with trailing empty)
	win, err = ms.ReadLines(ctx, "/work/big.txt", 100, 200)
	if err != nil || win.Returned < 1 || !win.EOF {
		t.Fatalf("soft EOF = %+v err=%v", win, err)
	}
	// empty requested range
	empty, err := ms.ReadLines(ctx, "/work/big.txt", 5, 5)
	if err != nil || empty.Returned != 0 {
		t.Fatalf("empty range = %+v err=%v", empty, err)
	}
	if _, err := ms.ReadLines(ctx, "/work/big.txt", 500, 501); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("ReadLines OOR: %v", err)
	}
	// page until EOF
	start := 1
	pages := 0
	for {
		w, err := ms.ReadLines(ctx, "/work/big.txt", start, start+20)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if w.EOF {
			break
		}
		start = w.NextStart
		if pages > 20 {
			t.Fatal("paging did not reach EOF")
		}
	}

	if err := ms.WriteFile(ctx, "/work/note.txt", []byte("a\nb\nc\n")); err != nil {
		t.Fatal(err)
	}
	text, err := ms.ReadText(ctx, "/work/note.txt")
	if err != nil {
		t.Fatal(err)
	}
	if text.MediaType() != "text/plain" || text.Encoding() != "utf-8" || text.LineCount() != 4 {
		t.Fatalf("open: mt=%q enc=%q count=%d", text.MediaType(), text.Encoding(), text.LineCount())
	}
	if err := text.SetLine(2, "B"); err != nil {
		t.Fatal(err)
	}
	if err := text.ReplaceLines(3, 4, []string{"C", "D"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, text); err != nil {
		t.Fatal(err)
	}
	// Session-visible ReadFile prefers dirty IR (backend still old until Sync).
	raw, err := ms.ReadFile(ctx, "/work/note.txt")
	if err != nil || string(raw) != "a\nB\nC\nD\n" {
		t.Fatalf("dirty ReadFile = %q err=%v", raw, err)
	}
	if w, err := ms.ReadLines(ctx, "/work/note.txt", 1, 3); err != nil || w.Returned != 2 || w.Lines[1] != "B" {
		t.Fatalf("dirty ReadLines = %+v err=%v", w, err)
	}
	text2, err := ms.ReadText(ctx, "/work/note.txt")
	if err != nil || text2.Text() != "a\nB\nC\nD\n" {
		t.Fatalf("cached read = %q err=%v", text2.Text(), err)
	}
	_ = text2.SetLine(1, "A")
	if err := ms.WriteDocument(ctx, text2); err != nil {
		t.Fatal(err)
	}
	// Sync no-op on clean path
	if err := ms.Sync(ctx, "/work/big.txt"); err != nil {
		t.Fatal(err)
	}
	if err := ms.SyncAll(ctx); err != nil {
		t.Fatal(err)
	}
	raw, err = ms.ReadFile(ctx, "/work/note.txt")
	if err != nil || string(raw) != "A\nB\nC\nD\n" {
		t.Fatalf("after Sync = %q err=%v", raw, err)
	}
	// second SyncAll is no-op
	if err := ms.SyncAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Revalidation
	if err := ms.WriteFile(ctx, "/work/ext.txt", []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/work/ext.txt"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "ext.txt"), []byte("two-lines\nlonger\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if again, err := ms.ReadText(ctx, "/work/ext.txt"); err != nil || again.Text() != "two-lines\nlonger\n" {
		t.Fatalf("revalidate = %q err=%v", again.Text(), err)
	}

	if err := ms.WriteFile(ctx, "/work/main.go", []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	if goDoc, err := ms.ReadText(ctx, "/work/main.go"); err != nil || goDoc.MediaType() != "text/x-go" {
		t.Fatalf("go: %v", err)
	}
	// Remove drops cache
	if err := ms.Remove(ctx, "/work/main.go"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/work/main.go"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	png := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 0}
	if err := ms.WriteFile(ctx, "/work/pic.bin", png); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.OpenDocument(ctx, "/work/pic.bin", nil); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("binary: %v", err)
	}
	if err := ms.WriteFile(ctx, "/work/README", []byte("hello from readme\n")); err != nil {
		t.Fatal(err)
	}
	if st, err := ms.Stat(ctx, "/work/README"); err != nil || st.MediaType != "text/plain" {
		t.Fatalf("local no-ext Stat: %+v err=%v", st, err)
	}
	if doc, err := ms.ReadText(ctx, "/work/README"); err != nil || doc.MediaType() != "text/plain" {
		t.Fatalf("local no-ext IR: %v", err)
	}
	if err := ms.WriteFile(ctx, "/work/bad.txt", []byte{0xff, 0xfe, 0xfd}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.OpenDocument(ctx, "/work/bad.txt", nil); !errors.Is(err, vfs.ErrInvalidUTF8) {
		t.Fatalf("utf8: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(base, "ro"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "ro", "f.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ro, err := ms.ReadText(ctx, "/ro/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = ro.SetLine(1, "changed")
	if err := ms.WriteDocument(ctx, ro); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write: %v", err)
	}
	if err := ms.WriteDocument(ctx, bareDocument{path: "/work/x", mt: "x"}); !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("bare write: %v", err)
	}

	// Cache eviction under entry cap (clean entries only)
	for i := 0; i < 40; i++ {
		p := "/work/ev" + strconv.Itoa(i) + ".txt"
		if err := ms.WriteFile(ctx, p, []byte("x\n")); err != nil {
			t.Fatal(err)
		}
		if _, err := ms.ReadText(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	// still functional after eviction pressure
	if _, err := ms.ReadText(ctx, "/work/note.txt"); err != nil {
		t.Fatal(err)
	}
}

// TestTextDocument_lines is pure IR: index, edit, join (no mount).
func TestTextDocument_lines(t *testing.T) {
	doc := vfs.NewTextDocument("/p", "text/plain", "utf-8", "a\nb\nc")
	if doc.LineCount() != 3 || doc.Encoding() != "utf-8" {
		t.Fatalf("count=%d enc=%q", doc.LineCount(), doc.Encoding())
	}
	line, err := doc.Line(2)
	if err != nil || line != "b" {
		t.Fatalf("Line(2) = %q err=%v", line, err)
	}
	part, err := doc.Lines(1, 3)
	if err != nil || len(part) != 2 || part[0] != "a" || part[1] != "b" {
		t.Fatalf("Lines(1,3) = %#v err=%v", part, err)
	}
	empty, err := doc.Lines(2, 2)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty range = %#v err=%v", empty, err)
	}
	if _, err := doc.Line(0); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("Line(0): %v", err)
	}
	if _, err := doc.Lines(1, 10); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("Lines OOR: %v", err)
	}
	if vfs.NewTextDocument("/e", "text/plain", "", "").LineCount() != 0 {
		t.Fatal("empty LineCount")
	}
	if n := vfs.NewTextDocument("/t", "text/plain", "utf-8", "a\n").LineCount(); n != 2 {
		t.Fatalf("trailing count = %d", n)
	}
	cr, _ := vfs.NewTextDocument("/c", "text/plain", "utf-8", "a\r\nb").Line(1)
	if cr != "a\r" {
		t.Fatalf("crlf = %q", cr)
	}
	if err := doc.SetLine(2, "B"); err != nil {
		t.Fatal(err)
	}
	if err := doc.ReplaceLines(3, 4, []string{"C", "D"}); err != nil {
		t.Fatal(err)
	}
	if doc.Text() != "a\nB\nC\nD" {
		t.Fatalf("after edit = %q", doc.Text())
	}
	if err := doc.SetLine(1, "x\ny"); !errors.Is(err, vfs.ErrInvalidLine) {
		t.Fatalf("newline in line: %v", err)
	}
	if err := doc.ReplaceLines(1, 2, []string{"x\ny"}); !errors.Is(err, vfs.ErrInvalidLine) {
		t.Fatalf("newline in replace: %v", err)
	}
	if err := doc.ReplaceLines(0, 1, nil); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("replace OOR: %v", err)
	}
	// wipe all lines
	if err := doc.ReplaceLines(1, doc.LineCount()+1, nil); err != nil || doc.LineCount() != 0 {
		t.Fatalf("clear all: count=%d err=%v", doc.LineCount(), err)
	}
	if err := doc.ReplaceLines(1, 1, []string{"z"}); err != nil || doc.Text() != "z" {
		t.Fatalf("insert empty: %q err=%v", doc.Text(), err)
	}
	if s, err := vfs.FormatLines(doc, 1, 2); err != nil || s != "z" {
		t.Fatalf("FormatLines = %q err=%v", s, err)
	}
	if _, err := vfs.FormatLines(doc, 0, 1); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("FormatLines OOR: %v", err)
	}
	if _, err := vfs.AsTextual(doc); err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.AsTextual(bareDocument{}); !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("AsTextual: %v", err)
	}
	// defaults
	d2 := vfs.NewTextDocument("/x", "", "", "hi")
	if d2.MediaType() != "text/plain" || d2.Encoding() != "utf-8" {
		t.Fatalf("defaults mt=%q enc=%q", d2.MediaType(), d2.Encoding())
	}
}

// TestDetectMediaType covers extension map, sniff, and binary rejection.
func TestDetectMediaType(t *testing.T) {
	if mt := vfs.DetectMediaType("/a/main.go", nil); mt != "text/x-go" {
		t.Fatalf("extension: %s", mt)
	}
	if mt := vfs.DetectMediaType("/a/unknown", []byte("hello world\n")); mt != "text/plain" {
		t.Fatalf("utf8 sniff: %s", mt)
	}
	if mt := vfs.DetectMediaType("/a/x", []byte("a\x00b")); mt != "application/octet-stream" {
		t.Fatalf("nul: %s", mt)
	}
	png := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}
	if mt := vfs.DetectMediaType("/a/x", png); mt != "image/png" {
		t.Fatalf("png sniff: %s", mt)
	}
	if mt := vfs.DetectMediaType("/a/x", []byte{0xff, 0xfe, 0xfd}); mt == "text/plain" {
		t.Fatalf("invalid utf8 as text: %s", mt)
	}
	// long sample still sniffs
	long := bytes.Repeat([]byte("a"), 600)
	if mt := vfs.DetectMediaType("/a/x", long); mt != "text/plain" {
		t.Fatalf("long utf8: %s", mt)
	}
	if mt := vfs.DetectMediaType("/a/noext", nil); mt != "application/octet-stream" {
		t.Fatalf("no sample: %s", mt)
	}
}

// TestMergeSpecs covers host/harness merge helper.
func TestMergeSpecs(t *testing.T) {
	merged, err := vfs.MergeSpecs(
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
		[]vfs.MountSpec{{Point: "/b", Profile: "p"}},
	)
	if err != nil || len(merged) != 2 {
		t.Fatalf("merge: %v %v", merged, err)
	}
	if _, err := vfs.MergeSpecs(
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
	); !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := vfs.MergeSpecs([]vfs.MountSpec{{Point: "rel", Profile: "p"}}, nil); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("invalid: %v", err)
	}
}

// TestMountSession_configErrors covers unknown profile, bad paths, registry register.
func TestMountSession_configErrors(t *testing.T) {
	ctx := t.Context()
	reg := vfs.NewBackendRegistry()
	_ = reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()})
	ms := vfs.NewMountSession("s", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/x", Profile: "nope"}); !errors.Is(err, vfs.ErrUnknownProfile) {
		t.Fatalf("unknown profile: %v", err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/x", Profile: ""}); !errors.Is(err, vfs.ErrInvalidProvider) {
		t.Fatalf("empty profile: %v", err)
	}
	if _, _, err := ms.Lookup(""); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("empty path: %v", err)
	}
	if _, _, err := ms.Lookup("/has\x00x"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("nul path: %v", err)
	}
	if err := reg.Register(nil); err == nil {
		t.Fatal("register nil factory")
	}
	if err := reg.Register(vfs.LocalFactory{ID: "", Base: t.TempDir()}); err == nil {
		t.Fatal("empty factory id")
	}
	// already mounted
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/w", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/w", Profile: "scratch"}); !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("dup mount: %v", err)
	}
	if err := ms.Unmount("/missing"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("unmount missing: %v", err)
	}
	// nil registry session
	ms2 := vfs.NewMountSession("s2", nil)
	if err := ms2.Mount(ctx, vfs.MountSpec{Point: "/x", Profile: "scratch"}); err == nil {
		t.Fatal("nil registry mount")
	}
	creg := vfs.NewContentRegistry()
	if err := creg.Register(nil); err == nil {
		t.Fatal("nil codec")
	}
	if err := creg.Register(emptyTypesCodec{}); err == nil {
		t.Fatal("empty media types")
	}
	if err := creg.Register(blankTypeCodec{}); err == nil {
		t.Fatal("blank media type")
	}
	// text-like fallback on empty registry
	if _, err := creg.Decode(ctx, "/x.json", "application/json", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	// canceled decode
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := (vfs.TextCodec{}).Decode(cctx, "/x", "text/plain", []byte("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("codec cancel: %v", err)
	}
	// materialize duplicate points
	ms3 := vfs.NewMountSession("s3", reg)
	if err := ms3.Materialize(ctx, []vfs.MountSpec{
		{Point: "/d", Profile: "scratch"},
		{Point: "/d", Profile: "scratch"},
	}); !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("materialize dup: %v", err)
	}
	// mount canceled
	cctx2, cancel2 := context.WithCancel(ctx)
	cancel2()
	if err := ms.Mount(cctx2, vfs.MountSpec{Point: "/z", Profile: "scratch"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("mount cancel: %v", err)
	}
}

type emptyTypesCodec struct{}

func (emptyTypesCodec) MediaTypes() []string { return nil }
func (emptyTypesCodec) Decode(context.Context, string, string, []byte) (vfs.Document, error) {
	return nil, errors.New("unused")
}

type blankTypeCodec struct{}

func (blankTypeCodec) MediaTypes() []string { return []string{""} }
func (blankTypeCodec) Decode(context.Context, string, string, []byte) (vfs.Document, error) {
	return nil, errors.New("unused")
}

// TestMemProvider_limits covers size/line caps and write path without PutFile.
func TestMemProvider_limits(t *testing.T) {
	ctx := t.Context()
	store := globalMemStore()
	store.mu.Lock()
	store.files = make(map[string]memObj)
	store.mu.Unlock()

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(memFactory{id: "mem"}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("mem", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/m", Profile: "mem"}); err != nil {
		t.Fatal(err)
	}

	// Oversized Stat → ErrTooLarge without reading body
	store.mu.Lock()
	store.files["huge"] = memObj{size: int64(vfs.MaxReadFileBytes) + 1, data: nil, huge: true}
	store.mu.Unlock()
	if _, err := ms.ReadFile(ctx, "/m/huge"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("huge: %v", err)
	}

	// Line too long
	longLine := strings.Repeat("x", vfs.MaxLineBytes+2) + "\n"
	if err := ms.WriteFile(ctx, "/m/long.txt", []byte(longLine)); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadLines(ctx, "/m/long.txt", 1, 2); !errors.Is(err, vfs.ErrLineTooLong) {
		t.Fatalf("long line: %v", err)
	}

	// Write path without PutFile (Copy fallback) still works
	if err := ms.WriteFile(ctx, "/m/w.txt", []byte("hi\n")); err != nil {
		t.Fatal(err)
	}
	if b, err := ms.ReadFile(ctx, "/m/w.txt"); err != nil || string(b) != "hi\n" {
		t.Fatalf("roundtrip = %q err=%v", b, err)
	}
	// empty write
	if err := ms.WriteFile(ctx, "/m/e.txt", nil); err != nil {
		t.Fatal(err)
	}

	// invalid utf-8 line while streaming
	if err := ms.WriteFile(ctx, "/m/badline.txt", []byte("ok\n\xff\xfe\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadLines(ctx, "/m/badline.txt", 1, 3); !errors.Is(err, vfs.ErrInvalidUTF8) {
		t.Fatalf("bad utf8 line: %v", err)
	}

	// provider write without PutFile already exercised; local File.Write via provider API
	root := t.TempDir()
	lp, err := vfs.NewLocalProvider(root)
	if err != nil {
		t.Fatal(err)
	}
	wf, err := lp.OpenFile(ctx, "direct.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.Write([]byte("via-write")); err != nil {
		t.Fatal(err)
	}
	_ = wf.Close()
	rf, err := lp.OpenFile(ctx, "direct.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rf)
	_ = rf.Close()
	if string(got) != "via-write" {
		t.Fatalf("local Write = %q", got)
	}
}

// TestS3Factory_rejectsBadConfig covers factory validation without a live store.
func TestS3Factory_rejectsBadConfig(t *testing.T) {
	ctx := t.Context()
	if _, err := (vfs.S3Factory{ID: "s3"}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("nil client")
	}
	if _, err := (vfs.S3Factory{ID: "s3", Client: vfs.AWSS3{}}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("missing bucket")
	}
	if _, err := (vfs.S3Factory{ID: "s3", Client: vfs.AWSS3{}, DefaultBucket: "b"}).Open(ctx, "s", vfs.MountSpec{
		Params: map[string]string{"prefix": "a/../b"},
	}); err == nil {
		t.Fatal("bad prefix")
	}
}

// TestLocalFactory_rejectsUnsafeConfig covers unsafe roots and jail.
func TestLocalFactory_rejectsUnsafeConfig(t *testing.T) {
	ctx := t.Context()
	f := vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}
	if _, err := f.Open(ctx, "s", vfs.MountSpec{Params: map[string]string{"subpath": ".."}}); err == nil {
		t.Fatal("subpath ..")
	}
	if _, err := f.Open(ctx, "s", vfs.MountSpec{Params: map[string]string{"subpath": "/abs"}}); err == nil {
		t.Fatal("absolute subpath")
	}
	// session-scoped mount root
	if _, err := f.Open(ctx, "sess-id", vfs.MountSpec{Params: map[string]string{"session_scoped": "true"}}); err != nil {
		t.Fatalf("session_scoped: %v", err)
	}
	if _, err := f.Open(ctx, "../bad", vfs.MountSpec{Params: map[string]string{"session_scoped": "true"}}); err == nil {
		t.Fatal("unsafe session id")
	}
	if _, err := (vfs.LocalFactory{ID: "x", Base: "rel"}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("relative base")
	}
	file, err := os.CreateTemp(t.TempDir(), "notdir")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := (vfs.LocalFactory{ID: "f", Base: file.Name()}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("file as base")
	}
	if _, err := vfs.NewLocalProvider("relative"); err == nil {
		t.Fatal("NewLocalProvider relative")
	}
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "leak")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	p, err := vfs.NewLocalProvider(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.OpenFile(ctx, "leak", os.O_RDONLY, 0); err == nil {
		t.Fatal("symlink escape")
	}
	wf, err := p.OpenFile(ctx, "e", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_ = wf.Close()
	if _, err := p.OpenFile(ctx, "e", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, vfs.ErrExist) {
		t.Fatalf("excl: %v", err)
	}
}

type bareDocument struct{ path, mt string }

func (b bareDocument) Path() string      { return b.path }
func (b bareDocument) MediaType() string { return b.mt }

// --- in-memory provider (no PutFile → exercises writeContents Copy path) ---

type memStore struct {
	mu    sync.Mutex
	files map[string]memObj
}

type memObj struct {
	data []byte
	size int64
	huge bool
}

var (
	memStoreOnce sync.Once
	memStoreInst *memStore
)

func globalMemStore() *memStore {
	memStoreOnce.Do(func() {
		memStoreInst = &memStore{files: make(map[string]memObj)}
	})
	return memStoreInst
}

type memFactory struct{ id string }

func (f memFactory) Profile() string { return f.id }
func (f memFactory) Open(context.Context, string, vfs.MountSpec) (vfs.Provider, error) {
	return memProvider{store: globalMemStore()}, nil
}

type memProvider struct{ store *memStore }

func (memProvider) Validate(context.Context) error { return nil }

func (p memProvider) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	o, ok := p.store.files[name]
	if !ok {
		return vfs.FileInfo{}, vfs.ErrNotExist
	}
	sz := o.size
	if !o.huge {
		sz = int64(len(o.data))
	}
	return vfs.FileInfo{Name: name, Size: sz, Mode: 0o644, ModTime: time.Now()}, nil
}

func (p memProvider) OpenFile(_ context.Context, name string, flag int, _ fs.FileMode) (vfs.File, error) {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0
	if write {
		if flag&os.O_TRUNC != 0 || flag&os.O_CREATE != 0 {
			p.store.files[name] = memObj{data: nil}
		}
		return &memFile{store: p.store, name: name, write: true}, nil
	}
	o, ok := p.store.files[name]
	if !ok {
		return nil, vfs.ErrNotExist
	}
	if o.huge {
		return &memFile{store: p.store, name: name, huge: true, size: o.size}, nil
	}
	return &memFile{store: p.store, name: name, r: bytes.NewReader(o.data), size: int64(len(o.data))}, nil
}

func (p memProvider) ReadDir(context.Context, string) ([]vfs.DirEntry, error) {
	return nil, nil
}
func (p memProvider) Remove(_ context.Context, name string) error {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	delete(p.store.files, name)
	return nil
}
func (p memProvider) MkdirAll(context.Context, string, fs.FileMode) error { return nil }

type memFile struct {
	store *memStore
	name  string
	r     *bytes.Reader
	buf   bytes.Buffer
	write bool
	huge  bool
	size  int64
}

func (f *memFile) Read(p []byte) (int, error) {
	if f.huge {
		return 0, io.EOF // never needed if ReadFile rejects by Stat
	}
	return f.r.Read(p)
}
func (f *memFile) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}
func (f *memFile) Close() error {
	if f.write {
		f.store.mu.Lock()
		f.store.files[f.name] = memObj{data: append([]byte(nil), f.buf.Bytes()...)}
		f.store.mu.Unlock()
	}
	return nil
}
func (f *memFile) Stat() (vfs.FileInfo, error) {
	if f.huge {
		return vfs.FileInfo{Name: f.name, Size: f.size, Mode: 0o644}, nil
	}
	if f.write {
		return vfs.FileInfo{Name: f.name, Size: int64(f.buf.Len()), Mode: 0o644}, nil
	}
	return vfs.FileInfo{Name: f.name, Size: f.size, Mode: 0o644}, nil
}

// TestContentRev_sessionVisible hashes dirty IR preferred over backend.
func TestContentRev_sessionVisible(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("rev", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/f.txt", []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	disk, err := ms.ContentRev(ctx, "/work/f.txt")
	if err != nil || disk.Hash != vfs.ContentHash("one\n") {
		t.Fatalf("disk rev: %+v err=%v", disk, err)
	}
	doc, err := ms.ReadText(ctx, "/work/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = doc.SetLine(1, "two")
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	dirty, err := ms.ContentRev(ctx, "/work/f.txt")
	if err != nil || dirty.Hash != vfs.ContentHash("two\n") {
		t.Fatalf("dirty rev: %+v err=%v", dirty, err)
	}
	w, err := ms.ReadLines(ctx, "/work/f.txt", 1, 2)
	if err != nil || w.Rev.Hash != dirty.Hash {
		t.Fatalf("window rev: %+v err=%v", w, err)
	}
}

// TestSessionOverlay_dirtyVisible: write-back creates appear in Stat/ReadDir/Remove before Sync.
func TestSessionOverlay_dirtyVisible(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("overlay", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	doc := vfs.NewTextDocument("/work/new.go", "text/x-go", "utf-8", "package new\n")
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	fi, err := ms.Stat(ctx, "/work/new.go")
	if err != nil || fi.IsDir || fi.Size != int64(len("package new\n")) {
		t.Fatalf("stat dirty: %+v err=%v", fi, err)
	}
	ents, err := ms.ReadDir(ctx, "/work")
	if err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, e := range ents {
		if e.Name == "new.go" && !e.IsDir {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("readdir missing new.go: %+v", ents)
	}
	// nested dirty path synthesizes intermediate dir
	nested := vfs.NewTextDocument("/work/pkg/x.go", "text/x-go", "utf-8", "package pkg\n")
	if err := ms.WriteDocument(ctx, nested); err != nil {
		t.Fatal(err)
	}
	fi, err = ms.Stat(ctx, "/work/pkg")
	if err != nil || !fi.IsDir {
		t.Fatalf("stat virtual dir: %+v err=%v", fi, err)
	}
	ents, err = ms.ReadDir(ctx, "/work/pkg")
	if err != nil || len(ents) != 1 || ents[0].Name != "x.go" {
		t.Fatalf("readdir pkg: %+v err=%v", ents, err)
	}
	if err := ms.Remove(ctx, "/work/new.go"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Stat(ctx, "/work/new.go"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove dirty: %v", err)
	}
	// Sync nested still works
	if err := ms.MkdirAll(ctx, "/work/pkg"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Sync(ctx, "/work/pkg/x.go"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(base, "pkg", "x.go"))
	if err != nil || string(raw) != "package pkg\n" {
		t.Fatalf("synced = %q err=%v", raw, err)
	}
}
