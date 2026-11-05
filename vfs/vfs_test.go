package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestMountSession_localSession: a local tree mounts each member at its
// own point (a read-only member refuses writes), SpecAt matches whole path
// segments, a path cannot escape its mount, an unmounted path and a
// canceled read fail, and a second session reads what the first wrote.
func TestMountSession_localSession(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	ms, err := vfs.Tree(
		vfs.At("work", vfs.Local(base)),
		vfs.At("nested", vfs.Local(base)).ReadOnly(),
		vfs.At("ab", vfs.Local(base)),
	)(ctx, "sess-1", vfs.Request{Bindings: []mount.Binding{
		{Params: map[string]string{mount.ParamName: "nested", "subpath": "nested"}},
		{Params: map[string]string{mount.ParamName: "ab", "subpath": "ab"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/work/hello.go").WriteFile(ctx, []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	var points []string
	for _, spec := range ms.Specs() {
		points = append(points, spec.Point)
	}
	work, workErr := ms.SpecAt("/workspace/work/hello.go")
	ab, abErr := ms.SpecAt("/workspace/ab/x")
	short, _ := ms.SpecAt("/workspace/a/file")
	roErr := ms.Route(ctx, "/workspace/nested/x.txt").WriteFile(ctx, []byte("no"))
	escapeErr := ms.Route(ctx, "/workspace/work/foo/../../etc/passwd").WriteFile(ctx, []byte("x"))
	_, unmountedErr := ms.Route(ctx, "/nosuch/x").ReadFile(ctx)
	_, canceledErr := ms.Route(canceled, "/workspace/work/hello.go").ReadFile(canceled)
	ms2, err := vfs.Tree(vfs.At("work", vfs.Local(base)))(ctx, "sess-2", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	again, againErr := ms2.Route(ctx, "/workspace/work/hello.go").ReadFile(ctx)

	if !slices.Equal(points, []string{"/workspace/ab", "/workspace/nested", "/workspace/work"}) {
		t.Fatalf("Specs points = %v", points)
	}
	if workErr != nil || work.Point != "/workspace/work" || abErr != nil || ab.Point != "/workspace/ab" || short.Point == "/workspace/ab" {
		t.Fatalf("SpecAt work = %+v %v, ab = %+v %v, /workspace/a = %+v", work, workErr, ab, abErr, short)
	}
	if !errors.Is(roErr, vfs.ErrReadOnly) || escapeErr == nil || !errors.Is(unmountedErr, vfs.ErrNotMounted) || !errors.Is(canceledErr, context.Canceled) {
		t.Fatalf("read-only = %v, escape = %v, unmounted = %v, canceled = %v", roErr, escapeErr, unmountedErr, canceledErr)
	}
	if againErr != nil || string(again) != "package main\n" {
		t.Fatalf("second session ReadFile = %q err=%v", again, againErr)
	}
}

// TestMountSession_writeLimits: oversize writes and unmounted or invalid paths fail closed.
func TestMountSession_writeLimits(t *testing.T) {
	ctx := t.Context()
	ms, err := vfs.Tree(vfs.At("mem", vfs.Local(t.TempDir())))(ctx, "mem-sess", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/mem/a.txt").
		WriteFile(ctx, []byte("hello-mem\n")); err != nil {
		t.Fatal(err)
	}

	if got, err := ms.Route(ctx, "/workspace/mem/a.txt").
		ReadFile(ctx); err != nil || string(got) != "hello-mem\n" {
		t.Fatalf("ReadFile=%q err=%v", got, err)
	}
	if rev, err := ms.ContentRev(ctx, "/workspace/mem/a.txt"); err != nil || rev.Hash != vfs.ContentHash("hello-mem\n") {
		t.Fatalf("ContentRev: %+v err=%v", rev, err)
	}

	if err := ms.Route(ctx, "/workspace/mem/empty.txt").
		WriteFile(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if b, err := ms.Route(ctx, "/workspace/mem/empty.txt").
		ReadFile(ctx); err != nil || len(b) != 0 {
		t.Fatalf("empty: %q err=%v", b, err)
	}

	if err := ms.Route(ctx, "/workspace/mem/too-big").
		WriteFile(ctx, bytes.Repeat([]byte("x"), vfs.MaxReadFileBytes+1)); err == nil {
		t.Fatal("oversize write")
	}

	if _, err := ms.ContentRev(ctx, "rel"); err == nil {
		t.Fatal("ContentRev relative")
	}
	for _, p := range []string{"", "rel", "/has\x00x"} {
		if _, err := ms.Route(ctx, p).
			Stat(ctx); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("Stat %q: %v", p, err)
		}

		if _, err := ms.Route(ctx, p).
			Open(ctx); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("Open %q: %v", p, err)
		}

		if _, err := ms.Route(ctx, p).
			ReadFile(ctx); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("ReadFile %q: %v", p, err)
		}

		if err := ms.Route(ctx, p).
			WriteFile(ctx, []byte("x")); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("WriteFile %q: %v", p, err)
		}

		if _, err := ms.Route(ctx, p).
			ReadDir(ctx); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("ReadDir %q: %v", p, err)
		}

		if err := ms.Route(ctx, p).
			Remove(ctx); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("Remove %q: %v", p, err)
		}
	}

	if err := ms.Route(ctx, "/nomount/dir").
		MkdirAll(ctx); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("MkdirAll unmounted: %v", err)
	}

	if err := ms.Route(ctx, "/nomount/x").
		Remove(ctx); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("Remove unmounted: %v", err)
	}
}

// TestDocument_session: IR persist, revalidation, codec rejects, RO.
func TestDocument_session(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	ms, err := vfs.Tree(
		vfs.At("work", vfs.Local(base)),
		vfs.At("ro", vfs.Local(base)).ReadOnly(),
	)(ctx, "doc-sess", vfs.Request{Bindings: []mount.Binding{{
		Params: map[string]string{mount.ParamName: "ro", "subpath": "ro"},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	var b strings.Builder
	for i := 1; i <= 100; i++ {
		b.WriteString("L")
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('\n')
	}

	if err := ms.Route(ctx, "/workspace/work/big.txt").
		WriteFile(ctx, []byte(b.String())); err != nil {
		t.Fatal(err)
	}

	win, err := ms.Route(ctx, "/workspace/work/big.txt").
		ReadLines(ctx, 10, 13)
	if err != nil || win.Returned != 3 || win.Lines[0] != "L10" || win.Lines[2] != "L12" || win.EOF || win.NextStart != 13 {
		t.Fatalf("ReadLines = %+v err=%v", win, err)
	}
	// Soft EOF: request past last line ("L1\n"…"L100\n" → 101 segments with trailing empty)

	if win, err := ms.Route(ctx, "/workspace/work/big.txt").
		ReadLines(ctx, 100, 200); err != nil || win.Returned < 1 || !win.EOF {
		t.Fatalf("soft EOF = %+v err=%v", win, err)
	}
	// empty requested range

	if empty, err := ms.Route(ctx, "/workspace/work/big.txt").
		ReadLines(ctx, 5, 5); err != nil || empty.Returned != 0 {
		t.Fatalf("empty range = %+v err=%v", empty, err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/big.txt").
		ReadLines(ctx, 500, 501); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("ReadLines OOR: %v", err)
	}

	// page until EOF
	start := 1
	pages := 0
	for {
		w, err := ms.Route(ctx, "/workspace/work/big.txt").
			ReadLines(ctx, start, start+20)
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

	if err := ms.Route(ctx, "/workspace/work/note.txt").
		WriteFile(ctx, []byte("a\nb\nc\n")); err != nil {
		t.Fatal(err)
	}

	text, err := ms.Route(ctx, "/workspace/work/note.txt").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if text.Path() != "/workspace/work/note.txt" || text.MediaType() != "text/plain" || text.Encoding() != "utf-8" || text.LineCount() != 4 {
		t.Fatalf("open: path=%q mt=%q enc=%q count=%d", text.Path(), text.MediaType(), text.Encoding(), text.LineCount())
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

	raw, err := ms.Route(ctx, "/workspace/work/note.txt").
		ReadFile(ctx)
	if err != nil || string(raw) != "a\nB\nC\nD\n" {
		t.Fatalf("ReadFile after WriteDocument = %q err=%v", raw, err)
	}

	w, err := ms.Route(ctx, "/workspace/work/note.txt").
		ReadLines(ctx, 1, 3)
	if err != nil || w.Returned != 2 || w.Lines[1] != "B" {
		t.Fatalf("ReadLines = %+v err=%v", w, err)
	}

	text2, err := ms.Route(ctx, "/workspace/work/note.txt").
		ReadText(ctx)
	if err != nil || text2.Text() != "a\nB\nC\nD\n" {
		t.Fatalf("reread = %q err=%v", text2.Text(), err)
	}
	_ = text2.SetLine(1, "A")
	if err := ms.WriteDocument(ctx, text2); err != nil {
		t.Fatal(err)
	}

	raw, err = ms.Route(ctx, "/workspace/work/note.txt").
		ReadFile(ctx)
	if err != nil || string(raw) != "A\nB\nC\nD\n" {
		t.Fatalf("after second write = %q err=%v", raw, err)
	}
	if disk, err := os.ReadFile(filepath.Join(base, "note.txt")); err != nil || string(disk) != "A\nB\nC\nD\n" {
		t.Fatalf("disk = %q err=%v", disk, err)
	}
	if rev, err := ms.ContentRev(ctx, "/workspace/work/note.txt"); err != nil || rev.Hash != vfs.ContentHash("A\nB\nC\nD\n") {
		t.Fatalf("rev: %+v err=%v", rev, err)
	}

	nested := vfs.NewTextDocument("/workspace/work/pkg/x.go", "text/x-go", "utf-8", "package pkg\n")
	if err := ms.WriteDocument(ctx, nested); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(base, "pkg", "x.go")); err != nil || string(raw) != "package pkg\n" {
		t.Fatalf("nested disk = %q err=%v", raw, err)
	}

	if _, err := ms.Route(ctx, "/workspace/work").
		ReadText(ctx); err == nil {
		t.Fatal("ReadText on directory")
	}

	if _, err := ms.Route(ctx, "/workspace/work/big.txt").
		ReadLines(ctx, 0, 1); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("ReadLines start 0: %v", err)
	}

	w, err = ms.Route(ctx, "/workspace/work/big.txt").
		ReadLines(ctx, 1, 10000)
	if err != nil || w.Returned > 500 {
		t.Fatalf("clamp: returned=%d err=%v", w.Returned, err)
	}

	long := strings.Repeat("y", vfs.MaxLineBytes+2) + "\n"

	if err := ms.Route(ctx, "/workspace/work/long.txt").
		WriteFile(ctx, []byte(long)); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/long.txt").
		ReadLines(ctx, 1, 2); !errors.Is(err, vfs.ErrLineTooLong) {
		t.Fatalf("long line: %v", err)
	}

	// Revalidation

	if err := ms.Route(ctx, "/workspace/work/ext.txt").
		WriteFile(ctx, []byte("one\n")); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/ext.txt").
		ReadText(ctx); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(base, "ext.txt"), []byte("two-lines\nlonger\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if again, err := ms.Route(ctx, "/workspace/work/ext.txt").
		ReadText(ctx); err != nil || again.Text() != "two-lines\nlonger\n" {
		t.Fatalf("revalidate = %q err=%v", again.Text(), err)
	}

	if err := ms.Route(ctx, "/workspace/work/main.go").
		WriteFile(ctx, []byte("package main\n")); err != nil {
		t.Fatal(err)
	}

	if goDoc, err := ms.Route(ctx, "/workspace/work/main.go").
		ReadText(ctx); err != nil || goDoc.MediaType() != "text/x-go" {
		t.Fatalf("go: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/work/main.go").
		Remove(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/main.go").
		ReadText(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	png := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 0}

	if err := ms.Route(ctx, "/workspace/work/pic.bin").
		WriteFile(ctx, png); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/pic.bin").
		OpenDocument(ctx); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("binary: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/work/blob.bin").
		WriteFile(ctx, []byte("hello\nworld\x00\n")); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/blob.bin").
		ReadText(ctx); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("blob IR: %v", err)
	}

	// No IR codec; ReadLines still pages the UTF-8 body.

	w, err = ms.Route(ctx, "/workspace/work/blob.bin").
		ReadLines(ctx, 1, 4)
	if err != nil || w.Returned != 2 || w.Lines[0] != "hello" {
		t.Fatalf("ReadLines blob: %+v err=%v", w, err)
	}

	if w, err := ms.Route(ctx, "/workspace/work/blob.bin").
		ReadLines(ctx, 1, 1); err != nil || w.Returned != 0 {
		t.Fatalf("empty stream window: %+v err=%v", w, err)
	}

	if _, err := ms.Route(ctx, "rel").
		ReadLines(ctx, 1, 2); err == nil {
		t.Fatal("ReadLines relative")
	}

	if _, err := ms.Route(ctx, "/workspace/work/missing.txt").
		ReadLines(ctx, 1, 2); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadLines missing: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/work/bad.bin").
		WriteFile(ctx, []byte("ok\n\xff\xfe\n")); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/bad.bin").
		ReadLines(ctx, 1, 3); !errors.Is(err, vfs.ErrInvalidUTF8) {
		t.Fatalf("stream utf8: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/work/README").
		WriteFile(ctx, []byte("hello from readme\n")); err != nil {
		t.Fatal(err)
	}

	if st, err := ms.Route(ctx, "/workspace/work/README").
		Stat(ctx); err != nil || st.MediaType != "text/plain" {
		t.Fatalf("local no-ext Stat: %+v err=%v", st, err)
	}

	if doc, err := ms.Route(ctx, "/workspace/work/README").
		ReadText(ctx); err != nil || doc.MediaType() != "text/plain" {
		t.Fatalf("local no-ext IR: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/work/bad.txt").
		WriteFile(ctx, []byte{0xff, 0xfe, 0xfd}); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/bad.txt").
		OpenDocument(ctx); !errors.Is(err, vfs.ErrInvalidUTF8) {
		t.Fatalf("utf8: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(base, "ro"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "ro", "f.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ro, err := ms.Route(ctx, "/workspace/ro/f.txt").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = ro.SetLine(1, "changed")
	if err := ms.WriteDocument(ctx, ro); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write: %v", err)
	}
	if err := ms.WriteDocument(ctx, imageDocument{path: "/workspace/work/x"}); !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("bare write: %v", err)
	}

	huge := vfs.NewTextDocument("/workspace/work/huge.txt", "text/plain", "utf-8", strings.Repeat("x", vfs.MaxReadFileBytes+1))
	if err := ms.WriteDocument(ctx, huge); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize write: %v", err)
	}

	note, err := ms.Route(ctx, "/workspace/work/note.txt").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := vfs.EncodeTextual(note); err != nil || string(b) != "A\nB\nC\nD\n" {
		t.Fatalf("EncodeTextual = %q err=%v", b, err)
	}
	if _, err := vfs.EncodeTextual(huge); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize encode: %v", err)
	}

	if spec, err := ms.SpecAt("/workspace/work/note.txt"); err != nil || spec.Point != "/workspace/work" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}
	if _, err := ms.SpecAt("/nope/x"); err == nil {
		t.Fatal("SpecAt unmounted")
	}

	if _, err := ms.Route(ctx, "/workspace/work/note.txt").
		ReadText(ctx); err != nil {
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
	// defaults
	d2 := vfs.NewTextDocument("/x", "", "", "hi")
	if d2.MediaType() != "text/plain" || d2.Encoding() != "utf-8" {
		t.Fatalf("defaults mt=%q enc=%q", d2.MediaType(), d2.Encoding())
	}
}

// A session needs an id, and SpecAt refuses an empty or NUL path.
func TestMountSession_needsAnIDAndRejectsBadPaths(t *testing.T) {
	ms, err := vfs.NewMountSession("s")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.SpecAt(""); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("empty path: %v", err)
	}
	if _, err := ms.SpecAt("/has\x00x"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("nul path: %v", err)
	}
	if _, err := vfs.NewMountSession(""); err == nil {
		t.Fatal("empty session id construct")
	}
}

func TestLocal_rejectsUnsafeConfig(t *testing.T) {
	ctx := t.Context()
	open := vfs.Local(t.TempDir())
	if _, err := open(ctx, "s", mount.Binding{Params: map[string]string{"subpath": ".."}}); err == nil {
		t.Fatal("subpath ..")
	}
	if _, err := open(ctx, "s", mount.Binding{Params: map[string]string{"subpath": "/abs"}}); err == nil {
		t.Fatal("absolute subpath")
	}
	if _, err := open(ctx, "sess-id", mount.Binding{Params: map[string]string{"session_scoped": "true"}}); err != nil {
		t.Fatalf("session_scoped: %v", err)
	}
	if _, err := open(ctx, "../bad", mount.Binding{Params: map[string]string{"session_scoped": "true"}}); err == nil {
		t.Fatal("unsafe session id")
	}
	if _, err := vfs.Local("rel")(ctx, "s", mount.Binding{}); err == nil {
		t.Fatal("relative base")
	}
	file, err := os.CreateTemp(t.TempDir(), "notdir")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := vfs.Local(file.Name())(ctx, "s", mount.Binding{}); err == nil {
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
		t.Fatal(err)
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
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.Validate(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Validate cancel: %v", err)
	}
	if _, err := p.Stat(cctx, "e"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat cancel: %v", err)
	}
	if _, err := p.OpenFile(cctx, "e", os.O_RDONLY, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenFile cancel: %v", err)
	}
	if _, err := p.ReadDir(cctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir cancel: %v", err)
	}
	if err := p.Remove(cctx, "e"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Remove cancel: %v", err)
	}
	if err := p.MkdirAll(cctx, "d", 0o755); !errors.Is(err, context.Canceled) {
		t.Fatalf("MkdirAll cancel: %v", err)
	}
}
