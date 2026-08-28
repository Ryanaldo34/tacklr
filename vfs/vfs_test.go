package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestMountSession_localSession is the primary local-VFS outcome test:
// mounts, nested lookup, I/O, read-only, jail, remount, unmount.
func TestMountSession_localSession(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	ms, err := vfs.Tree(
		vfs.At("work", builtins.Local(base)),
		vfs.At("nested", builtins.Local(base)).ReadOnly(),
		vfs.At("ab", builtins.Local(base)),
	)(ctx, "sess-1", vfs.Request{Bindings: []vfs.Binding{
		{Params: map[string]string{vfs.ParamName: "nested", "subpath": "nested"}},
		{Params: map[string]string{vfs.ParamName: "ab", "subpath": "ab"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	specs := ms.Specs()
	if len(specs) != 1 || specs[0].Point != vfs.WorkspacePoint || len(specs[0].Members) != 3 {
		t.Fatalf("Specs = %+v", specs)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/hello.go", []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	if mt, err := ms.Classify(ctx, "/workspace/work/hello.go", nil); err != nil || mt != "text/x-go" {
		t.Fatalf("Classify: %q err=%v", mt, err)
	}
	if spec, err := ms.SpecAt("/workspace/work/hello.go"); err != nil || spec.Point != "/workspace/work" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}
	// empty write-through
	if err := ms.WriteFile(ctx, "/workspace/work/empty.txt", nil); err != nil {
		t.Fatal(err)
	}
	if b, err := ms.ReadFile(ctx, "/workspace/work/empty.txt"); err != nil || len(b) != 0 {
		t.Fatalf("empty = %q err=%v", b, err)
	}

	f, err := ms.Open(ctx, "/workspace/work/hello.go")
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	_ = f.Close()
	if err != nil || fi.Name != "hello.go" {
		t.Fatalf("Open/Stat handle: %+v err=%v", fi, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/nested/x.txt", []byte("no")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro nested write: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/other.txt", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	b, err := ms.ReadFile(ctx, "/workspace/work/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("ReadFile = %q err=%v", b, err)
	}
	info, err := ms.Stat(ctx, "/workspace/work/hello.go")
	if err != nil || info.IsDir || info.Name != "hello.go" {
		t.Fatalf("Stat = %+v err=%v", info, err)
	}

	if err := ms.MkdirAll(ctx, "/workspace/work/sub/dir"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/sub/dir/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/work/sub/dir")
	if err != nil || len(ents) != 1 || ents[0].Name != "a.txt" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}
	if err := ms.Remove(ctx, "/workspace/work/sub/dir/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Stat(ctx, "/workspace/work/sub/dir/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	// Segment boundary: /workspace/ab must not claim /workspace/a
	if spec, err := ms.SpecAt("/workspace/a/file"); err == nil && spec.Point == "/workspace/ab" {
		t.Fatalf("SpecAt /workspace/a claimed by /workspace/ab: %+v", spec)
	}
	spec, err := ms.SpecAt("/workspace/ab/x")
	if err != nil || spec.Point != "/workspace/ab" {
		t.Fatalf("SpecAt /workspace/ab: %+v err=%v", spec, err)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/foo/../../etc/passwd", []byte("x")); err == nil {
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
			_, _ = ms.ReadFile(ctx, "/workspace/work/hello.go")
		}()
	}
	wg.Wait()

	if err := ms.WriteFile(ctx, "/workspace/ab/c.txt", []byte("cached\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/ab/c.txt"); err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.ReadFile(cctx, "/workspace/work/hello.go"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}

	if err := ms.Unmount("/workspace"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/ab/c.txt"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("after unmount: %v", err)
	}

	ms2, err := vfs.Tree(vfs.At("work", builtins.Local(base)))(ctx, "sess-2", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms2.Close() })
	b, err = ms2.ReadFile(ctx, "/workspace/work/hello.go")
	if err != nil || string(b) != "package main\n" {
		t.Fatalf("remount ReadFile = %q err=%v", b, err)
	}
	if err := ms2.Unmount("/workspace"); err != nil {
		t.Fatal(err)
	}
	if n := len(ms2.Specs()); n != 0 {
		t.Fatalf("unmount specs = %d", n)
	}
}

// TestMountSession_byteProviderWriteAndLimits: providers without PutFile still
// persist through OpenFile; oversize Stat is rejected; unknown size streams.
func TestMountSession_byteProviderWriteAndLimits(t *testing.T) {
	ctx := t.Context()
	store := &memStore{files: make(map[string]memObj)}
	ms, err := vfs.Tree(vfs.At("mem", memFactory{store: store}.Open))(ctx, "mem-sess", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.WriteFile(ctx, "/workspace/mem/a.txt", []byte("hello-mem\n")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/mem/a.txt")
	if err != nil || string(got) != "hello-mem\n" {
		t.Fatalf("ReadFile=%q err=%v", got, err)
	}
	if rev, err := ms.ContentRev(ctx, "/workspace/mem/a.txt"); err != nil || rev.Hash != vfs.ContentHash("hello-mem\n") {
		t.Fatalf("ContentRev: %+v err=%v", rev, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/mem/empty.txt", nil); err != nil {
		t.Fatal(err)
	}
	if b, err := ms.ReadFile(ctx, "/workspace/mem/empty.txt"); err != nil || len(b) != 0 {
		t.Fatalf("empty: %q err=%v", b, err)
	}
	store.mu.Lock()
	store.files["huge.bin"] = memObj{huge: true, size: int64(vfs.MaxReadFileBytes) + 1}
	store.files["stream.txt"] = memObj{data: []byte("stream-body\n"), size: -1}
	store.files["statonly.txt"] = memObj{data: []byte("x"), size: 1, statOnly: true}
	store.files["short.bin"] = memObj{data: []byte("ab"), size: 10, short: true}
	store.mu.Unlock()
	if _, err := ms.ReadFile(ctx, "/workspace/mem/huge.bin"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("huge: %v", err)
	}
	if b, err := ms.ReadFile(ctx, "/workspace/mem/stream.txt"); err != nil || string(b) != "stream-body\n" {
		t.Fatalf("stream: %q err=%v", b, err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/mem/statonly.txt"); err == nil || !strings.Contains(err.Error(), "not readable") {
		t.Fatalf("stat-only ReadFile: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/mem/short.bin"); err == nil {
		t.Fatal("short ReadFull")
	}
	if err := ms.WriteFile(ctx, "/workspace/mem/too-big", bytes.Repeat([]byte("x"), vfs.MaxReadFileBytes+1)); err == nil {
		t.Fatal("oversize write")
	}
	store.failOpen = true
	if err := ms.WriteFile(ctx, "/workspace/mem/nope.txt", []byte("x")); err == nil {
		t.Fatal("OpenFile write fail")
	}
	store.failOpen = false
	store.noWriter = true
	if err := ms.WriteFile(ctx, "/workspace/mem/ro-handle.txt", []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("not writer: %v", err)
	}
	store.noWriter = false
	store.shortWrite = true
	if err := ms.WriteFile(ctx, "/workspace/mem/short-write.txt", []byte("hello")); err == nil {
		t.Fatal("short write")
	}
	store.shortWrite = false
	store.writeErr = true
	if err := ms.WriteFile(ctx, "/workspace/mem/err-write.txt", []byte("hello")); err == nil {
		t.Fatal("write error")
	}
	store.writeErr = false
	if _, err := ms.ContentRev(ctx, "rel"); err == nil {
		t.Fatal("ContentRev relative")
	}
	if _, err := ms.Classify(ctx, "/nomount/x.txt", nil); err == nil {
		t.Fatal("Classify unmounted")
	}
	for _, p := range []string{"", "rel", "/has\x00x"} {
		if _, err := ms.Stat(ctx, p); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("Stat %q: %v", p, err)
		}
		if _, err := ms.Open(ctx, p); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("Open %q: %v", p, err)
		}
		if _, err := ms.ReadFile(ctx, p); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("ReadFile %q: %v", p, err)
		}
		if err := ms.WriteFile(ctx, p, []byte("x")); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("WriteFile %q: %v", p, err)
		}
		if _, err := ms.ReadDir(ctx, p); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("ReadDir %q: %v", p, err)
		}
		if err := ms.Remove(ctx, p); !errors.Is(err, vfs.ErrInvalidPath) {
			t.Fatalf("Remove %q: %v", p, err)
		}
	}
	if err := ms.MkdirAll(ctx, "/nomount/dir"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("MkdirAll unmounted: %v", err)
	}
	if err := ms.Remove(ctx, "/nomount/x"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("Remove unmounted: %v", err)
	}
}

// TestDocument_session: IR persist, revalidation, codec rejects, RO.
func TestDocument_session(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	ms, err := vfs.Tree(
		vfs.At("work", builtins.Local(base)),
		vfs.At("ro", builtins.Local(base)).ReadOnly(),
	)(ctx, "doc-sess", vfs.Request{Bindings: []vfs.Binding{{
		Params: map[string]string{vfs.ParamName: "ro", "subpath": "ro"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	var b strings.Builder
	for i := 1; i <= 100; i++ {
		b.WriteString("L")
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('\n')
	}
	if err := ms.WriteFile(ctx, "/workspace/work/big.txt", []byte(b.String())); err != nil {
		t.Fatal(err)
	}
	win, err := ms.ReadLines(ctx, "/workspace/work/big.txt", 10, 13)
	if err != nil || win.Returned != 3 || win.Lines[0] != "L10" || win.Lines[2] != "L12" || win.EOF || win.NextStart != 13 {
		t.Fatalf("ReadLines = %+v err=%v", win, err)
	}
	// Soft EOF: request past last line ("L1\n"…"L100\n" → 101 segments with trailing empty)
	win, err = ms.ReadLines(ctx, "/workspace/work/big.txt", 100, 200)
	if err != nil || win.Returned < 1 || !win.EOF {
		t.Fatalf("soft EOF = %+v err=%v", win, err)
	}
	// empty requested range
	empty, err := ms.ReadLines(ctx, "/workspace/work/big.txt", 5, 5)
	if err != nil || empty.Returned != 0 {
		t.Fatalf("empty range = %+v err=%v", empty, err)
	}
	if _, err := ms.ReadLines(ctx, "/workspace/work/big.txt", 500, 501); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("ReadLines OOR: %v", err)
	}
	// page until EOF
	start := 1
	pages := 0
	for {
		w, err := ms.ReadLines(ctx, "/workspace/work/big.txt", start, start+20)
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

	if err := ms.WriteFile(ctx, "/workspace/work/note.txt", []byte("a\nb\nc\n")); err != nil {
		t.Fatal(err)
	}
	text, err := ms.ReadText(ctx, "/workspace/work/note.txt")
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
	raw, err := ms.ReadFile(ctx, "/workspace/work/note.txt")
	if err != nil || string(raw) != "a\nB\nC\nD\n" {
		t.Fatalf("ReadFile after WriteDocument = %q err=%v", raw, err)
	}
	if w, err := ms.ReadLines(ctx, "/workspace/work/note.txt", 1, 3); err != nil || w.Returned != 2 || w.Lines[1] != "B" {
		t.Fatalf("ReadLines = %+v err=%v", w, err)
	}
	text2, err := ms.ReadText(ctx, "/workspace/work/note.txt")
	if err != nil || text2.Text() != "a\nB\nC\nD\n" {
		t.Fatalf("reread = %q err=%v", text2.Text(), err)
	}
	_ = text2.SetLine(1, "A")
	if err := ms.WriteDocument(ctx, text2); err != nil {
		t.Fatal(err)
	}
	raw, err = ms.ReadFile(ctx, "/workspace/work/note.txt")
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

	if _, err := ms.ReadText(ctx, "/workspace/work"); err == nil {
		t.Fatal("ReadText on directory")
	}
	if _, err := ms.ReadLines(ctx, "/workspace/work/big.txt", 0, 1); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("ReadLines start 0: %v", err)
	}
	if w, err := ms.ReadLines(ctx, "/workspace/work/big.txt", 1, 10000); err != nil || w.Returned > 500 {
		t.Fatalf("clamp: returned=%d err=%v", w.Returned, err)
	}
	long := strings.Repeat("y", vfs.MaxLineBytes+2) + "\n"
	if err := ms.WriteFile(ctx, "/workspace/work/long.txt", []byte(long)); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadLines(ctx, "/workspace/work/long.txt", 1, 2); !errors.Is(err, vfs.ErrLineTooLong) {
		t.Fatalf("long line: %v", err)
	}

	// Revalidation
	if err := ms.WriteFile(ctx, "/workspace/work/ext.txt", []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/work/ext.txt"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "ext.txt"), []byte("two-lines\nlonger\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if again, err := ms.ReadText(ctx, "/workspace/work/ext.txt"); err != nil || again.Text() != "two-lines\nlonger\n" {
		t.Fatalf("revalidate = %q err=%v", again.Text(), err)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/main.go", []byte("package main\n")); err != nil {
		t.Fatal(err)
	}
	if goDoc, err := ms.ReadText(ctx, "/workspace/work/main.go"); err != nil || goDoc.MediaType() != "text/x-go" {
		t.Fatalf("go: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/work/main.go"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/work/main.go"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("after remove: %v", err)
	}

	png := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 0}
	if err := ms.WriteFile(ctx, "/workspace/work/pic.bin", png); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.OpenDocument(ctx, "/workspace/work/pic.bin", nil); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("binary: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/blob.bin", []byte("hello\nworld\x00\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/work/blob.bin"); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("blob IR: %v", err)
	}
	// No IR codec; ReadLines still pages the UTF-8 body.
	if w, err := ms.ReadLines(ctx, "/workspace/work/blob.bin", 1, 4); err != nil || w.Returned != 2 || w.Lines[0] != "hello" {
		t.Fatalf("ReadLines blob: %+v err=%v", w, err)
	}
	if w, err := ms.ReadLines(ctx, "/workspace/work/blob.bin", 1, 1); err != nil || w.Returned != 0 {
		t.Fatalf("empty stream window: %+v err=%v", w, err)
	}
	if _, err := ms.ReadLines(ctx, "rel", 1, 2); err == nil {
		t.Fatal("ReadLines relative")
	}
	if _, err := ms.ReadLines(ctx, "/workspace/work/missing.txt", 1, 2); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadLines missing: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/bad.bin", []byte("ok\n\xff\xfe\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadLines(ctx, "/workspace/work/bad.bin", 1, 3); !errors.Is(err, vfs.ErrInvalidUTF8) {
		t.Fatalf("stream utf8: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/README", []byte("hello from readme\n")); err != nil {
		t.Fatal(err)
	}
	if st, err := ms.Stat(ctx, "/workspace/work/README"); err != nil || st.MediaType != "text/plain" {
		t.Fatalf("local no-ext Stat: %+v err=%v", st, err)
	}
	if doc, err := ms.ReadText(ctx, "/workspace/work/README"); err != nil || doc.MediaType() != "text/plain" {
		t.Fatalf("local no-ext IR: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/bad.txt", []byte{0xff, 0xfe, 0xfd}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.OpenDocument(ctx, "/workspace/work/bad.txt", nil); !errors.Is(err, vfs.ErrInvalidUTF8) {
		t.Fatalf("utf8: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(base, "ro"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "ro", "f.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ro, err := ms.ReadText(ctx, "/workspace/ro/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = ro.SetLine(1, "changed")
	if err := ms.WriteDocument(ctx, ro); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro write: %v", err)
	}
	if err := ms.WriteDocument(ctx, bareDocument{path: "/workspace/work/x", mt: "x"}); !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("bare write: %v", err)
	}

	huge := vfs.NewTextDocument("/workspace/work/huge.txt", "text/plain", "utf-8", strings.Repeat("x", vfs.MaxReadFileBytes+1))
	if err := ms.WriteDocument(ctx, huge); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize write: %v", err)
	}
	note, err := ms.ReadText(ctx, "/workspace/work/note.txt")
	if err != nil {
		t.Fatal(err)
	}
	if b, err := vfs.EncodeTextual(note); err != nil || string(b) != "A\nB\nC\nD\n" {
		t.Fatalf("EncodeTextual = %q err=%v", b, err)
	}
	if _, err := vfs.EncodeTextual(huge); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize encode: %v", err)
	}

	if mt, err := ms.Classify(ctx, "/workspace/work/note.txt", nil); err != nil || mt != "text/plain" {
		t.Fatalf("Classify: %q err=%v", mt, err)
	}
	if mt, err := ms.Classify(ctx, "/workspace/work/new.json", []byte(`{"a":1}`)); err != nil || mt != "application/json" {
		t.Fatalf("Classify new json: %q err=%v", mt, err)
	}
	if mt, err := ms.Classify(ctx, "/workspace/work/sniff", []byte("a\x00b")); err != nil || mt != "application/octet-stream" {
		t.Fatalf("Classify nul: %q err=%v", mt, err)
	}
	if spec, err := ms.SpecAt("/workspace/work/note.txt"); err != nil || spec.Point != "/workspace/work" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}
	if _, err := ms.SpecAt("/nope/x"); err == nil {
		t.Fatal("SpecAt unmounted")
	}
	if _, err := ms.Classify(ctx, "rel", nil); err == nil {
		t.Fatal("Classify relative")
	}

	if _, err := ms.ReadText(ctx, "/workspace/work/note.txt"); err != nil {
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

// TestMountSession_configErrors covers Attach, SpecAt, and codec registry.
func TestMountSession_configErrors(t *testing.T) {
	ctx := t.Context()
	p, err := builtins.Local(t.TempDir())(ctx, "s", vfs.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s")
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Attach(ctx, vfs.MountSpec{Point: "/x"}, p); !errors.Is(err, vfs.ErrInvalidProvider) {
		t.Fatalf("empty profile: %v", err)
	}
	if _, err := ms.SpecAt(""); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("empty path: %v", err)
	}
	if _, err := ms.SpecAt("/has\x00x"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("nul path: %v", err)
	}
	if err := ms.Attach(ctx, vfs.MountSpec{Point: vfs.WorkspacePoint, Profile: "workspace"}, p); err != nil {
		t.Fatal(err)
	}
	if err := ms.Attach(ctx, vfs.MountSpec{Point: vfs.WorkspacePoint, Profile: "workspace"}, p); !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("dup mount: %v", err)
	}
	if err := ms.Unmount("/missing"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("unmount missing: %v", err)
	}
	if _, err := vfs.NewMountSession(""); err == nil {
		t.Fatal("empty session id construct")
	}
	if err := ms.Attach(ctx, vfs.MountSpec{Point: "/z", Profile: "z"}, nil); err == nil {
		t.Fatal("nil provider")
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
	// empty registry has no codec — Decode fails closed
	if _, err := creg.Decode(ctx, "/x.json", "application/json", []byte(`{}`)); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("empty registry decode: %v", err)
	}
	// canceled decode
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := (vfs.TextCodec{}).Decode(cctx, "/x", "text/plain", []byte("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("codec cancel: %v", err)
	}
	ms3, err := vfs.NewMountSession("s3")
	if err != nil {
		t.Fatal(err)
	}
	if err := ms3.Attach(ctx, vfs.MountSpec{Point: vfs.WorkspacePoint, Profile: "workspace"}, p); err != nil {
		t.Fatal(err)
	}
	if err := ms3.Attach(ctx, vfs.MountSpec{Point: vfs.WorkspacePoint, Profile: "workspace"}, p); !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("attach dup: %v", err)
	}
	cctx2, cancel2 := context.WithCancel(ctx)
	cancel2()
	ms4, err := vfs.NewMountSession("s4")
	if err != nil {
		t.Fatal(err)
	}
	if err := ms4.Attach(cctx2, vfs.MountSpec{Point: vfs.WorkspacePoint, Profile: "workspace"}, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("attach cancel: %v", err)
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
func TestS3_rejectsBadConfig(t *testing.T) {
	ctx := t.Context()
	if _, err := builtins.S3(nil, "")(ctx, "s", vfs.Binding{}); err == nil {
		t.Fatal("nil client")
	}
	if _, err := builtins.S3(builtins.AWSS3{}, "")(ctx, "s", vfs.Binding{}); err == nil {
		t.Fatal("missing bucket")
	}
	if _, err := builtins.S3(builtins.AWSS3{}, "b")(ctx, "s", vfs.Binding{
		Params: map[string]string{"prefix": "a/../b"},
	}); err == nil {
		t.Fatal("bad prefix")
	}
	var aws builtins.AWSS3
	if _, _, _, err := aws.Head(ctx, "b", "k"); err == nil {
		t.Fatal("nil AWS Head")
	}
	if _, _, _, err := aws.Get(ctx, "b", "k"); err == nil {
		t.Fatal("nil AWS Get")
	}
	if err := aws.Put(ctx, "b", "k", bytes.NewReader(nil), 0); err == nil {
		t.Fatal("nil AWS Put")
	}
	if err := aws.Delete(ctx, "b", "k"); err == nil {
		t.Fatal("nil AWS Delete")
	}
	if _, _, err := aws.List(ctx, "b", ""); err == nil {
		t.Fatal("nil AWS List")
	}
}

func TestBlob_rejectsBadConfig(t *testing.T) {
	ctx := t.Context()
	if _, err := builtins.Blob(nil, "")(ctx, "s", vfs.Binding{}); err == nil {
		t.Fatal("nil client")
	}
	if _, err := builtins.Blob(builtins.AzureBlob{}, "")(ctx, "s", vfs.Binding{}); err == nil {
		t.Fatal("missing container")
	}
	if _, err := builtins.Blob(builtins.AzureBlob{}, "c")(ctx, "s", vfs.Binding{
		Params: map[string]string{"prefix": "a/../b"},
	}); err == nil {
		t.Fatal("bad prefix")
	}
	var azure builtins.AzureBlob
	if _, _, _, err := azure.Head(ctx, "c", "k"); err == nil {
		t.Fatal("nil Azure Head")
	}
	if _, _, _, err := azure.Get(ctx, "c", "k"); err == nil {
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
	if _, err := builtins.Blob(builtins.AzureBlob{}, "c")(canceled, "s", vfs.Binding{}); !errors.Is(err, context.Canceled) {
		t.Fatal("Open canceled")
	}
}

func TestLocal_rejectsUnsafeConfig(t *testing.T) {
	ctx := t.Context()
	open := builtins.Local(t.TempDir())
	if _, err := open(ctx, "s", vfs.Binding{Params: map[string]string{"subpath": ".."}}); err == nil {
		t.Fatal("subpath ..")
	}
	if _, err := open(ctx, "s", vfs.Binding{Params: map[string]string{"subpath": "/abs"}}); err == nil {
		t.Fatal("absolute subpath")
	}
	if _, err := open(ctx, "sess-id", vfs.Binding{Params: map[string]string{"session_scoped": "true"}}); err != nil {
		t.Fatalf("session_scoped: %v", err)
	}
	if _, err := open(ctx, "../bad", vfs.Binding{Params: map[string]string{"session_scoped": "true"}}); err == nil {
		t.Fatal("unsafe session id")
	}
	if _, err := builtins.Local("rel")(ctx, "s", vfs.Binding{}); err == nil {
		t.Fatal("relative base")
	}
	file, err := os.CreateTemp(t.TempDir(), "notdir")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := builtins.Local(file.Name())(ctx, "s", vfs.Binding{}); err == nil {
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

type bareDocument struct{ path, mt string }

func (b bareDocument) Path() string      { return b.path }
func (b bareDocument) MediaType() string { return b.mt }

// --- in-memory provider (no PutFile → exercises writeContents Copy path) ---

type memStore struct {
	mu         sync.Mutex
	files      map[string]memObj
	failOpen   bool
	noWriter   bool
	shortWrite bool
	writeErr   bool
}

type memObj struct {
	data     []byte
	size     int64
	huge     bool
	statOnly bool
	short    bool
}

type memFactory struct {
	store *memStore
}

func (f memFactory) Open(context.Context, string, vfs.Binding) (vfs.Provider, error) {
	return memProvider{store: f.store}, nil
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
	sz := int64(len(o.data))
	if o.huge || o.size < 0 || o.short {
		sz = o.size
	}
	return vfs.FileInfo{Name: name, Size: sz, Mode: 0o644, ModTime: time.Now(), MediaType: "text/plain"}, nil
}

func (p memProvider) OpenFile(_ context.Context, name string, flag int, _ fs.FileMode) (vfs.File, error) {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0
	if write {
		if p.store.failOpen {
			return nil, errors.New("open failed")
		}
		if p.store.noWriter {
			return &memStatFile{name: name, size: 0}, nil
		}
		if p.store.shortWrite {
			return &shortWriteFile{name: name}, nil
		}
		if p.store.writeErr {
			return &errWriteFile{name: name}, nil
		}
		if flag&os.O_TRUNC != 0 || flag&os.O_CREATE != 0 {
			p.store.files[name] = memObj{data: nil}
		}
		return &memWriteFile{store: p.store, name: name}, nil
	}
	o, ok := p.store.files[name]
	if !ok {
		return nil, vfs.ErrNotExist
	}
	if o.huge || o.statOnly {
		return &memStatFile{name: name, size: o.size}, nil
	}
	sz := int64(len(o.data))
	if o.size < 0 || o.short {
		sz = o.size
	}
	return &memReadFile{Reader: bytes.NewReader(o.data), name: name, size: sz}, nil
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

type memReadFile struct {
	*bytes.Reader
	name string
	size int64
}

func (f *memReadFile) Close() error { return nil }
func (f *memReadFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: f.size, Mode: 0o644}, nil
}

type memWriteFile struct {
	buf   bytes.Buffer
	store *memStore
	name  string
}

func (f *memWriteFile) Write(p []byte) (int, error) { return f.buf.Write(p) }
func (f *memWriteFile) Close() error {
	f.store.mu.Lock()
	f.store.files[f.name] = memObj{data: append([]byte(nil), f.buf.Bytes()...)}
	f.store.mu.Unlock()
	return nil
}
func (f *memWriteFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: int64(f.buf.Len()), Mode: 0o644}, nil
}

type shortWriteFile struct{ name string }

func (f *shortWriteFile) Write([]byte) (int, error) { return 0, nil }
func (f *shortWriteFile) Close() error              { return nil }
func (f *shortWriteFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: 0, Mode: 0o644}, nil
}

type errWriteFile struct{ name string }

func (f *errWriteFile) Write([]byte) (int, error) { return 0, errors.New("write broken") }
func (f *errWriteFile) Close() error              { return nil }
func (f *errWriteFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: 0, Mode: 0o644}, nil
}

type memStatFile struct {
	name string
	size int64
}

func (f *memStatFile) Close() error { return nil }
func (f *memStatFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: f.size, Mode: 0o644}, nil
}
