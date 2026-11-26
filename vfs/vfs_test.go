package vfs_test

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// wordDocument is a Word document whose body is the WordprocessingML
// paragraphs of body.
func wordDocument(t *testing.T, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>`+body+`</w:body></w:document>`); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestMountSession_specsNameEachMountByWholeSegments: a tree lists each
// member's spec at its own point, and SpecAt finds the member that holds a
// path by whole path segments.
func TestMountSession_specsNameEachMountByWholeSegments(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	base := t.TempDir()
	ms, err := vfs.Tree(
		vfs.At("work", vfs.Local(base)),
		vfs.At("ab", vfs.Local(base)).ReadOnly(),
	)(ctx, "sess-1", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	specs := ms.Specs()
	ab, abErr := ms.SpecAt("/workspace/ab/x")
	_, shortErr := ms.SpecAt("/workspace/a/file")

	if len(specs) != 2 || specs[0].Point != "/workspace/ab" || !specs[0].ReadOnly || specs[1].Point != "/workspace/work" || specs[1].ReadOnly {
		t.Fatalf("Specs = %+v, want ab read-only and work writable", specs)
	}
	if abErr != nil || ab.Point != "/workspace/ab" || !errors.Is(shortErr, vfs.ErrNotMounted) {
		t.Fatalf("SpecAt ab = %+v %v, /workspace/a = %v, want ab and not mounted", ab, abErr, shortErr)
	}
}

// TestMountSession_aSecondSessionReadsWhatTheFirstWrote: two sessions over
// one local directory share its files.
func TestMountSession_aSecondSessionReadsWhatTheFirstWrote(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	tree := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir())))
	first, err := tree(ctx, "sess-1", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := tree(ctx, "sess-2", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	writeErr := first.Route(ctx, "/workspace/work/hello.go").WriteFile(ctx, []byte("package main\n"))
	got, readErr := second.Route(ctx, "/workspace/work/hello.go").ReadFile(ctx)

	if writeErr != nil || readErr != nil || string(got) != "package main\n" {
		t.Fatalf("write = %v, second session read = %q %v", writeErr, got, readErr)
	}
}

// TestLocal_readsTextByItsContentAndRefusesWhatIsNotText: a local file
// reads as text by its extension, or by its bytes when it has none, and a
// binary file or invalid UTF-8 is no text.
func TestLocal_readsTextByItsContentAndRefusesWhatIsNotText(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		file, body string
		mediaType  string
		err        error
	}{
		"a Go file is Go source":                       {"main.go", "package main\n", "text/x-go", nil},
		"a file with no extension sniffs as text":      {"README", "hello from readme\n", "text/plain", nil},
		"a PNG is no text":                             {"pic.bin", "\x89PNG\r\n\x1a\n\x00\x00\x00\x00", "", vfs.ErrNoCodec},
		"bytes with a NUL are no text":                 {"blob.bin", "hello\nworld\x00\n", "", vfs.ErrNoCodec},
		"a text file of invalid UTF-8 does not decode": {"bad.txt", "\xff\xfe\xfd", "", vfs.ErrInvalidUTF8},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, dir := t.Context(), t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, c.file), []byte(c.body), 0o600); err != nil {
				t.Fatal(err)
			}
			ms, err := vfs.Tree(vfs.At("work", vfs.Local(dir)))(ctx, t.Name(), vfs.Request{})
			if err != nil {
				t.Fatal(err)
			}

			doc, err := ms.Route(ctx, "/workspace/work/"+c.file).ReadText(ctx)

			if c.err != nil && !errors.Is(err, c.err) {
				t.Fatalf("ReadText = %v, want %v", err, c.err)
			}
			if c.err == nil && (err != nil || doc.MediaType() != c.mediaType || doc.Text() != c.body) {
				t.Fatalf("ReadText = %+v %v, want %s text", doc, err, c.mediaType)
			}
		})
	}
}

// TestTextDocument_lines is pure IR: line index and ranges (no mount).
func TestTextDocument_lines(t *testing.T) {
	t.Parallel()
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
	// defaults
	d2 := vfs.NewTextDocument("/x", "", "", "hi")
	if d2.MediaType() != "text/plain" || d2.Encoding() != "utf-8" {
		t.Fatalf("defaults mt=%q enc=%q", d2.MediaType(), d2.Encoding())
	}
}

// A session needs an id, and SpecAt refuses an empty or NUL path. A session
// with no members lists nothing at / and mounts no other path.
func TestMountSession_needsAnIDAndRejectsBadPaths(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms, err := vfs.NewMountSession("s")
	if err != nil {
		t.Fatal(err)
	}
	if ents, err := ms.Route(ctx, "/").ReadDir(ctx); err != nil || len(ents) != 0 {
		t.Fatalf("list / of an empty session = %+v err=%v", ents, err)
	}
	if _, err := ms.Route(ctx, "/workspace/x").Stat(ctx); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("stat in an empty session = %v, want ErrNotMounted", err)
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
