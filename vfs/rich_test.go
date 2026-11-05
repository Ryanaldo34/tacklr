package vfs_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

const googleDocMIME = "application/vnd.google-apps.document"

func TestRichDocument_setTextAndReplaceLinesApplyHTML(t *testing.T) {
	td := vfs.NewTextDocument("/n.txt", "text/plain", "utf-8", "old")
	if err := td.SetText("new"); err != nil {
		t.Fatalf("TextDocument.SetText: %v", err)
	}
	if td.Text() != "new" {
		t.Fatalf("TextDocument text = %q", td.Text())
	}
	rd := vfs.NewRichDocument("/Spec", googleDocMIME, []vfs.Block{{
		Kind: vfs.BlockKindParagraph, Text: "hello",
	}})
	if err := rd.SetText("<p>nope</p>"); err != nil {
		t.Fatalf("RichDocument.SetText: %v", err)
	}
	if !strings.Contains(rd.Text(), "<p>nope</p>") {
		t.Fatalf("SetText HTML = %q", rd.Text())
	}
	paraLine := 0
	lines, err := rd.Lines(1, rd.LineCount()+1)
	if err != nil {
		t.Fatal(err)
	}
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "<p>") {
			paraLine = i + 1
			break
		}
	}
	if paraLine == 0 {
		t.Fatalf("paragraph line missing: %v", lines)
	}
	if err := rd.SetLine(paraLine, "<p>one</p>"); err != nil {
		t.Fatalf("SetLine: %v", err)
	}
	if got := rd.Blocks(); len(got) != 1 || got[0].Kind != vfs.BlockKindParagraph || got[0].Text != "one" {
		t.Fatalf("SetLine blocks = %+v", got)
	}
	if err := rd.SetText("<div></div>"); err == nil || !errors.Is(err, vfs.ErrEmptyReplace) {
		t.Fatalf("empty HTML: %v", err)
	}
	if err := rd.ReplaceLines(paraLine, paraLine+1, []string{
		"<h1>Title</h1>",
		"<table><tr><td>A</td><td>B</td></tr></table>",
	}); err != nil {
		t.Fatalf("ReplaceLines: %v", err)
	}
	got := rd.Blocks()
	if len(got) != 2 || got[0].Kind != vfs.BlockKindHeading || got[0].Text != "Title" ||
		got[1].Kind != vfs.BlockKindTable || got[1].Text != "A\tB" {
		t.Fatalf("after line splice = %+v", got)
	}
	if _, err := rd.Line(0); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("Line(0): %v", err)
	}
	if line, err := rd.Line(1); err != nil || line == "" {
		t.Fatalf("Line(1)=%q err=%v", line, err)
	}
	if empty, err := rd.Lines(1, 1); err != nil || len(empty) != 0 {
		t.Fatalf("Lines empty: %v %v", empty, err)
	}
	if _, err := rd.Lines(1, rd.LineCount()+2); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("Lines range: %v", err)
	}
	if got := vfs.NewRichDocument("/x", "", []vfs.Block{{Kind: vfs.BlockKindParagraph, Text: "hi"}}); got.MediaType() == "" {
		t.Fatal("default media type")
	}
}

func TestDocsCodec_decodeZipVariants(t *testing.T) {
	mk := func(name, body string) []byte {
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(body))
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	if _, err := (vfs.DocsCodec{}).Decode(t.Context(), "/x", "", mk("index.html", "<p>Hi</p>")); err != nil {
		t.Fatal(err)
	}
	if _, err := (vfs.DocsCodec{}).Decode(t.Context(), "/x", googleDocMIME, mk("nested/foo.html", "<p>Hi</p>")); err != nil {
		t.Fatal(err)
	}
	if _, err := (vfs.DocsCodec{}).Decode(t.Context(), "/x", googleDocMIME, mk("readme.txt", "nope")); err == nil {
		t.Fatal("zip without html")
	}
	if _, err := (vfs.DocsCodec{}).Decode(t.Context(), "/x", googleDocMIME, []byte("PK")); err == nil {
		t.Fatal("truncated zip")
	}
}

func TestDocsCodec_invalidUTF8AndTableTSV(t *testing.T) {
	_, err := vfs.DocsCodec{}.Decode(t.Context(), "/x", googleDocMIME, []byte("\xff\xfe not utf8"))
	if !errors.Is(err, vfs.ErrInvalidUTF8) {
		t.Fatalf("invalid utf8: %v", err)
	}
	doc, err := vfs.DocsCodec{}.Decode(t.Context(), "/x", googleDocMIME, []byte("<table><tr><td>A\tB</td><td>C\nD</td></tr></table>"))
	if err != nil {
		t.Fatal(err)
	}
	bl := doc.(vfs.Structured).Blocks()
	if len(bl) != 1 || bl[0].Kind != vfs.BlockKindTable {
		t.Fatalf("blocks = %+v", bl)
	}
	if bl[0].Text != "A B\tC D" {
		t.Fatalf("TSV sanitize = %q", bl[0].Text)
	}
}

func TestRichDocument_replaceBlockMatrix(t *testing.T) {
	d := vfs.NewRichDocument("/Spec", googleDocMIME, []vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Title", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph, Text: "hello"},
		{Kind: vfs.BlockKindTable, Text: "A\tB", Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "1", "cols": "2"}}},
		{Kind: vfs.BlockKindImage, Style: vfs.StyleMeta{Attributes: map[string]string{"object_id": "kix.x"}}},
		{Kind: vfs.BlockKindTable, Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "2", "cols": "3"}}},
		{Kind: vfs.BlockKindTable},
		{Kind: vfs.BlockKindTable, Text: "a\tb\nc"},
		{Kind: vfs.BlockKindTable, Text: "| A | B |\n| 1 | 2 |"},
	})
	rd, ok := vfs.AsRich(d)
	if !ok {
		t.Fatalf("type %T", d)
	}
	if err := rd.ReplaceBlock(d.Blocks()[0].ID, "New", false); err == nil {
		t.Fatal("heading without include_heading")
	}
	if err := rd.ReplaceBlock(d.Blocks()[0].ID, "New", true); err != nil {
		t.Fatal(err)
	}
	if d.Blocks()[0].Text != "New" {
		t.Fatalf("heading = %q", d.Blocks()[0].Text)
	}
	pre := d.ContentFingerprint()
	if err := rd.ReplaceBlock(d.Blocks()[1].ID, "world", false); err != nil {
		t.Fatal(err)
	}
	if d.ContentFingerprint() == pre {
		t.Fatal("fingerprint unchanged after paragraph replace")
	}
	if tok := vfs.ContentToken(d); tok != d.ContentFingerprint() || tok == vfs.ContentHash(d.Text()) {
		t.Fatal("vfs.ContentToken must be IR fingerprint, not HTML hash")
	}
	if err := rd.ReplaceBlock(d.Blocks()[2].ID, "X\tY\nZ\tW", false); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("table shape: %v", err)
	}
	if err := rd.ReplaceBlock(d.Blocks()[2].ID, "X\tY", false); err != nil {
		t.Fatal(err)
	}
	if err := rd.ReplaceBlock(d.Blocks()[3].ID, "nope", false); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("image replace: %v", err)
	}
	// A table keeps the shape of its text, else of its rows and cols.
	if err := rd.ReplaceBlock(d.Blocks()[4].ID, "X\tY", false); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("2x3 table by its attributes: %v", err)
	}
	if err := rd.ReplaceBlock(d.Blocks()[5].ID, "X", false); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("table with no shape: %v", err)
	}
	if err := rd.ReplaceBlock(d.Blocks()[6].ID, "X", false); err == nil {
		t.Fatal("ragged table replaced")
	}
	// A pipe table is 2x2, not one column of pipes.
	if err := rd.ReplaceBlock(d.Blocks()[7].ID, "X\tY\nZ\tW", false); err != nil {
		t.Fatalf("2x2 pipe table: %v", err)
	}
	if err := rd.ReplaceBlock("missing", "x", false); err == nil {
		t.Fatal("unknown block")
	}
}

func TestDocsCodec_contextAndTooLarge(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (vfs.DocsCodec{}).Decode(canceled, "/x", googleDocMIME, []byte("<p>a</p>")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled decode: %v", err)
	}
	huge := bytes.Repeat([]byte("a"), vfs.MaxReadFileBytes+1)
	if _, err := (vfs.DocsCodec{}).Decode(t.Context(), "/x", googleDocMIME, huge); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("too large: %v", err)
	}
}

// Plain text with a pipe table creates a Docs document with the paragraphs
// around it and a table block.
func TestDocsCodec_createLiftsAPipeTable(t *testing.T) {
	text := "Intro\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\n\nOutro"

	doc, err := (vfs.DocsCodec{}).Create("/x", "", vfs.Mutation{Content: &text})

	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, b := range doc.(vfs.Structured).Blocks() {
		got = append(got, b.Kind+":"+b.Text)
	}
	if want := []string{"paragraph:Intro", "table:A\tB\n1\t2", "paragraph:Outro"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("blocks = %q, want %q", got, want)
	}
}
