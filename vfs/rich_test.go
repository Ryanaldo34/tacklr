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

// TestRichDocument_setTextAndReplaceLinesApplyHTML: SetText, SetLine and
// ReplaceLines of a rich document apply HTML to its blocks. Empty HTML and a
// line out of range fail.
func TestRichDocument_setTextAndReplaceLinesApplyHTML(t *testing.T) {
	t.Parallel()
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
	if err := rd.ReplaceLines(0, 1, nil); !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("ReplaceLines(0, 1): %v", err)
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

// TestDocsCodec_decodeZipVariants: the Docs codec reads the HTML part of a
// zip export and refuses a zip with no HTML and a truncated zip.
func TestDocsCodec_decodeZipVariants(t *testing.T) {
	t.Parallel()
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

// TestDocsCodec_invalidUTF8AndTableTSV: the Docs codec refuses bytes that
// are not UTF-8 and puts tabs and newlines of a table cell as spaces.
func TestDocsCodec_invalidUTF8AndTableTSV(t *testing.T) {
	t.Parallel()
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

// TestRichDocument_replaceBlockMatrix: ReplaceBlock replaces a heading (with
// include_heading), a paragraph, a block found by its heading_path, and a
// table of the same shape (TSV or a pipe table with a blank line). It
// refuses a heading without include_heading, an image, a table of a new
// shape, a ragged table, a table with no shape and an unknown block. Block
// IDs number a second heading of the same title and put a list item with
// no list_id in the list "list".
func TestRichDocument_replaceBlockMatrix(t *testing.T) {
	t.Parallel()
	d := vfs.NewRichDocument("/Spec", googleDocMIME, []vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Title", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph, Text: "hello"},
		{Kind: vfs.BlockKindTable, Text: "A\tB", Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "1", "cols": "2"}}},
		{Kind: vfs.BlockKindImage, Style: vfs.StyleMeta{Attributes: map[string]string{"object_id": "kix.x"}}},
		{Kind: vfs.BlockKindTable, Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "2", "cols": "3"}}},
		{Kind: vfs.BlockKindTable},
		{Kind: vfs.BlockKindTable, Text: "a\tb\nc"},
		{Kind: vfs.BlockKindTable, Text: "| A | B |\n| 1 | 2 |"},
		{Kind: vfs.BlockKindParagraph, Text: "kept", Style: vfs.StyleMeta{Attributes: map[string]string{"heading_path": "old-path"}}},
		{Kind: vfs.BlockKindHeading, Text: "Title", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindListItem, Text: "item"},
	})
	rd, ok := vfs.AsRich(d)
	if !ok {
		t.Fatalf("type %T", d)
	}
	if ids := blockIDs(d.Blocks()[9:]); strings.Join(ids, ",") != "title-2,list/1" {
		t.Fatalf("ids = %v, want title-2,list/1", ids)
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
	if err := rd.ReplaceBlock(d.Blocks()[2].ID, "X\tY\nZ", false); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("ragged replacement: %v", err)
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
	if err := rd.ReplaceBlock(d.Blocks()[7].ID, "| X | Y |\n\n| Z | W |", false); err != nil || d.Blocks()[7].Text != "X\tY\nZ\tW" {
		t.Fatalf("2x2 pipe table = %q err=%v", d.Blocks()[7].Text, err)
	}
	if err := rd.ReplaceBlock("old-path", "moved", false); err != nil || d.Blocks()[8].Text != "moved" {
		t.Fatalf("block by heading_path = %+v err=%v", d.Blocks()[8], err)
	}
	if err := rd.ReplaceBlock("missing", "x", false); err == nil {
		t.Fatal("unknown block")
	}
}

// TestDocsCodec_contextAndTooLarge: the Docs codec refuses a canceled
// context and bytes past MaxReadFileBytes.
func TestDocsCodec_contextAndTooLarge(t *testing.T) {
	t.Parallel()
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

// TestDocsCodec_createLiftsPlainText: plain text creates a Docs document
// with one paragraph per blank-line block and a table per pipe table. A
// pipe table pads its short rows and keeps a row of empty or dashless
// cells. One pipe column, or only separator rows, is a paragraph. Blank
// paragraphs are dropped, text of only spaces is one paragraph, and empty
// text has no blocks.
func TestDocsCodec_createLiftsPlainText(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, text string
		want       []string
	}{
		{name: "a pipe table between paragraphs", text: "Intro\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\n\nOutro", want: []string{"paragraph:Intro", "table:A\tB\n1\t2", "paragraph:Outro"}},
		{name: "a pipe table with a short row", text: "| A | B |\n| 1 |", want: []string{"table:A\tB\n1\t"}},
		{name: "a pipe row of empty and dashless cells", text: "| | B |\n| : | : |", want: []string{"table:\tB\n:\t:"}},
		{name: "one pipe column", text: "| A |", want: []string{"paragraph:| A |"}},
		{name: "only separator rows", text: "|---|---|", want: []string{"paragraph:|---|---|"}},
		{name: "blank paragraphs", text: "a\n\n  \n\nb", want: []string{"paragraph:a", "paragraph:b"}},
		{name: "only spaces", text: "   ", want: []string{"paragraph:   "}},
		{name: "empty text", text: "\n\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doc, err := (vfs.DocsCodec{}).Create("/x", "", vfs.Mutation{Content: &tc.text})

			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, b := range doc.(vfs.Structured).Blocks() {
				got = append(got, b.Kind+":"+b.Text)
			}
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("blocks = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestIR_replaceLinesAtTheEnd: ReplaceLines past the last line appends
// lines, or keeps the text when there are no lines to add, and a range to
// the end with no lines deletes the last lines.
func TestIR_replaceLinesAtTheEnd(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		start, end int
		lines      []string
		want       string
	}{
		{name: "append a line", start: 3, end: 3, lines: []string{"c"}, want: "a\nb\nc"},
		{name: "append no lines", start: 3, end: 3, want: "a\nb"},
		{name: "delete the last line", start: 2, end: 3, want: "a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := vfs.NewTextDocument("/n.txt", "text/plain", "utf-8", "a\nb")

			err := doc.ReplaceLines(tc.start, tc.end, tc.lines)

			if err != nil || doc.Text() != tc.want {
				t.Fatalf("text = %q err=%v, want %q", doc.Text(), err, tc.want)
			}
		})
	}
}
