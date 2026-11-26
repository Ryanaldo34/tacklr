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

// TestRichDocument_projectsBlocksAsHTMLLines: a rich document with no media
// type is a Google Doc. Its blocks get IDs (an untitled heading is a
// section, a repeated heading gets a suffix, a list item with no list is in
// "list"), a nested list closes back to its first level, and an empty
// paragraph or table projects empty. Its text has one line per block, and
// AsRich gives its blocks and tabs; a document that is not a checkout is
// neither rich nor a grid.
func TestRichDocument_projectsBlocksAsHTMLLines(t *testing.T) {
	t.Parallel()
	doc := vfs.NewRichDocument("/Spec", "", []vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "!!!", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindHeading, Text: "Intro", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindHeading, Text: "Intro", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindListItem, Text: "a", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindListItem, Text: "b", Style: vfs.StyleMeta{Level: 2}},
		{Kind: vfs.BlockKindListItem, Text: "c", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph},
		{Kind: vfs.BlockKindTable},
	})

	rd, ok := vfs.AsRich(doc)
	line, lineErr := doc.Line(3)

	if !ok || doc.MediaType() != googleDocMIME || len(rd.Tabs()) != 0 {
		t.Fatalf("AsRich = %v, media type %q, tabs %v", ok, doc.MediaType(), rd.Tabs())
	}
	if ids := blockIDs(rd.Blocks()); strings.Join(ids, ",") != "section,intro,intro-2,list/1,list/2,list/3,p-1,table-1" {
		t.Fatalf("ids = %v", ids)
	}
	if lineErr != nil || line != "<h1>Intro</h1>" || doc.LineCount() != strings.Count(doc.Text(), "\n")+1 {
		t.Fatalf("line 3 = %q err=%v, %d lines of %q", line, lineErr, doc.LineCount(), doc.Text())
	}
	for _, part := range []string{"<ul>\n<li>a<ul>\n<li>b</li>\n</ul>\n<li>c</li>\n</ul>", "<p></p>", "<table></table>"} {
		if !strings.Contains(doc.Text(), part) {
			t.Fatalf("text has no %q:\n%s", part, doc.Text())
		}
	}
	if _, ok := vfs.AsRich(imageDocument{path: "/x.png"}); ok {
		t.Fatal("a host document is rich")
	}
	if _, ok := vfs.AsGrid(imageDocument{path: "/x.png"}); ok {
		t.Fatal("a host document is a grid")
	}
	if got := (vfs.Block{Kind: vfs.BlockKindParagraph, Text: "**x**"}).PlainText(); got != "x" {
		t.Fatalf("PlainText of a block with no runs = %q", got)
	}
}
