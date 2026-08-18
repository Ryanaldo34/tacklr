package vfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTextual_setTextReturnsError(t *testing.T) {
	td := NewTextDocument("/n.txt", "text/plain", "utf-8", "old")
	if err := td.SetText("new"); err != nil {
		t.Fatalf("TextDocument.SetText: %v", err)
	}
	if td.Text() != "new" {
		t.Fatalf("TextDocument text = %q", td.Text())
	}
	rd := NewRichDocument("/Spec", mimeGoogleDocument, []Block{{
		Kind: BlockKindParagraph, Text: "hello",
	}})
	if err := rd.SetText("<p>nope</p>"); !errors.Is(err, ErrProjected) {
		t.Fatalf("RichDocument.SetText: %v", err)
	}
	if err := rd.SetLine(1, "x"); !errors.Is(err, ErrProjected) {
		t.Fatalf("SetLine: %v", err)
	}
	if err := rd.ReplaceLines(1, 2, []string{"x"}); !errors.Is(err, ErrProjected) {
		t.Fatalf("ReplaceLines: %v", err)
	}
	if rd.Text() == "<p>nope</p>" {
		t.Fatal("SetText must not replace projection")
	}
}

func TestDocsCodec_realExportFixture(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "drive_export_spec.zip"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := DocsCodec{}.Decode(t.Context(), "/contracts/Spec", mimeGoogleDocument, raw)
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := doc.(*RichDocument)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	kinds := map[string]int{}
	var sawTable, sawImage, sawList bool
	for _, b := range rd.Blocks() {
		kinds[b.Kind]++
		switch b.Kind {
		case BlockKindHeading:
			if b.Text != "Spec" {
				t.Fatalf("heading text = %q", b.Text)
			}
		case BlockKindParagraph:
			if !strings.Contains(b.Text, "Hello") && !strings.Contains(b.Text, "Body paragraph") {
				t.Fatalf("unexpected paragraph %q", b.Text)
			}
		case BlockKindListItem:
			sawList = true
		case BlockKindTable:
			sawTable = true
			if b.Text != "A1\tB1\nC2\tD2" {
				t.Fatalf("table TSV = %q", b.Text)
			}
			if b.Style.Attributes["rows"] != "2" || b.Style.Attributes["cols"] != "2" {
				t.Fatalf("table attrs = %+v", b.Style.Attributes)
			}
		case BlockKindImage:
			sawImage = true
			if b.Style.Attributes["object_id"] != "kix.img1" {
				t.Fatalf("image attrs = %+v", b.Style.Attributes)
			}
		}
	}
	if !sawTable || !sawImage || !sawList {
		t.Fatalf("kinds = %+v", kinds)
	}
	html := rd.Text()
	if strings.Contains(html, "c1") || strings.Contains(html, "font-weight") {
		t.Fatalf("projection leaked Drive classes: %s", html)
	}
	if !strings.Contains(html, "<h1>Spec</h1>") || !strings.Contains(html, "<table>") {
		t.Fatalf("projection = %s", html)
	}
	if !IsProjected(mimeGoogleDocument) {
		t.Fatal("DocsCodec must be projected")
	}
}

func TestDocsCodec_canonicalRoundTrip(t *testing.T) {
	src := []Block{
		{Kind: BlockKindHeading, Text: "Title", Style: StyleMeta{Level: 1}},
		{Kind: BlockKindParagraph, Text: "Hello world"},
		{Kind: BlockKindListItem, Text: "a", Style: StyleMeta{Level: 1, Attributes: map[string]string{"list_type": "ul", "list_id": "l1"}}},
		{Kind: BlockKindListItem, Text: "b", Style: StyleMeta{Level: 2, Attributes: map[string]string{"list_type": "ul", "list_id": "l1"}}},
		{Kind: BlockKindTable, Text: "A\tB\nC\tD", Style: StyleMeta{Attributes: map[string]string{"rows": "2", "cols": "2"}}},
		{Kind: BlockKindImage, Text: "", Style: StyleMeta{Attributes: map[string]string{"object_id": "kix.abc", "content_uri": "https://example.com/i.png"}}},
	}
	orig := NewRichDocument("/Spec", mimeGoogleDocument, src)
	got, err := DocsCodec{}.Decode(t.Context(), "/Spec", mimeGoogleDocument, []byte(orig.Text()))
	if err != nil {
		t.Fatal(err)
	}
	if !blocksEqual(stripIDs(orig.Blocks()), stripIDs(got.(*RichDocument).Blocks())) {
		t.Fatalf("round-trip\nwant %+v\ngot  %+v", orig.Blocks(), got.(*RichDocument).Blocks())
	}
}

func TestDocsCodec_invalidUTF8AndTableTSV(t *testing.T) {
	_, err := DocsCodec{}.Decode(t.Context(), "/x", mimeGoogleDocument, []byte("\xff\xfe not utf8"))
	if !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("invalid utf8: %v", err)
	}
	doc, err := DocsCodec{}.Decode(t.Context(), "/x", mimeGoogleDocument, []byte("<table><tr><td>A\tB</td><td>C\nD</td></tr></table>"))
	if err != nil {
		t.Fatal(err)
	}
	bl := doc.(*RichDocument).Blocks()
	if len(bl) != 1 || bl[0].Kind != BlockKindTable {
		t.Fatalf("blocks = %+v", bl)
	}
	if bl[0].Text != "A B\tC D" {
		t.Fatalf("TSV sanitize = %q", bl[0].Text)
	}
}

func TestRichDocument_replaceBlockMatrix(t *testing.T) {
	d := NewRichDocument("/Spec", mimeGoogleDocument, []Block{
		{Kind: BlockKindHeading, Text: "Title", Style: StyleMeta{Level: 1}},
		{Kind: BlockKindParagraph, Text: "hello"},
		{Kind: BlockKindTable, Text: "A\tB", Style: StyleMeta{Attributes: map[string]string{"rows": "1", "cols": "2"}}},
		{Kind: BlockKindImage, Style: StyleMeta{Attributes: map[string]string{"object_id": "kix.x"}}},
	})
	if err := d.ReplaceBlock(d.Blocks()[0].ID, "New", false); err == nil {
		t.Fatal("heading without include_heading")
	}
	if err := d.ReplaceBlock(d.Blocks()[0].ID, "New", true); err != nil {
		t.Fatal(err)
	}
	if d.Blocks()[0].Text != "New" {
		t.Fatalf("heading = %q", d.Blocks()[0].Text)
	}
	pre := d.ContentFingerprint()
	if err := d.ReplaceBlock(d.Blocks()[1].ID, "world", false); err != nil {
		t.Fatal(err)
	}
	if d.ContentFingerprint() == pre {
		t.Fatal("fingerprint unchanged after paragraph replace")
	}
	if tok := ContentToken(d); tok != d.ContentFingerprint() || tok == ContentHash(d.Text()) {
		t.Fatal("ContentToken must be IR fingerprint, not HTML hash")
	}
	if err := d.ReplaceBlock(d.Blocks()[2].ID, "X\tY\nZ\tW", false); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("table shape: %v", err)
	}
	if err := d.ReplaceBlock(d.Blocks()[2].ID, "X\tY", false); err != nil {
		t.Fatal(err)
	}
	if err := d.ReplaceBlock(d.Blocks()[3].ID, "nope", false); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("image replace: %v", err)
	}
	if err := d.ReplaceBlock("missing", "x", false); err == nil {
		t.Fatal("unknown block")
	}
}

func stripIDs(in []Block) []Block {
	out := cloneBlocks(in)
	for i := range out {
		out[i].ID = ""
		out[i].Style.Span = Span{}
	}
	return assignBlockIDs(out, nil)
}

func blocksEqual(a, b []Block) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || a[i].Text != b[i].Text || a[i].Style.Level != b[i].Style.Level {
			return false
		}
		if blockAttr(a[i], "list_type") != blockAttr(b[i], "list_type") {
			return false
		}
		if a[i].Kind == BlockKindImage && blockAttr(a[i], "object_id") != blockAttr(b[i], "object_id") {
			return false
		}
		if a[i].Kind == BlockKindTable && a[i].Text != b[i].Text {
			return false
		}
	}
	return true
}

func TestDocsCodec_contextAndTooLarge(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (DocsCodec{}).Decode(canceled, "/x", mimeGoogleDocument, []byte("<p>a</p>")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled decode: %v", err)
	}
	huge := bytes.Repeat([]byte("a"), MaxReadFileBytes+1)
	if _, err := (DocsCodec{}).Decode(t.Context(), "/x", mimeGoogleDocument, huge); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("too large: %v", err)
	}
}
