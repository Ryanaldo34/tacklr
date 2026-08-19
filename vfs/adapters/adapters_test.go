package adapters

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestHTMLRoundTripPreservesBlocksAndMarks(t *testing.T) {
	doc, err := (HTML{}).DecodeRich(context.Background(), "/x.html", HTMLMediaType, []byte(`<h1>Title</h1><p>Hello <strong>world</strong> <a href="https://example.com">link</a></p>`))
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Blocks) != 2 || doc.Blocks[0].Kind != "heading" || doc.Blocks[1].Runs[1].Attributes["bold"] != "true" {
		t.Fatalf("doc=%#v", doc)
	}
	out, err := (HTML{}).EncodeRich(context.Background(), doc)
	if err != nil || !strings.Contains(string(out), "<strong>world</strong>") || !strings.Contains(string(out), "https://example.com") {
		t.Fatalf("out=%q err=%v", out, err)
	}
	more, err := (HTML{}).EncodeRich(context.Background(), &vfs.RichTextDocument{Blocks: []vfs.RichTextBlock{
		{Kind: "heading", Level: 2, Runs: []vfs.RichTextRun{{Text: "H"}}},
		{Kind: "list-item", Runs: []vfs.RichTextRun{{Text: "L", Attributes: map[string]string{"italic": "true", "strike": "true", "u": "true"}}}},
		{Kind: "quote", Runs: []vfs.RichTextRun{{Text: "Q"}}},
	}})
	if err != nil || !strings.Contains(string(more), "<h2>") || !strings.Contains(string(more), "<li>") || !strings.Contains(string(more), "<blockquote>") {
		t.Fatalf("more=%q err=%v", more, err)
	}
}

func TestDOCXRoundTripReadsAndWritesMainDocument(t *testing.T) {
	var input bytes.Buffer
	zw := zip.NewWriter(&input)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(w, `<w:document xmlns:w="x"><w:body><w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:rPr><w:b/></w:rPr><w:t>Title</w:t></w:r></w:p></w:body></w:document>`)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	doc, err := (DOCX{}).DecodeRich(context.Background(), "/x.docx", DOCXMediaType, input.Bytes())
	if err != nil || len(doc.Blocks) != 1 || doc.Blocks[0].Kind != "heading" {
		t.Fatalf("doc=%#v err=%v", doc, err)
	}
	out, err := (DOCX{}).EncodeRich(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	part, err := zipPart(out, "word/document.xml")
	if err != nil || !strings.Contains(string(part), "Heading1") || !strings.Contains(string(part), "Title") {
		t.Fatalf("part=%q err=%v", part, err)
	}
}

func TestDOCX_marksListMissingAndBadZip(t *testing.T) {
	var input bytes.Buffer
	zw := zip.NewWriter(&input)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(w, `<w:document xmlns:w="x"><w:body>
<w:p><w:pPr><w:pStyle w:val="Heading2"/></w:pPr><w:r><w:rPr><w:i/><w:u/><w:strike/></w:rPr><w:t>Hi</w:t><w:tab/><w:br/></w:r></w:p>
<w:p><w:pPr><w:numPr/></w:pPr><w:r><w:t>Item</w:t></w:r></w:p>
<w:p></w:p>
</w:body></w:document>`)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	doc, err := (DOCX{}).DecodeRich(context.Background(), "/x.docx", DOCXMediaType, input.Bytes())
	if err != nil || len(doc.Blocks) < 2 {
		t.Fatalf("doc=%#v err=%v", doc, err)
	}
	if doc.Blocks[0].Kind != "heading" || doc.Blocks[1].Kind != "list-item" {
		t.Fatalf("kinds=%v %v", doc.Blocks[0].Kind, doc.Blocks[1].Kind)
	}
	out, err := (DOCX{}).EncodeRich(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	part, err := zipPart(out, "word/document.xml")
	if err != nil || !strings.Contains(string(part), "Heading2") || !strings.Contains(string(part), "numPr") {
		t.Fatalf("part=%q err=%v", part, err)
	}
	if _, err := zipPart(out, "nope.xml"); err == nil {
		t.Fatal("missing part")
	}
	if _, err := (DOCX{}).DecodeRich(context.Background(), "/x.docx", DOCXMediaType, []byte("not-zip")); err == nil {
		t.Fatal("bad zip")
	}
	if _, err := parseDOCX([]byte("<not")); err == nil {
		t.Fatal("bad xml")
	}
}

func TestDOCXAndHTML_canceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (HTML{}).DecodeRich(ctx, "/x.html", HTMLMediaType, []byte("<p>x</p>")); err == nil {
		t.Fatal("html decode")
	}
	if _, err := (HTML{}).EncodeRich(ctx, &vfs.RichTextDocument{}); err == nil {
		t.Fatal("html encode")
	}
	if _, err := (DOCX{}).DecodeRich(ctx, "/x.docx", DOCXMediaType, []byte("PK")); err == nil {
		t.Fatal("docx decode")
	}
	if _, err := (DOCX{}).EncodeRich(ctx, &vfs.RichTextDocument{}); err == nil {
		t.Fatal("docx encode")
	}
}

func TestRegisterCommon(t *testing.T) {
	reg := vfs.NewContentRegistry()
	if err := RegisterCommon(reg); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Decode(context.Background(), "/x.html", HTMLMediaType, []byte("<p>x</p>")); err == nil {
		t.Fatal("text/html must stay unregistered")
	}
}
