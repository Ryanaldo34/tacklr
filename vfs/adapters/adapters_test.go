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

func TestRegisterCommon(t *testing.T) {
	reg := vfs.NewContentRegistry()
	if err := RegisterCommon(reg); err != nil {
		t.Fatal(err)
	}
}
