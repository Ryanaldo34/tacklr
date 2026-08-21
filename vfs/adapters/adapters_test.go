package adapters

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestHTMLRoundTripPreservesBlocksAndMarks(t *testing.T) {
	doc, err := (HTML{}).DecodeBlocks(context.Background(), "/x.html", HTMLMediaType, []byte(`<h1>Title</h1><p>Hello <strong>world</strong> <a href="https://example.com">link</a></p>`))
	if err != nil {
		t.Fatal(err)
	}
	if len(doc) != 2 || doc[0].Kind != vfs.BlockKindHeading || doc[1].Runs[1].Marks[vfs.MarkBold] != "true" {
		t.Fatalf("doc=%#v", doc)
	}
	out, err := (HTML{}).EncodeBlocks(context.Background(), doc)
	if err != nil || !strings.Contains(string(out), "<strong>world</strong>") || !strings.Contains(string(out), "https://example.com") {
		t.Fatalf("out=%q err=%v", out, err)
	}
	more, err := (HTML{}).EncodeBlocks(context.Background(), []vfs.Block{
		{Kind: vfs.BlockKindHeading, Style: vfs.StyleMeta{Level: 2}, Runs: []vfs.Run{{Text: "H"}}},
		{Kind: vfs.BlockKindListItem, Runs: []vfs.Run{{Text: "L", Marks: map[string]string{vfs.MarkItalic: "true", vfs.MarkStrike: "true"}}}},
		{Kind: "quote", Runs: []vfs.Run{{Text: "Q"}}},
	})
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
	doc, err := (DOCX{}).DecodeBlocks(context.Background(), "/x.docx", DOCXMediaType, input.Bytes())
	if err != nil || len(doc) != 1 || doc[0].Kind != vfs.BlockKindHeading {
		t.Fatalf("doc=%#v err=%v", doc, err)
	}
	out, err := (DOCX{}).EncodeBlocks(context.Background(), doc)
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
	doc, err := (DOCX{}).DecodeBlocks(context.Background(), "/x.docx", DOCXMediaType, input.Bytes())
	if err != nil || len(doc) < 2 {
		t.Fatalf("doc=%#v err=%v", doc, err)
	}
	if doc[0].Kind != vfs.BlockKindHeading || doc[1].Kind != vfs.BlockKindListItem {
		t.Fatalf("kinds=%v %v", doc[0].Kind, doc[1].Kind)
	}
	out, err := (DOCX{}).EncodeBlocks(context.Background(), doc)
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
	if _, err := (DOCX{}).DecodeBlocks(context.Background(), "/x.docx", DOCXMediaType, []byte("not-zip")); err == nil {
		t.Fatal("bad zip")
	}
	if _, err := parseDOCX([]byte("<not")); err == nil {
		t.Fatal("bad xml")
	}
}

func TestDOCXAndHTML_canceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (HTML{}).DecodeBlocks(ctx, "/x.html", HTMLMediaType, []byte("<p>x</p>")); err == nil {
		t.Fatal("html decode")
	}
	if _, err := (HTML{}).EncodeBlocks(ctx, nil); err == nil {
		t.Fatal("html encode")
	}
	if _, err := (DOCX{}).DecodeBlocks(ctx, "/x.docx", DOCXMediaType, []byte("PK")); err == nil {
		t.Fatal("docx decode")
	}
	if _, err := (DOCX{}).EncodeBlocks(ctx, nil); err == nil {
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
	_, err := reg.Decode(context.Background(), "/x.xlsx", XLSXMediaType, []byte("not-zip"))
	if err == nil || errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("xlsx must be registered: %v", err)
	}
}

func TestXLSX_usedRangeFormulaFormatRoundTrip(t *testing.T) {
	ctx := context.Background()
	src, err := vfs.NewTabularDocument("/work/Budget.xlsx", XLSXMediaType, []vfs.Sheet{{
		ID: "1", Title: "Budget",
		Cells: [][]vfs.Cell{
			{
				{Input: "Amount", Value: "Amount"},
				{Input: "42", Value: "42", Format: vfs.CellFormat{
					Number: "$#,##0.00", Bold: true, Italic: true, Strike: true,
					Fill: "#ffcc00", Color: "#003366", Align: "right",
					Border: &vfs.CellBorder{Style: "thin", Edges: "bottom"},
				}},
			},
			{{Input: "=A1+1", Value: "43"}},
		},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (XLSX{}).Encode(ctx, vfs.NewTextDocument("/work/x.txt", "text/plain", "utf-8", "x")); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("Encode text: %v", err)
	}
	raw, err := (XLSX{}).Encode(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	styles, err := zipPart(raw, "xl/styles.xml")
	if err != nil || !strings.Contains(string(styles), "$#,##0.00") ||
		!strings.Contains(string(styles), `style="thin"`) || !strings.Contains(string(styles), "<b/>") ||
		!strings.Contains(string(styles), "<i/>") || !strings.Contains(string(styles), "<strike/>") ||
		!strings.Contains(string(styles), "FFCC00") || !strings.Contains(string(styles), "003366") ||
		!strings.Contains(string(styles), `horizontal="right"`) {
		t.Fatalf("styles.xml = %s err=%v", styles, err)
	}
	doc, err := (XLSX{}).Decode(ctx, "/work/Budget.xlsx", XLSXMediaType, raw)
	if err != nil {
		t.Fatal(err)
	}
	td, ok := vfs.AsGrid(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	sh := td.Sheets()[0]
	if sh.Title != "Budget" || sh.Rows < 2 || sh.Cols < 2 {
		t.Fatalf("sheet = %+v", sh)
	}
	b1 := sh.Cells[0][1]
	if b1.Input != "42" || !b1.Format.Bold || !b1.Format.Italic || !b1.Format.Strike ||
		b1.Format.Number != "$#,##0.00" || b1.Format.Fill != "#ffcc00" || b1.Format.Color != "#003366" ||
		b1.Format.Align != "right" || b1.Format.Border == nil || b1.Format.Border.Style != "thin" {
		t.Fatalf("B1 = %+v format=%+v", b1, b1.Format)
	}
	a2 := sh.Cells[1][0]
	if a2.Input != "=A1+1" {
		t.Fatalf("formula = %+v", a2)
	}
}
