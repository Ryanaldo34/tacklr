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

func TestHTMLRoundTripPreservesKindsLevelsMarksTables(t *testing.T) {
	src := []vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Title", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph, Text: "Hello **world** [link](https://example.com)"},
		{Kind: vfs.BlockKindListItem, Text: "Item", Style: vfs.StyleMeta{Level: 1, Attributes: map[string]string{"list_type": "ul", "list_id": "l1"}}},
		{Kind: vfs.BlockKindTable, Text: "A\tB\nC\tD", Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "2", "cols": "2"}}},
	}
	out, err := (HTML{}).EncodeBlocks(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	html := string(out)
	if !strings.Contains(html, "<h1>Title</h1>") || !strings.Contains(html, "<strong>world</strong>") ||
		!strings.Contains(html, "https://example.com") || !strings.Contains(html, "<li>") ||
		!strings.Contains(html, "<table>") {
		t.Fatalf("encode=%q", html)
	}
	h1Line, pLine, tableLine := -1, -1, -1
	for i, line := range strings.Split(html, "\n") {
		trim := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trim, "<h1>"):
			h1Line = i
		case strings.HasPrefix(trim, "<p>"):
			pLine = i
		case strings.HasPrefix(trim, "<table>"):
			tableLine = i
		}
	}
	if h1Line < 0 || pLine < 0 || tableLine < 0 || h1Line == pLine || pLine == tableLine || h1Line == tableLine {
		t.Fatalf("want distinct heading/paragraph/table lines: h1=%d p=%d table=%d html=%q", h1Line, pLine, tableLine, html)
	}
	got, err := (HTML{}).DecodeBlocks(context.Background(), "/x.html", HTMLMediaType, out)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0].Kind != vfs.BlockKindHeading || got[0].Style.Level != 1 ||
		got[1].Kind != vfs.BlockKindParagraph || got[2].Kind != vfs.BlockKindListItem ||
		got[2].Style.Level != 1 || got[2].Style.Attributes["list_type"] != "ul" ||
		got[3].Kind != vfs.BlockKindTable || got[3].Text != "A\tB\nC\tD" {
		t.Fatalf("decode=%#v", got)
	}
	var sawBold, sawHref bool
	for _, r := range got[1].Runs {
		if r.Marks[vfs.MarkBold] == "true" {
			sawBold = true
		}
		if r.Marks[vfs.MarkHref] != "" {
			sawHref = true
		}
	}
	if !sawBold || !sawHref {
		t.Fatalf("marks=%#v", got[1].Runs)
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
	_, err := reg.Decode(context.Background(), "/x.xlsx", XLSXMediaType, []byte("not-zip"))
	if err == nil || errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("xlsx must be registered: %v", err)
	}
	if err := RegisterCommon(reg); err != nil {
		t.Fatal(err)
	}
}

func TestMountSession_xlsxCreateFormatPersists(t *testing.T) {
	if err := RegisterCommon(vfs.DefaultContentRegistry()); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("xlsx-create", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	body := "Amount,Note\n42,ok"
	_, err = ms.Apply(ctx, "/work/Budget.xlsx", vfs.Mutation{Content: &body})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := ms.ReadText(ctx, "/work/Budget.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Apply(ctx, "/work/Budget.xlsx", vfs.Mutation{
		Rev: vfs.ContentToken(doc), BlockID: "Budget!B2",
	}); err == nil || !strings.Contains(err.Error(), "sheet cell needs a value") {
		t.Fatalf("block_id without value or format: %v", err)
	}
	on := true
	_, err = ms.Apply(ctx, "/work/Budget.xlsx", vfs.Mutation{
		Rev: vfs.ContentToken(doc), BlockID: "Budget!B2",
		Format: &vfs.FormatPatch{Bold: &on, Number: strPtr("$#,##0.00")},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/work/Budget.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	g, ok := vfs.AsGrid(got)
	if !ok {
		t.Fatalf("type %T", got)
	}
	cell, err := g.Cell("Budget", "B2")
	if err != nil || cell.Display() != "ok" || !cell.Format.Bold || cell.Format.Number != "$#,##0.00" {
		t.Fatalf("persisted B2 = %+v err=%v", cell, err)
	}
	html := "<h1>x</h1>"
	if _, err := ms.Apply(ctx, "/work/notes.html", vfs.Mutation{Content: &html}); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/work/notes.html")
	if err != nil || st.MediaType != HTMLMediaType {
		t.Fatalf("notes.html Stat=%+v err=%v", st, err)
	}
	raw, err := ms.ReadText(ctx, "/work/notes.html")
	if err != nil || raw.Text() != html {
		t.Fatalf("notes.html body=%q err=%v", raw.Text(), err)
	}
	if _, err := ms.Apply(ctx, "/work/SPIKE", vfs.Mutation{Content: &html}); err != nil {
		t.Fatal(err)
	}
	st, err = ms.Stat(ctx, "/work/SPIKE")
	if err != nil || st.MediaType != HTMLMediaType {
		t.Fatalf("SPIKE Stat=%+v err=%v", st, err)
	}
	spike, err := ms.ReadText(ctx, "/work/SPIKE")
	if err != nil || spike.Text() != html {
		t.Fatalf("SPIKE body=%q err=%v", spike.Text(), err)
	}
}

func strPtr(s string) *string { return &s }

func TestXLSX_usedRangeFormulaFormatRoundTrip(t *testing.T) {
	ctx := context.Background()
	src, err := vfs.NewTabularDocument("/work/Budget.xlsx", XLSXMediaType, []vfs.Sheet{{
		ID: "1", Title: "Budget",
		Cells: [][]vfs.Cell{
			{
				{Input: "Amount", Value: "Amount"},
				{Input: "42", Value: "42", Format: vfs.CellFormat{
					Number: "$#,##0.00", Bold: true, Italic: true, Strike: true, Underline: true,
					Fill: "#ffcc00", Color: "#003366", Align: "right", VAlign: "middle", Wrap: "wrap",
					Border: &vfs.CellBorder{Style: "thin", Edges: "bottom", Color: "#000000"},
				}},
			},
			{{Input: "=A1+1", Value: "43"}},
		},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if (XLSX{}).MediaTypes()[0] != XLSXMediaType {
		t.Fatal("MediaTypes")
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
		!strings.Contains(string(styles), "<u/>") || !strings.Contains(string(styles), "FFCC00") ||
		!strings.Contains(string(styles), "003366") || !strings.Contains(string(styles), `horizontal="right"`) ||
		!strings.Contains(string(styles), `vertical="middle"`) || !strings.Contains(string(styles), `wrapText="1"`) ||
		!strings.Contains(string(styles), "FF000000") {
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
	if b1.Input != "42" || !b1.Format.Bold || !b1.Format.Italic || !b1.Format.Strike || !b1.Format.Underline ||
		b1.Format.Number != "$#,##0.00" || b1.Format.Fill != "#ffcc00" || b1.Format.Color != "#003366" ||
		b1.Format.Align != "right" || b1.Format.VAlign != "middle" || b1.Format.Wrap != "wrap" ||
		b1.Format.Border == nil || b1.Format.Border.Style != "thin" {
		t.Fatalf("B1 = %+v format=%+v", b1, b1.Format)
	}
	a2 := sh.Cells[1][0]
	if a2.Input != "=A1+1" {
		t.Fatalf("formula = %+v", a2)
	}
}
