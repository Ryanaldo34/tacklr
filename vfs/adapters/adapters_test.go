package adapters

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestHTMLDecodesKindsLevelsMarksTables: HTML blocks decode with their
// kinds, levels, marks and tables.
func TestHTMLDecodesKindsLevelsMarksTables(t *testing.T) {
	t.Parallel()
	html := `<h1>Title</h1>
<p>Hello <strong>world</strong> <a href="https://example.com">link</a></p>
<ul>
<li>Item</li>
</ul>
<table><tr><td>A</td><td>B</td></tr><tr><td>C</td><td>D</td></tr></table>`

	got, err := (HTML{}).DecodeBlocks(context.Background(), "/x.html", htmlMediaType, []byte(html))

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

// zipOf is a zip archive with one part.
func zipOf(t *testing.T, name, body string) []byte {
	t.Helper()
	return rawZip(t, rawPart{name: name, body: body})
}

// rawPart is one stored part of a zip that rawZip writes. A zero size or
// crc is the true one.
type rawPart struct {
	name, body string
	method     uint16
	size       uint64
	crc        uint32
}

// rawZip is a zip archive of stored parts, in order.
func rawZip(t *testing.T, parts ...rawPart) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, p := range parts {
		h := &zip.FileHeader{Name: p.name, Method: p.method, CRC32: p.crc, CompressedSize64: uint64(len(p.body)), UncompressedSize64: p.size}
		if h.UncompressedSize64 == 0 {
			h.UncompressedSize64 = uint64(len(p.body))
		}
		if h.CRC32 == 0 {
			h.CRC32 = crc32.ChecksumIEEE([]byte(p.body))
		}
		w, err := zw.CreateRaw(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(w, p.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestDOCX_marksListMissingAndBadZip: a Word heading, its run marks, a bold
// run and a list item decode. A style with no value
// is a paragraph, and text outside a run is not read. Bytes that are not a
// zip, a zip without the document part, a part with an unknown method or a
// bad checksum, and a part that is not XML fail.
func TestDOCX_marksListMissingAndBadZip(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	input := zipOf(t, "word/document.xml", `<w:document xmlns:w="x"><w:body>
<w:p><w:pPr><w:pStyle w:val="Heading2"/></w:pPr><w:r><w:rPr><w:i/><w:u/><w:strike/></w:rPr><w:t>Hi</w:t><w:tab/><w:br/></w:r></w:p>
<w:p><w:pPr><w:numPr/></w:pPr><w:r><w:t>Item</w:t></w:r></w:p>
<w:p></w:p>
<w:p><w:pPr><w:pStyle/></w:pPr><w:t>stray</w:t><w:r><w:t>Plain</w:t></w:r></w:p>
<w:p><w:r><w:rPr><w:b/></w:rPr><w:t>Bold</w:t></w:r></w:p>
</w:body></w:document>`)

	doc, err := (DOCX{}).DecodeBlocks(ctx, "/x.docx", DOCXMediaType, input)

	if err != nil || len(doc) != 4 {
		t.Fatalf("decode = %#v %v", doc, err)
	}
	if doc[0].Kind != vfs.BlockKindHeading || doc[0].Style.Level != 2 || doc[1].Kind != vfs.BlockKindListItem || doc[1].Text != "Item" ||
		doc[2].Kind != vfs.BlockKindParagraph || doc[2].Text != "Plain" {
		t.Fatalf("blocks = %#v, want a level-2 heading, a list item and a paragraph", doc)
	}
	if bold := doc[3]; bold.Text != "**Bold**" || bold.Runs[0].Marks[vfs.MarkBold] != "true" {
		t.Fatalf("bold paragraph = %#v, want a bold run", bold)
	}
	for name, data := range map[string][]byte{
		"bytes that are not a zip":          []byte("not-zip"),
		"a zip without the document":        zipOf(t, "nope.xml", "<x/>"),
		"a document with an unknown method": rawZip(t, rawPart{name: "word/document.xml", body: "<x/>", method: 99}),
		"a document with a bad checksum":    rawZip(t, rawPart{name: "word/document.xml", body: "<x/>", crc: 1}),
		"a document that is not XML":        zipOf(t, "word/document.xml", "<not"),
		"a text that is not XML":            zipOf(t, "word/document.xml", "<w:p><w:r><w:t>a<b></w:t></w:r></w:p>"),
	} {
		if _, err := (DOCX{}).DecodeBlocks(ctx, "/x.docx", DOCXMediaType, data); err == nil {
			t.Errorf("%s: want a decode error", name)
		}
	}
}

// TestCodecs_canceledContext: the HTML, Word and Excel codecs refuse a
// canceled context.
func TestCodecs_canceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (HTML{}).DecodeBlocks(ctx, "/x.html", htmlMediaType, []byte("<p>x</p>")); err == nil {
		t.Fatal("html decode")
	}
	if _, err := (DOCX{}).DecodeBlocks(ctx, "/x.docx", DOCXMediaType, []byte("PK")); err == nil {
		t.Fatal("docx decode")
	}
	if _, _, err := (XLSX{}).DecodeSheets(ctx, "/x.xlsx", XLSXMediaType, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("xlsx decode: %v", err)
	}
}

// TestCodecs_registerWordAndExcel: Codecs registers the Excel codec and the
// Word codec as a projected type.
func TestCodecs_registerWordAndExcel(t *testing.T) {
	t.Parallel()
	reg, err := vfs.NewContentRegistry(Codecs()...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reg.Decode(context.Background(), "/x.xlsx", XLSXMediaType, []byte("not-zip"))
	if err == nil || errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("xlsx must be registered: %v", err)
	}
	if !reg.Projected(DOCXMediaType) {
		t.Fatal("docx must be a projected type")
	}
}

// budgetWorkbook is the parts of an Excel workbook of two sheets: Budget,
// with values, a formula and its computed value, a string with edge
// spaces, a value with no input, and formats (one used twice, a border with
// no color), and Sheet2 with one string.
var budgetWorkbook = map[string]string{
	"xl/workbook.xml":            `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Budget" sheetId="1" r:id="rId1"/><sheet name="Sheet2" sheetId="2" r:id="rId2"/></sheets></workbook>`,
	"xl/_rels/workbook.xml.rels": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/><Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`,
	"xl/worksheets/sheet1.xml":   `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" s="1"><v>42</v></c></row><row r="2"><c r="A2"><f>A1+1</f><v>43</v></c><c r="B2" s="1"><v>7</v></c></row><row r="3"><c r="A3" t="s"><v>0</v></c><c r="B3" t="s"><v>1</v></c><c r="C3" s="2" t="s"><v>2</v></c></row></sheetData></worksheet>`,
	"xl/worksheets/sheet2.xml":   `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="s"><v>3</v></c></row></sheetData></worksheet>`,
	"xl/sharedStrings.xml":       `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="4" uniqueCount="4"><si><t>Amount</t></si><si><t>computed</t></si><si><t xml:space="preserve"> edge </t></si><si><t>x</t></si></sst>`,
	"xl/styles.xml":              `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><numFmts count="1"><numFmt numFmtId="164" formatCode="$#,##0.00"/></numFmts><fonts count="3"><font><sz val="11"/><name val="Calibri"/></font><font><b/><i/><strike/><u/><color rgb="FF003366"/><sz val="11"/><name val="Calibri"/></font><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="4"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FFFFCC00"/></patternFill></fill><fill><patternFill patternType="none"/></fill></fills><borders count="3"><border><left/><right/><top/><bottom/><diagonal/></border><border><left/><right/><top/><bottom style="thin"><color rgb="FF000000"/></bottom><diagonal/></border><border><left/><right/><top style="thin"/><bottom/><diagonal/></border></borders><cellXfs count="3"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="164" fontId="1" fillId="2" borderId="1" xfId="0" applyNumberFormat="1" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="right" vertical="middle" wrapText="1"/></xf><xf numFmtId="0" fontId="2" fillId="3" borderId="2" xfId="0" applyBorder="1"/></cellXfs></styleSheet>`,
}

// TestMountSession_readsAWorkbookAndKeepsHTMLAsText: a mounted Excel
// workbook reads as a grid with its formats, and HTML at a .html path or a
// path with no extension stays HTML text.
func TestMountSession_readsAWorkbookAndKeepsHTMLAsText(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir()))).WithCodecs(Codecs()...)(ctx, "xlsx-read", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	html := "<h1>x</h1>"
	for p, body := range map[string][]byte{
		"/workspace/work/Budget.xlsx": xlsxOf(t, budgetWorkbook),
		"/workspace/work/notes.html":  []byte(html),
		"/workspace/work/SPIKE":       []byte(html),
	} {
		if err := ms.Route(ctx, p).WriteFile(ctx, body); err != nil {
			t.Fatal(err)
		}
	}

	got, err := ms.Route(ctx, "/workspace/work/Budget.xlsx").ReadText(ctx)

	if err != nil {
		t.Fatal(err)
	}
	g, ok := vfs.AsGrid(got)
	if !ok {
		t.Fatalf("type %T", got)
	}
	if b1 := g.Sheets()[0].Cells[0][1]; b1.Display() != "42" || !b1.Format.Bold || b1.Format.Number != "$#,##0.00" {
		t.Fatalf("B1 = %+v", b1)
	}
	for _, p := range []string{"/workspace/work/notes.html", "/workspace/work/SPIKE"} {
		st, statErr := ms.Route(ctx, p).Stat(ctx)
		text, textErr := ms.Route(ctx, p).ReadText(ctx)
		if statErr != nil || st.MediaType != htmlMediaType || textErr != nil || text.Text() != html {
			t.Fatalf("%s: Stat = %+v %v, text = %v %v", p, st, statErr, text, textErr)
		}
	}
}

// TestXLSX_readsUsedRangeFormulasAndFormats: the Excel codec reads a
// workbook's sheets, values, formulas, computed values, strings with edge
// spaces and formats (a format or a string used twice, a border with no
// color).
func TestXLSX_readsUsedRangeFormulasAndFormats(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	reg, err := vfs.NewContentRegistry(Codecs()...)
	if err != nil {
		t.Fatal(err)
	}
	doc, decodeErr := reg.Decode(ctx, "/work/Budget.xlsx", XLSXMediaType, xlsxOf(t, budgetWorkbook))

	if decodeErr != nil {
		t.Fatalf("decode %v", decodeErr)
	}
	td, _ := vfs.AsGrid(doc)
	sh := td.Sheets()[0]
	if len(td.Sheets()) != 2 || sh.Title != "Budget" || td.Sheets()[1].Title != "Sheet2" || sh.Rows != 3 || sh.Cols != 3 {
		t.Fatalf("sheets = %+v", td.Sheets())
	}
	for _, b := range []vfs.Cell{sh.Cells[0][1], sh.Cells[1][1]} {
		if !b.Format.Bold || !b.Format.Italic || !b.Format.Strike || !b.Format.Underline ||
			b.Format.Number != "$#,##0.00" || b.Format.Fill != "#ffcc00" || b.Format.Color != "#003366" ||
			b.Format.Align != "right" || b.Format.VAlign != "middle" || b.Format.Wrap != "wrap" ||
			b.Format.Border == nil || b.Format.Border.Style != "thin" || b.Format.Border.Color != "#000000" {
			t.Fatalf("money cell = %+v format=%+v", b, b.Format)
		}
	}
	got := []string{sh.Cells[0][1].Input, sh.Cells[1][0].Input, sh.Cells[2][0].Input, sh.Cells[2][1].Input, sh.Cells[2][2].Input}
	if want := []string{"42", "=A1+1", "Amount", "computed", " edge "}; !slices.Equal(got, want) {
		t.Fatalf("inputs = %q, want %q", got, want)
	}
	if b := sh.Cells[2][2].Format.Border; b == nil || b.Edges != "top" || b.Color != "" {
		t.Fatalf("border with no color = %+v", b)
	}
}

// xlsxOf is a workbook zip with the given parts.
func xlsxOf(t *testing.T, parts map[string]string) []byte {
	t.Helper()
	raw := make([]rawPart, 0, len(parts))
	for _, name := range slices.Sorted(maps.Keys(parts)) {
		raw = append(raw, rawPart{name: name, body: parts[name]})
	}
	return rawZip(t, raw...)
}

const (
	xlsxWorkbook = `<workbook><sheets><sheet name="S" sheetId="1" r:id="rId1"/></sheets></workbook>`
	xlsxRels     = `<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>`
)

// xlsxSheet is a workbook of one sheet "S" with sheet XML and styles XML.
func xlsxSheet(t *testing.T, sheet, styles string) []byte {
	t.Helper()
	parts := map[string]string{"xl/workbook.xml": xlsxWorkbook, "xl/_rels/workbook.xml.rels": xlsxRels, "xl/worksheets/sheet1.xml": sheet}
	if styles != "" {
		parts["xl/styles.xml"] = styles
	}
	return xlsxOf(t, parts)
}

// TestXLSX_decodeSheetsVariants: the Excel normalizer reads a workbook with
// no shared strings or styles, inline strings, booleans and pretty XML. It
// drops a cell with no reference or a range reference, skips a sheet with
// no relationship, numbers a sheet with no sheetId by its place, and gives
// one Sheet1 when no sheet is left. It reads border edges (four edges are
// all edges; borders outside the border list do not count). It refuses
// bytes past MaxReadFileBytes, a missing workbook, parts that are not XML,
// a part with an unknown method or a header size past the cap, and a cell
// past MaxSheetCells.
func TestXLSX_decodeSheetsVariants(t *testing.T) {
	t.Parallel()
	borders := `<styleSheet><borders count="3"><border><left/><right/><top/><bottom/></border>
<border><left/><top style="thin"/><bottom style="thin"/></border>
<border><left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/></border></borders>
<dxfs><dxf><border><left style="thick"/></border></dxf></dxfs>
<cellXfs><xf/><xf borderId="1"/><xf borderId="2"/></cellXfs></styleSheet>`
	cases := []struct {
		name    string
		data    []byte
		want    string
		wantErr error
		errMsg  string
	}{
		{
			name: "inline strings, booleans and pretty XML",
			data: xlsxSheet(t, `<worksheet>
<sheetData>
<row r="1">
<c r="A1" t="inlineStr"><is><t>hi</t></is></c>
<c r="B1" t="b"><v>1</v></c>
<c r="C1" t="b"><v>0</v></c>
<c><v>no ref</v></c>
<c r="A1:B1"><v>range</v></c>
</row>
</sheetData>
</worksheet>`, ""),
			want: "S#1:hi|TRUE|FALSE",
		},
		{
			name: "a sheet with no relationship and a sheet with no sheetId",
			data: xlsxOf(t, map[string]string{
				"xl/workbook.xml":            `<workbook><sheets><sheet name="Gone" r:id="rX"/><sheet name="S" r:id="rId1"/></sheets></workbook>`,
				"xl/_rels/workbook.xml.rels": xlsxRels,
				"xl/worksheets/sheet1.xml":   `<worksheet><sheetData/></worksheet>`,
			}),
			want: "S#2:",
		},
		{
			name: "no sheet left",
			data: xlsxOf(t, map[string]string{"xl/workbook.xml": `<workbook><sheets/></workbook>`}),
			want: "Sheet1#1:",
		},
		{
			name: "border edges",
			data: xlsxSheet(t, `<worksheet><sheetData><row r="1"><c r="A1" s="1" t="inlineStr"><is><t>a</t></is></c><c r="B1" s="2" t="inlineStr"><is><t>b</t></is></c></row></sheetData></worksheet>`, borders),
			want: "S#1:a[top,bottom]|b[]",
		},
		{name: "bytes past the cap", data: make([]byte, vfs.MaxReadFileBytes+1), wantErr: vfs.ErrTooLarge},
		{name: "a missing workbook", data: xlsxOf(t, map[string]string{"x.xml": "<x/>"}), errMsg: "missing workbook.xml"},
		{name: "a workbook that is not XML", data: xlsxOf(t, map[string]string{"xl/workbook.xml": "<a"}), errMsg: "xlsx workbook"},
		{name: "relationships that are not XML", data: xlsxOf(t, map[string]string{"xl/workbook.xml": xlsxWorkbook, "xl/_rels/workbook.xml.rels": "<a"}), errMsg: "xlsx rels"},
		{name: "shared strings that are not XML", data: xlsxOf(t, map[string]string{"xl/workbook.xml": xlsxWorkbook, "xl/sharedStrings.xml": "<a"}), errMsg: "xlsx sharedStrings"},
		{name: "styles that are not XML", data: xlsxSheet(t, "<worksheet/>", "<a"), errMsg: "xlsx styles"},
		{name: "a sheet that is not XML", data: xlsxSheet(t, "<a", ""), errMsg: "xlsx sheet"},
		{name: "a part with an unknown method", data: rawZip(t, rawPart{name: "xl/workbook.xml", body: xlsxWorkbook, method: 99}), wantErr: zip.ErrAlgorithm},
		{name: "a part with a header size past the cap", data: rawZip(t, rawPart{name: "xl/workbook.xml", body: xlsxWorkbook, size: vfs.MaxReadFileBytes + 1}), wantErr: vfs.ErrTooLarge},
		{name: "a cell past MaxSheetCells", data: xlsxSheet(t, `<worksheet><sheetData><row><c r="A200001"><v>1</v></c></row></sheetData></worksheet>`, ""), wantErr: vfs.ErrTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sheets, _, err := (XLSX{}).DecodeSheets(t.Context(), "/x.xlsx", XLSXMediaType, tc.data)

			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			case tc.errMsg != "":
				if err == nil || !strings.Contains(err.Error(), tc.errMsg) {
					t.Fatalf("err = %v, want %q", err, tc.errMsg)
				}
			case err != nil:
				t.Fatal(err)
			default:
				var cells []string
				for _, row := range sheets[0].Cells {
					for _, c := range row {
						if b := c.Format.Border; b != nil {
							cells = append(cells, c.Input+"["+b.Edges+"]")
						} else {
							cells = append(cells, c.Input)
						}
					}
				}
				if got := sheets[0].Title + "#" + sheets[0].ID + ":" + strings.Join(cells, "|"); len(sheets) != 1 || got != tc.want {
					t.Fatalf("sheets = %q (%d), want %q", got, len(sheets), tc.want)
				}
			}
		})
	}
}
