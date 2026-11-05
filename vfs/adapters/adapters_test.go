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

// TestHTMLRoundTripPreservesKindsLevelsMarksTables: HTML blocks encode one
// block per line and decode back with their kinds, levels, marks and tables.
func TestHTMLRoundTripPreservesKindsLevelsMarksTables(t *testing.T) {
	t.Parallel()
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
	got, err := (HTML{}).DecodeBlocks(context.Background(), "/x.html", htmlMediaType, out)
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

// TestDOCX_marksListMissingAndBadZip: a Word heading, its run marks and a
// list item decode, encode and decode again the same (a block with no runs
// encodes its marked text). A style with no value
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
</w:body></w:document>`)

	doc, decodeErr := (DOCX{}).DecodeBlocks(ctx, "/x.docx", DOCXMediaType, input)
	out, encodeErr := (DOCX{}).EncodeBlocks(ctx, append(slices.Clone(doc), vfs.Block{Kind: vfs.BlockKindParagraph, Text: "**Bold**"}))
	again, againErr := (DOCX{}).DecodeBlocks(ctx, "/x.docx", DOCXMediaType, out)

	if decodeErr != nil || encodeErr != nil || againErr != nil || len(again) != 4 {
		t.Fatalf("decode %v, encode %v, decode again = %#v %v", decodeErr, encodeErr, again, againErr)
	}
	if bold := again[3]; bold.Text != "**Bold**" || bold.Runs[0].Marks[vfs.MarkBold] != "true" {
		t.Fatalf("block with no runs = %#v, want a bold run", bold)
	}
	for _, blocks := range [][]vfs.Block{doc, again[:3]} {
		if len(blocks) != 3 || blocks[0].Kind != vfs.BlockKindHeading || blocks[0].Style.Level != 2 || blocks[1].Kind != vfs.BlockKindListItem || blocks[1].Text != "Item" ||
			blocks[2].Kind != vfs.BlockKindParagraph || blocks[2].Text != "Plain" {
			t.Fatalf("blocks = %#v, want a level-2 heading, a list item and a paragraph", blocks)
		}
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
	if _, err := (HTML{}).EncodeBlocks(ctx, nil); err == nil {
		t.Fatal("html encode")
	}
	if _, err := (DOCX{}).DecodeBlocks(ctx, "/x.docx", DOCXMediaType, []byte("PK")); err == nil {
		t.Fatal("docx decode")
	}
	if _, err := (DOCX{}).EncodeBlocks(ctx, nil); err == nil {
		t.Fatal("docx encode")
	}
	if _, _, err := (XLSX{}).DecodeSheets(ctx, "/x.xlsx", XLSXMediaType, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("xlsx decode: %v", err)
	}
	if _, err := (XLSX{}).EncodeSheets(ctx, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("xlsx encode: %v", err)
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

// TestMountSession_xlsxCreateFormatPersists: a workbook created from CSV
// keeps a format patch, a cell write needs a value or a format, and HTML
// at a .html path or a path with no extension stays HTML.
func TestMountSession_xlsxCreateFormatPersists(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir()))).WithCodecs(Codecs()...)(ctx, "xlsx-create", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	body := "Amount,Note\n42,ok"
	_, err = ms.Apply(ctx, "/workspace/work/Budget.xlsx", vfs.Mutation{Content: &body})
	if err != nil {
		t.Fatal(err)
	}

	doc, err := ms.Route(ctx, "/workspace/work/Budget.xlsx").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Apply(ctx, "/workspace/work/Budget.xlsx", vfs.Mutation{
		Rev: vfs.ContentToken(doc), BlockID: "Budget!B2",
	}); err == nil || !strings.Contains(err.Error(), "sheet cell needs a value") {
		t.Fatalf("block_id without value or format: %v", err)
	}
	on := true
	_, err = ms.Apply(ctx, "/workspace/work/Budget.xlsx", vfs.Mutation{
		Rev: vfs.ContentToken(doc), BlockID: "Budget!B2",
		Format: &vfs.FormatPatch{Bold: &on, Number: strPtr("$#,##0.00")},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := ms.Route(ctx, "/workspace/work/Budget.xlsx").
		ReadText(ctx)
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
	if _, err := ms.Apply(ctx, "/workspace/work/notes.html", vfs.Mutation{Content: &html}); err != nil {
		t.Fatal(err)
	}

	st, err := ms.Route(ctx, "/workspace/work/notes.html").
		Stat(ctx)
	if err != nil || st.MediaType != htmlMediaType {
		t.Fatalf("notes.html Stat=%+v err=%v", st, err)
	}

	if raw, err := ms.Route(ctx, "/workspace/work/notes.html").
		ReadText(ctx); err != nil || raw.Text() != html {
		t.Fatalf("notes.html body=%q err=%v", raw.Text(), err)
	}
	if _, err := ms.Apply(ctx, "/workspace/work/SPIKE", vfs.Mutation{Content: &html}); err != nil {
		t.Fatal(err)
	}

	if st, err := ms.Route(ctx, "/workspace/work/SPIKE").
		Stat(ctx); err != nil || st.MediaType != htmlMediaType {
		t.Fatalf("SPIKE Stat=%+v err=%v", st, err)
	}

	if spike, err := ms.Route(ctx, "/workspace/work/SPIKE").
		ReadText(ctx); err != nil || spike.Text() != html {
		t.Fatalf("SPIKE body=%q err=%v", spike.Text(), err)
	}
}

func strPtr(s string) *string { return &s }

// TestXLSX_usedRangeFormulaFormatRoundTrip: the Excel codec writes a
// workbook and reads back its sheets (an untitled one is SheetN), values,
// formulas, computed values, strings with edge spaces and formats (a format
// or a string used twice, a border with no color). A workbook with no
// sheets is one Sheet1.
func TestXLSX_usedRangeFormulaFormatRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	reg, err := vfs.NewContentRegistry(Codecs()...)
	if err != nil {
		t.Fatal(err)
	}
	money := vfs.CellFormat{
		Number: "$#,##0.00", Bold: true, Italic: true, Strike: true, Underline: true,
		Fill: "#ffcc00", Color: "#003366", Align: "right", VAlign: "middle", Wrap: "wrap",
		Border: &vfs.CellBorder{Style: "thin", Edges: "bottom", Color: "#000000"},
	}
	src, err := vfs.NewTabularDocument("/work/Budget.xlsx", XLSXMediaType, []vfs.Sheet{
		{
			ID: "1", Title: "Budget",
			Cells: [][]vfs.Cell{
				{{Input: "Amount", Value: "Amount"}, {Input: "42", Value: "42", Format: money}},
				{{Input: "=A1+1", Value: "43"}, {Input: "7", Value: "7", Format: money}},
				{{Input: "Amount"}, {Value: "computed"}, {Input: " edge ", Format: vfs.CellFormat{Border: &vfs.CellBorder{Style: "thin", Edges: "top"}}}},
			},
		},
		{Cells: [][]vfs.Cell{{{Input: "x"}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	raw, encodeErr := reg.Encode(ctx, src)
	doc, decodeErr := reg.Decode(ctx, "/work/Budget.xlsx", XLSXMediaType, raw)
	empty, emptyErr := (XLSX{}).EncodeSheets(ctx, nil, nil)
	none, _, noneErr := (XLSX{}).DecodeSheets(ctx, "/work/None.xlsx", XLSXMediaType, empty)

	if encodeErr != nil || decodeErr != nil {
		t.Fatalf("encode %v, decode %v", encodeErr, decodeErr)
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
	if emptyErr != nil || noneErr != nil || len(none) != 1 || none[0].Title != "Sheet1" {
		t.Fatalf("no sheets = %+v err=%v %v", none, emptyErr, noneErr)
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
