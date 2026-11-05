package vfs

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"os"
	"strings"
	"testing"
)

func mustGrid(t testing.TB, doc Document) *gridBody {
	t.Helper()
	g, ok := asGridBody(doc)
	if !ok {
		t.Fatalf("grid body: %T", doc)
	}
	return g
}

func twoSheetBudget() *IR {
	long := strings.Repeat("Hdr", 40)
	d, err := NewTabularDocument("/contracts/Budget", mimeGoogleSpreadsheet, []Sheet{
		{
			ID: "1", Title: "Budget", Index: 0,
			Cells: [][]Cell{
				{{Input: "Date"}, {Input: "Amount"}, {Input: "Note"}, {Input: long}},
				{{Input: "2026-01-01"}, {Input: "42"}, {Input: "ok\tline"}, {Input: "a\\b"}},
				{{Input: "=A1+1"}, {Value: "43"}},
			},
		},
		{
			ID: "2", Title: "Q1 Budget", Index: 1,
			Cells: [][]Cell{
				{{Input: "Hello"}, {Input: "World"}},
			},
		},
		{
			ID: "3", Title: "Budget", Index: 2,
			Cells: [][]Cell{{{Input: "dup"}}},
		},
	}, []NamedRange{{Name: "Total", SheetID: "1", A1: "B2"}})
	if err != nil {
		panic(err)
	}
	return d
}

// TestGrid_projectionA1AndFingerprint: a grid has one block per sheet (a
// long header preview is cut), finds a sheet by title or slug, reads cells,
// ranges and rows with escapes, refuses line writes, and its fingerprint
// ignores the value of a formula.
func TestGrid_projectionA1AndFingerprint(t *testing.T) {
	t.Parallel()
	d := twoSheetBudget()
	blocks := d.Blocks()
	if len(blocks) != 3 || blocks[0].Kind != BlockKindSheet || blocks[0].ID != "budget" ||
		blocks[1].ID != "q1-budget" || blocks[2].ID != "budget-2" {
		t.Fatalf("blocks = %+v", blocks)
	}
	if !strings.Contains(blocks[0].Text, "…") {
		t.Fatalf("preview must truncate long header: %q", blocks[0].Text)
	}
	g := mustGrid(t, d)
	if _, ok := g.findSheet("Budget"); !ok {
		t.Fatal("findSheet title")
	}
	if _, ok := g.findSheet("budget-2"); !ok {
		t.Fatal("findSheet duplicate slug")
	}
	if _, ok := FindBlock(blocks, "budget"); !ok {
		t.Fatal("FindBlock slug")
	}
	if err := d.ReplaceLines(1, 2, []string{"x"}); !errors.Is(err, ErrProjected) {
		t.Fatalf("ReplaceLines: %v", err)
	}
	got, err := g.ReadCell("Budget", "B2")
	if err != nil || got != "42" {
		t.Fatalf("B2 = %q err=%v", got, err)
	}
	if formula, err := g.ReadCell("budget", "A3"); err != nil || formula != "=A1+1" {
		t.Fatalf("formula = %q err=%v", formula, err)
	}
	sheet, a1 := SplitSheetAddr("'Q1 Budget'!A1:B1")
	if sheet != "Q1 Budget" || a1 != "A1:B1" {
		t.Fatalf("quoted addr = %q %q", sheet, a1)
	}
	if rect, err := g.ReadRangeTSV(sheet, a1); err != nil || rect != "Hello\tWorld" {
		t.Fatalf("range = %q err=%v", rect, err)
	}
	_, rows, err := g.ReadRows("Budget", 2, 3)
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0], `ok\tline`) || !strings.Contains(rows[0], `a\\b`) {
		t.Fatalf("ReadRows = %q err=%v", rows, err)
	}
	if cell, err := g.Cell("Budget", "C2"); err != nil || cell.Display() != "ok\tline" {
		t.Fatalf("escaped cell = %+v err=%v", cell, err)
	}
	if rect, err := g.ReadRangeTSV("Budget", "C2:D2"); err != nil || !strings.Contains(rect, `\t`) || !strings.Contains(rect, `\`) {
		t.Fatalf("escaped range = %q err=%v", rect, err)
	}
	twin := twoSheetBudget()
	mustGrid(t, twin).sheets[0].Cells[2][0].Value = "999"
	if d.ContentFingerprint() != twin.ContentFingerprint() {
		t.Fatal("formatted formula value must not change rev")
	}
}

// TestSheetsHTML_decodesZipAndTable: the Sheets codec reads a Drive zip
// export, inline HTML tables (a <br> in a cell is a newline), a page with
// no table, and a zip whose only HTML part is index.html.
func TestSheetsHTML_decodesZipAndTable(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/drive_export_budget.zip")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := (SheetsCodec{}).Decode(t.Context(), "/contracts/Budget", mimeGoogleSpreadsheet, raw)
	if err != nil {
		t.Fatal(err)
	}
	td, ok := AsGrid(doc)
	if !ok || len(td.Sheets()) != 2 || td.Sheets()[0].Title != "Budget" {
		t.Fatalf("zip = %+v ok=%v", doc, ok)
	}
	if td.Sheets()[0].Cells[1][1].Input != "42" {
		t.Fatalf("zip B2 = %+v", td.Sheets()[0].Cells[1][1])
	}

	inline := []byte(`<html><head><title>Inline</title></head><body><table>
<tr><td>Date</td><td>Amount</td></tr>
<tr><td>2026-01-01</td><td>42</td></tr>
<tr><td>=A1+1</td><td>a<br>b</td></tr>
</table><table><tr><td>Second</td></tr></table></body></html>`)
	doc, err = (SheetsCodec{}).Decode(t.Context(), "/contracts/Budget", mimeGoogleSpreadsheet, inline)
	if err != nil {
		t.Fatal(err)
	}
	td, ok = AsGrid(doc)
	if !ok || td.Sheets()[0].Cells[2][0].Input != "=A1+1" || td.Sheets()[0].Cells[2][1].Input != "a\nb" || td.Sheets()[0].Title != "Inline" ||
		len(td.Sheets()) < 2 || td.Sheets()[1].Cells[0][0].Input != "Second" {
		t.Fatalf("html = %+v ok=%v", doc, ok)
	}

	plain, err := (SheetsCodec{}).Decode(t.Context(), "/contracts/Empty", mimeGoogleSpreadsheet, []byte(`<html><head><title>Solo</title></head><body><p>no table</p></body></html>`))
	if err != nil {
		t.Fatal(err)
	}
	g, ok := AsGrid(plain)
	if !ok || len(g.Sheets()) != 1 || g.Sheets()[0].Title != "Solo" || g.Sheets()[0].Rows != 0 {
		t.Fatalf("no table = %+v ok=%v", plain, ok)
	}
	if _, err := (SheetsHTML{}).EncodeSheets(t.Context(), nil, nil); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("HTML encode: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (TabularCodec{Types: []string{mimeGoogleSpreadsheet}, Normalizer: SheetsHTML{}}).Decode(canceled, "/x", "", nil); err == nil {
		t.Fatal("canceled decode")
	}

	var zbuf bytes.Buffer
	zw := zip.NewWriter(&zbuf)
	_, _ = zw.Create("folder/")
	txt, err := zw.Create("notes.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = txt.Write([]byte("skip"))
	idx, err := zw.Create("index.html")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = idx.Write([]byte(`<html><head><title>IndexOnly</title></head><body><table><tr><td>Only</td></tr></table></body></html>`))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	indexed, err := (SheetsCodec{}).Decode(t.Context(), "/contracts/Index", mimeGoogleSpreadsheet, zbuf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	ig, ok := AsGrid(indexed)
	if !ok || len(ig.Sheets()) != 1 || ig.Sheets()[0].Cells[0][0].Input != "Only" {
		t.Fatalf("index zip = %+v ok=%v sheets=%+v", indexed, ok, ig.Sheets())
	}
}

// TestCreator_liftsGridAndRich: the Sheets codec creates a grid from CSV
// (a short row is padded), TSV, empty content and sheet blocks (a block
// with no title or ID takes the file name). It refuses HTML content and a
// grid past MaxSheetCells. The Docs codec lifts plain text and HTML.
func TestCreator_liftsGridAndRich(t *testing.T) {
	t.Parallel()
	csv := "A,B\n1"
	grid, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Content: &csv})
	if err != nil {
		t.Fatal(err)
	}
	g, ok := AsGrid(grid)
	if !ok || len(g.Sheets()) != 1 || g.Sheets()[0].Cols != 2 || g.Sheets()[0].Cells[1][0].Input != "1" {
		t.Fatalf("csv lift = %+v ok=%v", grid, ok)
	}
	wide := strings.Repeat("x,", MaxSheetCells) + "x"
	if _, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Content: &wide}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("grid past MaxSheetCells: %v", err)
	}
	tsv := "A\tB\n1\t2"
	tabbed, err := (SheetsCodec{}).Create("/Ledger", "", Mutation{Content: &tsv})
	if err != nil {
		t.Fatal(err)
	}
	g, ok = AsGrid(tabbed)
	if !ok || g.Sheets()[0].Cells[0][0].Input != "A" || g.Sheets()[0].Title != "Ledger" {
		t.Fatalf("tsv lift = %+v ok=%v", tabbed, ok)
	}
	empty := ""
	blank, err := (SheetsCodec{}).Create("", mimeGoogleSpreadsheet, Mutation{Content: &empty})
	if err != nil {
		t.Fatal(err)
	}
	g, ok = AsGrid(blank)
	if !ok || g.Sheets()[0].Title != "Sheet1" || g.Sheets()[0].Rows != 0 {
		t.Fatalf("empty lift = %+v ok=%v", blank, ok)
	}
	blocks := []Block{
		{Kind: BlockKindParagraph, Text: "skip"},
		{Kind: BlockKindSheet, Text: "X\tY", Style: StyleMeta{Attributes: map[string]string{"title": "Sheet1"}}},
		{Kind: BlockKindSheet, ID: "extra", Text: "Z"},
		{Kind: BlockKindSheet, Text: "hello\\tworld\tok\\nline\ta\\\\b\tend\\\tcr\\r\tunk\\x", Style: StyleMeta{Attributes: map[string]string{"title": "Esc"}}},
		{Kind: BlockKindSheet, Text: "untitled"},
	}
	fromBlocks, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Blocks: blocks})
	if err != nil {
		t.Fatal(err)
	}
	g, ok = AsGrid(fromBlocks)
	if !ok || len(g.Sheets()) != 4 || g.Sheets()[0].Title != "Sheet1" || g.Sheets()[0].Cells[0][0].Input != "X" ||
		g.Sheets()[1].Title != "extra" || g.Sheets()[3].Title != "Ledger" {
		t.Fatalf("blocks lift = %+v ok=%v", fromBlocks, ok)
	}
	esc := g.Sheets()[2].Cells[0]
	if len(esc) < 6 || esc[0].Input != "hello\tworld" || esc[1].Input != "ok\nline" ||
		esc[2].Input != "a\\b" || esc[3].Input != "end\\" || esc[4].Input != "cr\r" || esc[5].Input != "unk\\x" {
		t.Fatalf("escaped lift = %+v", esc)
	}
	fallback, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Blocks: []Block{{Kind: BlockKindParagraph, Text: "x"}}})
	if err != nil {
		t.Fatal(err)
	}
	g, ok = AsGrid(fallback)
	if !ok || len(g.Sheets()) != 1 || g.Sheets()[0].Title != "Ledger" {
		t.Fatalf("non-sheet blocks = %+v ok=%v", fallback, ok)
	}
	html := "<p>nope</p>"
	if _, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Content: &html}); err == nil {
		t.Fatal("HTML content")
	}

	plain := "Hello\n\nWorld"
	rich, err := (DocsCodec{}).Create("/Spec", mimeGoogleDocument, Mutation{Content: &plain})
	if err != nil {
		t.Fatal(err)
	}
	r, ok := AsRich(rich)
	if !ok || len(r.Blocks()) != 2 || r.Blocks()[0].Text != "Hello" {
		t.Fatalf("plaintext lift = %+v ok=%v", rich, ok)
	}
	lifted, err := (DocsCodec{}).Create("/Spec", mimeGoogleDocument, Mutation{Content: &html})
	if err != nil {
		t.Fatal(err)
	}
	lr, ok := AsRich(lifted)
	if !ok || len(lr.Blocks()) != 1 || lr.Blocks()[0].Kind != BlockKindParagraph || lr.Blocks()[0].Text != "nope" {
		t.Fatalf("HTML lift = %+v ok=%v", lifted, ok)
	}
}

// TestCellFormat_colorsAndPatches: colors parse from hex (bad hex is none),
// a patch writes false and empty values, and String lists each set field (a
// zero format is empty, an unknown border style is thin).
func TestCellFormat_colorsAndPatches(t *testing.T) {
	t.Parallel()
	for in, want := range map[string][2]string{
		"nope":     {"", "FF000000"},
		"zzzzzz":   {"", "FF000000"},
		"#AABBCC":  {"#aabbcc", "FFAABBCC"},
		"FF112233": {"#112233", "FF112233"},
	} {
		if got := [2]string{HexColor(in), ExcelARGB(in)}; got != want {
			t.Fatalf("colors of %q = %q, want %q", in, got, want)
		}
	}
	cleared := CellFormat{Bold: true, Wrap: "WRAP", Align: "LEFT", Border: &CellBorder{}}
	cleared.Normalize()
	off, empty := false, ""
	cleared.ApplyPatch(FormatPatch{
		Bold: &off, Italic: &off, Strike: &off, Underline: &off,
		Fill: &empty, Color: &empty, Align: &empty, VAlign: &empty, Wrap: &empty,
		Border: &CellBorder{Style: "dotted", Edges: "left"},
	})
	if cleared.Bold || cleared.Wrap != "" || cleared.Border == nil || cleared.Border.Style != "dotted" {
		t.Fatalf("patch = %+v border=%+v", cleared, cleared.Border)
	}
	for _, tc := range []struct {
		format CellFormat
		want   string
	}{
		{format: cleared, want: "bold=false,italic=false,strike=false,underline=false,border=dotted:left"},
		{format: CellFormat{}, want: ""},
		{
			format: CellFormat{
				Number: "0.00", Bold: true, Italic: true, Strike: true, Underline: true, Fill: "#ff0000", Color: "#00ff00",
				Align: "left", VAlign: "top", Wrap: "clip", Border: &CellBorder{Style: "wavy", Edges: "top", Color: "#000000"},
			},
			want: "number=0.00,bold,italic,strike,underline,fill=#ff0000,color=#00ff00,align=left,valign=top,wrap=clip,border=thin:top:#000000",
		},
	} {
		if got := tc.format.String(); got != tc.want {
			t.Fatalf("String = %q, want %q", got, tc.want)
		}
	}
}

// tabGridHost is a host document that is not a checkout.
type tabGridHost struct{}

func (tabGridHost) Path() string      { return "/host" }
func (tabGridHost) MediaType() string { return mimeGoogleSpreadsheet }

// TestGrid_refusesBadAddresses: grid reads refuse an unknown sheet, a bad or
// inverted A1, a range where a cell is wanted and rows out of range. A sheet
// key may be empty only in a one-sheet grid. A range past the sheet reads
// empty cells, A1 parses lowercase, and a grid refuses SetText and SetLine.
// Blocks of an untitled sheet use the slug "sheet", a header preview is cut
// at a cell edge, and an empty sheet has no preview.
func TestGrid_refusesBadAddresses(t *testing.T) {
	t.Parallel()
	d := twoSheetBudget()
	g := mustGrid(t, d)
	solo, err := NewTabularDocument("/Solo", "", []Sheet{
		{Title: "!!!", Cells: [][]Cell{{{Input: strings.Repeat("h", headerPreview)}, {Input: "next"}}}},
		{Title: "Empty"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	one, err := NewTabularDocument("/One", "", []Sheet{{Title: "Only", Cells: [][]Cell{{{Input: "v"}}}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		read    func() (string, error)
		want    string
		wantErr error
		errMsg  string
	}{
		{name: "rows of an unknown sheet", read: func() (string, error) { _, _, err := g.ReadRows("nope", 1, 2); return "", err }, wantErr: ErrUnknownBlock},
		{name: "rows from line zero", read: func() (string, error) { _, _, err := g.ReadRows("Budget", 0, 2); return "", err }, wantErr: ErrLineOutOfRange},
		{name: "rows past the end", read: func() (string, error) { _, _, err := g.ReadRows("Budget", 9, 10); return "", err }, wantErr: ErrLineOutOfRange},
		{name: "a cell with a bad A1", read: func() (string, error) { return g.ReadCell("Budget", "1A") }, wantErr: ErrInvalidA1},
		{name: "a cell from a range", read: func() (string, error) { c, err := g.Cell("Budget", "A1:B2"); return c.Display(), err }, errMsg: "not a single cell"},
		{name: "a range with a bad A1", read: func() (string, error) { return g.ReadRangeTSV("Budget", "") }, wantErr: ErrInvalidA1},
		{name: "a range with a bad end", read: func() (string, error) { return g.ReadRangeTSV("Budget", "A1:") }, wantErr: ErrInvalidA1},
		{name: "an inverted range", read: func() (string, error) { return g.ReadRangeTSV("Budget", "B2:A1") }, wantErr: ErrInvalidA1},
		{name: "a range past the sheet", read: func() (string, error) { return g.ReadRangeTSV("Q1 Budget", "a1:c1") }, want: "Hello\tWorld\t"},
		{name: "an empty key in a grid of three sheets", read: func() (string, error) { return g.ReadCell("", "A1") }, wantErr: ErrUnknownBlock},
		{name: "an empty key in a grid of one sheet", read: func() (string, error) { return mustGrid(t, one).ReadCell("", "A1") }, want: "v"},
		{name: "SetText", read: func() (string, error) { return "", d.SetText("x") }, wantErr: ErrProjected},
		{name: "SetLine", read: func() (string, error) { return "", d.SetLine(1, "x") }, wantErr: ErrProjected},
		{name: "the blocks of untitled and empty sheets", read: func() (string, error) {
			var parts []string
			for _, b := range solo.Blocks() {
				parts = append(parts, b.ID+":"+b.Text)
			}
			return strings.Join(parts, "|"), nil
		}, want: "sheet:rows=1 cols=2 | " + strings.Repeat("h", headerPreview) + "…|empty:rows=0 cols=0"},
		{name: "a column before A", read: func() (string, error) { return FormatA1(3, 0), nil }, want: "3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.read()

			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			case tc.errMsg != "":
				if err == nil || !strings.Contains(err.Error(), tc.errMsg) {
					t.Fatalf("err = %v, want %q", err, tc.errMsg)
				}
			case err != nil || got != tc.want:
				t.Fatalf("got %q err=%v, want %q", got, err, tc.want)
			}
		})
	}
}

// tabZipPart is one stored part of a zip that tabZip writes. A zero size or
// crc is the true one.
type tabZipPart struct {
	name, body string
	method     uint16
	size       uint64
	crc        uint32
}

func tabZip(t *testing.T, parts ...tabZipPart) []byte {
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
		if _, err := w.Write([]byte(p.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestTabularCodec_decodeAndEncodeEdges: a tabular codec refuses a missing
// normalizer, bytes past MaxReadFileBytes and a host document that is not a
// grid. With no media type it takes its first type (or text/plain when it
// has none). The Sheets codec refuses bytes that are not UTF-8, a broken
// zip, a zip part that is not UTF-8, has an unknown method, fails its
// checksum or is too large, and a zip with no HTML. A page with no body or
// title is one sheet "Sheet1", and an empty table is no table.
func TestTabularCodec_decodeAndEncodeEdges(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sheets := TabularCodec{Types: []string{mimeGoogleSpreadsheet}, Normalizer: SheetsHTML{}}
	page := "<table><tr><td>A</td></tr></table>"
	decode := func(c TabularCodec, mediaType string, data []byte) func() (string, error) {
		return func() (string, error) {
			doc, err := c.Decode(ctx, "/x", mediaType, data)
			if err != nil {
				return "", err
			}
			g, _ := AsGrid(doc)
			return doc.MediaType() + ":" + g.Sheets()[0].Title, nil
		}
	}
	cases := []struct {
		name    string
		run     func() (string, error)
		want    string
		wantErr error
		errMsg  string
	}{
		{name: "decode with no normalizer", run: decode(TabularCodec{}, "", nil), errMsg: "normalizer required"},
		{name: "encode with no normalizer", run: func() (string, error) { _, err := (TabularCodec{}).Encode(ctx, twoSheetBudget()); return "", err }, errMsg: "normalizer required"},
		{name: "encode a host document", run: func() (string, error) { _, err := sheets.Encode(ctx, tabGridHost{}); return "", err }, wantErr: ErrNotTextual},
		{name: "bytes past the cap", run: decode(sheets, "", make([]byte, MaxReadFileBytes+1)), wantErr: ErrTooLarge},
		{name: "no media type", run: decode(sheets, "", []byte(page)), want: mimeGoogleSpreadsheet + ":Sheet1"},
		{name: "no media type and no types", run: decode(TabularCodec{Normalizer: SheetsHTML{}}, "", []byte(page)), want: "text/plain:Sheet1"},
		{name: "bytes that are not UTF-8", run: decode(sheets, "", []byte("\xff<table>")), wantErr: ErrInvalidUTF8},
		{name: "a broken zip", run: decode(sheets, "", []byte("PK\x03\x04 broken")), errMsg: "zip"},
		{name: "a zip part that is not UTF-8", run: decode(sheets, "", tabZip(t, tabZipPart{name: "a.html", body: "\xff"})), wantErr: ErrInvalidUTF8},
		{name: "a zip part with an unknown method", run: decode(sheets, "", tabZip(t, tabZipPart{name: "a.html", body: page, method: 99})), wantErr: zip.ErrAlgorithm},
		{name: "a zip part that fails its checksum", run: decode(sheets, "", tabZip(t, tabZipPart{name: "a.html", body: page, crc: 1})), wantErr: zip.ErrChecksum},
		{name: "a zip part that is too large", run: decode(sheets, "", tabZip(t, tabZipPart{name: "a.html", body: page, size: MaxReadFileBytes + 1})), wantErr: ErrTooLarge},
		{name: "a zip with no HTML", run: decode(sheets, "", tabZip(t, tabZipPart{name: "a.txt", body: "x"})), wantErr: ErrNotSupported},
		{name: "a page with no body or title", run: decode(sheets, "", []byte("<frameset></frameset>")), want: mimeGoogleSpreadsheet + ":Sheet1"},
		{name: "an empty table", run: decode(sheets, "", []byte("<title>T</title><table></table>")), want: mimeGoogleSpreadsheet + ":T"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.run()

			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			case tc.errMsg != "":
				if err == nil || !strings.Contains(err.Error(), tc.errMsg) {
					t.Fatalf("err = %v, want %q", err, tc.errMsg)
				}
			case err != nil || got != tc.want:
				t.Fatalf("got %q err=%v, want %q", got, err, tc.want)
			}
		})
	}
}
