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

	"github.com/ryanaldo34/tacklr/internal/google/sheets"
)

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

// TestGrid_projectsSheetsAsBlocksAndEscapedText: a grid has one block per
// sheet (a long header preview is cut, a repeated title gets a suffix), and
// its text escapes tabs and backslashes in cells.
func TestGrid_projectsSheetsAsBlocksAndEscapedText(t *testing.T) {
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
	if text := d.Text(); !strings.Contains(text, `ok\tline`) || !strings.Contains(text, `a\\b`) || !strings.Contains(text, "=A1+1") {
		t.Fatalf("text = %q, want escaped cells and the formula", text)
	}
	if d.LineCount() != strings.Count(d.Text(), "\n")+1 {
		t.Fatalf("LineCount = %d of %q", d.LineCount(), d.Text())
	}
}

// TestNewTabularDocument_trimsAndCopiesItsSheets: a grid keeps a copy of
// the sheets it is given, trims empty trailing cells, escapes a carriage
// return, and refuses sheets with more cells than MaxSheetCells.
func TestNewTabularDocument_trimsAndCopiesItsSheets(t *testing.T) {
	t.Parallel()
	border := &CellBorder{Style: "thin"}
	sheets := []Sheet{{Title: "Wide", Cells: [][]Cell{
		{{Input: "x\ry", Format: CellFormat{Border: border}}, {}},
	}}}
	wide := []Sheet{{Title: "Huge", Cells: [][]Cell{make([]Cell, MaxSheetCells+1)}}}
	wide[0].Cells[0][MaxSheetCells].Input = "last"

	d, err := NewTabularDocument("/Wide", "", sheets, nil)
	border.Style = "thick"
	_, hugeErr := NewTabularDocument("/Huge", "", wide, nil)

	if err != nil {
		t.Fatal(err)
	}
	g, _ := AsGrid(d)
	sh := g.Sheets()[0]
	if sh.Cols != 1 || len(sh.Cells[0]) != 1 || sh.Cells[0][0].Format.Border.Style != "thin" {
		t.Fatalf("sheet = %+v border=%+v, want one column and its own border", sh, sh.Cells[0][0].Format.Border)
	}
	if !strings.Contains(d.Text(), `x\ry`) {
		t.Fatalf("text = %q, want the carriage return escaped", d.Text())
	}
	if !errors.Is(hugeErr, ErrTooLarge) {
		t.Fatalf("a sheet over the cell cap = %v, want ErrTooLarge", hugeErr)
	}
}

// TestSheetsHTML_decodesZipAndTable: the Sheets codec reads a Drive zip
// export, inline HTML tables (a <br> in a cell is a newline, a short row
// is padded), a page with
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
<tr><td>ragged</td></tr>
</table><table><tr><td>Second</td></tr></table></body></html>`)
	doc, err = (SheetsCodec{}).Decode(t.Context(), "/contracts/Budget", mimeGoogleSpreadsheet, inline)
	if err != nil {
		t.Fatal(err)
	}
	td, ok = AsGrid(doc)
	if !ok || td.Sheets()[0].Cells[2][0].Input != "=A1+1" || td.Sheets()[0].Cells[2][1].Input != "a\nb" || td.Sheets()[0].Title != "Inline" ||
		len(td.Sheets()) < 2 || td.Sheets()[1].Cells[0][0].Input != "Second" || len(td.Sheets()[0].Cells[3]) != 2 {
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

// TestCellFormat_colorsAndString: colors parse from hex (bad hex is none),
// and String lists each set field and each field a Sheets read set off (a
// zero format is empty, an unknown border style is thin).
func TestCellFormat_colorsAndString(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"nope":     "",
		"zzzzzz":   "",
		"#AABBCC":  "#aabbcc",
		"FF112233": "#112233",
	} {
		if got := HexColor(in); got != want {
			t.Fatalf("color of %q = %q, want %q", in, got, want)
		}
	}
	normalized := CellFormat{Wrap: "WRAP", Align: "LEFT", Border: &CellBorder{}}
	normalized.Normalize()
	odd := CellFormat{Wrap: "sideways", VAlign: "TOP", Border: &CellBorder{Style: "Thick", Edges: "top"}}
	odd.Normalize()
	bare := CellFormat{Align: "middle"}
	bare.Normalize()
	off := cellFormatFromSheets(&sheets.CellFormat{
		TextFormat: &sheets.TextFormat{},
		Borders:    &sheets.Borders{Left: &sheets.Border{Style: "DOTTED"}},
	})
	for _, tc := range []struct {
		format CellFormat
		want   string
	}{
		{format: normalized, want: "align=left,wrap=wrap"},
		{format: odd, want: "valign=top,border=thick:top"},
		{format: bare, want: ""},
		{format: off, want: "bold=false,italic=false,strike=false,underline=false,border=dotted:left"},
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

// TestGrid_blocksOfUntitledAndEmptySheets: blocks of an untitled sheet use
// the slug "sheet", a header preview is cut at a cell edge, and an empty
// sheet has no preview.
func TestGrid_blocksOfUntitledAndEmptySheets(t *testing.T) {
	t.Parallel()
	solo, err := NewTabularDocument("/Solo", "", []Sheet{
		{Title: "!!!", Cells: [][]Cell{{{Input: strings.Repeat("h", headerPreview)}, {Input: "next"}}}},
		{Title: "Empty"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var parts []string
	for _, b := range solo.Blocks() {
		parts = append(parts, b.ID+":"+b.Text)
	}

	if got, want := strings.Join(parts, "|"), "sheet:rows=1 cols=2 | "+strings.Repeat("h", headerPreview)+"…|empty:rows=0 cols=0"; got != want {
		t.Fatalf("blocks = %q, want %q", got, want)
	}
}

// TestParseA1_readsCellsAndRangesAndRefusesBadAddresses: A1 parses a cell
// and a lowercase range, and refuses a bad, empty, open-ended or inverted
// address. A column before A formats as the row alone.
func TestParseA1_readsCellsAndRangesAndRefusesBadAddresses(t *testing.T) {
	t.Parallel()
	for in, want := range map[string][4]int{"B2": {2, 2, 2, 2}, "a1:c3": {1, 1, 3, 3}} {
		r1, c1, r2, c2, err := ParseA1(in)
		if got := [4]int{r1, c1, r2, c2}; err != nil || got != want {
			t.Fatalf("ParseA1(%q) = %v err=%v, want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"1A", "", "A1:", "B2:A1", "A0"} {
		if _, _, _, _, err := ParseA1(in); !errors.Is(err, ErrInvalidA1) {
			t.Fatalf("ParseA1(%q) = %v, want ErrInvalidA1", in, err)
		}
	}
	if got := FormatA1(3, 0); got != "3" {
		t.Fatalf("FormatA1(3, 0) = %q", got)
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

// TestTabularCodec_decodeEdges: a tabular codec refuses a missing
// normalizer and bytes past MaxReadFileBytes. With no media type it takes its first type (or text/plain when it
// has none). The Sheets codec refuses bytes that are not UTF-8, a broken
// zip, a zip part that is not UTF-8, has an unknown method, fails its
// checksum or is too large, and a zip with no HTML. A page with no body or
// title is one sheet "Sheet1", and an empty table is no table.
func TestTabularCodec_decodeEdges(t *testing.T) {
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
