package vfs

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
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

func TestGrid_projectionA1AndFingerprint(t *testing.T) {
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

func TestSheetsHTML_decodesZipAndTable(t *testing.T) {
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
<tr><td>=A1+1</td><td></td></tr>
</table><table><tr><td>Second</td></tr></table></body></html>`)
	doc, err = (SheetsCodec{}).Decode(t.Context(), "/contracts/Budget", mimeGoogleSpreadsheet, inline)
	if err != nil {
		t.Fatal(err)
	}
	td, ok = AsGrid(doc)
	if !ok || td.Sheets()[0].Cells[2][0].Input != "=A1+1" || td.Sheets()[0].Title != "Inline" ||
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

func TestCreator_liftsGridAndRich(t *testing.T) {
	csv := "A,B\n1,2"
	grid, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Content: &csv})
	if err != nil {
		t.Fatal(err)
	}
	g, ok := AsGrid(grid)
	if !ok || len(g.Sheets()) != 1 || g.Sheets()[0].Cells[1][1].Input != "2" {
		t.Fatalf("csv lift = %+v ok=%v", grid, ok)
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
	}
	fromBlocks, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Blocks: blocks})
	if err != nil {
		t.Fatal(err)
	}
	g, ok = AsGrid(fromBlocks)
	if !ok || len(g.Sheets()) != 3 || g.Sheets()[0].Title != "Sheet1" || g.Sheets()[0].Cells[0][0].Input != "X" ||
		g.Sheets()[1].Title != "extra" {
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

func TestParseCellFormat_stringRoundTrip(t *testing.T) {
	src := CellFormat{
		Number: "$#,##0.00", Bold: true, Italic: true, Strike: true, Underline: true,
		Fill: "#ffcc00", Color: "#003366", Align: "right", VAlign: "middle", Wrap: "wrap",
		Border: &CellBorder{Style: "thin", Edges: "bottom", Color: "#ff0000"},
	}
	got, err := ParseCellFormat(src.String())
	if err != nil {
		t.Fatal(err)
	}
	if got.Number != src.Number || !got.Bold || !got.Italic || !got.Strike || !got.Underline ||
		got.Fill != src.Fill || got.Color != src.Color || got.Align != src.Align ||
		got.VAlign != src.VAlign || got.Wrap != src.Wrap ||
		got.Border == nil || got.Border.Style != "thin" || got.Border.Edges != "bottom" ||
		got.Border.Color != "#ff0000" {
		t.Fatalf("parse = %+v border=%+v from %q", got, got.Border, src.String())
	}
	bag, err := ParseCellFormat("number=#,##0.00,bold=false,italic=false,strike=false,underline=false,border=medium:top:#00ff00")
	if err != nil || bag.Bold || !bag.has(fmtBold) || bag.Number != "#,##0.00" ||
		bag.Border == nil || bag.Border.Style != "medium" || bag.Border.Edges != "top" ||
		bag.Border.Color != "#00ff00" {
		t.Fatalf("flags = %+v border=%+v err=%v", bag, bag.Border, err)
	}
	if HexColor("nope") != "" || ExcelARGB("nope") != "FF000000" {
		t.Fatalf("invalid color: %q %q", HexColor("nope"), ExcelARGB("nope"))
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
	if s := cleared.String(); !strings.Contains(s, "bold=false") || !strings.Contains(s, "border=dotted:left") {
		t.Fatalf("string = %q", s)
	}
}
