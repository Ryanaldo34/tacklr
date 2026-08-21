package vfs

import (
	"errors"
	"os"
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
	d, err := NewTabularDocument("/contracts/Budget", mimeGoogleSpreadsheet, []Sheet{
		{
			ID: "1", Title: "Budget", Index: 0,
			Cells: [][]Cell{
				{{Input: "Date"}, {Input: "Amount"}, {Input: "Note"}},
				{{Input: "2026-01-01"}, {Input: "42"}, {Input: "ok"}},
				{{Input: "=A1+1"}, {Value: "43"}},
			},
		},
		{
			ID: "2", Title: "Notes", Index: 1,
			Cells: [][]Cell{
				{{Input: "Hello"}, {Input: "World"}},
			},
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
	if len(blocks) != 2 || blocks[0].Kind != BlockKindSheet || blocks[0].ID != "budget" || blocks[1].ID != "notes" {
		t.Fatalf("blocks = %+v", blocks)
	}
	if _, ok := mustGrid(t, d).findSheet("Budget"); !ok {
		t.Fatal("findSheet title")
	}
	if _, ok := FindBlock(blocks, "budget"); !ok {
		t.Fatal("FindBlock slug")
	}
	if err := d.SetText("nope"); !errors.Is(err, ErrProjected) {
		t.Fatalf("SetText: %v", err)
	}
	g := mustGrid(t, d)
	got, err := g.ReadCell("Budget", "B2")
	if err != nil || got != "42" {
		t.Fatalf("B2 = %q err=%v", got, err)
	}
	if formula, err := g.ReadCell("budget", "A3"); err != nil || formula != "=A1+1" {
		t.Fatalf("formula = %q err=%v", formula, err)
	}
	if rect, err := g.ReadRangeTSV("Notes", "A1:B1"); err != nil || rect != "Hello\tWorld" {
		t.Fatalf("range = %q err=%v", rect, err)
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

	inline := []byte(`<html><body><table>
<tr><td>Date</td><td>Amount</td></tr>
<tr><td>2026-01-01</td><td>42</td></tr>
<tr><td>=A1+1</td><td></td></tr>
</table></body></html>`)
	doc, err = (SheetsCodec{}).Decode(t.Context(), "/contracts/Budget", mimeGoogleSpreadsheet, inline)
	if err != nil {
		t.Fatal(err)
	}
	td, ok = AsGrid(doc)
	if !ok || td.Sheets()[0].Cells[2][0].Input != "=A1+1" {
		t.Fatalf("html = %+v ok=%v", doc, ok)
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
	blocks := []Block{{Kind: BlockKindSheet, Text: "X\tY", Style: StyleMeta{Attributes: map[string]string{"title": "Sheet1"}}}}
	fromBlocks, err := (SheetsCodec{}).Create("/Ledger", mimeGoogleSpreadsheet, Mutation{Blocks: blocks})
	if err != nil {
		t.Fatal(err)
	}
	g, ok = AsGrid(fromBlocks)
	if !ok || g.Sheets()[0].Title != "Sheet1" || g.Sheets()[0].Cells[0][0].Input != "X" {
		t.Fatalf("blocks lift = %+v ok=%v", fromBlocks, ok)
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
	if _, err := (DocsCodec{}).Create("/Spec", mimeGoogleDocument, Mutation{Content: &html}); err == nil {
		t.Fatal("HTML content")
	}
}

func TestParseCellFormat_stringRoundTrip(t *testing.T) {
	src := CellFormat{
		Number: "$#,##0.00", Bold: true, Italic: true, Underline: true,
		Fill: "#ffcc00", Color: "#003366", Align: "right", VAlign: "middle", Wrap: "wrap",
		Border: &CellBorder{Style: "thin", Edges: "bottom"},
	}
	got, err := ParseCellFormat(src.String())
	if err != nil {
		t.Fatal(err)
	}
	if got.Number != src.Number || !got.Bold || !got.Italic || !got.Underline ||
		got.Fill != src.Fill || got.Color != src.Color || got.Align != src.Align ||
		got.VAlign != src.VAlign || got.Wrap != src.Wrap ||
		got.Border == nil || got.Border.Style != "thin" || got.Border.Edges != "bottom" {
		t.Fatalf("parse = %+v border=%+v from %q", got, got.Border, src.String())
	}
	cleared, err := ParseCellFormat("bold=false")
	if err != nil || cleared.Bold || !cleared.has(fmtBold) {
		t.Fatalf("bold=false = %+v err=%v", cleared, err)
	}
}
