package vfs

import (
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

func TestTabularDocument_fingerprintIgnoresFormattedValue(t *testing.T) {
	a := twoSheetBudget()
	b := twoSheetBudget()
	mustGrid(t, b).sheets[0].Cells[2][0].Value = "999"
	if a.ContentFingerprint() != b.ContentFingerprint() {
		t.Fatal("formula formatted value must not change rev")
	}
}

func TestTabularDocument_outlineProjectionAndA1(t *testing.T) {
	d := twoSheetBudget()
	blocks := d.Blocks()
	if len(blocks) != 2 {
		t.Fatalf("blocks = %d", len(blocks))
	}
	if blocks[0].Kind != BlockKindSheet || blocks[0].ID != "budget" {
		t.Fatalf("sheet0 = %+v", blocks[0])
	}
	if blocks[1].ID != "notes" {
		t.Fatalf("sheet1 id = %q", blocks[1].ID)
	}
	if blocks[0].Style.Attributes["sheet_id"] != "1" || blocks[0].Style.Attributes["title"] != "Budget" {
		t.Fatalf("attrs = %+v", blocks[0].Style.Attributes)
	}
	if !strings.HasPrefix(blocks[0].Text, "rows=3 cols=3 | Date\tAmount\tNote") {
		t.Fatalf("preview = %q", blocks[0].Text)
	}
	if _, ok := mustGrid(t, d).findSheet("Budget"); !ok {
		t.Fatal("findSheet title")
	}
	if _, ok := mustGrid(t, d).findSheet("1"); !ok {
		t.Fatal("findSheet sheet_id")
	}
	if _, ok := FindBlock(blocks, "budget"); !ok {
		t.Fatal("FindBlock slug")
	}

	mustGrid(t, d).sheets[0].Cells[1][1].Format = CellFormat{Number: "$#,##0.00", Bold: true, Fill: "#ffcc00"}
	mustGrid(t, d).reproject()
	text := d.Text()
	if !strings.Contains(text, "# Sheet: Budget") ||
		!strings.Contains(text, "# Sheet: Notes") ||
		!strings.Contains(text, "42") ||
		!strings.Contains(text, "=A1+1") {
		t.Fatalf("projection = %s", text)
	}

	got, err := d.ReadCell("Budget", "B2")
	if err != nil || got != "42" {
		t.Fatalf("B2 = %q err=%v", got, err)
	}
	formula, err := d.ReadCell("budget", "A3")
	if err != nil || formula != "=A1+1" {
		t.Fatalf("formula = %q err=%v", formula, err)
	}
	rect, err := d.ReadRangeTSV("Notes", "A1:B1")
	if err != nil || rect != "Hello\tWorld" {
		t.Fatalf("range = %q err=%v", rect, err)
	}
	if !IsProjected(mimeGoogleSpreadsheet) {
		t.Fatal("SheetsCodec must be projected")
	}
}

func TestTabularDocument_projectedMutators(t *testing.T) {
	d := twoSheetBudget()
	if err := d.SetText("nope"); !errors.Is(err, ErrProjected) {
		t.Fatalf("SetText: %v", err)
	}
	if err := d.SetLine(1, "x"); !errors.Is(err, ErrProjected) {
		t.Fatalf("SetLine: %v", err)
	}
	if err := d.ReplaceLines(1, 2, []string{"x"}); !errors.Is(err, ErrProjected) {
		t.Fatalf("ReplaceLines: %v", err)
	}
	if strings.Contains(d.Text(), "nope") {
		t.Fatal("projection mutated")
	}
}

func TestSheetsCodec_htmlZipDecode(t *testing.T) {
	raw, err := os.ReadFile("testdata/drive_export_budget.zip")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := (SheetsCodec{}).Decode(t.Context(), "/contracts/Budget", mimeGoogleSpreadsheet, raw)
	if err != nil {
		t.Fatal(err)
	}
	td, ok := AsGrid(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	if len(td.Sheets()) != 2 || td.Sheets()[0].Title != "Budget" || td.Sheets()[1].Title != "Notes" {
		t.Fatalf("sheets = %+v", td.Sheets())
	}
	b2 := td.Sheets()[0].Cells[1][1]
	if b2.Input != "42" || b2.Value != "42" || !b2.Format.IsZero() {
		t.Fatalf("RO ZIP B2 = %+v", b2)
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
	if !ok {
		t.Fatalf("type %T", doc)
	}
	if len(td.Sheets()) != 1 || td.Sheets()[0].Cells[2][0].Input != "=A1+1" {
		t.Fatalf("table HTML = %+v", td.Sheets())
	}
	if !td.Sheets()[0].Cells[1][1].Format.IsZero() {
		t.Fatalf("RO HTML must be value-only: %+v", td.Sheets()[0].Cells[1][1])
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
