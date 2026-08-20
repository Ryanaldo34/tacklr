package vfs

import (
	"errors"
	"strings"
	"testing"
)

func twoSheetBudget() *TabularDocument {
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
	b.sheets[0].Cells[2][0].Value = "999"
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
	if _, ok := FindBlock(blocks, "Budget"); !ok {
		t.Fatal("FindBlock title")
	}
	if _, ok := FindBlock(blocks, "1"); !ok {
		t.Fatal("FindBlock sheet_id")
	}
	if _, ok := FindBlock(blocks, "budget"); !ok {
		t.Fatal("FindBlock slug")
	}

	html := d.Text()
	if !strings.Contains(html, `<h1 class="tacklr-tab">Budget</h1>`) ||
		!strings.Contains(html, `<h1 class="tacklr-tab">Notes</h1>`) ||
		!strings.Contains(html, "<table>") ||
		!strings.Contains(html, "<td>2026-01-01</td>") ||
		!strings.Contains(html, "=A1+1") {
		t.Fatalf("projection = %s", html)
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

func TestSheetsCodec_projectionRoundTrip(t *testing.T) {
	src := twoSheetBudget()
	doc, err := (SheetsCodec{}).Decode(t.Context(), "/contracts/Budget", mimeGoogleSpreadsheet, []byte(src.Text()))
	if err != nil {
		t.Fatal(err)
	}
	td := doc.(*TabularDocument)
	if len(td.Sheets()) != 2 || td.Sheets()[0].Title != "Budget" || td.Sheets()[1].Title != "Notes" {
		t.Fatalf("sheets = %+v", td.Sheets())
	}
	if td.Sheets()[0].Cells[2][0].Input != "=A1+1" {
		t.Fatalf("formula cell = %+v", td.Sheets()[0].Cells[2][0])
	}
}
