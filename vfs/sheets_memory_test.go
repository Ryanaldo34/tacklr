package vfs

import (
	"context"
	"testing"
)

func TestMemorySheets_seedGetAndBatchUpdates(t *testing.T) {
	ctx := t.Context()
	m := NewMemorySheets()

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.Get(canceled, "x"); err == nil {
		t.Fatal("Get canceled")
	}
	if err := m.BatchUpdate(canceled, "x", SheetsBatch{}); err == nil {
		t.Fatal("BatchUpdate canceled")
	}
	if err := m.BatchUpdateValues(canceled, "x", SheetsValuesBatch{}); err == nil {
		t.Fatal("BatchUpdateValues canceled")
	}

	got, err := m.Get(ctx, "wb")
	if err != nil || got.SpreadsheetID != "wb" || len(got.Sheets) != 1 || got.Sheets[0].Title != "Sheet1" {
		t.Fatalf("default Get = %+v err=%v", got, err)
	}
	got.Sheets[0].Cells = [][]Cell{{{Input: "leak"}}}
	again, err := m.Get(ctx, "wb")
	if err != nil || len(again.Sheets[0].Cells) != 0 {
		t.Fatalf("Get must clone: %+v err=%v", again, err)
	}

	if err := m.BatchUpdateValues(ctx, "wb", SheetsValuesBatch{Data: []SheetsValueRange{
		{Range: "", Values: [][]string{{"bare"}}},
		{Range: "Sheet1!B2", Values: [][]string{{"99"}}},
		{Range: "Sheet1!A3", Values: [][]string{{"=A1+1"}}},
		{Range: "Extra!A1", Values: [][]string{{"x"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := m.BatchUpdate(ctx, "wb", SheetsBatch{Requests: []SheetsRepeatCell{
		{SheetID: 99, StartRow: 0, StartCol: 0, EndRow: 1, EndCol: 1, Format: CellFormat{Bold: true}},
		{SheetID: 0, StartRow: 1, StartCol: 1, EndRow: 2, EndCol: 2, Format: CellFormat{Italic: true, Number: "$#,##0.00"}},
		{SheetID: 0, StartRow: 5, StartCol: 0, EndRow: 6, EndCol: 1, Format: CellFormat{Underline: true}},
	}}); err != nil {
		t.Fatal(err)
	}

	live, err := m.Get(ctx, "wb")
	if err != nil {
		t.Fatal(err)
	}
	if len(live.Sheets) != 2 || live.Sheets[1].Title != "Extra" || live.Sheets[1].Cells[0][0].Value != "x" {
		t.Fatalf("extra sheet = %+v", live.Sheets)
	}
	a1 := live.Sheets[0].Cells[0][0]
	if a1.Value != "bare" || a1.Format.Bold {
		t.Fatalf("unqualified A1 = %+v", a1)
	}
	b2 := live.Sheets[0].Cells[1][1]
	if b2.Value != "99" || !b2.Format.Italic || b2.Format.Number != "$#,##0.00" {
		t.Fatalf("B2 = %+v", b2)
	}
	a3 := live.Sheets[0].Cells[2][0]
	if a3.Input != "=A1+1" || a3.Value != "" {
		t.Fatalf("formula must keep computed Value empty: %+v", a3)
	}
	if live.Sheets[0].Rows < 6 || !live.Sheets[0].Cells[5][0].Format.Underline {
		t.Fatalf("format beyond used range = rows=%d a6=%+v", live.Sheets[0].Rows, live.Sheets[0].Cells[5][0])
	}
	m.Seed("wb", SheetsSnapshot{Sheets: []Sheet{{ID: "7", Title: "Only"}}})
	seeded, err := m.Get(ctx, "wb")
	if err != nil || seeded.SpreadsheetID != "wb" || seeded.Sheets[0].Title != "Only" {
		t.Fatalf("Seed = %+v err=%v", seeded, err)
	}

	if err := m.BatchUpdateValues(ctx, "fresh", SheetsValuesBatch{Data: []SheetsValueRange{
		{Range: "Sheet1!A1", Values: [][]string{{"n"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	fresh, err := m.Get(ctx, "fresh")
	if err != nil || fresh.Sheets[0].Cells[0][0].Value != "n" {
		t.Fatalf("loaded miss = %+v err=%v", fresh, err)
	}
}
