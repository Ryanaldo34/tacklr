package vfs_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func seedSheets(id string, sheets []vfs.Sheet, named []vfs.NamedRange) *vfs.MemorySheets {
	m := vfs.NewMemorySheets()
	m.Seed(id, vfs.SheetsSnapshot{SpreadsheetID: id, Sheets: sheets, Named: named})
	return m
}

func budgetSheets() []vfs.Sheet {
	return []vfs.Sheet{
		{
			ID: "1", Title: "Budget", Index: 0,
			Rows: 3, Cols: 3,
			Cells: [][]vfs.Cell{
				{{Input: "Date", Value: "Date"}, {Input: "Amount", Value: "Amount"}, {Input: "Note", Value: "Note"}},
				{{Input: "2026-01-01", Value: "2026-01-01"}, {Input: "42", Value: "42", Format: vfs.CellFormat{Number: "$#,##0.00", Bold: true}}, {Input: "ok", Value: "ok"}},
				{{Input: "=A1+1", Value: "43"}, {Input: "", Value: ""}, {Input: "", Value: ""}},
			},
		},
		{
			ID: "2", Title: "Notes", Index: 1,
			Rows: 1, Cols: 2,
			Cells: [][]vfs.Cell{
				{{Input: "Hello", Value: "Hello"}, {Input: "World", Value: "World"}},
			},
		},
	}
}

func exportBudgetZip(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/drive_export_budget.zip")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mountDriveSheets(t *testing.T, api *memDrive, docs vfs.DocsAPI, sheets vfs.SheetsAPI, writable bool) *vfs.MountSession {
	t.Helper()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: vfs.Credential{Token: "t"}, Writable: writable,
		Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api, Docs: docs, Sheets: sheets}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(t.Context(), vfs.BindingSpec(vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Writable: writable,
		Params: map[string]string{vfs.ParamFolderID: "root-a"},
	})); err != nil {
		t.Fatal(err)
	}
	return ms
}

func TestDrive_sheetStatAndExportRead(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	api.nodes["sheet1"].export = exportBudgetZip(t)

	ms := mountDrive(t, api, nil, false)
	doc, err := ms.ReadText(ctx, "/contracts/Budget")
	if err != nil {
		t.Fatal(err)
	}
	td, ok := doc.(*vfs.IR)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	if td.Path() != "/contracts/Budget" {
		t.Fatalf("path = %s", td.Path())
	}
	if len(td.Sheets()) != 2 {
		t.Fatalf("sheets = %d", len(td.Sheets()))
	}
	b2 := td.Sheets()[0].Cells[1][1]
	if td.Sheets()[0].Title != "Budget" || b2.Input != "42" || b2.Value != "42" {
		t.Fatalf("budget B2 = %+v sheet=%+v", b2, td.Sheets()[0])
	}
	a1 := td.Sheets()[1].Cells[0][0]
	if td.Sheets()[1].Title != "Notes" || a1.Input != "Hello" || a1.Value != "Hello" {
		t.Fatalf("notes A1 = %+v sheet=%+v", a1, td.Sheets()[1])
	}
	text := td.Text()
	if !strings.Contains(text, "# Sheet: Budget") || !strings.Contains(text, "42") ||
		strings.Contains(text, "<table>") || strings.Contains(text, "bold") {
		t.Fatalf("projection = %s", text)
	}

	if !vfs.FuseAvailable() {
		return
	}
	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	host := filepath.Join(dir, "contracts", "Budget")
	hst, err := os.Stat(host)
	if err != nil {
		t.Fatal(err)
	}
	if hst.Size() != 0 {
		t.Fatalf("FUSE getattr size=%d want 0", hst.Size())
	}
	got, err := os.ReadFile(host)
	if err != nil || !strings.Contains(string(got), "# Sheet: Budget") || !strings.Contains(string(got), "42") {
		t.Fatalf("FUSE cat = %q err=%v", got, err)
	}
}

func TestDrive_sheetOverlayFormatAndMerge(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	sheets := seedSheets("sheet1", budgetSheets(), []vfs.NamedRange{{Name: "Total", SheetID: "1", A1: "B2"}})
	api.add("root-a", vfs.DriveMeta{ID: "merged1", Name: "Merged", MimeType: "application/vnd.google-apps.spreadsheet"}, nil)
	sheets.Seed("merged1", vfs.SheetsSnapshot{
		SpreadsheetID: "merged1",
		Sheets: []vfs.Sheet{
			vfs.WithMerge(vfs.Sheet{
				ID: "1", Title: "Merged", Rows: 2, Cols: 2,
				Cells: [][]vfs.Cell{
					{{Input: "a", Value: "a"}, {Input: "b", Value: "b"}},
					{{Input: "c", Value: "c"}, {Input: "d", Value: "d"}},
				},
			}, 0, 0, 2, 2),
		},
	})
	ms := mountDriveSheets(t, api, nil, sheets, true)

	doc, err := ms.ReadText(ctx, "/contracts/Budget")
	if err != nil {
		t.Fatal(err)
	}
	td := doc.(*vfs.IR)
	if !td.Sheets()[0].Cells[1][1].Format.Bold {
		t.Fatalf("checkout format B2 = %+v", td.Sheets()[0].Cells[1][1])
	}
	_, err = ms.Apply(ctx, "/contracts/Budget", vfs.Mutation{
		Rev: vfs.ContentToken(td), BlockID: "Budget!B2", Body: strPtr("99"),
		Format: &vfs.FormatPatch{Italic: boolPtr(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/contracts/Budget")
	if err != nil {
		t.Fatal(err)
	}
	td = got.(*vfs.IR)
	b2, err := td.Cell("Budget", "B2")
	if err != nil || b2.Display() != "99" || !b2.Format.Italic || !b2.Format.Bold {
		t.Fatalf("overlay B2 = %+v err=%v", b2, err)
	}
	if formula, _ := td.ReadCell("Budget", "A3"); formula != "=A1+1" {
		t.Fatalf("formula = %q", formula)
	}

	mergedDoc, err := ms.ReadText(ctx, "/contracts/Merged")
	if err != nil {
		t.Fatal(err)
	}
	_, err = ms.Apply(ctx, "/contracts/Merged", vfs.Mutation{
		Rev: vfs.ContentToken(mergedDoc), BlockID: "Merged!B2", Body: strPtr("x"),
	})
	if !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("merge slave: %v", err)
	}
	still, err := ms.ReadText(ctx, "/contracts/Merged")
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := still.(*vfs.IR).ReadCell("Merged", "B2"); v != "d" {
		t.Fatalf("slave write landed: %q", v)
	}
	_, err = ms.Apply(ctx, "/contracts/Merged", vfs.Mutation{
		Rev: vfs.ContentToken(still), BlockID: "Merged!A1", Body: strPtr("master"),
	})
	if err != nil {
		t.Fatalf("merge master: %v", err)
	}
	after, err := ms.ReadText(ctx, "/contracts/Merged")
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := after.(*vfs.IR).ReadCell("Merged", "A1"); v != "master" {
		t.Fatalf("merge master A1: %q", v)
	}

	plain := vfs.NewTextDocument("/contracts/Budget", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, plain); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("identity WriteDocument: %v", err)
	}
	if err := ms.WriteFile(ctx, "/contracts/Budget", []byte("x")); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("PutFile native: %v", err)
	}
}

func TestDrive_sheetCheckoutTooLarge(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	row := make([]vfs.Cell, vfs.MaxSheetCells+1)
	for i := range row {
		row[i] = vfs.Cell{Input: "x", Value: "x"}
	}
	ms := mountDriveSheets(t, api, nil, seedSheets("sheet1", []vfs.Sheet{{ID: "1", Title: "Budget", Cells: [][]vfs.Cell{row}}}, nil), true)
	if _, err := ms.ReadText(ctx, "/contracts/Budget"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("checkout cap: %v", err)
	}
}

func strPtr(s string) *string { return &s }

func boolPtr(v bool) *bool { return &v }
