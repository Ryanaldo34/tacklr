package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

type memSheets struct {
	snaps   map[string]vfs.SheetsSnapshot
	batches []vfs.SheetsValuesBatch
	fail    error
}

func newMemSheets(id string, sheets []vfs.Sheet, named []vfs.NamedRange) *memSheets {
	return &memSheets{
		snaps: map[string]vfs.SheetsSnapshot{
			id: {SpreadsheetID: id, Sheets: sheets, Named: named},
		},
	}
}

func (m *memSheets) Get(ctx context.Context, spreadsheetID string) (vfs.SheetsSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return vfs.SheetsSnapshot{}, err
	}
	if m.fail != nil {
		return vfs.SheetsSnapshot{}, m.fail
	}
	s, ok := m.snaps[spreadsheetID]
	if !ok {
		s = vfs.SheetsSnapshot{
			SpreadsheetID: spreadsheetID,
			Sheets:        []vfs.Sheet{{ID: "0", Title: "Sheet1"}},
		}
		if m.snaps == nil {
			m.snaps = map[string]vfs.SheetsSnapshot{}
		}
		m.snaps[spreadsheetID] = s
		return s, nil
	}
	return cloneSheetsSnap(s), nil
}

func (m *memSheets) BatchUpdateValues(ctx context.Context, spreadsheetID string, req vfs.SheetsValuesBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.fail != nil {
		return m.fail
	}
	m.batches = append(m.batches, req)
	s := m.snaps[spreadsheetID]
	if s.SpreadsheetID == "" {
		s.SpreadsheetID = spreadsheetID
	}
	applySheetsValues(&s, req)
	if m.snaps == nil {
		m.snaps = map[string]vfs.SheetsSnapshot{}
	}
	m.snaps[spreadsheetID] = s
	return nil
}

func cloneSheetsSnap(s vfs.SheetsSnapshot) vfs.SheetsSnapshot {
	out := s
	out.Sheets = append([]vfs.Sheet(nil), s.Sheets...)
	out.Named = append([]vfs.NamedRange(nil), s.Named...)
	return out
}

func applySheetsValues(s *vfs.SheetsSnapshot, req vfs.SheetsValuesBatch) {
	for _, vr := range req.Data {
		title, a1 := vfs.SplitSheetAddr(vr.Range)
		idx := -1
		for i, sh := range s.Sheets {
			if sh.Title == title || sh.ID == title {
				idx = i
				break
			}
		}
		if idx < 0 && len(s.Sheets) == 1 {
			idx = 0
		}
		if idx < 0 {
			s.Sheets = append(s.Sheets, vfs.Sheet{Title: title})
			idx = len(s.Sheets) - 1
		}
		r1, c1 := 1, 1
		if a1 != "" {
			rr, cc, _, _, err := vfs.ParseA1(strings.Split(a1, ":")[0])
			if err == nil {
				r1, c1 = rr, cc
			}
		}
		sh := s.Sheets[idx]
		needR := r1 - 1 + len(vr.Values)
		needC := c1 - 1
		for _, row := range vr.Values {
			if c1-1+len(row) > needC {
				needC = c1 - 1 + len(row)
			}
		}
		for len(sh.Cells) < needR {
			sh.Cells = append(sh.Cells, nil)
		}
		for r, row := range vr.Values {
			rr := r1 - 1 + r
			for len(sh.Cells[rr]) < needC {
				sh.Cells[rr] = append(sh.Cells[rr], vfs.Cell{})
			}
			for c, val := range row {
				cell := sh.Cells[rr][c1-1+c]
				cell.Input = val
				if !strings.HasPrefix(val, "=") {
					cell.Value = val
				}
				sh.Cells[rr][c1-1+c] = cell
			}
		}
		sh.Rows = len(sh.Cells)
		if sh.Rows > 0 {
			sh.Cols = len(sh.Cells[0])
			for _, row := range sh.Cells {
				if len(row) > sh.Cols {
					sh.Cols = len(row)
				}
			}
		}
		s.Sheets[idx] = sh
	}
}

func budgetSheets() []vfs.Sheet {
	return []vfs.Sheet{
		{
			ID: "1", Title: "Budget", Index: 0,
			Rows: 3, Cols: 3,
			Cells: [][]vfs.Cell{
				{{Input: "Date", Value: "Date"}, {Input: "Amount", Value: "Amount"}, {Input: "Note", Value: "Note"}},
				{{Input: "2026-01-01", Value: "2026-01-01"}, {Input: "42", Value: "42"}, {Input: "ok", Value: "ok"}},
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
	td, ok := doc.(*vfs.TabularDocument)
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
	html := td.Text()
	if !strings.Contains(html, `class="tacklr-tab"`) || !strings.Contains(html, "<table>") {
		t.Fatalf("projection = %s", html)
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
	if err != nil || !strings.Contains(string(got), `class="tacklr-tab"`) {
		t.Fatalf("FUSE cat = %q err=%v", got, err)
	}
}

func TestDrive_sheetOverlayAndCreate(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	sheets := newMemSheets("sheet1", budgetSheets(), []vfs.NamedRange{{Name: "Total", SheetID: "1", A1: "B2"}})
	api.add("root-a", vfs.DriveMeta{ID: "merged1", Name: "Merged", MimeType: "application/vnd.google-apps.spreadsheet"}, nil)
	sheets.snaps["merged1"] = vfs.SheetsSnapshot{
		SpreadsheetID: "merged1",
		Sheets: []vfs.Sheet{
			vfs.WithMerge(vfs.Sheet{
				ID: "1", Title: "Merged", Rows: 2, Cols: 2,
				Cells: [][]vfs.Cell{
					{{Input: "a", Value: "a"}, {Input: "b", Value: "b"}},
					{{Input: "c", Value: "c"}, {Input: "d", Value: "d"}},
				},
			}, 1, 1, 2, 2),
		},
	}
	ms := mountDriveSheets(t, api, nil, sheets, true)

	doc, err := ms.ReadText(ctx, "/contracts/Budget")
	if err != nil {
		t.Fatal(err)
	}
	td := doc.(*vfs.TabularDocument)
	rev := vfs.ContentToken(td)
	_, err = ms.Apply(ctx, "/contracts/Budget", vfs.Mutation{
		Rev: rev, BlockID: "Budget!B2", Body: strPtr("99"),
	})
	if err != nil {
		t.Fatal(err)
	}
	got99, gotFormula := false, false
	for _, batch := range sheets.batches {
		for _, vr := range batch.Data {
			for _, row := range vr.Values {
				for _, c := range row {
					if c == "99" {
						got99 = true
					}
					if c == "=A1+1" {
						gotFormula = true
					}
				}
			}
		}
	}
	if !got99 || !gotFormula {
		t.Fatalf("batch missing USER_ENTERED overlay: %+v", sheets.batches)
	}

	mergedDoc, err := ms.ReadText(ctx, "/contracts/Merged")
	if err != nil {
		t.Fatal(err)
	}
	_, err = ms.Apply(ctx, "/contracts/Merged", vfs.Mutation{
		Rev: vfs.ContentToken(mergedDoc), BlockID: "Merged!B2", Body: strPtr("x"),
	})
	if !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("merge write: %v", err)
	}
	still, err := ms.ReadText(ctx, "/contracts/Merged")
	if err != nil {
		t.Fatal(err)
	}
	md := still.(*vfs.TabularDocument)
	if v, _ := md.ReadCell("Merged", "B2"); v != "d" {
		t.Fatalf("merge mutated B2: %q", v)
	}
	if v, _ := md.ReadCell("Merged", "A1"); v != "a" {
		t.Fatalf("merge mutated A1: %q", v)
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
	sheets := newMemSheets("sheet1", []vfs.Sheet{{ID: "1", Title: "Budget", Cells: [][]vfs.Cell{row}}}, nil)
	ms := mountDriveSheets(t, api, nil, sheets, true)
	if _, err := ms.ReadText(ctx, "/contracts/Budget"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("checkout cap: %v", err)
	}
}

func strPtr(s string) *string { return &s }
