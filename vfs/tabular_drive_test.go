package vfs_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

// tabDrive is a Drive fixture with only the folder that mountDrive mounts.
// It leaves out the large file of testdrive.Tree, so these parallel tests
// stay small under the race detector.
func tabDrive() *testdrive.FX {
	api := testdrive.New()
	api.Add("", vfs.DriveMeta{ID: "root-a", Name: "Contracts", MimeType: "application/vnd.google-apps.folder"}, nil)
	return api
}

// TestDrive_sheetWriteKeepsFormatBlocksAndQuotedTitles: a cell write to a
// Google Sheet with a quoted title sends the values of each sheet (a
// one-cell sheet too) and the format of a block of equal cells, so the
// workbook keeps every value and format.
func TestDrive_sheetWriteKeepsFormatBlocksAndQuotedTitles(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	api := tabDrive()
	boxed := vfs.CellFormat{Bold: true, Border: &vfs.CellBorder{Style: "thin", Edges: "bottom"}}
	cell := func(v string) vfs.Cell { return vfs.Cell{Input: v, Value: v, Format: boxed} }
	api.Add("root-a", vfs.DriveMeta{ID: "sheet1", Name: "Budget", MimeType: "application/vnd.google-apps.spreadsheet"}, nil)
	api.SeedSheet("sheet1", vfs.SheetsSnapshot{SpreadsheetID: "sheet1", Sheets: []vfs.Sheet{
		{ID: "1", Title: "Q1 Budget", Cells: [][]vfs.Cell{{cell("a"), cell("b")}, {cell("c"), cell("d")}}},
		{ID: "2", Title: "Solo", Cells: [][]vfs.Cell{{{Input: "s", Value: "s"}}}},
	}})
	ms := mountDrive(t, api, true)
	if _, err := ms.Route(ctx, "/workspace/contracts/Budget").ReadText(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := ms.Apply(ctx, "/workspace/contracts/Budget", vfs.Mutation{BlockID: "'Q1 Budget'!B2", Body: richStr("99")})

	if err != nil {
		t.Fatal(err)
	}
	snap := api.SheetSnap("sheet1")
	q1, solo := snap.Sheets[0], snap.Sheets[1]
	if q1.Cells[1][1].Input != "99" || q1.Cells[0][0].Input != "a" || solo.Cells[0][0].Input != "s" {
		t.Fatalf("values = %+v / %+v", q1.Cells, solo.Cells)
	}
	for _, row := range q1.Cells {
		for _, c := range row {
			if !c.Format.Bold || c.Format.Border == nil || c.Format.Border.Edges != "bottom" {
				t.Fatalf("cell %q format = %+v", c.Input, c.Format)
			}
		}
	}
}

// TestDrive_tabbedDocIDsAndTabChecks: a Google Doc with two tabs prefixes
// the block IDs of a tab with the slug of its title (none when the slug is
// empty). A block write with the tab_id of another tab and a blocks write
// with no tab_id fail.
func TestDrive_tabbedDocIDsAndTabChecks(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	api := tabDrive()
	api.Add("root-a", vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}, nil)
	api.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{TabID: "t.a", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.a", StartIndex: 2, EndIndex: 8, Kind: "paragraph", Text: "Alpha"},
		{TabID: "t.b", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.b", StartIndex: 2, EndIndex: 7, Kind: "paragraph", Text: "Beta"},
	}, []vfs.DocTab{{ID: "t.a", Title: "!!!", Index: 0}, {ID: "t.b", Title: "Two", Index: 1}})
	ms := mountDrive(t, api, true)

	doc, readErr := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	_, blockErr := ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{BlockID: "p-1", TabID: "t.b", Body: richStr("x")})
	_, blocksErr := ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Blocks: []vfs.Block{{Kind: vfs.BlockKindParagraph, Text: "x"}}})

	if readErr != nil {
		t.Fatal(readErr)
	}
	if ids := blockIDs(doc.(vfs.Structured).Blocks()); strings.Join(ids, ",") != "p-1,two/p-1" {
		t.Fatalf("ids = %v, want p-1,two/p-1", ids)
	}
	if blockErr == nil || !strings.Contains(blockErr.Error(), `tab_id "t.b" does not match`) {
		t.Fatalf("block write with another tab_id: %v", blockErr)
	}
	if !errors.Is(blocksErr, vfs.ErrTabIDRequired) {
		t.Fatalf("blocks write with no tab_id: %v", blocksErr)
	}
}

// TestDrive_docWriteRetryNeedsTheDoc: a Google Doc write that fails its
// revision check reads the doc again to retry. When the doc is gone by then,
// the write fails with ErrNotExist.
func TestDrive_docWriteRetryNeedsTheDoc(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	api := tabDrive()
	api.Add("root-a", vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}, nil)
	api.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "paragraph", Text: "Hello"},
	}, nil)
	ms := mountDrive(t, api, true)
	if _, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx); err != nil {
		t.Fatal(err)
	}
	api.Fail(testdrive.Fault{
		Call: "documents.batchUpdate", Status: http.StatusBadRequest, Message: "requiredRevisionId must be the latest revision",
		Then: func() { delete(api.Nodes, "doc1") },
	})

	_, err := ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: richStr("<p>new</p>")})

	if !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("retry after the doc is gone: %v", err)
	}
}
