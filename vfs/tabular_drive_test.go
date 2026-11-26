package vfs_test

import (
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

// TestDrive_tabbedDocIDs: a Google Doc with two tabs prefixes the block IDs
// of a tab with the slug of its title (none when the slug is empty). A
// table reads as its rows and columns.
func TestDrive_tabbedDocIDs(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	api := tabDrive()
	api.Add("root-a", vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}, nil)
	api.SeedDoc("doc1", []vfs.DocsSpan{
		{TabID: "t.a", Kind: "paragraph", Text: "Alpha"},
		{TabID: "t.b", Kind: "paragraph", Text: "Beta"},
		{TabID: "t.b", Kind: "table", Text: "A\tB\nC\tD"},
	}, []vfs.DocTab{{ID: "t.a", Title: "!!!", Index: 0}, {ID: "t.b", Title: "Two", Index: 1}})
	ms := mountDrive(t, api, true)

	doc, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)

	if err != nil {
		t.Fatal(err)
	}
	blocks := doc.(vfs.Structured).Blocks()
	if ids := blockIDs(blocks); strings.Join(ids, ",") != "p-1,two/p-1,two/table-1" {
		t.Fatalf("ids = %v, want p-1,two/p-1,two/table-1", ids)
	}
	if table := blocks[2].Style.Attributes; table["rows"] != "2" || table["cols"] != "2" {
		t.Fatalf("table attributes = %v, want 2 rows and 2 columns", table)
	}
}
