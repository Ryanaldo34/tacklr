package vfs_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

// A read-only Drive mount keeps the HTML export of a Google Doc per
// modification time: a second read does not export again, a Doc that
// Drive reports as modified exports again, and the oldest of more than
// 32 exports is dropped first.
func TestDrive_exportsAreKeptPerModificationTime(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.New()
	fx.Add("", vfs.DriveMeta{ID: "root-a", Name: "Contracts", MimeType: "application/vnd.google-apps.folder"}, nil)
	for i := range 34 {
		id := fmt.Sprintf("d%d", i)
		fx.Add("root-a", vfs.DriveMeta{ID: id, Name: id, MimeType: "application/vnd.google-apps.document"}, nil)
		fx.Nodes[id].Export = []byte(fmt.Sprintf("<p>v1 %d</p>", i))
	}
	ms := mountDrive(t, fx, false)
	read := func(name string) string {
		t.Helper()
		doc, err := ms.Route(ctx, "/workspace/contracts/"+name).ReadText(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return doc.(vfs.Structured).Blocks()[0].Text
	}

	read("d0")
	again := read("d0")
	exportsAfterTwoReads := fx.Exports
	fx.Nodes["d0"].Export = []byte("<p>v2</p>")
	fx.Nodes["d0"].Meta.ModTime = fx.Nodes["d0"].Meta.ModTime.Add(time.Second)
	modified := read("d0")
	for i := 1; i < 33; i++ {
		read(fmt.Sprintf("d%d", i))
	}
	exportsBefore := fx.Exports
	read("d1")
	kept := fx.Exports
	read("d0")
	read("d33")
	read("d0")

	if again != "v1 0" || exportsAfterTwoReads != 1 || modified != "v2" {
		t.Fatalf("second read = %q after %d exports, modified read = %q", again, exportsAfterTwoReads, modified)
	}
	if kept != exportsBefore || fx.Exports != exportsBefore+2 {
		t.Fatalf("exports = %d -> %d -> %d, want d1 kept and d0 and d33 exported", exportsBefore, kept, fx.Exports)
	}
}

// The version of a Google file is its Drive version, else its
// modification time; a file with neither has no version.
func TestDrive_googleFileVersions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	mod := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fx.Nodes["doc1"].Meta.Version, fx.Nodes["doc1"].Meta.ModTime = "", mod
	fx.Nodes["sheet1"].Meta.Version, fx.Nodes["sheet1"].Meta.ModTime = "", time.Time{}
	ms := mountDrive(t, fx, false)

	doc, docErr := ms.Route(ctx, "/workspace/contracts/Spec").Stat(ctx)
	sheet, sheetErr := ms.Route(ctx, "/workspace/contracts/Budget").Stat(ctx)

	if docErr != nil || doc.Version != mod.Format(time.RFC3339Nano) || sheetErr != nil || sheet.Version != "" {
		t.Fatalf("Doc version = %q err=%v, Sheet version = %q err=%v", doc.Version, docErr, sheet.Version, sheetErr)
	}
}
