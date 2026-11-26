package vfs_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

// readSpec mounts fx read-only, reads the Doc Spec once, and returns a
// read of its first block.
func readSpec(t *testing.T, fx *testdrive.FX) func() (string, error) {
	t.Helper()
	ctx := t.Context()
	export(t, fx, "doc1", []byte("<p>v1</p>"))
	ms := mountDrive(t, fx, false)
	read := func() (string, error) {
		doc, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
		if err != nil {
			return "", err
		}
		return doc.(vfs.Structured).Blocks()[0].Text, nil
	}
	if _, err := read(); err != nil {
		t.Fatal(err)
	}
	return read
}

// TestDrive_aReadOnlyMountReadsAnUnchangedDocWithoutAnExport: a second read
// of a Google Doc that Drive reports unchanged reads while exports fail.
func TestDrive_aReadOnlyMountReadsAnUnchangedDocWithoutAnExport(t *testing.T) {
	t.Parallel()
	fx := testdrive.Tree()
	read := readSpec(t, fx)
	fx.Fail(testdrive.Fault{Call: "files.export", ID: "doc1", Status: http.StatusInternalServerError})

	got, err := read()

	if err != nil || got != "v1" {
		t.Fatalf("second read = %q %v, want the kept v1", got, err)
	}
}

// TestDrive_aReadOnlyMountReadsAChangedDocsNewExport: a Doc that Drive
// reports as modified reads its new export.
func TestDrive_aReadOnlyMountReadsAChangedDocsNewExport(t *testing.T) {
	t.Parallel()
	fx := testdrive.Tree()
	read := readSpec(t, fx)
	fx.Edit(t, "doc1", func(n *testdrive.Node) {
		n.Export = []byte("<p>v2</p>")
		n.Meta.ModTime = n.Meta.ModTime.Add(time.Second)
	})

	got, err := read()

	if err != nil || got != "v2" {
		t.Fatalf("read after a change = %q %v, want v2", got, err)
	}
}

// TestDrive_anUploadKeepsTheReadsOfOtherFiles: an upload over a file drops
// what a writable Drive mount keeps for that file only: the Doc read before
// it reads again without a Docs call.
func TestDrive_anUploadKeepsTheReadsOfOtherFiles(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	fx.SeedDoc("doc1", []vfs.DocsSpan{{Kind: "paragraph", Text: "Hello"}}, nil)
	ms := mountDrive(t, fx, true)
	if _, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx); err != nil {
		t.Fatal(err)
	}
	fx.Fail(testdrive.Fault{Call: "documents.get", Status: http.StatusInternalServerError})

	writeErr := ms.Route(ctx, "/workspace/contracts/nda.pdf").WriteFile(ctx, []byte("b"))
	doc, readErr := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)

	if writeErr != nil || readErr != nil || !blockHasText(doc, "Hello") {
		t.Fatalf("write = %v, read = %v", writeErr, readErr)
	}
}

// TestDrive_googleFileVersions: the version of a Google file is its Drive
// version, else its modification time; a file with neither has no version.
func TestDrive_googleFileVersions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	mod := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fx.Edit(t, "doc1", func(n *testdrive.Node) { n.Meta.Version, n.Meta.ModTime = "", mod })
	fx.Edit(t, "sheet1", func(n *testdrive.Node) { n.Meta.Version, n.Meta.ModTime = "", time.Time{} })
	ms := mountDrive(t, fx, false)

	doc, docErr := ms.Route(ctx, "/workspace/contracts/Spec").Stat(ctx)
	sheet, sheetErr := ms.Route(ctx, "/workspace/contracts/Budget").Stat(ctx)

	if docErr != nil || doc.Version != mod.Format(time.RFC3339Nano) || sheetErr != nil || sheet.Version != "" {
		t.Fatalf("Doc version = %q err=%v, Sheet version = %q err=%v", doc.Version, docErr, sheet.Version, sheetErr)
	}
}
