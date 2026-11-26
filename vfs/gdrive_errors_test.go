package vfs_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"runtime/debug"
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestDrive_aFailedDriveCallEndsTheOperation: a Drive call that the server
// refuses ends the read or write that made it with the error of the call,
// and a reply that the connection cuts short ends it with an unexpected EOF.
func TestDrive_aFailedDriveCallEndsTheOperation(t *testing.T) {
	t.Parallel()
	refused := func(call, id string) testdrive.Fault {
		return testdrive.Fault{Call: call, ID: id, Status: http.StatusForbidden}
	}
	cut := func(call, id string) testdrive.Fault { return testdrive.Fault{Call: call, ID: id, Cut: true} }
	write := func(ctx context.Context, r vfs.Route) error { return r.WriteFile(ctx, []byte("x")) }
	for name, c := range map[string]struct {
		writable bool
		fault    testdrive.Fault
		path     string
		op       func(context.Context, vfs.Route) error
		want     error
	}{
		"a refused listing":           {false, refused("files.list", "root-a"), "", routeOps["list"], vfs.ErrPermission},
		"a refused file":              {false, refused("files.media", "note"), "acme/note.md", routeOps["text"], vfs.ErrPermission},
		"a refused Doc export":        {false, refused("files.export", "doc1"), "Spec", routeOps["text"], vfs.ErrPermission},
		"a refused Sheet export":      {false, refused("files.export", "sheet1"), "Budget", routeOps["text"], vfs.ErrPermission},
		"a refused root of a stat":    {false, refused("files.get", "root-a"), "nda.pdf", routeOps["stat"], vfs.ErrPermission},
		"a refused root of a folder":  {true, refused("files.get", "root-a"), "new", routeOps["mkdir"], vfs.ErrPermission},
		"a cut file":                  {false, cut("files.media", "note"), "acme/note.md", routeOps["text"], io.ErrUnexpectedEOF},
		"a cut export":                {false, cut("files.export", "doc1"), "Spec", routeOps["text"], io.ErrUnexpectedEOF},
		"a refused Docs read":         {true, refused("documents.get", "doc1"), "Spec", routeOps["text"], vfs.ErrPermission},
		"a refused Sheets read":       {true, refused("spreadsheets.get", "sheet1"), "Budget", routeOps["text"], vfs.ErrPermission},
		"a refused trash":             {true, refused("files.update", "nda"), "nda.pdf", routeOps["remove"], vfs.ErrPermission},
		"a refused upload":            {true, refused("files.update", "nda"), "nda.pdf", write, vfs.ErrPermission},
		"a refused new file":          {true, refused("files.create", ""), "new.txt", write, vfs.ErrPermission},
		"a refused root of a write":   {true, refused("files.get", "root-a"), "nda.pdf", write, vfs.ErrPermission},
		"a refused parent of a write": {true, testdrive.Fault{Call: "files.get", ID: "root-a", Skip: 1, Status: http.StatusForbidden}, "new.txt", write, vfs.ErrPermission},
		"a refused new folder":        {true, refused("files.create", ""), "new", routeOps["mkdir"], vfs.ErrPermission},
		"a refused lookup of a path":  {true, refused("files.list", "sub"), "acme/x", routeOps["mkdir"], vfs.ErrPermission},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fx := testdrive.Tree()
			ms := mountDrive(t, fx, c.writable)
			fx.Fail(c.fault)

			err := c.op(ctx, ms.Route(ctx, "/workspace/contracts/"+c.path))

			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

// TestDrive_contentAtTheSizeCapsDoesNotLoad: content at a size cap does not
// load: a file over the read cap, a file that grew after Drive reported its
// size, a Doc export over the export cap or that unzips to more than the
// read cap, and a sheet with more cells than the cap. The test is serial:
// each step holds tens of megabytes, and in parallel with the other tests
// they take the race run out of memory.
func TestDrive_contentAtTheSizeCapsDoesNotLoad(t *testing.T) {
	ctx := t.Context()
	readText := func(ms *vfs.MountSession, name string) error {
		_, err := ms.Route(ctx, "/workspace/contracts/"+name).ReadText(ctx)
		return err
	}
	for name, step := range map[string]func() error{
		"a file over the cap": func() error {
			return readText(mountDrive(t, testdrive.Tree(), false), "huge.bin")
		},
		"a file over the cap read as bytes": func() error {
			_, err := mountDrive(t, testdrive.Tree(), false).Route(ctx, "/workspace/contracts/huge.bin").ReadFile(ctx)
			return err
		},
		"a file that grew": func() error {
			fx := testdrive.Tree()
			fx.Add("root-a", vfs.DriveMeta{ID: "grown", Name: "grown.txt", MimeType: "text/plain", Size: 1}, nil)
			fx.Edit(t, "grown", func(n *testdrive.Node) { n.Body = make([]byte, vfs.MaxReadFileBytes+1) })
			return readText(mountDrive(t, fx, true), "grown.txt")
		},
		"an export over the export cap": func() error {
			fx := testdrive.Tree()
			export(t, fx, "doc1", bytes.Repeat([]byte("a"), vfs.MaxDocsExportBytes+2))
			return readText(mountDrive(t, fx, false), "Spec")
		},
		"an export that unzips over the read cap": func() error {
			fx := testdrive.Tree()
			var zipped bytes.Buffer
			zw := zip.NewWriter(&zipped)
			w, err := zw.Create("Spec.html")
			if err != nil {
				t.Fatal(err)
			}
			chunk := bytes.Repeat([]byte("a"), 1<<20)
			for range vfs.MaxReadFileBytes/len(chunk) + 1 {
				if _, err := w.Write(chunk); err != nil {
					t.Fatal(err)
				}
			}
			if err := zw.Close(); err != nil {
				t.Fatal(err)
			}
			export(t, fx, "doc1", zipped.Bytes())
			return readText(mountDrive(t, fx, false), "Spec")
		},
		"a sheet over the cell cap": func() error {
			fx := testdrive.Tree()
			row := slices.Repeat([]vfs.Cell{{Input: "x", Value: "x"}}, vfs.MaxSheetCells+1)
			fx.SeedSheet("sheet1", vfs.SheetsSnapshot{Sheets: []vfs.Sheet{{ID: "1", Title: "Budget", Cells: [][]vfs.Cell{row}}}})
			return readText(mountDrive(t, fx, true), "Budget")
		},
	} {
		debug.FreeOSMemory()
		if err := step(); !errors.Is(err, vfs.ErrTooLarge) {
			t.Fatalf("%s: err = %v, want ErrTooLarge", name, err)
		}
	}
}
