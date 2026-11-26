package vfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
)

// mountDrive mounts folder root-a of api at /workspace/contracts with a
// user binding.
func mountDrive(t *testing.T, api *testdrive.FX, writable bool) *vfs.MountSession {
	t.Helper()
	return mountDriveWith(t, testdrive.Open(t, api, nil), mount.Binding{
		Provider: "gdrive", Writable: writable,
		Params: map[string]string{mount.ParamFolderID: "root-a"},
		Auth:   mount.Credential{Token: "t"},
	})
}

// mountDriveWith mounts open at /workspace/contracts with the binding b.
func mountDriveWith(t *testing.T, open vfs.Open, b mount.Binding) *vfs.MountSession {
	t.Helper()
	ms, err := mountDriveErr(t, open, b)
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func mountDriveErr(t *testing.T, open vfs.Open, b mount.Binding) (*vfs.MountSession, error) {
	t.Helper()
	if b.Params == nil {
		b.Params = map[string]string{}
	}
	b.Params[mount.ParamName] = "contracts"
	return vfs.Tree(vfs.At("contracts", open))(t.Context(), "s", vfs.Request{Bindings: []mount.Binding{b}})
}

func blockHasText(doc vfs.Textual, want string) bool {
	s, ok := doc.(vfs.Structured)
	if !ok {
		return false
	}
	for _, b := range s.Blocks() {
		if b.Text == want {
			return true
		}
	}
	return false
}

// TestProviders_panicWithoutTheirAPIClient: building a Drive or Graph
// provider with no API client, or a Docs client with no token holder, is a
// programming error.
func TestProviders_panicWithoutTheirAPIClient(t *testing.T) {
	t.Parallel()
	for name, build := range map[string]func(){
		"drive": func() { _ = vfs.Drive(nil, nil) },
		"graph": func() { _ = vfs.Graph(nil, nil, "") },
		"docs":  func() { _ = vfs.NewGoogleDocs(nil, "", nil) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Fatal("want panic")
				}
			}()
			build()
		})
	}
}

// TestDrive_aFolderIDMustNameAFolderThatResolves: a binding mounts My
// Drive with no folder ID, and does not mount a folder ID that names a
// file, nothing, or a shortcut to a file that is gone. A shortcut to itself
// and a path below a file are not found.
func TestDrive_aFolderIDMustNameAFolderThatResolves(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	fx.Add("", vfs.DriveMeta{ID: "root", Name: "My Drive", MimeType: "application/vnd.google-apps.folder"}, nil)
	fx.Add("root-a", vfs.DriveMeta{ID: "dangle", Name: "dangle", MimeType: "application/vnd.google-apps.shortcut", TargetID: "gone"}, nil)
	fx.Add("root-a", vfs.DriveMeta{ID: "loop", Name: "loop", MimeType: "application/vnd.google-apps.shortcut", TargetID: "loop"}, nil)
	open := testdrive.Open(t, fx, nil)
	at := func(folderID string) error {
		_, err := mountDriveErr(t, open, mount.Binding{Params: map[string]string{mount.ParamFolderID: folderID}})
		return err
	}
	ms := mountDrive(t, fx, false)

	myDrive := at("")
	file, missing, dangling := at("nda"), at("missing"), at("dangle")
	_, loopErr := ms.Route(ctx, "/workspace/contracts/loop").Stat(ctx)
	_, belowErr := ms.Route(ctx, "/workspace/contracts/nda.pdf/x").Stat(ctx)

	if myDrive != nil {
		t.Errorf("mount of My Drive = %v", myDrive)
	}
	if file == nil || !strings.Contains(file.Error(), "not a folder") {
		t.Errorf("mount of a file = %v, want not a folder", file)
	}
	for name, err := range map[string]error{"a missing folder": missing, "a shortcut to a gone file": dangling, "a shortcut to itself": loopErr, "a path below a file": belowErr} {
		if !errors.Is(err, vfs.ErrNotExist) {
			t.Errorf("%s = %v, want ErrNotExist", name, err)
		}
	}
}

// TestDrive_anAmbiguousNameIsRefused: a name that two files in a folder
// share does not read or remove.
func TestDrive_anAmbiguousNameIsRefused(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms := mountDrive(t, testdrive.Tree(), true)

	_, readErr := ms.Route(ctx, "/workspace/contracts/dup.txt").ReadFile(ctx)
	removeErr := ms.Route(ctx, "/workspace/contracts/dup.txt").Remove(ctx)

	if !errors.Is(readErr, vfs.ErrAmbiguous) || !errors.Is(removeErr, vfs.ErrAmbiguous) {
		t.Fatalf("read = %v, remove = %v, want ErrAmbiguous", readErr, removeErr)
	}
}

// TestDrive_aTokenDriveRefusesRefreshesOrExpires: a token about to expire
// refreshes before the call, a token Drive refuses refreshes once, and a
// refused token with no refresh, or no token at all, is expired.
func TestDrive_aTokenDriveRefusesRefreshesOrExpires(t *testing.T) {
	t.Parallel()
	fresh := func(context.Context) (mount.Credential, error) {
		return mount.Credential{Token: "fresh", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	for name, c := range map[string]struct {
		token   mount.Credential
		refused bool
		refresh func(context.Context) (mount.Credential, error)
		want    error
	}{
		"a token about to expire refreshes first": {token: mount.Credential{Token: "t", ExpiresAt: time.Now().Add(10 * time.Second)}, refresh: fresh},
		"a refused token refreshes once":          {token: mount.Credential{Token: "t"}, refused: true, refresh: fresh},
		"a refused token with no refresh expires": {token: mount.Credential{Token: "t"}, refused: true, want: vfs.ErrAuthExpired},
		"no token expires":                        {want: vfs.ErrAuthExpired},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fx := testdrive.Tree()
			holder := vfs.NewTokenHolder(mount.Credential{Token: "t"})
			ms := mountDriveWith(t, testdrive.Open(t, fx, holder), mount.Binding{Params: map[string]string{mount.ParamFolderID: "root-a"}})
			holder.Set(c.token)
			holder.SetRefresh(c.refresh)
			if c.refused {
				fx.Fail(testdrive.Fault{Call: "files.get", ID: "root-a", Status: http.StatusUnauthorized})
			}

			got, err := ms.Route(ctx, "/workspace/contracts/nda.pdf").ReadFile(ctx)

			if !errors.Is(err, c.want) {
				t.Fatalf("read = %q %v, want %v", got, err, c.want)
			}
			if c.want == nil && (string(got) != "%PDF" || holder.Current().Token != "fresh") {
				t.Fatalf("read = %q with token %q, want %%PDF with the fresh token", got, holder.Current().Token)
			}
		})
	}
}

// TestDrive_aDriveAPIWithNoFindResolvesByListing: a host DriveAPI that
// embeds the Google client has no Find, so a path resolves by listing
// whole folders. With no holder, the provider takes the binding's token.
func TestDrive_aDriveAPIWithNoFindResolvesByListing(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	srv := testhttp.New(t, testdrive.Tree())
	noFind := struct{ vfs.DriveAPI }{vfs.NewGoogleDrive(vfs.NewTokenHolder(mount.Credential{Token: "t"}), srv.URL+"/", srv.Client())}
	ms := mountDriveWith(t, vfs.Drive(noFind, nil), mount.Binding{
		Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamFolderID: "root-a"},
	})

	got, err := ms.Route(ctx, "/workspace/contracts/acme/note.md").ReadFile(ctx)

	if err != nil || string(got) != "# hi\n\n" {
		t.Fatalf("read = %q %v", got, err)
	}
}

// TestDrive_withNoHolderARefusedTokenIsExpired: a provider with no token
// holder and a binding with no token cannot refresh the token Drive
// refuses, so it does not mount.
func TestDrive_withNoHolderARefusedTokenIsExpired(t *testing.T) {
	t.Parallel()
	srv := testhttp.New(t, testdrive.Tree())
	api := vfs.NewGoogleDrive(vfs.NewTokenHolder(mount.Credential{}), srv.URL+"/", srv.Client())

	_, err := mountDriveErr(t, vfs.Drive(api, nil), mount.Binding{Params: map[string]string{mount.ParamFolderID: "root-a"}})

	if !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("mount = %v, want ErrAuthExpired", err)
	}
}

// TestDrive_aGoogleDocReadsTabsHeadingsMarksListsImagesAndTables: a
// writable mount reads a Google Doc from the Docs API: every tab and
// child tab (a tab with no title is named by its id), headings at levels 1
// to 6, inline marks, ordered and bulleted lists, images with and without
// properties, and tables. A document with a body and no tabs reads, and a
// document with neither has no blocks.
func TestDrive_aGoogleDocReadsTabsHeadingsMarksListsImagesAndTables(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	fx.Add("root-a", vfs.DriveMeta{ID: "legacy", Name: "Legacy", MimeType: "application/vnd.google-apps.document"}, nil)
	fx.Add("root-a", vfs.DriveMeta{ID: "empty", Name: "Empty", MimeType: "application/vnd.google-apps.document"}, nil)
	tabs, err := os.ReadFile("testdata/docs_get_tabs.json")
	if err != nil {
		t.Fatal(err)
	}
	fx.SeedDocJSON("doc1", json.RawMessage(tabs))
	fx.SeedDocJSON("empty", json.RawMessage(`{}`))
	fx.SeedDocJSON("legacy", json.RawMessage(`{"body": {"content": [{"startIndex": 1, "endIndex": 5, "paragraph": {"elements": [{"textRun": {"content": "Old\n"}}]}}]}}`))
	ms := mountDrive(t, fx, true)

	doc, docErr := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	legacy, legacyErr := ms.Route(ctx, "/workspace/contracts/Legacy").ReadText(ctx)
	empty, emptyErr := ms.Route(ctx, "/workspace/contracts/Empty").ReadText(ctx)

	if err := errors.Join(docErr, legacyErr, emptyErr); err != nil {
		t.Fatal(err)
	}
	type got struct{ kind, text, attrs string }
	var blocks []got
	for _, b := range doc.(vfs.Structured).Blocks() {
		attrs := fmt.Sprint(b.Style.Level)
		for _, k := range []string{"tab_id", "list_type", "object_id", "content_uri"} {
			if v := b.Style.Attributes[k]; v != "" {
				attrs += " " + k + "=" + v
			}
		}
		blocks = append(blocks, got{b.Kind, b.PlainText(), attrs})
	}
	want := []got{
		{vfs.BlockKindHeading, "Intro", "1 tab_id=t.a"},
		{vfs.BlockKindHeading, "Deep", "6 tab_id=t.a"},
		{vfs.BlockKindHeading, "Low", "1 tab_id=t.a"},
		{vfs.BlockKindParagraph, "See x i and Maya", "0 tab_id=t.a"},
		{vfs.BlockKindListItem, "a", "2 tab_id=t.a list_type=ol"},
		{vfs.BlockKindListItem, "b", "1 tab_id=t.a list_type=ul"},
		{vfs.BlockKindImage, "Chart", "0 tab_id=t.a object_id=kix.pic content_uri=https://img"},
		{vfs.BlockKindParagraph, "Fig", "0 tab_id=t.a"},
		{vfs.BlockKindImage, "", "0 tab_id=t.a object_id=kix.bare"},
		{vfs.BlockKindTable, "\n\tCell\t", "0 tab_id=t.a"},
		{vfs.BlockKindParagraph, "Untitled", "0 tab_id=t.c"},
		{vfs.BlockKindParagraph, "More", "0 tab_id=t.b"},
	}
	if !slices.Equal(blocks, want) {
		t.Fatalf("blocks =\n%v\nwant\n%v", blocks, want)
	}
	for _, part := range []string{
		"<strong>x</strong>", "<s><em>i</em></s>", `<a href="mailto:maya">Maya</a>`,
		`<h1 class="tacklr-tab">Appendix</h1>`, `<h1 class="tacklr-tab">t.c</h1>`, "<h6>Deep</h6>", "<ol>\n<li>a</li>",
	} {
		if !strings.Contains(doc.Text(), part) {
			t.Fatalf("projection has no %q:\n%s", part, doc.Text())
		}
	}
	if !blockHasText(legacy, "Old") || len(empty.(vfs.Structured).Blocks()) != 0 {
		t.Fatalf("legacy body = %+v, empty document = %+v", legacy.(vfs.Structured).Blocks(), empty.(vfs.Structured).Blocks())
	}
}

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// export sets the HTML export of the Drive file id.
func export(t *testing.T, fx *testdrive.FX, id string, data []byte) {
	t.Helper()
	fx.Edit(t, id, func(n *testdrive.Node) { n.Export = data })
}

// TestDrive_aReadOnlyMountReadsGoogleFilesFromTheirExport: a read-only
// mount stats a Google Doc and Sheet with their Google media type and no
// size, refuses raw reads of them, and reads a Doc from its zip export or
// from plain HTML. An export that is neither does not read, and Slides has
// no codec.
func TestDrive_aReadOnlyMountReadsGoogleFilesFromTheirExport(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	export(t, fx, "doc1", testdata(t, "drive_export_spec.zip"))
	fx.Add("root-a", vfs.DriveMeta{ID: "plain", Name: "Plain", MimeType: "application/vnd.google-apps.document"}, nil)
	export(t, fx, "plain", []byte("<html><body><p>plain</p></body></html>"))
	fx.Add("root-a", vfs.DriveMeta{ID: "junk", Name: "Junk", MimeType: "application/vnd.google-apps.document"}, nil)
	export(t, fx, "junk", []byte("junk"))
	ms := mountDrive(t, fx, false)
	route := func(name string) vfs.Route { return ms.Route(ctx, "/workspace/contracts/"+name) }

	docStat, docStatErr := route("Spec").Stat(ctx)
	sheetStat, sheetStatErr := route("Budget").Stat(ctx)
	_, docRawErr := route("Spec").ReadFile(ctx)
	_, sheetRawErr := route("Budget").ReadFile(ctx)
	doc, docErr := route("Spec").ReadText(ctx)
	plain, plainErr := route("Plain").ReadText(ctx)
	_, junkErr := route("Junk").ReadText(ctx)
	_, slidesErr := route("Deck").ReadText(ctx)

	if docStatErr != nil || docStat.MediaType != "application/vnd.google-apps.document" || docStat.Size != 0 {
		t.Errorf("stat of a Doc = %+v %v", docStat, docStatErr)
	}
	if sheetStatErr != nil || sheetStat.MediaType != "application/vnd.google-apps.spreadsheet" || sheetStat.Size != 0 {
		t.Errorf("stat of a Sheet = %+v %v", sheetStat, sheetStatErr)
	}
	if !errors.Is(docRawErr, vfs.ErrNotSupported) || !errors.Is(sheetRawErr, vfs.ErrNotSupported) {
		t.Errorf("raw reads = %v, %v, want ErrNotSupported", docRawErr, sheetRawErr)
	}
	if docErr != nil || !strings.Contains(doc.Text(), "<h1>Spec</h1>") || doc.MediaType() != "application/vnd.google-apps.document" {
		t.Errorf("Doc from its zip export = %v %v", doc, docErr)
	}
	if plainErr != nil || !blockHasText(plain, "plain") {
		t.Errorf("Doc from plain HTML = %v %v", plain, plainErr)
	}
	if !errors.Is(junkErr, vfs.ErrNotSupported) || !errors.Is(slidesErr, vfs.ErrNoCodec) {
		t.Errorf("junk export = %v, Slides = %v", junkErr, slidesErr)
	}
}

// TestDrive_aGoogleDocRefusesARawWriteAndTrashes: a writable mount refuses
// raw bytes over a Google Doc, which keeps its blocks, and a removed Doc
// is gone.
func TestDrive_aGoogleDocRefusesARawWriteAndTrashes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	fx.SeedDoc("doc1", []vfs.DocsSpan{{Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"}, {Kind: "paragraph", Text: "Hello"}}, nil)
	ms := mountDrive(t, fx, true)
	spec := ms.Route(ctx, "/workspace/contracts/Spec")

	writeErr := spec.WriteFile(ctx, []byte("x"))
	kept, keptErr := spec.ReadText(ctx)
	removeErr := spec.Remove(ctx)
	_, goneErr := spec.ReadText(ctx)

	if !errors.Is(writeErr, vfs.ErrNotSupported) || keptErr != nil || !blockHasText(kept, "Hello") {
		t.Fatalf("raw write = %v, then read = %v %v, want it refused and the Doc kept", writeErr, kept, keptErr)
	}
	if removeErr != nil || !errors.Is(goneErr, vfs.ErrNotExist) {
		t.Fatalf("remove = %v, then read = %v, want it gone", removeErr, goneErr)
	}
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

// TestDrive_aReadOnlyMountReadsAGoogleSheetFromItsExport: each sheet with
// its inputs and values, and a projection with no HTML.
func TestDrive_aReadOnlyMountReadsAGoogleSheetFromItsExport(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	export(t, fx, "sheet1", testdata(t, "drive_export_budget.zip"))
	ms := mountDrive(t, fx, false)

	doc, err := ms.Route(ctx, "/workspace/contracts/Budget").ReadText(ctx)

	if err != nil {
		t.Fatal(err)
	}
	td, ok := vfs.AsGrid(doc)
	if !ok || doc.Path() != "/workspace/contracts/Budget" || len(td.Sheets()) != 2 {
		t.Fatalf("doc = %T at %s", doc, doc.Path())
	}
	if b2 := td.Sheets()[0].Cells[1][1]; td.Sheets()[0].Title != "Budget" || b2.Input != "42" || b2.Value != "42" {
		t.Fatalf("budget B2 = %+v", b2)
	}
	if a1 := td.Sheets()[1].Cells[0][0]; td.Sheets()[1].Title != "Notes" || a1.Input != "Hello" || a1.Value != "Hello" {
		t.Fatalf("notes A1 = %+v", a1)
	}
	if text := doc.Text(); !strings.Contains(text, "# Sheet: Budget") || !strings.Contains(text, "42") ||
		strings.Contains(text, "<table>") || strings.Contains(text, "bold") {
		t.Fatalf("projection = %s", text)
	}
}

// TestDrive_aWritableMountReadsAGoogleSheetFromTheSheetsAPI: values,
// formulas, formats and named ranges, and a raw write is refused.
func TestDrive_aWritableMountReadsAGoogleSheetFromTheSheetsAPI(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	fx.SeedSheet("sheet1", vfs.SheetsSnapshot{Sheets: budgetSheets(), Named: []vfs.NamedRange{{Name: "Total", SheetID: "1", A1: "B2"}}})
	ms := mountDrive(t, fx, true)

	doc, err := ms.Route(ctx, "/workspace/contracts/Budget").ReadText(ctx)
	writeErr := ms.Route(ctx, "/workspace/contracts/Budget").WriteFile(ctx, []byte("x"))

	if err != nil {
		t.Fatal(err)
	}
	td, ok := vfs.AsGrid(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	budget := td.Sheets()[0]
	if b2 := budget.Cells[1][1]; b2.Display() != "42" || !b2.Format.Bold || b2.Format.Number != "$#,##0.00" {
		t.Fatalf("B2 = %+v", b2)
	}
	if a3 := budget.Cells[2][0]; a3.Display() != "=A1+1" || a3.Value != "43" {
		t.Fatalf("A3 = %+v", a3)
	}
	if named := td.NamedRanges(); len(named) != 1 || named[0].Name != "Total" || named[0].SheetID != "1" {
		t.Fatalf("named = %+v", named)
	}
	if !errors.Is(writeErr, vfs.ErrNotSupported) {
		t.Fatalf("raw write to a Google Sheet = %v, want ErrNotSupported", writeErr)
	}
}
