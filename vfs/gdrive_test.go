package vfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// A read-only Drive mount reads files, Google Docs projections and folder
// shortcuts. It refuses writes, an ambiguous name, a file over the read cap,
// file reads of a folder, folder reads of a file and a canceled context. It
// refreshes a token that is about to expire or that Drive refuses, and a
// refused token with no refresh is expired.
func TestMountSession_gdriveReadOnlySession(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	holder := vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	open := testdrive.Open(t, api, holder)
	ms, err := vfs.Tree(
		vfs.At("contracts", open),
		vfs.At("notes", open),
	)(ctx, "sess-gd", vfs.Request{Bindings: []mount.Binding{
		{Provider: mount.ProviderGoogleDrive, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"}, Auth: mount.Credential{Token: "tok"}},
		{Provider: mount.ProviderGoogleDrive, Params: map[string]string{mount.ParamName: "notes", mount.ParamFolderID: "root-b"}, Auth: mount.Credential{Token: "tok"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	route := func(name string) vfs.Route { return ms.Route(ctx, "/workspace/"+name) }

	for _, m := range ms.Specs() {
		if !m.ReadOnly {
			t.Fatalf("member = %+v, want read-only", m)
		}
	}
	if st, err := route("contracts/nda.pdf").Stat(ctx); err != nil || st.MediaType != "application/pdf" || st.Size != 4 {
		t.Fatalf("Stat nda = %+v err=%v", st, err)
	}
	if text, err := route("contracts/acme/note.md").ReadText(ctx); err != nil || !strings.Contains(text.Text(), "# hi") {
		t.Fatalf("ReadText note = %v err=%v", text, err)
	}
	// A shortcut to a folder walks by the shortcut name.
	if got, err := route("notes/alias/note.md").ReadFile(ctx); err != nil || string(got) != "# hi\n\n" {
		t.Fatalf("shortcut walk = %q err=%v", got, err)
	}
	doc, err := route("contracts/acme/note.md").OpenDocument(ctx)
	if err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for name, err := range map[string]error{
		"write":          route("contracts/nda.pdf").WriteFile(ctx, []byte("x")),
		"mkdir":          route("contracts/new").MkdirAll(ctx),
		"remove":         route("contracts/nda.pdf").Remove(ctx),
		"write document": ms.WriteDocument(ctx, doc),
	} {
		if !errors.Is(err, vfs.ErrReadOnly) {
			t.Fatalf("%s: %v, want ErrReadOnly", name, err)
		}
	}
	_, ambiguous := route("contracts/dup.txt").ReadFile(ctx)
	_, huge := route("contracts/huge.bin").ReadFile(ctx)
	_, canceledErr := ms.Route(canceled, "/workspace/contracts/nda.pdf").Stat(canceled)
	_, readDirFile := route("contracts/nda.pdf").ReadDir(ctx)
	_, readFileDir := route("contracts/acme").ReadFile(ctx)
	_, readTextDir := route("contracts/acme").ReadText(ctx)
	if !errors.Is(ambiguous, vfs.ErrAmbiguous) || !errors.Is(huge, vfs.ErrTooLarge) || !errors.Is(canceledErr, context.Canceled) {
		t.Fatalf("ambiguous = %v, huge = %v, canceled = %v", ambiguous, huge, canceledErr)
	}
	if readDirFile == nil || readFileDir == nil || readTextDir == nil {
		t.Fatalf("ReadDir of a file = %v, ReadFile of a folder = %v, ReadText of a folder = %v", readDirFile, readFileDir, readTextDir)
	}

	// A near-expiry token refreshes before the provider request.
	holder.Set(mount.Credential{Token: "expiring", ExpiresAt: time.Now().Add(10 * time.Second)})
	proactiveRefreshes := 0
	holder.SetRefresh(func(context.Context) (mount.Credential, error) {
		proactiveRefreshes++
		return mount.Credential{Token: "proactive", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	if raw, err := route("notes/readme.txt").ReadFile(ctx); err != nil || string(raw) != "hello" || proactiveRefreshes != 1 {
		t.Fatalf("proactive refresh read = %q refreshes=%d err=%v", raw, proactiveRefreshes, err)
	}

	// A 401 and one reactive refresh succeeds.
	api.OnceMedia("readme", http.StatusUnauthorized)
	holder.SetRefresh(func(context.Context) (mount.Credential, error) {
		return mount.Credential{Token: "fresh"}, nil
	})
	if raw, err := route("notes/readme.txt").ReadFile(ctx); err != nil || string(raw) != "hello" {
		t.Fatalf("refresh read = %q err=%v", raw, err)
	}

	// A 401 without a refresh stays expired.
	api.FailMeta(http.StatusUnauthorized)
	holder.SetRefresh(nil)
	if _, err := route("notes/readme.txt").Stat(ctx); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("expired: %v", err)
	}
}

// TestProviders_panicWithoutTheirAPIClient: building a Drive or Graph
// provider with no API client, or a Docs client with no token holder, is a
// programming error.
func TestProviders_panicWithoutTheirAPIClient(t *testing.T) {
	for name, build := range map[string]func(){
		"drive": func() { _ = vfs.Drive(nil, nil) },
		"graph": func() { _ = vfs.Graph(nil, nil, "") },
		"docs":  func() { _ = vfs.NewGoogleDocs(nil, "", nil) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("want panic")
				}
			}()
			build()
		})
	}
}

func TestDrive_validateRejectsFileID(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	open := testdrive.Open(t, api, nil)
	folder, err := open(ctx, "s", mount.Binding{
		Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamFolderID: "root-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := folder.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	file, err := open(ctx, "s", mount.Binding{
		Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamFolderID: "nda"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Validate(ctx); err == nil {
		t.Fatal("file id must fail Validate")
	}
}

// googleError answers every request with status and a Google error
// message.
func googleError(t *testing.T, status int, message string) *httptest.Server {
	return testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": status, "message": message}})
	}))
}

// A Drive or Docs error reply maps to the matching vfs error. A status with
// no vfs error is an error that names the HTTP status. A call to an
// endpoint that does not answer or to a malformed URL fails.
func TestGoogleAPIs_mapErrorRepliesToVFSErrors(t *testing.T) {
	ctx := t.Context()
	holder := vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	for _, c := range []struct {
		status      int
		message     string
		drive, docs error // nil: an error that names the HTTP status
	}{
		{http.StatusUnauthorized, "", vfs.ErrAuthExpired, vfs.ErrAuthExpired},
		{http.StatusNotFound, "", vfs.ErrNotExist, vfs.ErrNotExist},
		{http.StatusForbidden, "", vfs.ErrPermission, vfs.ErrPermission},
		{http.StatusForbidden, "DLP blocked", vfs.ErrPermission, vfs.ErrPermission},
		{http.StatusForbidden, "exportSizeLimitExceeded", vfs.ErrTooLarge, vfs.ErrPermission},
		{http.StatusBadRequest, "requiredRevisionId must be the latest revision", nil, vfs.ErrConflict},
		{http.StatusBadRequest, "not the latest revision", nil, vfs.ErrConflict},
		{http.StatusBadRequest, "invalid document structure", nil, vfs.ErrInvalidWrite},
		{http.StatusTooManyRequests, "slow down", nil, nil},
	} {
		srv := googleError(t, c.status, c.message)
		drive := vfs.NewGoogleDrive(holder, srv.URL+"/", srv.Client())
		docs := vfs.NewGoogleDocs(holder, srv.URL+"/", srv.Client())

		_, metaErr := drive.GetMeta(ctx, "id")
		_, _, exportErr := drive.Export(ctx, "id", "application/zip")
		_, getErr := docs.Get(ctx, "id")
		_, batchErr := docs.BatchUpdate(ctx, "id", vfs.DocsBatch{})

		for _, got := range []struct {
			err, want error
		}{{metaErr, c.drive}, {exportErr, c.drive}, {getErr, c.docs}, {batchErr, c.docs}} {
			named := got.err != nil && strings.Contains(got.err.Error(), fmt.Sprintf("HTTP %d", c.status))
			if (got.want == nil && !named) || (got.want != nil && !errors.Is(got.err, got.want)) {
				t.Fatalf("%d %q: %v, want %v", c.status, c.message, got.err, got.want)
			}
		}
		if c.message != "" && c.status == http.StatusForbidden && !strings.Contains(getErr.Error(), c.message) {
			t.Fatalf("403 %q = %v, want the message", c.message, getErr)
		}
	}

	gone := googleError(t, http.StatusOK, "")
	gone.Close()
	closed := vfs.NewGoogleDrive(holder, gone.URL+"/", nil)
	_, putErr := closed.PutMedia(ctx, "id", "text/plain", strings.NewReader("x"), 1)
	_, listErr := closed.List(ctx, "root")
	_, docsErr := vfs.NewGoogleDocs(holder, gone.URL+"/", nil).Get(ctx, "id")
	_, malformedErr := vfs.NewGoogleDrive(holder, "http://bad host/", nil).GetMeta(ctx, "id")
	if putErr == nil || listErr == nil || docsErr == nil || malformedErr == nil {
		t.Fatalf("closed endpoint: put=%v list=%v docs=%v; malformed URL: %v", putErr, listErr, docsErr, malformedErr)
	}
}

// A writable mount reads a Google Doc from a Docs API server: every tab and
// child tab (a tab with no title is named by its id), headings at levels 1
// to 6, inline marks, ordered and bulleted lists, images with and without
// properties, and tables. A document with a body and no tabs reads, and a
// document with neither has no blocks.
func TestDrive_aGoogleDocReadsTabsHeadingsMarksListsImagesAndTables(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.Add("root-a", vfs.DriveMeta{ID: "legacy", Name: "Legacy", MimeType: "application/vnd.google-apps.document"}, nil)
	api.Add("root-a", vfs.DriveMeta{ID: "empty", Name: "Empty", MimeType: "application/vnd.google-apps.document"}, nil)
	tabs, err := os.ReadFile("testdata/docs_get_tabs.json")
	if err != nil {
		t.Fatal(err)
	}
	documents := map[string][]byte{
		"/v1/documents/doc1":   tabs,
		"/v1/documents/empty":  []byte(`{}`),
		"/v1/documents/legacy": []byte(`{"body": {"content": [{"startIndex": 1, "endIndex": 5, "paragraph": {"elements": [{"textRun": {"content": "Old\n"}}]}}]}}`),
	}
	docsSrv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(documents[r.URL.Path])
	}))
	driveSrv := testhttp.New(t, api)
	holder := vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	open := vfs.DriveWith(
		vfs.NewGoogleDrive(holder, driveSrv.URL+"/", driveSrv.Client()),
		vfs.NewGoogleDocs(holder, docsSrv.URL+"/", docsSrv.Client()), nil, holder)
	ms, err := vfs.Tree(vfs.At("contracts", open))(ctx, "s", vfs.Request{Bindings: []mount.Binding{{
		Writable: true, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	doc, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := ms.Route(ctx, "/workspace/contracts/Legacy").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := ms.Route(ctx, "/workspace/contracts/Empty").ReadText(ctx)
	if err != nil {
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

func exportZip(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/drive_export_spec.zip")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDrive_exportReadHonestStat(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.Nodes["doc1"].Export = exportZip(t)
	api.Add("root-a", vfs.DriveMeta{ID: "plain", Name: "Plain", MimeType: "application/vnd.google-apps.document"}, nil)
	api.Nodes["plain"].Export = []byte("<html><body><p>plain</p></body></html>")
	api.Add("root-a", vfs.DriveMeta{ID: "junk", Name: "Junk", MimeType: "application/vnd.google-apps.document"}, nil)
	api.Nodes["junk"].Export = []byte("junk")

	ms := mountDrive(t, api, false)

	if st, err := ms.Route(ctx, "/workspace/contracts/Spec").
		Stat(ctx); err != nil || st.MediaType != "application/vnd.google-apps.document" || st.Size != 0 {
		t.Fatalf("Stat = %+v err=%v", st, err)
	}

	if sheet, err := ms.Route(ctx, "/workspace/contracts/Budget").
		Stat(ctx); err != nil || sheet.MediaType != "application/vnd.google-apps.spreadsheet" || sheet.Size != 0 {
		t.Fatalf("sheet Stat = %+v err=%v", sheet, err)
	}

	if _, err := ms.Route(ctx, "/workspace/contracts/Budget").
		ReadFile(ctx); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("sheet ReadFile: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadFile(ctx); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("OpenFile native: %v", err)
	}

	doc, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Text(), "<h1>Spec</h1>") {
		t.Fatalf("projection = %s", doc.Text())
	}
	if doc.MediaType() != "application/vnd.google-apps.document" {
		t.Fatalf("mt = %s", doc.MediaType())
	}
	if plain, err := ms.Route(ctx, "/workspace/contracts/Plain").ReadText(ctx); err != nil || !blockHasText(plain, "plain") {
		t.Fatalf("an HTML export that is not a zip: %v", err)
	}
	if _, err := ms.Route(ctx, "/workspace/contracts/Junk").ReadText(ctx); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("an export that is neither zip nor HTML: %v", err)
	}
}

func TestDrive_exportTooLarge(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.Nodes["doc1"].Export = bytesRepeat(vfs.MaxDocsExportBytes + 2)
	ms := mountDrive(t, api, false)

	if _, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
}

func bytesRepeat(n int) []byte {
	return []byte(strings.Repeat("a", n))
}

// A writable Drive mount refuses to remove its root or an ambiguous name,
// and refuses raw or plain-text writes to a Google Doc, which keeps its
// blocks. A removed Google Doc is gone.
func TestDrive_writablePlaintextAndTrash(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	spans := []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	api.SeedDoc("doc1", "R0", spans, nil)
	ms := mountDrive(t, api, true)

	if err := ms.Route(ctx, "/workspace/contracts").
		Remove(ctx); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("root: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/contracts/dup.txt").
		Remove(ctx); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("ambiguous: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/contracts/Spec").
		WriteFile(ctx, []byte("x")); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("native PutFile: %v", err)
	}

	plain := vfs.NewTextDocument("/workspace/contracts/Spec", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, plain); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("identity WriteDocument: %v", err)
	}

	if still, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx); err != nil || !blockHasText(still, "Hello") {
		t.Fatalf("Doc after a refused plain write: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/contracts/Spec").
		Remove(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/contracts/Spec").
		Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed doc Stat: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed doc ReadText: %v", err)
	}
}

func TestDrive_docsWriteCAS(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	spans := []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	api.SeedDoc("doc1", "R0", spans, nil)
	ms := mountDrive(t, api, true)

	empty := vfs.NewRichDocument("/workspace/contracts/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "Nope"},
	})
	if err := ms.WriteDocument(ctx, empty); !errors.Is(err, vfs.ErrConflict) {
		t.Fatalf("empty hint: %v", err)
	}

	orig, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil || !blockHasText(orig, "Hello") {
		t.Fatalf("after empty hint: %v", err)
	}

	rd, ok := vfs.AsRich(orig)
	if !ok {
		t.Fatalf("type %T", orig)
	}
	before := vfs.ContentToken(orig)
	var paraID string
	for _, b := range rd.Blocks() {
		if b.Kind == vfs.BlockKindParagraph {
			paraID = b.ID
		}
	}
	if err := rd.ReplaceBlock(paraID, "World", false); err != nil {
		t.Fatal(err)
	}
	api.SetDocRev("doc1", "R1")
	if err := ms.WriteDocument(ctx, orig); !errors.Is(err, vfs.ErrConflict) {
		t.Fatalf("CAS: %v", err)
	}

	if again, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx); err != nil || !blockHasText(again, "Hello") || vfs.ContentToken(again) != before {
		t.Fatalf("sibling CAS must keep Hello: %v", err)
	}
}

func TestDrive_writeSheetRejected(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.SeedDoc("sheet1", "R0", nil, nil)
	ms := mountDrive(t, api, true)
	if err := ms.WriteDocument(ctx, vfs.NewRichDocument("/workspace/contracts/Budget", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "x"},
	})); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("sheet: %v", err)
	}
}

func TestDrive_writeDocumentEdges(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}, nil)
	ro := mountDrive(t, api, false)
	if err := ro.WriteDocument(ctx, vfs.NewRichDocument("/workspace/contracts/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "x"},
	})); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("readonly: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	ms := mountDrive(t, api, true)
	if err := ms.WriteDocument(canceled, vfs.NewRichDocument("/workspace/contracts/Spec", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "x"},
	})); err == nil {
		t.Fatal("canceled write")
	}
	nested := vfs.NewTextDocument("/workspace/contracts/sub/dir/edge.txt", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, nested); err != nil {
		t.Fatal(err)
	}

	if gotNested, err := ms.Route(ctx, "/workspace/contracts/sub/dir/edge.txt").
		ReadFile(ctx); err != nil || string(gotNested) != "plain" {
		t.Fatalf("nested txt = %q err=%v", gotNested, err)
	}
	txt := vfs.NewTextDocument("/workspace/contracts/edge.txt", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, txt); err != nil {
		t.Fatal(err)
	}

	if got, err := ms.Route(ctx, "/workspace/contracts/edge.txt").
		ReadFile(ctx); err != nil || string(got) != "plain" {
		t.Fatalf("txt = %q err=%v", got, err)
	}

	doc, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := vfs.AsRich(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	rd.SetBlocks([]vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "**Spec**", Style: vfs.StyleMeta{Level: 1}},
		{Kind: vfs.BlockKindParagraph, Text: "See [x](https://e)"},
	})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
}

// Setting the blocks of a Google Doc writes clamped headings, nested
// bulleted and ordered lists and paragraphs, and deletes the image the
// blocks leave out.
func TestDrive_docsSetBlocksNestedAndOmitImage(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	spans := []vfs.DocsSpan{
		{TabID: "t.abc", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.abc", StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{TabID: "t.abc", StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
		{TabID: "t.abc", StartIndex: 14, EndIndex: 15, Kind: "image", ObjectID: "kix.pic"},
	}
	api.SeedDoc("doc1", "R0", spans, []vfs.DocTab{{ID: "t.abc", Title: "Intro", Index: 0}})
	ms := mountDrive(t, api, true)
	attrs := func(kv ...string) vfs.StyleMeta {
		m := map[string]string{"tab_id": "t.abc"}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return vfs.StyleMeta{Attributes: m}
	}
	level := func(n int, s vfs.StyleMeta) vfs.StyleMeta { s.Level = n; return s }

	doc, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := vfs.AsRich(doc)
	rd.SetBlocks([]vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Title", Style: level(0, attrs())},
		{Kind: vfs.BlockKindHeading, Text: "Nine", Style: level(9, attrs())},
		{Kind: vfs.BlockKindListItem, Text: "a", Style: level(1, attrs("list_type", "ul", "list_id", "l1"))},
		{Kind: vfs.BlockKindListItem, Text: "nested", Style: level(2, attrs("list_type", "ul", "list_id", "l1"))},
		{Kind: vfs.BlockKindListItem, Text: "first", Style: level(1, attrs("list_type", "ol", "list_id", "l2"))},
		{Kind: vfs.BlockKindParagraph, Text: "kept", Style: attrs()},
	})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	got, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var blocks []string
	for _, b := range got.(vfs.Structured).Blocks() {
		if b.Text == "" {
			continue // the empty paragraph a Docs body always keeps
		}
		blocks = append(blocks, fmt.Sprintf("%s %d %s", b.Kind, b.Style.Level, b.Text))
	}
	want := []string{"heading 1 Title", "heading 6 Nine", "list_item 1 a", "list_item 2 nested", "list_item 1 first", "paragraph 0 kept"}
	if !slices.Equal(blocks, want) {
		t.Fatalf("blocks = %q, want %q", blocks, want)
	}
}

func TestDrive_htmlLineFullCreateAndConflictRetry(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	spans := []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	api.SeedDoc("doc1", "R0", spans, nil)
	ms := mountDrive(t, api, true)

	doc, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start, end := 0, 0
	lines, err := doc.Lines(1, doc.LineCount()+1)
	if err != nil {
		t.Fatal(err)
	}
	for i, line := range lines {
		if strings.Contains(line, "<h1>Spec</h1>") {
			start, end = i+1, i+2
			break
		}
	}
	if start == 0 {
		t.Fatalf("heading line missing: %v", lines)
	}
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{
		Start: &start, End: &end,
		Lines: []string{"<h1>SPIKE</h1>"},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !blockHasText(got, "SPIKE") || !blockHasText(got, "Hello") {
		t.Fatalf("line write persist = %+v", got.(vfs.Structured).Blocks())
	}

	api.SeedDoc("doc1", "R-out", []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 10, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Remote"},
		{StartIndex: 10, EndIndex: 18, Kind: "paragraph", Text: "Changed"},
	}, nil)
	n := api.Nodes["doc1"]
	n.Meta.Version = "99"
	n.Meta.ModTime = n.Meta.ModTime.Add(time.Second)

	stale := "<h1>Stale</h1>\n<p>write</p>"
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: &stale})
	if !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stored lastRev vs live: %v", err)
	}

	if live, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx); err != nil || !blockHasText(live, "Remote") {
		t.Fatalf("live after out-of-band: err=%v", err)
	}
	full := "<h1>Whole</h1>\n<p>Body</p>"
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: &full})
	if err != nil {
		t.Fatal(err)
	}

	again, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil || !blockHasText(again, "Whole") || !blockHasText(again, "Body") {
		t.Fatalf("full HTML after last read: err=%v blocks=%+v", err, again.(vfs.Structured).Blocks())
	}

	api.FailNextDocWrites(1)
	retryBody := "<h1>Retried</h1>\n<p>Saved</p>"
	prior := vfs.ContentToken(again)
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: &retryBody})
	if err != nil {
		t.Fatal(err)
	}

	retried, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil || !blockHasText(retried, "Retried") || !blockHasText(retried, "Saved") {
		t.Fatalf("persist retry success: err=%v", err)
	}
	if tok := vfs.ContentToken(retried); tok == "" || tok == prior {
		t.Fatalf("retry hash: prior=%s got=%s", prior, tok)
	}

	api.FailNextDocWrites(2)
	conflictBody := "<h1>Conflict</h1>\n<p>x</p>"
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: &conflictBody})
	if !errors.Is(err, vfs.ErrConflict) {
		t.Fatalf("conflict retry: %v", err)
	}

	html := "<h1>CRE SPIKE</h1>\n<table><tr><td>A</td><td>B</td></tr></table>"
	_, err = ms.Apply(ctx, "/workspace/contracts/CRE SPIKE Public Data", vfs.Mutation{Content: &html})
	if err != nil {
		t.Fatal(err)
	}

	if st, err := ms.Route(ctx, "/workspace/contracts/CRE SPIKE Public Data").
		Stat(ctx); err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("extensionless HTML create Stat=%+v err=%v", st, err)
	}

	created, err := ms.Route(ctx, "/workspace/contracts/CRE SPIKE Public Data").
		ReadText(ctx)
	if err != nil || !blockHasText(created, "CRE SPIKE") {
		t.Fatalf("create body: %v", err)
	}
	var sawTable bool
	for _, b := range created.(vfs.Structured).Blocks() {
		if b.Kind == vfs.BlockKindTable && strings.Contains(b.Text, "A") && strings.Contains(b.Text, "B") {
			sawTable = true
		}
	}
	if !sawTable {
		t.Fatalf("create table cells = %+v", created.(vfs.Structured).Blocks())
	}
}

func TestDrive_htmlReplaceOnSingleTabOmitsTabID(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	spans := []vfs.DocsSpan{
		{TabID: "t.abc", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.abc", StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{TabID: "t.abc", StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	api.SeedDoc("doc1", "R0", spans, []vfs.DocTab{{ID: "t.abc", Title: "Intro", Index: 0}})
	ms := mountDrive(t, api, true)

	html := "<h1>CRE SPIKE</h1>\n<p>Public data options</p>\n<table><tr><td>A</td><td>B</td></tr></table>\n<p>After table</p>"
	_, err := ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: &html})
	if err != nil {
		t.Fatalf("single-tab HTML replace without tab_id: %v", err)
	}

	got, err := ms.Route(ctx, "/workspace/contracts/Spec").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !blockHasText(got, "CRE SPIKE") || !blockHasText(got, "Public data options") || !blockHasText(got, "After table") {
		t.Fatalf("replaced body = %+v", got.(vfs.Structured).Blocks())
	}
	var sawTable bool
	for _, b := range got.(vfs.Structured).Blocks() {
		if b.Kind == vfs.BlockKindTable && strings.Contains(b.Text, "A") && strings.Contains(b.Text, "B") {
			sawTable = true
		}
	}
	if !sawTable {
		t.Fatalf("table missing after replace: %+v", got.(vfs.Structured).Blocks())
	}
}

func mountDrive(t *testing.T, api *testdrive.FX, writable bool) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(vfs.At("contracts", testdrive.Open(t, api, nil)))(t.Context(), "s", vfs.Request{Bindings: []mount.Binding{{
		Provider: "gdrive", Writable: writable,
		Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
		Auth:   mount.Credential{Token: "t"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return ms
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

func TestDrive_sheetStatAndExportRead(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.Nodes["sheet1"].Export = exportBudgetZip(t)

	ms := mountDrive(t, api, false)

	doc, err := ms.Route(ctx, "/workspace/contracts/Budget").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	td, ok := vfs.AsGrid(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	if doc.Path() != "/workspace/contracts/Budget" {
		t.Fatalf("path = %s", doc.Path())
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
	text := doc.Text()
	if !strings.Contains(text, "# Sheet: Budget") || !strings.Contains(text, "42") ||
		strings.Contains(text, "<table>") || strings.Contains(text, "bold") {
		t.Fatalf("projection = %s", text)
	}
}

func TestDrive_sheetOverlayFormatAndMerge(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.SeedSheet("sheet1", vfs.SheetsSnapshot{SpreadsheetID: "sheet1", Sheets: budgetSheets(), Named: []vfs.NamedRange{{Name: "Total", SheetID: "1", A1: "B2"}}})
	api.Add("root-a", vfs.DriveMeta{ID: "merged1", Name: "Merged", MimeType: "application/vnd.google-apps.spreadsheet"}, nil)
	api.SeedSheet("merged1", vfs.SheetsSnapshot{
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
	ms := mountDrive(t, api, true)

	doc, err := ms.Route(ctx, "/workspace/contracts/Budget").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	td, ok := vfs.AsGrid(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	if !td.Sheets()[0].Cells[1][1].Format.Bold {
		t.Fatalf("checkout format B2 = %+v", td.Sheets()[0].Cells[1][1])
	}
	on, fill, color, align, valign, wrap := true, "#ffcc00", "#003366", "right", "middle", "wrap"
	num := "$#,##0.00"
	_, err = ms.Apply(ctx, "/workspace/contracts/Budget", vfs.Mutation{
		Rev: vfs.ContentToken(doc), BlockID: "Budget!B2", Body: strPtr("99"),
		Format: &vfs.FormatPatch{
			Number: &num, Italic: boolPtr(true), Strike: &on, Underline: &on,
			Fill: &fill, Color: &color, Align: &align, VAlign: &valign, Wrap: &wrap,
			Border: &vfs.CellBorder{Style: "thin", Edges: "bottom", Color: "#000000"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := ms.Route(ctx, "/workspace/contracts/Budget").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	td, ok = vfs.AsGrid(got)
	if !ok {
		t.Fatalf("type %T", got)
	}
	b2, err := td.Cell("Budget", "B2")
	if err != nil || b2.Display() != "99" || !b2.Format.Italic || !b2.Format.Bold ||
		!b2.Format.Strike || !b2.Format.Underline || b2.Format.Number != num ||
		b2.Format.Fill != fill || b2.Format.Color != color || b2.Format.Align != align ||
		b2.Format.VAlign != valign || b2.Format.Wrap != wrap ||
		b2.Format.Border == nil || b2.Format.Border.Style != "thin" {
		t.Fatalf("overlay B2 = %+v err=%v", b2, err)
	}
	if formula, _ := td.ReadCell("Budget", "A3"); formula != "=A1+1" {
		t.Fatalf("formula = %q", formula)
	}

	mergedDoc, err := ms.Route(ctx, "/workspace/contracts/Merged").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ms.Apply(ctx, "/workspace/contracts/Merged", vfs.Mutation{
		Rev: vfs.ContentToken(mergedDoc), BlockID: "Merged!B2", Body: strPtr("x"),
	})
	if !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("merge slave: %v", err)
	}

	still, err := ms.Route(ctx, "/workspace/contracts/Merged").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if g, _ := vfs.AsGrid(still); g == nil {
		t.Fatalf("type %T", still)
	} else if v, _ := g.ReadCell("Merged", "B2"); v != "d" {
		t.Fatalf("slave write landed: %q", v)
	}
	_, err = ms.Apply(ctx, "/workspace/contracts/Merged", vfs.Mutation{
		Rev: vfs.ContentToken(still), BlockID: "Merged!A1", Body: strPtr("master"),
	})
	if err != nil {
		t.Fatalf("merge master: %v", err)
	}

	after, err := ms.Route(ctx, "/workspace/contracts/Merged").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if g, _ := vfs.AsGrid(after); g == nil {
		t.Fatalf("type %T", after)
	} else if v, _ := g.ReadCell("Merged", "A1"); v != "master" {
		t.Fatalf("merge master A1: %q", v)
	}

	plain := vfs.NewTextDocument("/workspace/contracts/Budget", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, plain); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("identity WriteDocument: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/contracts/Budget").
		WriteFile(ctx, []byte("x")); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("PutFile native: %v", err)
	}

	rev := vfs.ContentToken(got)
	html := "<p>nope</p>"
	if _, err := ms.Apply(ctx, "/workspace/contracts/Budget", vfs.Mutation{Content: &html}); !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("HTML content on sheet: %v", err)
	}
	start, end := 1, 2
	if _, err := ms.Apply(ctx, "/workspace/contracts/Budget", vfs.Mutation{
		Start: &start, End: &end, Lines: []string{"x"},
	}); !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("line write on sheet: %v", err)
	}

	stillSheet, err := ms.Route(ctx, "/workspace/contracts/Budget").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if g, ok := vfs.AsGrid(stillSheet); !ok {
		t.Fatalf("type %T", stillSheet)
	} else if stillB2, err := g.ReadCell("Budget", "B2"); err != nil || stillB2 != "99" {
		t.Fatalf("sheet B2 after projected writes = %q err=%v", stillB2, err)
	}
	if _, err := ms.Apply(ctx, "/workspace/contracts/Budget", vfs.Mutation{
		Rev: rev, BlockID: "Budget!A1:C3", Body: strPtr("x"),
	}); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("range write: %v", err)
	}
	if _, err := ms.Apply(ctx, "/workspace/contracts/Budget", vfs.Mutation{
		Rev: rev, BlockID: "Budget", Body: strPtr("x"),
	}); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("sheet replace: %v", err)
	}
	if _, err := ms.Apply(ctx, "/workspace/contracts/Budget", vfs.Mutation{
		Rev: rev, Blocks: []vfs.Block{{Kind: vfs.BlockKindSheet, Text: "A\tB"}},
	}); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("blocks replace: %v", err)
	}
}

func TestDrive_sheetCheckoutTooLarge(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	row := make([]vfs.Cell, vfs.MaxSheetCells+1)
	for i := range row {
		row[i] = vfs.Cell{Input: "x", Value: "x"}
	}
	api.SeedSheet("sheet1", vfs.SheetsSnapshot{SpreadsheetID: "sheet1", Sheets: []vfs.Sheet{{ID: "1", Title: "Budget", Cells: [][]vfs.Cell{row}}}})
	ms := mountDrive(t, api, true)

	if _, err := ms.Route(ctx, "/workspace/contracts/Budget").
		ReadText(ctx); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("checkout cap: %v", err)
	}
}

func strPtr(s string) *string { return &s }

func boolPtr(v bool) *bool { return &v }
