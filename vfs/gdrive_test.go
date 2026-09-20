package vfs_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMountSession_gdriveReadOnlySession(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	holder := vfs.NewTokenHolder(vfs.Credential{Token: "tok"})
	open := testdrive.Open(t, api, holder)
	ms, err := vfs.Tree(
		vfs.At("contracts", open),
		vfs.At("notes", open),
	)(ctx, "sess-gd", vfs.Request{Bindings: []vfs.Binding{
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"}, Auth: vfs.Credential{Token: "tok"}, Live: holder},
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "notes", vfs.ParamFolderID: "root-b"}, Auth: vfs.Credential{Token: "tok"}, Live: holder},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	specs := ms.Specs()
	if len(specs) != 1 || specs[0].Point != vfs.WorkspacePoint || len(specs[0].Members) != 2 {
		t.Fatalf("Specs = %+v", specs)
	}
	for _, m := range specs[0].Members {
		if !m.ReadOnly || m.Params[vfs.ParamName] == "" {
			t.Fatalf("member = %+v", m)
		}
	}

	st, err := ms.Stat(ctx, "/workspace/contracts/nda.pdf")
	if err != nil || st.IsDir || st.MediaType != "application/pdf" || st.Size != 4 {
		t.Fatalf("Stat nda = %+v err=%v", st, err)
	}
	raw, err := ms.ReadFile(ctx, "/workspace/contracts/nda.pdf")
	if err != nil || string(raw) != "%PDF" {
		t.Fatalf("ReadFile nda = %q err=%v", raw, err)
	}
	text, err := ms.ReadText(ctx, "/workspace/contracts/acme/note.md")
	if err != nil || !strings.Contains(text.Text(), "# hi") {
		t.Fatalf("ReadText note = %v err=%v", text, err)
	}

	ents, err := ms.ReadDir(ctx, "/workspace/contracts")
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	var acmeDir bool
	for _, e := range ents {
		present[e.Name] = true
		if e.Name == "acme" {
			acmeDir = e.IsDir
		}
	}
	if !present["acme"] || !acmeDir || !present["nda.pdf"] || !present["Spec"] {
		t.Fatalf("ReadDir names = %+v", ents)
	}

	if _, err := ms.ReadFile(ctx, "/workspace/contracts/dup.txt"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("collision: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/contracts/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/contracts/nda.pdf", []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("write: %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/contracts/new"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("mkdir: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/contracts/nda.pdf"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("remove: %v", err)
	}

	// Shortcut to a folder is walkable by the shortcut name.
	got, err := ms.ReadFile(ctx, "/workspace/notes/alias/note.md")
	if err != nil || string(got) != "# hi\n\n" {
		t.Fatalf("shortcut walk = %q err=%v", got, err)
	}

	if _, err := ms.ReadFile(ctx, "/workspace/contracts/huge.bin"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("huge: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(canceled, "/workspace/contracts/nda.pdf"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}

	// Near-expiry token refreshes before the provider request.
	holder.Set(vfs.Credential{Token: "expiring", ExpiresAt: time.Now().Add(10 * time.Second)})
	proactiveRefreshes := 0
	holder.SetRefresh(func(context.Context) (vfs.Credential, error) {
		proactiveRefreshes++
		return vfs.Credential{Token: "proactive", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	raw, err = ms.ReadFile(ctx, "/workspace/notes/readme.txt")
	if err != nil || string(raw) != "hello" || proactiveRefreshes != 1 {
		t.Fatalf("proactive refresh read = %q refreshes=%d err=%v", raw, proactiveRefreshes, err)
	}

	// 401 + one reactive refresh succeeds.
	api.OnceMedia("readme", http.StatusUnauthorized)
	holder.SetRefresh(func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{Token: "fresh"}, nil
	})
	raw, err = ms.ReadFile(ctx, "/workspace/notes/readme.txt")
	if err != nil || string(raw) != "hello" {
		t.Fatalf("refresh read = %q err=%v", raw, err)
	}

	// 401 without refresh stays expired.
	api.FailMeta(http.StatusUnauthorized)
	holder.SetRefresh(nil)
	if _, err := ms.Stat(ctx, "/workspace/notes/readme.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("expired: %v", err)
	}
}

func TestDrive_requiresClient(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	_ = builtins.Drive(nil)
}

func TestMountSession_gdriveDirectoryAndWriteDocument(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	ms, err := vfs.Tree(vfs.At("contracts", testdrive.Open(t, api, nil)))(ctx, "s", vfs.Request{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Params:   map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"},
		Auth:     vfs.Credential{Token: "t"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if _, err := ms.ReadFile(ctx, "/workspace/contracts/acme"); err == nil {
		t.Fatal("ReadFile on directory")
	}
	if _, err := ms.ReadText(ctx, "/workspace/contracts/acme"); err == nil {
		t.Fatal("ReadText on directory")
	}
	if _, err := ms.ReadDir(ctx, "/workspace/contracts/nda.pdf"); err == nil {
		t.Fatal("ReadDir on file")
	}
	doc, err := ms.OpenDocument(ctx, "/workspace/contracts/acme/note.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteDocument: %v", err)
	}
}

func TestDrive_validateRejectsFileID(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	open := testdrive.Open(t, api, nil)
	folder, err := open(ctx, "s", vfs.Binding{
		Auth: vfs.Credential{Token: "t"}, Params: map[string]string{vfs.ParamFolderID: "root-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := folder.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	file, err := open(ctx, "s", vfs.Binding{
		Auth: vfs.Credential{Token: "t"}, Params: map[string]string{vfs.ParamFolderID: "nda"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Validate(ctx); err == nil {
		t.Fatal("file id must fail Validate")
	}
}

func TestGoogleDrive_sdkAdapter(t *testing.T) {
	ctx := t.Context()
	var sawAuth []string
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
	fileJSON := func(id, name, mime string) map[string]any {
		return map[string]any{"id": id, "name": name, "mimeType": mime, "modifiedTime": "2026-01-02T03:04:05Z"}
	}
	var createdMetaOnly bool
	var lastListQ string
	handleFiles := func(w http.ResponseWriter, r *http.Request) {
		sawAuth = append(sawAuth, r.Header.Get("Authorization"))
		p := r.URL.Path
		p = strings.TrimPrefix(p, "/drive/v3")
		p = strings.TrimPrefix(p, "/upload/drive/v3")
		if (p == "/files" || p == "/files/") && r.Method == http.MethodGet {
			lastListQ = r.URL.Query().Get("q")
		}
		switch {
		case r.URL.Query().Get("alt") == "media" && strings.HasSuffix(p, "/bin1"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("blob"))
		case strings.HasSuffix(p, "/export") && strings.Contains(p, "/doczip"):
			w.Header().Set("Content-Type", "application/zip")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(mustZipHTML("<html><body><h1>Spec</h1></body></html>"))
		case strings.HasSuffix(p, "/export") && strings.Contains(p, "/huge"):
			writeJSON(w, 403, map[string]any{"error": map[string]any{"code": 403, "message": "exportSizeLimitExceeded"}})
		case r.Method == http.MethodPost && (p == "/files" || p == "/files/"):
			if r.Header.Get("Content-Type") == "text/html" || strings.Contains(r.URL.Path, "/upload/") {
				http.Error(w, "unexpected media upload", 400)
				return
			}
			createdMetaOnly = true
			writeJSON(w, 200, fileJSON("newdoc", "Policy", "application/vnd.google-apps.document"))
		case p == "/files" || p == "/files/":
			writeJSON(w, 200, map[string]any{"files": []any{fileJSON("bin1", "a.txt", "text/plain")}})
		case strings.HasSuffix(p, "/root1"):
			writeJSON(w, 200, fileJSON("root1", "Root", "application/vnd.google-apps.folder"))
		case strings.HasSuffix(p, "/missing"):
			writeJSON(w, 404, map[string]any{"error": map[string]any{"code": 404, "message": "not found"}})
		case strings.HasSuffix(p, "/denied"):
			writeJSON(w, 403, map[string]any{"error": map[string]any{"code": 403, "message": "forbidden"}})
		case strings.HasSuffix(p, "/expired"):
			writeJSON(w, 401, map[string]any{"error": map[string]any{"code": 401, "message": "unauthorized"}})
		default:
			http.NotFound(w, r)
		}
	}
	mux.HandleFunc("/files/", handleFiles)
	mux.HandleFunc("/files", handleFiles)
	mux.HandleFunc("/drive/v3/files/", handleFiles)
	mux.HandleFunc("/drive/v3/files", handleFiles)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	holder := vfs.NewTokenHolder(vfs.Credential{Token: "tok-1"})
	api, err := vfs.NewGoogleDriveHTTP(ctx, holder, ts.URL+"/", ts.Client())
	if err != nil {
		t.Fatal(err)
	}

	meta, err := api.GetMeta(ctx, "root1")
	if err != nil || meta.ID != "root1" || !meta.IsDir {
		t.Fatalf("GetMeta = %+v err=%v", meta, err)
	}
	kids, err := api.List(ctx, "root1")
	if err != nil || len(kids) != 1 || kids[0].ID != "bin1" {
		t.Fatalf("List = %+v err=%v", kids, err)
	}
	if !strings.Contains(lastListQ, "trashed = false") || strings.Contains(lastListQ, "name =") {
		t.Fatalf("List q = %q", lastListQ)
	}
	body, size, err := api.GetMedia(ctx, "bin1")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(body)
	_ = body.Close()
	if string(data) != "blob" || size != 4 && size != int64(len(data)) {
		t.Fatalf("GetMedia = %q size=%d", data, size)
	}

	if _, err := api.GetMeta(ctx, "missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("404: %v", err)
	}
	if _, err := api.GetMeta(ctx, "denied"); !errors.Is(err, vfs.ErrPermission) {
		t.Fatalf("403: %v", err)
	}
	if _, err := api.GetMeta(ctx, "expired"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("401: %v", err)
	}

	holder.Set(vfs.Credential{Token: "tok-2"})
	if _, err := api.GetMeta(ctx, "root1"); err != nil {
		t.Fatal(err)
	}
	if len(sawAuth) == 0 || !strings.Contains(strings.Join(sawAuth, ","), "Bearer tok-1") {
		t.Fatalf("auth headers = %v", sawAuth)
	}
	if !strings.Contains(strings.Join(sawAuth, ","), "Bearer tok-2") {
		t.Fatalf("refresh not sent: %v", sawAuth)
	}

	zipBody, _, err := api.Export(ctx, "doczip", "application/zip")
	if err != nil {
		t.Fatal(err)
	}
	zipData, _ := io.ReadAll(zipBody)
	_ = zipBody.Close()
	if len(zipData) < 4 || zipData[0] != 'P' {
		t.Fatalf("export zip = %q", zipData[:min(8, len(zipData))])
	}
	if _, _, err := api.Export(ctx, "huge", "application/zip"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("export oversize: %v", err)
	}
	created, err := api.Create(ctx, "root1", "Policy", "application/vnd.google-apps.document", "", nil, 0)
	if err != nil || created.ID != "newdoc" || !createdMetaOnly {
		t.Fatalf("metadata-only create = %+v saw=%v err=%v", created, createdMetaOnly, err)
	}
}

func mustZipHTML(html string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("index.html")
	if err != nil {
		panic(err)
	}
	_, _ = w.Write([]byte(html))
	_ = zw.Close()
	return buf.Bytes()
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

	ms := mountDrive(t, api, false)
	st, err := ms.Stat(ctx, "/workspace/contracts/Spec")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" || st.Size != 0 {
		t.Fatalf("Stat = %+v err=%v", st, err)
	}
	if api.Exports != 0 {
		t.Fatalf("Stat exported %d times", api.Exports)
	}
	sheet, err := ms.Stat(ctx, "/workspace/contracts/Budget")
	if err != nil || sheet.MediaType != "application/vnd.google-apps.spreadsheet" || sheet.Size != 0 {
		t.Fatalf("sheet Stat = %+v err=%v", sheet, err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/contracts/Budget"); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("sheet ReadFile: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/contracts/Spec"); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("OpenFile native: %v", err)
	}
	doc, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Text(), "<h1>Spec</h1>") {
		t.Fatalf("projection = %s", doc.Text())
	}
	if doc.MediaType() != "application/vnd.google-apps.document" {
		t.Fatalf("mt = %s", doc.MediaType())
	}

	if !vfs.FuseAvailable() {
		return
	}
	before := api.Exports
	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	host := filepath.Join(dir, "workspace", "contracts", "Spec")
	hst, err := os.Stat(host)
	if err != nil {
		t.Fatal(err)
	}
	if hst.Size() != 0 {
		t.Fatalf("FUSE getattr size=%d want 0", hst.Size())
	}
	if api.Exports != before {
		t.Fatalf("getattr exported (exports %d → %d)", before, api.Exports)
	}
	got, err := os.ReadFile(host)
	if err != nil || !strings.Contains(string(got), "<h1>Spec</h1>") {
		t.Fatalf("FUSE cat = %q err=%v", got, err)
	}
	if err := os.WriteFile(host, []byte("nope"), 0o644); err == nil {
		t.Fatal("projected Docs kernel write: want EROFS")
	} else if !errors.Is(err, syscall.EROFS) && !errors.Is(err, os.ErrPermission) &&
		!strings.Contains(err.Error(), "read-only") && !strings.Contains(err.Error(), "EROFS") {
		t.Fatalf("projected Docs write err = %v", err)
	}
}

func TestDrive_exportTooLarge(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.Nodes["doc1"].Export = bytesRepeat(vfs.MaxDocsExportBytes + 2)
	ms := mountDrive(t, api, false)
	if _, err := ms.ReadText(ctx, "/workspace/contracts/Spec"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
}

func bytesRepeat(n int) []byte {
	return []byte(strings.Repeat("a", n))
}

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
	if err := ms.WriteFile(ctx, "/workspace/contracts/new.txt", []byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/contracts/new.txt")
	if err != nil || string(got) != "hello\n" {
		t.Fatalf("read new = %q err=%v", got, err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/contracts/sub/dir"); err != nil {
		t.Fatal(err)
	}
	if err := ms.Remove(ctx, "/workspace/contracts/new.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/contracts/new.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/contracts"); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("root: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/contracts/dup.txt"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("ambiguous: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/contracts/Spec", []byte("x")); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("native PutFile: %v", err)
	}
	plain := vfs.NewTextDocument("/workspace/contracts/Spec", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, plain); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("identity WriteDocument: %v", err)
	}
	still, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil || !blockHasText(still, "Hello") {
		t.Fatalf("identity write must not replace Doc IR: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/contracts/Spec"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Stat(ctx, "/workspace/contracts/Spec"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed doc Stat: %v", err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/contracts/Spec"); !errors.Is(err, vfs.ErrNotExist) {
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
	if len(api.DocsBatches) != 0 {
		t.Fatalf("empty hint must not BatchUpdate: %d", len(api.DocsBatches))
	}
	orig, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
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
	again, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil || !blockHasText(again, "Hello") || vfs.ContentToken(again) != before {
		t.Fatalf("sibling CAS must keep Hello: %v", err)
	}
}

func TestDrive_docsReplaceSucceeds(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	spans := []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}
	api.SeedDoc("doc1", "R0", spans, nil)
	ms := mountDrive(t, api, true)
	doc, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	before := vfs.ContentToken(doc)
	rd, ok := vfs.AsRich(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	var paraID string
	for _, b := range rd.Blocks() {
		if b.Kind == vfs.BlockKindParagraph {
			paraID = b.ID
		}
	}
	if err := rd.ReplaceBlock(paraID, "World", false); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	if !blockHasText(got, "World") {
		t.Fatalf("blocks after replace: %+v", got.(vfs.Structured).Blocks())
	}
	if vfs.ContentToken(got) == before {
		t.Fatal("ContentToken unchanged after replace")
	}
}

func TestDrive_createAsDoc(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	ms := mountDrive(t, api, true)
	created := vfs.NewRichDocument("/workspace/contracts/Policy", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "Hi"},
	})
	if err := ms.WriteDocument(ctx, created); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, n := range api.Nodes {
		if n.Meta.Name == "Policy" && n.Meta.MimeType == "application/vnd.google-apps.document" {
			found = true
			if len(n.Body) != 0 {
				t.Fatalf("metadata-only create planted media: %q", n.Body)
			}
		}
	}
	if !found {
		t.Fatal("create-as-Doc missing")
	}
	st, err := ms.Stat(ctx, "/workspace/contracts/Policy")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("Stat Policy = %+v err=%v", st, err)
	}
	got, err := ms.ReadText(ctx, "/workspace/contracts/Policy")
	if err != nil || !blockHasText(got, "Hi") {
		t.Fatalf("create body: %v", err)
	}

	table := vfs.NewRichDocument("/workspace/contracts/Grid", "application/vnd.google-apps.document", []vfs.Block{{
		Kind: vfs.BlockKindTable, Text: "A\tB",
		Style: vfs.StyleMeta{Attributes: map[string]string{"rows": "1", "cols": "2"}},
	}})
	if err := ms.WriteDocument(ctx, table); err != nil {
		t.Fatal(err)
	}
	grid, err := ms.ReadText(ctx, "/workspace/contracts/Grid")
	if err != nil {
		t.Fatal(err)
	}
	var sawTable bool
	for _, b := range grid.(vfs.Structured).Blocks() {
		if b.Kind == vfs.BlockKindTable && strings.Contains(b.Text, "A") && strings.Contains(b.Text, "B") {
			sawTable = true
		}
	}
	if !sawTable {
		t.Fatalf("table cells not filled: %+v", grid.(vfs.Structured).Blocks())
	}
}

func TestDrive_createAsDoc_pipeMarkdownTableFillsCells(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	ms := mountDrive(t, api, true)
	raw := "| Option | Cost |\n| --- | --- |\n| Public-only | $1,000 |\n| Hybrid | $8,000 |"
	doc := vfs.NewRichDocument("/workspace/contracts/Compare", "application/vnd.google-apps.document", []vfs.Block{{
		Kind: vfs.BlockKindTable, Text: raw,
	}})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	var insertCols int64
	for _, batch := range api.DocsBatches {
		for _, r := range batch.Requests {
			if r.InsertTable != nil {
				insertCols = r.InsertTable.Columns
			}
		}
	}
	if insertCols != 2 {
		t.Fatalf("InsertTable columns=%d, want 2 (pipe table, not 1-col markdown dump)", insertCols)
	}
	got, err := ms.ReadText(ctx, "/workspace/contracts/Compare")
	if err != nil {
		t.Fatal(err)
	}
	var table vfs.Block
	for _, b := range got.(vfs.Structured).Blocks() {
		if b.Kind == vfs.BlockKindTable {
			table = b
		}
	}
	if table.Kind == "" {
		t.Fatalf("no table block: %+v", got.(vfs.Structured).Blocks())
	}
	if strings.Contains(table.Text, "| Option") {
		t.Fatalf("cell text still markdown pipes: %q", table.Text)
	}
	if !strings.Contains(table.Text, "Public-only") || !strings.Contains(table.Text, "$8,000") {
		t.Fatalf("missing cells: %q", table.Text)
	}
}

func TestDrive_createDocAppliesInlineMarksInFollowupBatch(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	ms := mountDrive(t, api, true)
	doc := vfs.NewRichDocument("/workspace/contracts/Marks", "application/vnd.google-apps.document", []vfs.Block{
		{Kind: vfs.BlockKindParagraph, Text: "See **x**"},
	})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	if len(api.DocsBatches) < 2 {
		t.Fatalf("want insert then style batches, got %d", len(api.DocsBatches))
	}
	var sawBold bool
	for _, r := range api.DocsBatches[len(api.DocsBatches)-1].Requests {
		if st := r.UpdateTextStyle; st != nil && st.TextStyle != nil && st.TextStyle.Bold {
			sawBold = true
		}
	}
	if !sawBold {
		t.Fatalf("follow-up batch missing bold: %+v", api.DocsBatches)
	}
	for _, r := range api.DocsBatches[0].Requests {
		if ins := r.InsertText; ins != nil && strings.Contains(ins.Text, "**") {
			t.Fatalf("insert kept markdown: %q", ins.Text)
		}
		if r.UpdateTextStyle != nil {
			t.Fatal("text style must not share the insert batch")
		}
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

func TestDrive_nestedPlainFilesAndDirs(t *testing.T) {
	ctx := t.Context()
	api := testdrive.Tree()
	api.SeedDoc("doc1", "R0", nil, nil)
	ms := mountDrive(t, api, true)
	if err := ms.WriteFile(ctx, "/workspace/contracts/a/b/c.txt", []byte("z")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/contracts/a/b/c.txt")
	if err != nil || string(got) != "z" {
		t.Fatalf("read = %q err=%v", got, err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/contracts/a")
	if err != nil || len(ents) == 0 {
		t.Fatalf("readdir: %+v err=%v", ents, err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/contracts/d/e"); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/workspace/contracts/d/e")
	if err != nil || !st.IsDir {
		t.Fatalf("stat dir %+v err=%v", st, err)
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
	gotNested, err := ms.ReadFile(ctx, "/workspace/contracts/sub/dir/edge.txt")
	if err != nil || string(gotNested) != "plain" {
		t.Fatalf("nested txt = %q err=%v", gotNested, err)
	}
	txt := vfs.NewTextDocument("/workspace/contracts/edge.txt", "text/plain", "utf-8", "plain")
	if err := ms.WriteDocument(ctx, txt); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/contracts/edge.txt")
	if err != nil || string(got) != "plain" {
		t.Fatalf("txt = %q err=%v", got, err)
	}
	doc, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
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
	doc, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := vfs.AsRich(doc)
	if !ok {
		t.Fatalf("type %T", doc)
	}
	rd.SetBlocks([]vfs.Block{
		{Kind: vfs.BlockKindHeading, Text: "Title", Style: vfs.StyleMeta{Level: 1, Attributes: map[string]string{"tab_id": "t.abc"}}},
		{Kind: vfs.BlockKindListItem, Text: "a", Style: vfs.StyleMeta{Level: 1, Attributes: map[string]string{"tab_id": "t.abc", "list_type": "ul", "list_id": "l1"}}},
		{Kind: vfs.BlockKindListItem, Text: "nested", Style: vfs.StyleMeta{Level: 2, Attributes: map[string]string{"tab_id": "t.abc", "list_type": "ul", "list_id": "l1"}}},
		{Kind: vfs.BlockKindParagraph, Text: "kept", Style: vfs.StyleMeta{Attributes: map[string]string{"tab_id": "t.abc"}}},
	})
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	var texts []string
	for _, b := range got.(vfs.Structured).Blocks() {
		kinds = append(kinds, b.Kind)
		texts = append(texts, b.Text)
		if b.Kind == vfs.BlockKindImage {
			t.Fatalf("omitted image still present: %+v", b)
		}
	}
	if !strings.Contains(strings.Join(kinds, ","), "heading") || !strings.Contains(strings.Join(kinds, ","), "list_item") {
		t.Fatalf("kinds = %v", kinds)
	}
	joined := strings.Join(texts, " ")
	if !strings.Contains(joined, "nested") || !strings.Contains(joined, "Title") || !strings.Contains(joined, "kept") {
		t.Fatalf("texts = %v", texts)
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

	doc, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
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
	got, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
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

	live, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil || !blockHasText(live, "Remote") {
		t.Fatalf("live after out-of-band: err=%v", err)
	}
	full := "<h1>Whole</h1>\n<p>Body</p>"
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: &full})
	if err != nil {
		t.Fatal(err)
	}
	again, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
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
	retried, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
	if err != nil || !blockHasText(retried, "Retried") || !blockHasText(retried, "Saved") {
		t.Fatalf("persist retry success: err=%v", err)
	}
	if tok := vfs.ContentToken(retried); tok == "" || tok == prior {
		t.Fatalf("retry hash: prior=%s got=%s", prior, tok)
	}

	api.FailNextDocWrites(2)
	conflictBody := "<h1>Conflict</h1>\n<p>x</p>"
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Content: &conflictBody})
	if !errors.Is(err, vfs.ErrInvalidWrite) || !strings.Contains(err.Error(), "was not saved") {
		t.Fatalf("conflict retry: %v", err)
	}

	html := "<h1>CRE SPIKE</h1>\n<table><tr><td>A</td><td>B</td></tr></table>"
	_, err = ms.Apply(ctx, "/workspace/contracts/CRE SPIKE Public Data", vfs.Mutation{Content: &html})
	if err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/workspace/contracts/CRE SPIKE Public Data")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("extensionless HTML create Stat=%+v err=%v", st, err)
	}
	created, err := ms.ReadText(ctx, "/workspace/contracts/CRE SPIKE Public Data")
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
	got, err := ms.ReadText(ctx, "/workspace/contracts/Spec")
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
	ms, err := vfs.Tree(vfs.At("contracts", testdrive.Open(t, api, nil)))(t.Context(), "s", vfs.Request{Bindings: []vfs.Binding{{
		Provider: "gdrive", Writable: writable,
		Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"},
		Auth:   vfs.Credential{Token: "t"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
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
	doc, err := ms.ReadText(ctx, "/workspace/contracts/Budget")
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

	if !vfs.FuseAvailable() {
		return
	}
	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	wst, err := os.Stat(filepath.Join(dir, "workspace"))
	if err != nil || !wst.IsDir() {
		t.Fatalf("getattr /workspace = %+v err=%v", wst, err)
	}
	host := filepath.Join(dir, "workspace", "contracts", "Budget")
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
	if err := os.WriteFile(host, []byte("nope"), 0o644); err == nil {
		t.Fatal("kernel write: want EROFS")
	} else if !errors.Is(err, syscall.EROFS) && !errors.Is(err, os.ErrPermission) &&
		!strings.Contains(err.Error(), "read-only") && !strings.Contains(err.Error(), "EROFS") {
		t.Fatalf("kernel write err = %v", err)
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

	doc, err := ms.ReadText(ctx, "/workspace/contracts/Budget")
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
	got, err := ms.ReadText(ctx, "/workspace/contracts/Budget")
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

	mergedDoc, err := ms.ReadText(ctx, "/workspace/contracts/Merged")
	if err != nil {
		t.Fatal(err)
	}
	_, err = ms.Apply(ctx, "/workspace/contracts/Merged", vfs.Mutation{
		Rev: vfs.ContentToken(mergedDoc), BlockID: "Merged!B2", Body: strPtr("x"),
	})
	if !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("merge slave: %v", err)
	}
	still, err := ms.ReadText(ctx, "/workspace/contracts/Merged")
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
	after, err := ms.ReadText(ctx, "/workspace/contracts/Merged")
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
	if err := ms.WriteFile(ctx, "/workspace/contracts/Budget", []byte("x")); !errors.Is(err, vfs.ErrNotSupported) {
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
	stillSheet, err := ms.ReadText(ctx, "/workspace/contracts/Budget")
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
	if _, err := ms.ReadText(ctx, "/workspace/contracts/Budget"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("checkout cap: %v", err)
	}
}

func strPtr(s string) *string { return &s }

func boolPtr(v bool) *bool { return &v }
