package vfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	"github.com/ryanaldo34/tacklr/vfs"
)

func driveTree() *memDrive {
	d := newMemDrive()
	d.add("", vfs.DriveMeta{ID: "root-a", Name: "Contracts", MimeType: "application/vnd.google-apps.folder"}, nil)
	d.add("", vfs.DriveMeta{ID: "root-b", Name: "Notes", MimeType: "application/vnd.google-apps.folder"}, nil)
	d.add("root-a", vfs.DriveMeta{ID: "nda", Name: "nda.pdf", MimeType: "application/pdf", Size: 4}, []byte("%PDF"))
	d.add("root-a", vfs.DriveMeta{ID: "sub", Name: "acme", MimeType: "application/vnd.google-apps.folder"}, nil)
	d.add("sub", vfs.DriveMeta{ID: "note", Name: "note.md", MimeType: "text/markdown", Size: 6}, []byte("# hi\n\n"))
	d.add("root-a", vfs.DriveMeta{ID: "dup1", Name: "dup.txt", MimeType: "text/plain", Size: 1}, []byte("a"))
	d.add("root-a", vfs.DriveMeta{ID: "dup2", Name: "dup.txt", MimeType: "text/plain", Size: 1}, []byte("b"))
	d.add("root-a", vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}, nil)
	d.add("root-a", vfs.DriveMeta{ID: "sheet1", Name: "Budget", MimeType: "application/vnd.google-apps.spreadsheet"}, nil)
	d.add("root-b", vfs.DriveMeta{ID: "readme", Name: "readme.txt", MimeType: "text/plain", Size: 5}, []byte("hello"))
	d.add("root-b", vfs.DriveMeta{
		ID: "sc-folder", Name: "alias", MimeType: "application/vnd.google-apps.shortcut",
		TargetID: "sub", TargetMime: "application/vnd.google-apps.folder",
	}, nil)
	d.add("root-a", vfs.DriveMeta{ID: "huge", Name: "huge.bin", MimeType: "application/octet-stream", Size: int64(vfs.MaxReadFileBytes) + 1}, make([]byte, vfs.MaxReadFileBytes+1))
	return d
}

func TestMountSession_gdriveReadOnlySession(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("sess-gd", vfs.Binding{
		Provider: vfs.ProviderGoogleDrive, Point: "/contracts",
		Auth: vfs.Credential{Token: "tok"}, Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("sess-gd", vfs.Binding{
		Provider: vfs.ProviderGoogleDrive, Point: "/notes",
		Auth: vfs.Credential{Token: "tok"}, Params: map[string]string{vfs.ParamFolderID: "root-b"},
	}); err != nil {
		t.Fatal(err)
	}

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("sess-gd", reg)
	if err := ms.Mount(ctx, vfs.BindingSpec(vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Params: map[string]string{vfs.ParamFolderID: "root-a"},
	})); err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.BindingSpec(vfs.Binding{
		Provider: "gdrive", Point: "/notes", Params: map[string]string{vfs.ParamFolderID: "root-b"},
	})); err != nil {
		t.Fatal(err)
	}

	specs := ms.Specs()
	if len(specs) != 2 {
		t.Fatalf("Specs = %d", len(specs))
	}
	for _, s := range specs {
		if !s.ReadOnly || s.Params[vfs.ParamFolderID] == "" {
			t.Fatalf("spec = %+v", s)
		}
	}

	st, err := ms.Stat(ctx, "/contracts/nda.pdf")
	if err != nil || st.IsDir || st.MediaType != "application/pdf" || st.Size != 4 {
		t.Fatalf("Stat nda = %+v err=%v", st, err)
	}
	raw, err := ms.ReadFile(ctx, "/contracts/nda.pdf")
	if err != nil || string(raw) != "%PDF" {
		t.Fatalf("ReadFile nda = %q err=%v", raw, err)
	}
	text, err := ms.ReadText(ctx, "/contracts/acme/note.md")
	if err != nil || !strings.Contains(text.Text(), "# hi") {
		t.Fatalf("ReadText note = %v err=%v", text, err)
	}

	ents, err := ms.ReadDir(ctx, "/contracts")
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

	if _, err := ms.ReadFile(ctx, "/contracts/dup.txt"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("collision: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/contracts/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/contracts/Spec"); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("native ReadFile: %v", err)
	}
	if _, err := ms.ReadText(ctx, "/contracts/Budget"); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("sheet ReadText: %v", err)
	}
	if err := ms.WriteFile(ctx, "/contracts/nda.pdf", []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("write: %v", err)
	}
	if err := ms.MkdirAll(ctx, "/contracts/new"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("mkdir: %v", err)
	}
	if err := ms.Remove(ctx, "/contracts/nda.pdf"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("remove: %v", err)
	}

	// Shortcut to a folder is walkable by the shortcut name.
	got, err := ms.ReadFile(ctx, "/notes/alias/note.md")
	if err != nil || string(got) != "# hi\n\n" {
		t.Fatalf("shortcut walk = %q err=%v", got, err)
	}

	if _, err := ms.ReadFile(ctx, "/contracts/huge.bin"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("huge: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(canceled, "/contracts/nda.pdf"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}

	// 401 + one refresh succeeds.
	api.once["GetMedia"] = vfs.ErrAuthExpired
	h := auth.Holder("sess-gd", vfs.ProviderGoogleDrive)
	h.SetRefresh(func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{Token: "fresh"}, nil
	})
	raw, err = ms.ReadFile(ctx, "/notes/readme.txt")
	if err != nil || string(raw) != "hello" {
		t.Fatalf("refresh read = %q err=%v", raw, err)
	}

	// 401 without refresh stays expired.
	api.fail["GetMeta"] = vfs.ErrAuthExpired
	h.SetRefresh(nil)
	if _, err := ms.Stat(ctx, "/notes/readme.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("expired: %v", err)
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
	handleFiles := func(w http.ResponseWriter, r *http.Request) {
		sawAuth = append(sawAuth, r.Header.Get("Authorization"))
		p := r.URL.Path
		p = strings.TrimPrefix(p, "/drive/v3")
		switch {
		case r.URL.Query().Get("alt") == "media" && strings.HasSuffix(p, "/bin1"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("blob"))
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
	hc := &http.Client{Transport: &oauth2.Transport{Source: holder, Base: ts.Client().Transport}}
	svc, err := drive.NewService(ctx, option.WithHTTPClient(hc), option.WithEndpoint(ts.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	api := vfs.GoogleDrive{Service: svc}

	meta, err := api.GetMeta(ctx, "root1")
	if err != nil || meta.ID != "root1" || !meta.IsDir {
		t.Fatalf("GetMeta = %+v err=%v", meta, err)
	}
	kids, err := api.List(ctx, "root1")
	if err != nil || len(kids) != 1 || kids[0].ID != "bin1" {
		t.Fatalf("List = %+v err=%v", kids, err)
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
}

func TestDriveFactory_openRequiresFolderAndToken(t *testing.T) {
	ctx := t.Context()
	auth := vfs.NewSessionAuth()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("s", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/d", Profile: "gdrive"}); err == nil {
		t.Fatal("want folderId error")
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/d", Profile: "gdrive", Params: map[string]string{vfs.ParamFolderID: "x"}}); err == nil {
		t.Fatal("want token error")
	}
	if _, err := (vfs.DriveFactory{}).Open(ctx, "s", vfs.MountSpec{Params: map[string]string{vfs.ParamFolderID: "x"}}); err == nil {
		t.Fatal("want factory id error")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := (vfs.DriveFactory{ID: "gdrive"}).Open(canceled, "s", vfs.MountSpec{Params: map[string]string{vfs.ParamFolderID: "x"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled open: %v", err)
	}
}

func TestGoogleDrive_requiresServiceAndToken(t *testing.T) {
	ctx := t.Context()
	if _, err := vfs.NewGoogleDrive(ctx, nil); err == nil {
		t.Fatal("nil holder")
	}
	empty := vfs.GoogleDrive{}
	if _, err := empty.GetMeta(ctx, "id"); err == nil {
		t.Fatal("GetMeta without service")
	}
	if _, _, err := empty.GetMedia(ctx, "id"); err == nil {
		t.Fatal("GetMedia without service")
	}
	if _, err := empty.List(ctx, "id"); err == nil {
		t.Fatal("List without service")
	}
	holder := vfs.NewTokenHolder(vfs.Credential{Token: "tok"})
	gd, err := vfs.NewGoogleDrive(ctx, holder)
	if err != nil || gd == nil || gd.Service == nil {
		t.Fatalf("NewGoogleDrive = %+v err=%v", gd, err)
	}
}

func TestMountSession_gdriveDirectoryAndWriteDocument(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: vfs.Credential{Token: "t"}, Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession("s", reg)
	if err := ms.Mount(ctx, vfs.BindingSpec(vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Params: map[string]string{vfs.ParamFolderID: "root-a"},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/contracts/acme"); err == nil {
		t.Fatal("ReadFile on directory")
	}
	if _, err := ms.ReadText(ctx, "/contracts/acme"); err == nil {
		t.Fatal("ReadText on directory")
	}
	if _, err := ms.ReadDir(ctx, "/contracts/nda.pdf"); err == nil {
		t.Fatal("ReadDir on file")
	}
	doc, err := ms.OpenDocument(ctx, "/contracts/acme/note.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteDocument: %v", err)
	}
}

func TestCheckMount_gdriveFolder(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	auth := vfs.NewSessionAuth()
	_ = auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Point: "/c", Auth: vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamFolderID: "root-a"},
	})
	reg := vfs.NewBackendRegistry()
	_ = reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api})
	if err := vfs.CheckMount(ctx, reg, "s", vfs.MountSpec{
		Point: "/c", Profile: "gdrive", ReadOnly: true,
		Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := vfs.CheckMount(ctx, reg, "s", vfs.MountSpec{
		Point: "/c", Profile: "gdrive", Params: map[string]string{vfs.ParamFolderID: "nda"},
	}); err == nil {
		t.Fatal("file id must fail Validate")
	}
}
