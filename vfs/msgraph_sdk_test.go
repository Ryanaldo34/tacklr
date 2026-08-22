package vfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/testhttp"
)

func TestGraphSDK_adapterMapsStatusesTokenAndRedirect(t *testing.T) {
	ctx := t.Context()
	var sawAuth []string
	writeJSON := func(w http.ResponseWriter, status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != nil {
			_ = json.NewEncoder(w).Encode(body)
		}
	}
	itemJSON := func(id, name string, folder bool) map[string]any {
		it := map[string]any{
			"id": id, "name": name, "size": 1,
			"lastModifiedDateTime": "2026-01-02T03:04:05Z",
			"parentReference":      map[string]any{"id": "root"},
		}
		if folder {
			it["folder"] = map[string]any{}
		} else {
			it["file"] = map[string]any{"mimeType": "text/plain"}
		}
		return it
	}
	var srv *testhttp.Server
	srv = testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = append(sawAuth, r.Header.Get("Authorization"))
		p := r.URL.Path
		switch {
		case p == "/me/drive" || p == "/users/me-token-to-replace/drive":
			writeJSON(w, 200, map[string]any{"id": "drv"})
		case p == "/drives/drv/root", p == "/drives/drv/items/root":
			writeJSON(w, 200, itemJSON("root", "Root", true))
		case p == "/drives/drv/items/note1":
			writeJSON(w, 200, map[string]any{
				"id": "note1", "name": "Notebook",
				"lastModifiedDateTime": "2026-01-02T03:04:05Z",
				"package":              map[string]any{"type": "oneNote"},
				"parentReference":      map[string]any{"id": "root"},
			})
		case p == "/drives/drv/items/denied":
			writeJSON(w, 403, map[string]any{"error": map[string]any{"code": "accessDenied", "message": "no"}})
		case p == "/drives/drv/items/clash":
			writeJSON(w, 409, map[string]any{"error": map[string]any{"code": "nameAlreadyExists", "message": "exists"}})
		case p == "/drives/drv/items/boom":
			writeJSON(w, 500, map[string]any{"error": map[string]any{"code": "server", "message": "fail"}})
		case p == "/drives/drv/items/empty":
			w.WriteHeader(http.StatusOK)
		case p == "/drives/drv/root:/a.txt", p == "/drives/drv/items/root:/a.txt":
			writeJSON(w, 200, itemJSON("f1", "a.txt", false))
		case p == "/drives/drv/items/root/children":
			writeJSON(w, 200, map[string]any{})
		case p == "/drives/drv/items/denied/children":
			writeJSON(w, 403, map[string]any{"error": map[string]any{"code": "accessDenied", "message": "no"}})
		case p == "/drives/drv/items/paged/children":
			writeJSON(w, 200, map[string]any{"value": []any{}, "@odata.nextLink": srv.URL + "/drives/drv/items/boom"})
		case strings.HasSuffix(p, "/redir/content"):
			http.Redirect(w, r, "/drives/drv/items/blob/content", http.StatusFound)
		case strings.HasSuffix(p, "/blob/content"):
			_, _ = w.Write([]byte("blob"))
		default:
			http.NotFound(w, r)
		}
	}))

	holder := NewTokenHolder(Credential{Token: "tok-1"})
	api, err := newGraphSDK(holder, srv.URL, &http.Client{Timeout: time.Second, Transport: srv.Client().Transport})
	if err != nil {
		t.Fatal(err)
	}
	prod, err := newGraphSDK(holder, "", nil)
	if err != nil || prod.adapter().GetBaseUrl() != graphAPIRoot {
		t.Fatalf("default base = %q err=%v", prod.adapter().GetBaseUrl(), err)
	}

	drive, item, err := api.resolveRoot(ctx, "", "", "")
	if err != nil || drive != "drv" || item != "root" {
		t.Fatalf("resolveRoot = %s %s err=%v", drive, item, err)
	}
	note, err := api.GetItem(ctx, "drv", "note1")
	if err != nil || note.Mime != "oneNote" || note.IsDir {
		t.Fatalf("package item = %+v err=%v", note, err)
	}
	if note.LastModified != "2026-01-02T03:04:05Z" {
		t.Fatalf("lastModified = %q", note.LastModified)
	}
	if _, err := api.GetItem(ctx, "drv", "denied"); !errors.Is(err, ErrPermission) {
		t.Fatalf("403: %v", err)
	}
	if _, err := api.GetItem(ctx, "drv", "clash"); !errors.Is(err, ErrExist) {
		t.Fatalf("409: %v", err)
	}
	if _, err := api.GetItem(ctx, "drv", "boom"); err == nil || errors.Is(err, ErrAuthExpired) || errors.Is(err, ErrNotExist) {
		t.Fatalf("500: %v", err)
	}
	if _, err := api.GetItem(ctx, "drv", "empty"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("empty body: %v", err)
	}
	kids, err := api.ListChildren(ctx, "drv", "root")
	if err != nil || len(kids) != 0 {
		t.Fatalf("empty children page = %v err=%v", kids, err)
	}

	body, n, err := api.GetContent(ctx, "drv", "redir")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(body)
	_ = body.Close()
	if string(data) != "blob" || n != 4 {
		t.Fatalf("redirect content = %q n=%d", data, n)
	}

	holder.Set(Credential{Token: "tok-2"})
	if _, err := api.GetItem(ctx, "drv", "root"); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(sawAuth, ",")
	if !strings.Contains(joined, "Bearer tok-1") || !strings.Contains(joined, "Bearer tok-2") {
		t.Fatalf("auth headers = %v", sawAuth)
	}
	byPath, err := api.GetByPath(ctx, "drv", "root", "")
	if err != nil || byPath.ID != "root" {
		t.Fatalf("GetByPath empty rel = %+v err=%v", byPath, err)
	}
	rooted, err := api.GetByPath(ctx, "drv", "", "a.txt")
	if err != nil || rooted.ID != "f1" {
		t.Fatalf("GetByPath root rel = %+v err=%v", rooted, err)
	}
	if _, err := api.ListChildren(ctx, "drv", "denied"); !errors.Is(err, ErrPermission) {
		t.Fatalf("children 403: %v", err)
	}
	if _, err := api.ListChildren(ctx, "drv", "paged"); err == nil {
		t.Fatal("paged iterate")
	}
	holder.Set(Credential{})
	if _, err := api.GetItem(ctx, "drv", "root"); !errors.Is(err, ErrAuthExpired) {
		t.Fatalf("empty token: %v", err)
	}
}

func TestGraphSDK_resolveRootRejectsFileAndMissingDrive(t *testing.T) {
	ctx := t.Context()
	writeJSON := func(w http.ResponseWriter, body any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/me/drive", "/users/me-token-to-replace/drive":
			writeJSON(w, map[string]any{})
		case "/sites/site-1/drive":
			writeJSON(w, map[string]any{"id": "site-drv"})
		case "/sites/bad/drive":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "accessDenied", "message": "no"}})
		case "/drives/site-drv/items/txt1", "/drives/file-drv/items/txt1":
			writeJSON(w, map[string]any{"id": "txt1", "name": "a.txt", "file": map[string]any{"mimeType": "text/plain"}})
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "itemNotFound", "message": "no"}})
		}
	}))

	api, err := newGraphSDK(NewTokenHolder(Credential{Token: "tok"}), srv.URL, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := api.resolveRoot(ctx, "", "", ""); !strings.Contains(err.Error(), "drive id missing") {
		t.Fatalf("missing drive: %v", err)
	}
	if _, _, err := api.resolveRoot(ctx, "file-drv", "txt1", ""); err == nil || !strings.Contains(err.Error(), "not a folder") {
		t.Fatalf("file root: %v", err)
	}
	if _, _, err := api.resolveRoot(ctx, "", "txt1", "site-1"); err == nil || !strings.Contains(err.Error(), "not a folder") {
		t.Fatalf("site file root: %v", err)
	}
	if _, _, err := api.resolveRoot(ctx, "", "", "bad"); !errors.Is(err, ErrPermission) {
		t.Fatalf("site drive denied: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := api.GetItem(canceled, "drv", "root"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if _, err := api.PutContent(ctx, "drv", "id", "n", "p", errReader{}, 1); err == nil {
		t.Fatal("reader error")
	}
	if _, _, err := api.resolveRoot(ctx, "missing-drv", "root", ""); !errors.Is(err, ErrNotExist) {
		t.Fatalf("missing item: %v", err)
	}
	if api.absURL("https://example.test/x") != "https://example.test/x" {
		t.Fatalf("absURL absolute: %q", api.absURL("https://example.test/x"))
	}
	if got := graphItemFrom(nil); got != (graphItem{}) {
		t.Fatalf("nil item: %+v", got)
	}
	if orRootID("") != "root" {
		t.Fatal("orRootID empty")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read fail") }

func TestGraphProvider_returnPaths(t *testing.T) {
	ctx := t.Context()
	writeJSON := func(w http.ResponseWriter, body any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}
	folder := map[string]any{"id": "root", "name": "root", "folder": map[string]any{}}
	plain := func(id, name, mime, last string, size int) map[string]any {
		it := map[string]any{"id": id, "name": name, "size": size, "parentReference": map[string]any{"id": "root"}}
		if mime != "" {
			it["file"] = map[string]any{"mimeType": mime}
		} else {
			it["file"] = map[string]any{}
		}
		if last != "" {
			it["lastModifiedDateTime"] = last
		}
		return it
	}
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case p == "/drives/drv/root", p == "/drives/drv/items/root":
			writeJSON(w, folder)
		case p == "/drives/drv/items/txt1":
			writeJSON(w, plain("txt1", "a.txt", "text/plain", "2026-01-02T03:04:05Z", 3))
		case p == "/drives/drv/items/root:/a.txt":
			writeJSON(w, plain("txt1", "a.txt", "text/plain", "2026-01-02T03:04:05Z", 3))
		case p == "/drives/drv/items/root:/bare.bin":
			writeJSON(w, plain("bare", "bare.bin", "", "", 1))
		case p == "/drives/drv/items/bare":
			writeJSON(w, plain("bare", "bare.bin", "", "", 1))
		case p == "/drives/drv/items/bare/content":
			_, _ = w.Write([]byte("x"))
		case p == "/drives/drv/items/root:/huge.bin":
			writeJSON(w, plain("huge", "huge.bin", "application/octet-stream", "2026-01-02T03:04:05Z", MaxReadFileBytes+1))
		case p == "/drives/drv/items/huge":
			writeJSON(w, plain("huge", "huge.bin", "application/octet-stream", "2026-01-02T03:04:05Z", MaxReadFileBytes+1))
		case p == "/drives/drv/items/txt1/content":
			if r.Header.Get("Authorization") == "Bearer stale" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "unauth", "message": "no"}})
				return
			}
			_, _ = w.Write([]byte("abc"))
		case p == "/drives/drv/items/root:/locked":
			writeJSON(w, map[string]any{"id": "locked", "name": "locked", "folder": map[string]any{}})
		case p == "/drives/drv/items/locked":
			writeJSON(w, map[string]any{"id": "locked", "name": "locked", "folder": map[string]any{}})
		case p == "/drives/drv/items/locked/children":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "accessDenied", "message": "no"}})
		case p == "/drives/drv/items/root:/gone.txt":
			writeJSON(w, plain("gone", "gone.txt", "text/plain", "2026-01-02T03:04:05Z", 1))
		case p == "/drives/drv/items/gone":
			writeJSON(w, plain("gone", "gone.txt", "text/plain", "2026-01-02T03:04:05Z", 1))
		case p == "/drives/drv/items/gone/content":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "itemNotFound", "message": "no"}})
		case p == "/drives/drv/items/root:/deny.txt":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "accessDenied", "message": "no"}})
		case p == "/drives/drv/items/root/children" && r.Method == http.MethodGet:
			writeJSON(w, map[string]any{"value": []any{
				map[string]any{"id": "sub", "name": "sub", "folder": map[string]any{}},
				plain("txt1", "a.txt", "text/plain", "2026-01-02T03:04:05Z", 3),
			}})
		case p == "/drives/drv/items/root/children" && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "nameAlreadyExists", "message": "exists"}})
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "itemNotFound", "message": "no"}})
		}
	}))

	holder := NewTokenHolder(Credential{Token: "tok"})
	sdk, err := newGraphSDK(holder, srv.URL, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	p := &graphProvider{api: sdk, driveID: "drv", rootID: "root", holder: holder, writable: true}

	if err := p.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.Validate(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("validate ctx: %v", err)
	}
	if _, err := p.Stat(canceled, "a.txt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("stat ctx: %v", err)
	}
	if _, err := p.OpenFile(canceled, "a.txt", os.O_RDONLY, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("open ctx: %v", err)
	}
	if _, err := p.ReadDir(canceled, "."); !errors.Is(err, context.Canceled) {
		t.Fatalf("readdir ctx: %v", err)
	}
	if err := p.Remove(canceled, "a.txt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("remove ctx: %v", err)
	}
	if err := p.MkdirAll(canceled, "sub", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("mkdir ctx: %v", err)
	}
	if _, err := p.OpenDocument(canceled, "a.txt", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("opendoc ctx: %v", err)
	}
	if err := p.PutFile(canceled, "a.txt", bytes.NewReader(nil), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("put ctx: %v", err)
	}
	if err := p.WriteDocument(canceled, "a.txt", NewTextDocument("a.txt", "text/plain", "utf-8", "x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("writedoc ctx: %v", err)
	}

	st, err := p.Stat(ctx, ".")
	if err != nil || !st.IsDir {
		t.Fatalf("stat root = %+v err=%v", st, err)
	}
	if _, err := p.resolve(ctx, ".."); err == nil {
		t.Fatal("resolve dirty")
	}
	bare, err := p.Stat(ctx, "bare.bin")
	if err != nil || bare.MediaType == "" {
		t.Fatalf("bare stat = %+v err=%v", bare, err)
	}
	if fi := graphFileInfo(graphItem{Name: "x.txt", LastModified: "not-a-date"}); !fi.ModTime.IsZero() {
		t.Fatalf("bad lastModified = %+v", fi)
	}
	if _, err := p.OpenFile(ctx, "bare.bin", os.O_RDONLY, 0); !errors.Is(err, ErrNoCodec) {
		t.Fatalf("bare open: %v", err)
	}
	if _, err := p.OpenFile(ctx, "huge.bin", os.O_RDONLY, 0); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("huge open: %v", err)
	}
	if _, err := p.OpenFile(ctx, "a.txt", os.O_WRONLY, 0); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("write open: %v", err)
	}
	p.writable = false
	if _, err := p.OpenFile(ctx, "a.txt", os.O_WRONLY, 0); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ro open: %v", err)
	}
	if err := p.Remove(ctx, "a.txt"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ro remove: %v", err)
	}
	if err := p.MkdirAll(ctx, "sub", fs.ModeDir); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ro mkdir: %v", err)
	}
	if err := p.PutFile(ctx, "a.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ro put: %v", err)
	}
	if err := p.WriteDocument(ctx, "a.txt", NewTextDocument("a.txt", "text/plain", "utf-8", "x")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ro writedoc: %v", err)
	}
	p.writable = true
	if err := p.Remove(ctx, ""); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("remove root: %v", err)
	}
	if err := p.MkdirAll(ctx, "", 0); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := p.PutFile(ctx, "", bytes.NewReader([]byte("x")), 1); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("put root: %v", err)
	}
	if _, err := p.OpenDocument(ctx, ".", nil); !errors.Is(err, ErrIsDir) {
		t.Fatalf("opendoc dir: %v", err)
	}
	if err := p.MkdirAll(ctx, "nope", 0); !errors.Is(err, ErrExist) {
		t.Fatalf("mkdir conflict: %v", err)
	}
	ents, err := p.ReadDir(ctx, ".")
	if err != nil || len(ents) < 2 {
		t.Fatalf("readdir root = %+v err=%v", ents, err)
	}
	if _, err := p.ensureDir(ctx, ""); err != nil {
		t.Fatalf("ensureDir root: %v", err)
	}
	if _, _, err := p.parentAndLeaf(ctx, ""); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("parentAndLeaf empty: %v", err)
	}
	if _, _, err := p.parentAndLeaf(ctx, ".."); err == nil {
		t.Fatal("parentAndLeaf dirty")
	}
	if err := p.MkdirAll(ctx, "deny.txt/x", 0); !errors.Is(err, ErrPermission) {
		t.Fatalf("mkdir through denied: %v", err)
	}
	if err := p.WriteDocument(ctx, "a.txt", nil); !errors.Is(err, ErrNotTextual) {
		t.Fatalf("writedoc nil: %v", err)
	}
	if _, err := p.getByPath(ctx, "root", ""); err != nil {
		t.Fatalf("getByPath empty: %v", err)
	}
	if _, err := p.ReadDir(ctx, "missing"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("readdir missing: %v", err)
	}
	if _, err := p.ReadDir(ctx, "a.txt"); !errors.Is(err, ErrNotDir) {
		t.Fatalf("readdir file: %v", err)
	}
	if _, err := p.ReadDir(ctx, "locked"); !errors.Is(err, ErrPermission) {
		t.Fatalf("readdir locked: %v", err)
	}
	if err := p.Remove(ctx, ".."); err == nil {
		t.Fatal("remove dirty")
	}
	if err := p.Remove(ctx, "missing"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}
	if err := p.MkdirAll(ctx, "..", 0); err == nil {
		t.Fatal("mkdir dirty")
	}
	if _, err := p.OpenDocument(ctx, "missing", nil); !errors.Is(err, ErrNotExist) {
		t.Fatalf("opendoc missing: %v", err)
	}
	if _, err := p.OpenDocument(ctx, "gone.txt", nil); !errors.Is(err, ErrNotExist) {
		t.Fatalf("opendoc gone: %v", err)
	}
	if err := p.PutFile(ctx, "..", bytes.NewReader([]byte("x")), 1); err == nil {
		t.Fatal("put dirty")
	}
	if err := p.PutFile(ctx, "big.bin", bytes.NewReader(nil), int64(MaxReadFileBytes)+1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("put oversize: %v", err)
	}
	if err := p.PutFile(ctx, "deny.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, ErrPermission) {
		t.Fatalf("put deny: %v", err)
	}
	if err := p.PutFile(ctx, "x.bin", errReader{}, 0); err == nil {
		t.Fatal("put reader")
	}
	missingRoot := &graphProvider{api: sdk, driveID: "drv", rootID: "no-root", holder: holder, writable: true}
	if err := missingRoot.Validate(ctx); !errors.Is(err, ErrNotExist) {
		t.Fatalf("validate missing: %v", err)
	}
	if _, err := missingRoot.Stat(ctx, "."); !errors.Is(err, ErrNotExist) {
		t.Fatalf("stat missing root: %v", err)
	}
	if err := missingRoot.MkdirAll(ctx, "sub", 0); !errors.Is(err, ErrNotExist) {
		t.Fatalf("mkdir missing root: %v", err)
	}
	if err := missingRoot.PutFile(ctx, "sub/a.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, ErrNotExist) {
		t.Fatalf("put nested missing root: %v", err)
	}
	if err := missingRoot.PutFile(ctx, "a.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, ErrNotExist) {
		t.Fatalf("put leaf missing root: %v", err)
	}
	if graphItemURL("d", "", "") != "/drives/d/root" || graphItemURL("d", "", "a") != "/drives/d/root:/a" || graphItemURL("d", "i", "") != "/drives/d/items/i" {
		t.Fatalf("graphItemURL = %q %q %q", graphItemURL("d", "", ""), graphItemURL("d", "", "a"), graphItemURL("d", "i", ""))
	}

	fileRoot := &graphProvider{api: sdk, driveID: "drv", rootID: "txt1", holder: holder, writable: true}
	if err := fileRoot.Validate(ctx); err == nil || !strings.Contains(err.Error(), "not a folder") {
		t.Fatalf("validate file: %v", err)
	}

	holder.Set(Credential{Token: "stale"})
	if _, err := p.OpenFile(ctx, "a.txt", os.O_RDONLY, 0); !errors.Is(err, ErrAuthExpired) {
		t.Fatalf("refresh missing: %v", err)
	}
	holder.Set(Credential{})
	if _, err := p.Stat(ctx, "a.txt"); !errors.Is(err, ErrAuthExpired) {
		t.Fatalf("ensure valid: %v", err)
	}
}
