package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

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
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = append(sawAuth, r.Header.Get("Authorization"))
		p := r.URL.Path
		switch {
		case p == "/me/drive":
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
		case p == "/drives/drv/items/root/children":
			writeJSON(w, 200, map[string]any{})
		case strings.HasSuffix(p, "/redir/content"):
			http.Redirect(w, r, "/drives/drv/items/blob/content", http.StatusFound)
		case strings.HasSuffix(p, "/blob/content"):
			_, _ = w.Write([]byte("blob"))
		default:
			http.NotFound(w, r)
		}
	}))

	holder := NewTokenHolder(Credential{Token: "tok-1"})
	api, err := newGraphSDK(holder, srv.URL, srv.Client())
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
		case "/me/drive":
			writeJSON(w, map[string]any{})
		case "/sites/site-1/drive":
			writeJSON(w, map[string]any{"id": "site-drv"})
		case "/drives/site-drv/items/txt1", "/drives/file-drv/items/txt1":
			writeJSON(w, map[string]any{"id": "txt1", "name": "a.txt", "file": map[string]any{"mimeType": "text/plain"}})
		default:
			http.NotFound(w, r)
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
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := api.GetItem(canceled, "drv", "root"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if _, err := api.PutContent(ctx, "drv", "id", "n", "p", errReader{}, 1); err == nil {
		t.Fatal("reader error")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read fail") }
