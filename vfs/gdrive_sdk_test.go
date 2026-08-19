package vfs

import (
	"archive/zip"
	"bytes"
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
)

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

	holder := NewTokenHolder(Credential{Token: "tok-1"})
	hc := &http.Client{Transport: &oauth2.Transport{Source: holder, Base: ts.Client().Transport}}
	svc, err := drive.NewService(ctx, option.WithHTTPClient(hc), option.WithEndpoint(ts.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	api := googleDrive{service: svc}

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
	found, err := api.Find(ctx, "root1", "a.txt")
	if err != nil || len(found) != 1 || found[0].ID != "bin1" {
		t.Fatalf("Find = %+v err=%v", found, err)
	}
	if !strings.Contains(lastListQ, "name = 'a.txt'") {
		t.Fatalf("Find q = %q", lastListQ)
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

	if _, err := api.GetMeta(ctx, "missing"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("404: %v", err)
	}
	if _, err := api.GetMeta(ctx, "denied"); !errors.Is(err, ErrPermission) {
		t.Fatalf("403: %v", err)
	}
	if _, err := api.GetMeta(ctx, "expired"); !errors.Is(err, ErrAuthExpired) {
		t.Fatalf("401: %v", err)
	}

	holder.Set(Credential{Token: "tok-2"})
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
	if _, _, err := api.Export(ctx, "huge", "application/zip"); !errors.Is(err, ErrTooLarge) {
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
