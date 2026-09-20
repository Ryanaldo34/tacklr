// Package testdrive is an internal httptest fixture for Drive, Docs, and Sheets.
package testdrive

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
)

const folderMIME = "application/vnd.google-apps.folder"

var (
	parentQ = regexp.MustCompile(`'([^']+)' in parents`)
	nameQ   = regexp.MustCompile(`name = '([^']+)'`)
)

// Node is one Drive file or folder in the fixture.
type Node struct {
	Meta   vfs.DriveMeta
	Parent string
	Body   []byte
	Export []byte
}

// FX is an in-memory Drive tree that speaks Drive v3, Docs, and Sheets HTTP.
type FX struct {
	mu        sync.Mutex
	Nodes     map[string]*Node
	Exports   int
	onceMedia map[string]int
	failMeta  int
	seq       int

	docs            map[string]*docState
	DocsBatches     []vfs.DocsBatch
	docConflictLeft int
	docBatchFail    bool
	books           *vfs.MemorySheets
}

// New returns an empty fixture.
func New() *FX {
	return &FX{
		Nodes:     make(map[string]*Node),
		onceMedia: make(map[string]int),
		docs:      make(map[string]*docState),
		books:     vfs.NewMemorySheets(),
	}
}

// Tree is the shared Drive fixture used by mount tests.
func Tree() *FX {
	d := New()
	d.Add("", vfs.DriveMeta{ID: "root-a", Name: "Contracts", MimeType: folderMIME}, nil)
	d.Add("", vfs.DriveMeta{ID: "root-b", Name: "Notes", MimeType: folderMIME}, nil)
	d.Add("root-a", vfs.DriveMeta{ID: "nda", Name: "nda.pdf", MimeType: "application/pdf", Size: 4}, []byte("%PDF"))
	d.Add("root-a", vfs.DriveMeta{ID: "sub", Name: "acme", MimeType: folderMIME}, nil)
	d.Add("sub", vfs.DriveMeta{ID: "note", Name: "note.md", MimeType: "text/markdown", Size: 6}, []byte("# hi\n\n"))
	d.Add("root-a", vfs.DriveMeta{ID: "dup1", Name: "dup.txt", MimeType: "text/plain", Size: 1}, []byte("a"))
	d.Add("root-a", vfs.DriveMeta{ID: "dup2", Name: "dup.txt", MimeType: "text/plain", Size: 1}, []byte("b"))
	d.Add("root-a", vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}, nil)
	d.Add("root-a", vfs.DriveMeta{ID: "sheet1", Name: "Budget", MimeType: "application/vnd.google-apps.spreadsheet"}, nil)
	d.Add("root-a", vfs.DriveMeta{ID: "slides1", Name: "Deck", MimeType: "application/vnd.google-apps.presentation"}, nil)
	d.Add("root-b", vfs.DriveMeta{ID: "readme", Name: "readme.txt", MimeType: "text/plain", Size: 5}, []byte("hello"))
	d.Add("root-b", vfs.DriveMeta{
		ID: "sc-folder", Name: "alias", MimeType: "application/vnd.google-apps.shortcut",
		TargetID: "sub", TargetMime: folderMIME,
	}, nil)
	d.Add("root-a", vfs.DriveMeta{ID: "huge", Name: "huge.bin", MimeType: "application/octet-stream", Size: int64(vfs.MaxReadFileBytes) + 1}, make([]byte, vfs.MaxReadFileBytes+1))
	return d
}

// Add inserts a node.
func (d *FX) Add(parent string, meta vfs.DriveMeta, body []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if meta.ModTime.IsZero() {
		meta.ModTime = time.Now().UTC()
	}
	if meta.Version == "" && !meta.IsDir && meta.MimeType != folderMIME {
		meta.Version = "1"
	}
	meta.IsDir = meta.MimeType == folderMIME
	d.Nodes[meta.ID] = &Node{Meta: meta, Parent: parent, Body: append([]byte(nil), body...)}
}

// OnceMedia makes the next media GET for id return HTTP status.
func (d *FX) OnceMedia(id string, status int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onceMedia[id] = status
}

// FailMeta makes every metadata GET return HTTP status.
func (d *FX) FailMeta(status int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failMeta = status
}

// Open builds Drive, Docs, and Sheets SDK clients against this fixture over httptest.
func Open(t testing.TB, fx *FX, holder *vfs.TokenHolder) vfs.Open {
	t.Helper()
	if holder == nil {
		holder = vfs.NewTokenHolder(vfs.Credential{Token: "tok"})
	}
	srv := testhttp.New(t, fx)
	base, hc := srv.URL+"/", srv.Client()
	api, err := vfs.NewGoogleDriveHTTP(t.Context(), holder, base, hc)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := vfs.NewGoogleDocsHTTP(t.Context(), holder, base, hc)
	if err != nil {
		t.Fatal(err)
	}
	sheets, err := vfs.NewGoogleSheetsHTTP(t.Context(), holder, base, hc)
	if err != nil {
		t.Fatal(err)
	}
	return vfs.DriveWith(api, docs, sheets)
}

func (d *FX) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		http.Error(w, `{"error":{"code":401,"message":"unauthorized"}}`, http.StatusUnauthorized)
		return
	}
	p := r.URL.Path
	d.mu.Lock()
	defer d.mu.Unlock()
	if strings.Contains(p, "/documents") {
		d.serveDocs(w, r)
		return
	}
	if strings.Contains(p, "/spreadsheets") {
		d.serveSheets(w, r)
		return
	}
	p = strings.TrimPrefix(p, "/drive/v3")
	p = strings.TrimPrefix(p, "/upload/drive/v3")

	switch {
	case r.Method == http.MethodGet && (p == "/files" || p == "/files/"):
		d.serveList(w, r)
	case r.Method == http.MethodPost && (p == "/files" || p == "/files/"):
		d.serveCreate(w, r)
	case strings.HasSuffix(p, "/export"):
		id := strings.TrimSuffix(strings.TrimPrefix(p, "/files/"), "/export")
		d.serveExport(w, id)
	case strings.HasPrefix(p, "/files/"):
		id := strings.TrimPrefix(p, "/files/")
		id = strings.TrimSuffix(id, "/")
		if r.URL.Query().Get("alt") == "media" {
			d.serveMedia(w, id)
			return
		}
		switch r.Method {
		case http.MethodGet:
			d.serveMeta(w, id)
		case http.MethodPatch:
			d.servePatch(w, r, id)
		default:
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}

func (d *FX) serveList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	parent := ""
	if m := parentQ.FindStringSubmatch(q); len(m) == 2 {
		parent = m[1]
	}
	name := ""
	if m := nameQ.FindStringSubmatch(q); len(m) == 2 {
		name = m[1]
	}
	if parent != "" {
		if _, ok := d.Nodes[parent]; !ok {
			writeDriveErr(w, http.StatusNotFound, "not found")
			return
		}
	}
	var files []map[string]any
	for _, n := range d.Nodes {
		if n.Parent != parent {
			continue
		}
		if name != "" && n.Meta.Name != name {
			continue
		}
		files = append(files, fileJSON(n))
	}
	writeDriveJSON(w, http.StatusOK, map[string]any{"files": files})
}

func (d *FX) serveMeta(w http.ResponseWriter, id string) {
	if d.failMeta != 0 {
		writeDriveErr(w, d.failMeta, "unauthorized")
		return
	}
	n, ok := d.Nodes[id]
	if !ok {
		writeDriveErr(w, http.StatusNotFound, "not found")
		return
	}
	writeDriveJSON(w, http.StatusOK, fileJSON(n))
}

func (d *FX) serveMedia(w http.ResponseWriter, id string) {
	if status, ok := d.onceMedia[id]; ok {
		delete(d.onceMedia, id)
		writeDriveErr(w, status, "unauthorized")
		return
	}
	n, ok := d.Nodes[id]
	if !ok {
		writeDriveErr(w, http.StatusNotFound, "not found")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(n.Body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(n.Body)
}

func (d *FX) serveExport(w http.ResponseWriter, id string) {
	n, ok := d.Nodes[id]
	if !ok {
		writeDriveErr(w, http.StatusNotFound, "not found")
		return
	}
	d.Exports++
	body := n.Export
	if len(body) == 0 {
		body = n.Body
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (d *FX) serveCreate(w http.ResponseWriter, r *http.Request) {
	meta, media := readDriveUpload(r)
	name, _ := meta["name"].(string)
	mimeType, _ := meta["mimeType"].(string)
	parent := ""
	if parents, ok := meta["parents"].([]any); ok && len(parents) > 0 {
		parent, _ = parents[0].(string)
	}
	d.seq++
	id := fmt.Sprintf("id-%s-%d", name, d.seq)
	n := &Node{
		Parent: parent,
		Body:   media,
		Meta: vfs.DriveMeta{
			ID: id, Name: name, MimeType: mimeType,
			Size: int64(len(media)), ModTime: time.Now().UTC(), Version: "1",
			IsDir: mimeType == folderMIME,
		},
	}
	d.Nodes[id] = n
	switch mimeType {
	case "application/vnd.google-apps.document":
		d.seedDocLocked(id, "R0", []vfs.DocsSpan{{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"}}, nil)
	case "application/vnd.google-apps.spreadsheet":
		d.books.Seed(id, vfs.SheetsSnapshot{SpreadsheetID: id, Sheets: []vfs.Sheet{{ID: "0", Title: "Sheet1"}}})
	}
	writeDriveJSON(w, http.StatusOK, fileJSON(n))
}

func (d *FX) servePatch(w http.ResponseWriter, r *http.Request, id string) {
	n, ok := d.Nodes[id]
	if !ok {
		writeDriveErr(w, http.StatusNotFound, "not found")
		return
	}
	meta, media := readDriveUpload(r)
	if trashed, _ := meta["trashed"].(bool); trashed {
		delete(d.Nodes, id)
		writeDriveJSON(w, http.StatusOK, fileJSON(n))
		return
	}
	if len(media) > 0 {
		n.Body = media
		n.Meta.Size = int64(len(media))
		n.Meta.ModTime = time.Now().UTC()
	}
	writeDriveJSON(w, http.StatusOK, fileJSON(n))
}

func fileJSON(n *Node) map[string]any {
	out := map[string]any{
		"id": n.Meta.ID, "name": n.Meta.Name, "mimeType": n.Meta.MimeType,
		"modifiedTime": n.Meta.ModTime.UTC().Format(time.RFC3339Nano),
	}
	if n.Meta.Size > 0 {
		out["size"] = strconv.FormatInt(n.Meta.Size, 10)
	}
	if n.Meta.Version != "" {
		out["version"] = n.Meta.Version
	}
	if n.Meta.TargetID != "" {
		out["shortcutDetails"] = map[string]any{
			"targetId": n.Meta.TargetID, "targetMimeType": n.Meta.TargetMime,
		}
	}
	return out
}

func writeDriveJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeDriveErr(w http.ResponseWriter, status int, msg string) {
	writeDriveJSON(w, status, map[string]any{"error": map[string]any{"code": status, "message": msg}})
}

func readDriveUpload(r *http.Request) (meta map[string]any, media []byte) {
	meta = map[string]any{}
	ct := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(ct)
	if err == nil && strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}
			body, _ := io.ReadAll(part)
			pct := part.Header.Get("Content-Type")
			if strings.Contains(pct, "json") || strings.HasPrefix(pct, "application/json") {
				_ = json.Unmarshal(body, &meta)
			} else {
				media = body
			}
		}
		return meta, media
	}
	body, _ := io.ReadAll(r.Body)
	if strings.Contains(ct, "json") || json.Valid(body) {
		_ = json.Unmarshal(body, &meta)
		return meta, nil
	}
	return meta, body
}
