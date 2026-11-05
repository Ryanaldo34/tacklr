// Package testdrive is an internal httptest fixture for Drive, Docs, and Sheets.
package testdrive

import (
	"bytes"
	"crypto/md5" //nolint:gosec // G501: Drive reports md5Checksum; the fake matches it
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
)

const folderMIME = "application/vnd.google-apps.folder"

// Node is one Drive file or folder in the fixture.
type Node struct {
	Meta   vfs.DriveMeta
	Parent string
	Body   []byte
	Export []byte
}

// FX is an in-memory Drive tree that speaks Drive v3, Docs, and Sheets HTTP.
type FX struct {
	mu      sync.Mutex
	Nodes   map[string]*Node
	Exports int
	faults  []*Fault
	seq     int

	docs  map[string]*vfs.DocsSnapshot
	books *vfs.MemorySheets
	mux   *http.ServeMux
}

// Fault is one planned failure of a call to the fixture.
type Fault struct {
	// Call is the API method that fails: files.get, files.media,
	// files.list, files.export, files.create, files.update, documents.get,
	// documents.batchUpdate, spreadsheets.get, spreadsheets.batchUpdate or
	// spreadsheets.values.batchUpdate.
	Call string
	// ID is the file, document or spreadsheet the call names (the folder
	// for files.list). An empty ID matches every call.
	ID string
	// Skip is the number of matching calls that pass before the fault.
	Skip int
	// Status is the HTTP status of the Google error reply.
	Status int
	// Message is the message of the Google error reply.
	Message string
	// Cut answers 200 and then drops the connection in the body. It is
	// for drops: a request that gets no reply at all is sent again by
	// net/http, so the call does not fail.
	Cut bool
	// Then runs when the fault occurs, before the reply.
	Then func()
}

// New returns an empty fixture.
func New() *FX {
	d := &FX{
		Nodes: make(map[string]*Node),
		docs:  make(map[string]*vfs.DocsSnapshot),
		books: vfs.NewMemorySheets(),
	}
	d.mux = d.routes()
	return d
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
	d.Add("root-a", vfs.DriveMeta{ID: "huge", Name: "huge.bin", MimeType: "application/octet-stream", Size: int64(vfs.MaxReadFileBytes) + 1}, nil)
	d.Nodes["huge"].Body, d.Nodes["huge"].Meta.MD5 = hugeBody()
	return d
}

// hugeBody is the bytes of huge.bin, one byte over the read cap, and
// their checksum. Every Tree shares them and no handler changes them in
// place.
var hugeBody = sync.OnceValues(func() ([]byte, string) {
	body := make([]byte, vfs.MaxReadFileBytes+1)
	return body, checksum("application/octet-stream", body)
})

// Add inserts a node.
func (d *FX) Add(parent string, meta vfs.DriveMeta, body []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if meta.ModTime.IsZero() {
		meta.ModTime = time.Now().UTC()
	}
	if meta.Version == "" && meta.MimeType != folderMIME {
		meta.Version = "1"
	}
	meta.IsDir = meta.MimeType == folderMIME
	meta.MD5 = checksum(meta.MimeType, body)
	d.Nodes[meta.ID] = &Node{Meta: meta, Parent: parent, Body: append([]byte(nil), body...)}
}

// checksum is Drive's md5Checksum: set for files with bytes, never for
// folders or Google-native files.
func checksum(mimeType string, body []byte) string {
	if len(body) == 0 || strings.HasPrefix(mimeType, "application/vnd.google-apps.") {
		return ""
	}
	return fmt.Sprintf("%x", md5.Sum(body)) //nolint:gosec // G401: Drive's checksum is md5
}

// Fail plans f. The faults that match one call occur in the order they
// were planned.
func (d *FX) Fail(f Fault) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.faults = append(d.faults, &f)
}

// Open builds Drive, Docs, and Sheets clients against this fixture over httptest.
func Open(t testing.TB, fx *FX, holder *vfs.TokenHolder) vfs.Open {
	t.Helper()
	if holder == nil {
		holder = vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	}
	srv := testhttp.New(t, fx)
	base, hc := srv.URL+"/", srv.Client()
	return vfs.DriveWith(vfs.NewGoogleDrive(holder, base, hc), vfs.NewGoogleDocs(holder, base, hc), vfs.NewGoogleSheets(holder, base, hc), holder)
}

// ServeHTTP answers the Drive v3, Docs v1 and Sheets v4 calls of the vfs
// clients. A request with no bearer token gets 401.
func (d *FX) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer")) == "" {
		writeDriveErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mux.ServeHTTP(w, r)
}

// routes maps each API method to its handler. Every handler runs with d.mu
// held, after the planned fault of its call.
func (d *FX) routes() *http.ServeMux {
	mux := http.NewServeMux()
	handle := func(pattern, call string, id func(*http.Request) string, h func(http.ResponseWriter, *http.Request, string)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			name, target := call, id(r)
			if r.URL.Query().Get("alt") == "media" {
				name = "files.media"
			}
			if d.fault(w, name, target) {
				return
			}
			h(w, r, target)
		})
	}
	file := func(r *http.Request) string { return r.PathValue("id") }
	rpc := func(r *http.Request) string { id, _, _ := strings.Cut(r.PathValue("rpc"), ":"); return id }
	parent := func(r *http.Request) string { return listQuery(r).parent }
	none := func(*http.Request) string { return "" }

	handle("GET /drive/v3/files", "files.list", parent, d.serveList)
	handle("POST /drive/v3/files", "files.create", none, d.serveCreate)
	handle("POST /upload/drive/v3/files", "files.create", none, d.serveCreate)
	handle("GET /drive/v3/files/{id}", "files.get", file, d.withNode(d.serveGet))
	handle("GET /drive/v3/files/{id}/export", "files.export", file, d.withNode(d.serveExport))
	handle("PATCH /drive/v3/files/{id}", "files.update", file, d.withNode(d.servePatch))
	handle("PATCH /upload/drive/v3/files/{id}", "files.update", file, d.withNode(d.servePatch))
	handle("GET /v1/documents/{id}", "documents.get", file, d.serveDocsGet)
	handle("POST /v1/documents/{rpc}", "documents.batchUpdate", rpc, d.serveDocsBatch)
	handle("GET /v4/spreadsheets/{id}", "spreadsheets.get", file, d.serveSheetsGet)
	handle("POST /v4/spreadsheets/{id}/values:batchUpdate", "spreadsheets.values.batchUpdate", file, d.serveSheetsValues)
	handle("POST /v4/spreadsheets/{rpc}", "spreadsheets.batchUpdate", rpc, d.serveSheetsBatch)
	return mux
}

// fault answers the first planned fault of call on id and reports whether
// one occurred.
func (d *FX) fault(w http.ResponseWriter, call, id string) bool {
	i := slices.IndexFunc(d.faults, func(f *Fault) bool {
		return f.Call == call && (f.ID == "" || f.ID == id)
	})
	if i < 0 {
		return false
	}
	f := d.faults[i]
	if f.Skip > 0 {
		f.Skip--
		return false
	}
	d.faults = slices.Delete(d.faults, i, i+1)
	if f.Then != nil {
		f.Then()
	}
	if !f.Cut {
		writeDriveErr(w, f.Status, f.Message)
		return true
	}
	w.Header().Set("Content-Length", "1024")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{"))
	_ = http.NewResponseController(w).Flush()
	panic(http.ErrAbortHandler)
}

// withNode answers 404 for an ID that names no node, and else calls h
// with the node.
func (d *FX) withNode(h func(http.ResponseWriter, *http.Request, *Node)) func(http.ResponseWriter, *http.Request, string) {
	return func(w http.ResponseWriter, r *http.Request, id string) {
		n, ok := d.Nodes[id]
		if !ok {
			writeDriveErr(w, http.StatusNotFound, "not found")
			return
		}
		h(w, r, n)
	}
}

// fileQuery is the folder and the name of a files.list q, which the vfs
// client writes as "'<folder>' in parents[ and name = '<name>'] and ...".
type fileQuery struct{ parent, name string }

func listQuery(r *http.Request) fileQuery {
	q := r.URL.Query().Get("q")
	parent, _, _ := strings.Cut(strings.TrimPrefix(q, "'"), "'")
	_, name, _ := strings.Cut(q, "name = '")
	name, _, _ = strings.Cut(name, "'")
	return fileQuery{parent: parent, name: name}
}

func (d *FX) serveList(w http.ResponseWriter, r *http.Request, parent string) {
	name := listQuery(r).name
	var nodes []*Node
	for _, n := range d.Nodes {
		if n.Parent == parent && (name == "" || n.Meta.Name == name) {
			nodes = append(nodes, n)
		}
	}
	slices.SortFunc(nodes, func(a, b *Node) int { return strings.Compare(a.Meta.ID, b.Meta.ID) })
	// Drive may return fewer files than pageSize; the fake pages by two, so
	// every listing follows nextPageToken.
	from, _ := strconv.Atoi(r.URL.Query().Get("pageToken"))
	to := min(from+2, len(nodes))
	files := []map[string]any{}
	for _, n := range nodes[from:to] {
		files = append(files, fileJSON(n))
	}
	page := map[string]any{"files": files}
	if to < len(nodes) {
		page["nextPageToken"] = strconv.Itoa(to)
	}
	writeDriveJSON(w, http.StatusOK, page)
}

// serveGet answers files.get: the metadata, or the bytes for alt=media.
func (d *FX) serveGet(w http.ResponseWriter, r *http.Request, n *Node) {
	if r.URL.Query().Get("alt") != "media" {
		writeDriveJSON(w, http.StatusOK, fileJSON(n))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(n.Body))
}

func (d *FX) serveExport(w http.ResponseWriter, _ *http.Request, n *Node) {
	d.Exports++
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.Itoa(len(n.Export)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(n.Export)
}

// serveCreate answers files.create. A new Google Doc or Sheet starts empty
// in the Docs and Sheets state.
func (d *FX) serveCreate(w http.ResponseWriter, r *http.Request, _ string) {
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
			IsDir: mimeType == folderMIME, MD5: checksum(mimeType, media),
		},
	}
	d.Nodes[id] = n
	writeDriveJSON(w, http.StatusOK, fileJSON(n))
}

// servePatch answers files.update: a trash, or new bytes.
func (d *FX) servePatch(w http.ResponseWriter, r *http.Request, n *Node) {
	meta, media := readDriveUpload(r)
	if trashed, _ := meta["trashed"].(bool); trashed {
		delete(d.Nodes, n.Meta.ID)
		writeDriveJSON(w, http.StatusOK, fileJSON(n))
		return
	}
	n.Body = media
	n.Meta.Size = int64(len(media))
	n.Meta.ModTime = time.Now().UTC()
	n.Meta.MD5 = checksum(n.Meta.MimeType, media)
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
	if n.Meta.MD5 != "" {
		out["md5Checksum"] = n.Meta.MD5
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

// readDriveUpload is the JSON metadata and the media of a multipart upload,
// or the JSON metadata of a plain request.
func readDriveUpload(r *http.Request) (meta map[string]any, media []byte) {
	meta = map[string]any{}
	mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if !strings.HasPrefix(mediaType, "multipart/") {
		_ = json.NewDecoder(r.Body).Decode(&meta)
		return meta, nil
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err != nil {
			return meta, media
		}
		body, _ := io.ReadAll(part)
		if strings.Contains(part.Header.Get("Content-Type"), "json") {
			_ = json.Unmarshal(body, &meta)
		} else {
			media = body
		}
	}
}
