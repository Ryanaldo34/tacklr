package vfs_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

func graphColonRel(p string) (string, bool) {
	_, rest, ok := strings.Cut(p, ":/")
	if !ok || strings.Contains(rest, ":/") {
		return "", false
	}
	name, err := url.PathUnescape(rest)
	if err != nil {
		return "", false
	}
	return name, true
}

func writeGraphJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func readGraphRequest(r *http.Request) []byte {
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil
		}
		defer gz.Close()
		reader = gz
	}
	body, _ := io.ReadAll(reader)
	return body
}

func writeGraphNamed(w http.ResponseWriter, name string, items ...map[string]any) bool {
	for _, it := range items {
		if it["name"] == name {
			writeGraphJSON(w, it)
			return true
		}
	}
	return false
}

func graphTree() *memGraph {
	g := newMemGraph("root-g", "Legal")
	g.add("root-g", vfs.GraphItem{ID: "txt1", Name: "a.txt", Mime: "text/plain", Size: 3}, []byte("one"))
	g.add("root-g", vfs.GraphItem{ID: "dup1", Name: "dup.txt", Mime: "text/plain", Size: 1}, []byte("a"))
	g.add("root-g", vfs.GraphItem{ID: "dup2", Name: "dup.txt", Mime: "text/plain", Size: 1}, []byte("b"))
	g.add("root-g", vfs.GraphItem{ID: "note1", Name: "Notebook", Mime: "oneNote"}, nil)
	g.add("root-g", vfs.GraphItem{
		ID: "xlsx1", Name: "Budget.xlsx", Size: 1,
		Mime: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	}, nil)
	return g
}

func mountGraph(t *testing.T, api *memGraph, writable bool) (*vfs.MountSession, *vfs.SessionAuth) {
	t.Helper()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Point: vfs.WorkspacePoint, Writable: writable,
		Auth:   vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "me-drive", vfs.ParamItemID: "root-g"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth, API: api}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(t.Context(), vfs.BindingSpec(vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Point: vfs.WorkspacePoint, Writable: writable,
		Params: map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "me-drive", vfs.ParamItemID: "root-g"},
	})); err != nil {
		t.Fatal(err)
	}
	return ms, auth
}

func TestGraph_readWriteTrashMkdirAmbiguousOversize(t *testing.T) {
	ctx := t.Context()
	api := graphTree()
	ms, auth := mountGraph(t, api, true)

	got, err := ms.ReadFile(ctx, "/workspace/legal/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("read = %q err=%v", got, err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/legal")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range ents {
		names[e.Name] = true
	}
	for _, n := range []string{"a.txt", "Budget.xlsx", "Notebook", "dup.txt"} {
		if !names[n] {
			t.Fatalf("ReadDir /workspace/legal = %+v", ents)
		}
	}

	if err := ms.WriteFile(ctx, "/workspace/legal/a.txt", []byte("two")); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/legal/a.txt")
	if err != nil || string(got) != "two" {
		t.Fatalf("after write = %q err=%v", got, err)
	}

	api.once["GetContent"] = vfs.ErrAuthExpired
	auth.Holder("s", vfs.ProviderMicrosoft).SetRefresh(func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{Token: "fresh"}, nil
	})
	got, err = ms.ReadFile(ctx, "/workspace/legal/a.txt")
	if err != nil || string(got) != "two" {
		t.Fatalf("refresh read = %q err=%v", got, err)
	}

	if err := ms.MkdirAll(ctx, "/workspace/legal/sub/dir"); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/workspace/legal/sub/dir")
	if err != nil || !st.IsDir {
		t.Fatalf("mkdir stat = %+v err=%v", st, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/legal/sub/dir/c.txt", []byte("nested")); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/legal/sub/dir/c.txt")
	if err != nil || string(got) != "nested" {
		t.Fatalf("nested write = %q err=%v", got, err)
	}

	if err := ms.Remove(ctx, "/workspace/legal/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/legal/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed: %v", err)
	}

	if _, err := ms.ReadFile(ctx, "/workspace/legal/dup.txt"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("ambiguous: %v", err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/legal/Notebook"); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("onenote: %v", err)
	}

	if err := ms.WriteFile(ctx, "/workspace/legal/huge.bin", make([]byte, vfs.MaxReadFileBytes+1)); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize put: %v", err)
	}

	ro, _ := mountGraph(t, graphTree(), false)
	got, err = ro.ReadFile(ctx, "/workspace/legal/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("ro read = %q err=%v", got, err)
	}
	if err := ro.WriteFile(ctx, "/workspace/legal/a.txt", []byte("nope")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro put: %v", err)
	}
}

func TestGraph_xlsxCodecCellOverlayPersists(t *testing.T) {
	if err := adapters.RegisterCommon(vfs.DefaultContentRegistry()); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	raw, err := (adapters.XLSX{}).EncodeSheets(ctx, []vfs.Sheet{{
		ID: "1", Title: "Budget", Rows: 2, Cols: 2,
		Cells: [][]vfs.Cell{
			{{Input: "A", Value: "A"}, {Input: "B", Value: "B"}},
			{{Input: "1", Value: "1"}, {Input: "42", Value: "42"}},
		},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	api := graphTree()
	api.nodes["xlsx1"].body = raw
	api.nodes["xlsx1"].meta.Size = int64(len(raw))
	ms, _ := mountGraph(t, api, true)

	doc, err := ms.ReadText(ctx, "/workspace/legal/Budget.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := vfs.AsGrid(doc); !ok {
		t.Fatalf("type %T", doc)
	}
	body := "99"
	if _, err := ms.Apply(ctx, "/workspace/legal/Budget.xlsx", vfs.Mutation{
		Rev: vfs.ContentToken(doc), BlockID: "Budget!B2", Body: &body,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/workspace/legal/Budget.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	g, ok := vfs.AsGrid(got)
	if !ok {
		t.Fatalf("reload type %T", got)
	}
	if g.Sheets()[0].Cells[1][1].Input != "99" && g.Sheets()[0].Cells[1][1].Value != "99" {
		t.Fatalf("cell = %+v", g.Sheets()[0].Cells[1][1])
	}
}

func TestGraphAndDrive_writesStayOnMatchingFakes(t *testing.T) {
	ctx := t.Context()
	driveAPI := driveTree()
	graphAPI := graphTree()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Writable: true, Auth: vfs.Credential{Token: "gd"},
		Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Writable: true, Auth: vfs.Credential{Token: "ms"},
		Params: map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "me-drive", vfs.ParamItemID: "root-g"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: driveAPI}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth, API: graphAPI}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.Workspace(
		vfs.BindingMember(vfs.Binding{Provider: "gdrive", Writable: true, Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"}}),
		vfs.BindingMember(vfs.Binding{Provider: vfs.ProviderMicrosoft, Writable: true, Params: map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "me-drive", vfs.ParamItemID: "root-g"}}),
	)); err != nil {
		t.Fatal(err)
	}

	if err := ms.WriteFile(ctx, "/workspace/contracts/new.txt", []byte("drive")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/legal/b.txt", []byte("graph")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/legal/b.txt")
	if err != nil || string(got) != "graph" {
		t.Fatalf("graph read = %q err=%v", got, err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/contracts/new.txt")
	if err != nil || string(got) != "drive" {
		t.Fatalf("drive read = %q err=%v", got, err)
	}
	if err := auth.Refresh("s", "gdrive", vfs.Credential{Token: "gd2"}); err != nil {
		t.Fatal(err)
	}
	if tok, ok := auth.Credential("s", vfs.ProviderMicrosoft); !ok || tok.Token != "ms" {
		t.Fatalf("graph token after drive refresh = %+v ok=%v", tok, ok)
	}
	got, err = ms.ReadFile(ctx, "/workspace/legal/b.txt")
	if err != nil || string(got) != "graph" {
		t.Fatalf("graph after drive refresh = %q err=%v", got, err)
	}
}

func TestGraphFactory_openRequiresFolder(t *testing.T) {
	ctx := t.Context()
	api := graphTree()
	auth := vfs.NewSessionAuth()
	_ = auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Auth: vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "me-drive", vfs.ParamItemID: "txt1"},
	})
	reg := vfs.NewBackendRegistry()
	_ = reg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth, API: api})
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.BindingSpec(vfs.Binding{
		Provider: vfs.ProviderMicrosoft,
		Params:   map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "me-drive", vfs.ParamItemID: "txt1"},
	})); err == nil {
		t.Fatal("file itemId must fail")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := (vfs.GraphFactory{ID: "msgraph"}).Open(canceled, "s", vfs.MountSpec{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

func mountGraphHTTP(t *testing.T, base string, members ...vfs.MountSpec) *vfs.MountSession {
	t.Helper()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Auth: vfs.Credential{Token: "tok"},
		Params: map[string]string{vfs.ParamName: "legal"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth, Base: base}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(t.Context(), vfs.Workspace(members...)); err != nil {
		t.Fatal(err)
	}
	return ms
}

func graphJSONFile(id, name string, size int) map[string]any {
	return map[string]any{"id": id, "name": name, "size": size, "file": map[string]any{"mimeType": "text/plain"}, "parentReference": map[string]any{"id": "root"}}
}

func graphJSONFolder(id, name string) map[string]any {
	return map[string]any{"id": id, "name": name, "folder": map[string]any{}, "parentReference": map[string]any{"id": "root"}}
}

func TestGraphHTTP_bytesPersistAndCreate(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	files := map[string][]byte{"f1": []byte("abc")}
	created := map[string]map[string]any{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		p := r.URL.Path
		mu.Lock()
		defer mu.Unlock()
		switch {
		case p == "/me/drive":
			writeGraphJSON(w, map[string]any{"id": "drv", "folder": map[string]any{}})
		case p == "/drives/drv/root" || p == "/drives/drv/items/root":
			writeGraphJSON(w, graphJSONFolder("root", "root"))
		case p == "/drives/drv/items/root/children" && r.Method == http.MethodGet:
			value := []any{graphJSONFile("f1", "a.txt", len(files["f1"]))}
			for _, it := range created {
				value = append(value, it)
			}
			writeGraphJSON(w, map[string]any{"value": value})
		case p == "/drives/drv/items/f1/content":
			switch r.Method {
			case http.MethodGet:
				_, _ = w.Write(files["f1"])
			case http.MethodPut:
				body := readGraphRequest(r)
				files["f1"] = append([]byte(nil), body...)
				writeGraphJSON(w, graphJSONFile("f1", "a.txt", len(body)))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		case r.Method == http.MethodPut && strings.Contains(p, ":/") && strings.HasSuffix(p, ":/content"):
			body := readGraphRequest(r)
			inner := strings.TrimPrefix(p, "/drives/drv/items/")
			_, rest, _ := strings.Cut(inner, ":/")
			name := strings.TrimSuffix(rest, ":/content")
			if decoded, err := url.PathUnescape(name); err == nil {
				name = decoded
			}
			id := "new-" + name
			files[id] = append([]byte(nil), body...)
			created[id] = graphJSONFile(id, name, len(body))
			writeGraphJSON(w, created[id])
		case strings.HasPrefix(p, "/drives/drv/items/") && strings.HasSuffix(p, "/content") && r.Method == http.MethodGet:
			id := strings.TrimSuffix(strings.TrimPrefix(p, "/drives/drv/items/"), "/content")
			if body, ok := files[id]; ok {
				_, _ = w.Write(body)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			if r.Method == http.MethodGet {
				if rel, ok := graphColonRel(p); ok {
					if rel == "a.txt" {
						writeGraphJSON(w, graphJSONFile("f1", "a.txt", len(files["f1"])))
						return
					}
					for _, it := range created {
						if writeGraphNamed(w, rel, it) {
							return
						}
					}
				}
			}
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)

	ms := mountGraphHTTP(t, ts.URL, vfs.MountSpec{
		Profile: vfs.ProviderMicrosoft,
		Params:  map[string]string{vfs.ParamName: "legal"},
	})
	got, err := ms.ReadFile(ctx, "/workspace/legal/a.txt")
	if err != nil || string(got) != "abc" {
		t.Fatalf("http read = %q err=%v", got, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/legal/a.txt", []byte("xyz")); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/legal/a.txt")
	if err != nil || string(got) != "xyz" {
		t.Fatalf("http write = %q err=%v", got, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/legal/my file.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/legal/my file.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("http create = %q err=%v", got, err)
	}
}

func TestGraphHTTP_siteDriveWinsOverEmpty(t *testing.T) {
	ctx := t.Context()
	serveDrive := func(w http.ResponseWriter, r *http.Request, drive, root, fileID, name, body string) bool {
		switch r.URL.Path {
		case "/drives/" + drive + "/root", "/drives/" + drive + "/items/" + root:
			writeGraphJSON(w, graphJSONFolder(root, "root"))
			return true
		case "/drives/" + drive + "/items/" + root + "/children":
			writeGraphJSON(w, map[string]any{"value": []any{graphJSONFile(fileID, name, len(body))}})
			return true
		case "/drives/" + drive + "/items/" + fileID + "/content":
			_, _ = w.Write([]byte(body))
			return true
		case "/drives/" + drive + "/items/" + fileID:
			writeGraphJSON(w, graphJSONFile(fileID, name, len(body)))
			return true
		}
		if r.Method == http.MethodGet {
			if rel, ok := graphColonRel(r.URL.Path); ok && rel == name && strings.Contains(r.URL.Path, "/drives/"+drive+"/") {
				writeGraphJSON(w, graphJSONFile(fileID, name, len(body)))
				return true
			}
		}
		return false
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sites/site-1/drive" {
			writeGraphJSON(w, map[string]any{"id": "site-drv", "folder": map[string]any{}})
			return
		}
		if serveDrive(w, r, "drv-set", "root-set", "fd", "note.txt", "from-drive") {
			return
		}
		if serveDrive(w, r, "site-drv", "root-site", "fs", "note.txt", "from-site") {
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)

	ms := mountGraphHTTP(t, ts.URL,
		vfs.MountSpec{Profile: vfs.ProviderMicrosoft, Params: map[string]string{vfs.ParamName: "sp", vfs.ParamSiteID: "site-1", vfs.ParamDriveID: "drv-set"}},
		vfs.MountSpec{Profile: vfs.ProviderMicrosoft, Params: map[string]string{vfs.ParamName: "lib", vfs.ParamSiteID: "site-1"}},
	)
	got, err := ms.ReadFile(ctx, "/workspace/sp/note.txt")
	if err != nil || string(got) != "from-drive" {
		t.Fatalf("driveId bind = %q err=%v", got, err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/lib/note.txt")
	if err != nil || string(got) != "from-site" {
		t.Fatalf("siteId bind = %q err=%v", got, err)
	}
}

func TestGraphHTTP_errorsPaginationMkdirTrash(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	kids := []map[string]any{
		graphJSONFile("f1", "a.txt", 3),
		graphJSONFile("gone", "gone.txt", 1),
		graphJSONFile("exp", "exp.txt", 1),
	}
	page2 := []map[string]any{graphJSONFile("f2", "c.txt", 1)}
	bodies := map[string][]byte{"f1": []byte("abc"), "f2": []byte("c")}
	dirs := map[string]map[string]any{}
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		p := r.URL.Path
		mu.Lock()
		defer mu.Unlock()
		switch {
		case p == "/me/drive":
			writeGraphJSON(w, map[string]any{"id": "drv", "folder": map[string]any{}})
		case p == "/drives/drv/root" || p == "/drives/drv/items/root":
			writeGraphJSON(w, graphJSONFolder("root", "root"))
		case p == "/drives/drv/items/root/children" && r.Method == http.MethodGet:
			if r.URL.Query().Get("skip") == "1" {
				writeGraphJSON(w, map[string]any{"value": page2})
				return
			}
			value := make([]any, 0, len(kids)+len(dirs))
			for _, k := range kids {
				value = append(value, k)
			}
			for _, d := range dirs {
				value = append(value, d)
			}
			writeGraphJSON(w, map[string]any{
				"value":           value,
				"@odata.nextLink": ts.URL + "/drives/drv/items/root/children?skip=1",
			})
		case p == "/drives/drv/items/root/children" && r.Method == http.MethodPost:
			var body map[string]any
			_ = json.Unmarshal(readGraphRequest(r), &body)
			name, _ := body["name"].(string)
			id := "dir-" + name
			dirs[id] = graphJSONFolder(id, name)
			writeGraphJSON(w, dirs[id])
		case p == "/drives/drv/items/gone/content":
			w.WriteHeader(http.StatusNotFound)
		case p == "/drives/drv/items/exp/content":
			w.WriteHeader(http.StatusUnauthorized)
		case strings.HasSuffix(p, "/content") && r.Method == http.MethodGet:
			id := strings.TrimSuffix(strings.TrimPrefix(p, "/drives/drv/items/"), "/content")
			if b, ok := bodies[id]; ok {
				_, _ = w.Write(b)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		case strings.HasPrefix(p, "/drives/drv/items/") && r.Method == http.MethodDelete:
			id := strings.TrimPrefix(p, "/drives/drv/items/")
			kept := kids[:0]
			for _, k := range kids {
				if k["id"] != id {
					kept = append(kept, k)
				}
			}
			kids = kept
			delete(bodies, id)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasPrefix(p, "/drives/drv/items/"):
			if rel, ok := graphColonRel(p); ok && r.Method == http.MethodGet {
				if writeGraphNamed(w, rel, kids...) {
					return
				}
				if writeGraphNamed(w, rel, page2...) {
					return
				}
				for _, d := range dirs {
					if writeGraphNamed(w, rel, d) {
						return
					}
				}
				w.WriteHeader(http.StatusNotFound)
				return
			}
			id := strings.TrimPrefix(p, "/drives/drv/items/")
			if d, ok := dirs[id]; ok {
				writeGraphJSON(w, d)
				return
			}
			for _, k := range kids {
				if k["id"] == id {
					writeGraphJSON(w, k)
					return
				}
			}
			for _, k := range page2 {
				if k["id"] == id {
					writeGraphJSON(w, k)
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)

	ms := mountGraphHTTP(t, ts.URL, vfs.MountSpec{
		Profile: vfs.ProviderMicrosoft,
		Params:  map[string]string{vfs.ParamName: "legal"},
	})
	ents, err := ms.ReadDir(ctx, "/workspace/legal")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range ents {
		names[e.Name] = true
	}
	if !names["a.txt"] || !names["c.txt"] {
		t.Fatalf("paginated ReadDir = %+v", ents)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/legal/gone.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("content 404: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/legal/exp.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("content 401: %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/legal/sub"); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/workspace/legal/sub")
	if err != nil || !st.IsDir {
		t.Fatalf("mkdir stat = %+v err=%v", st, err)
	}
	if err := ms.Remove(ctx, "/workspace/legal/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/legal/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed: %v", err)
	}
}
