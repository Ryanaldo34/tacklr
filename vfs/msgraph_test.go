package vfs_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
	"github.com/ryanaldo34/tacklr/vfs/testhttp"
)

type graphNode struct {
	id, name, parent, mime, pkg string
	folder                      bool
	size                        int64
	lastMod                     string
	body                        []byte
	noContent                   bool
	contentStatus               int
}

type graphFX struct {
	mu         sync.Mutex
	base       string
	meDrive    string
	sites      map[string]string
	drives     map[string]string // driveID → root item id
	nodes      map[string]*graphNode
	onceStatus map[string]int
	pageSize   int
}

func newGraphFX() *graphFX {
	return &graphFX{
		meDrive:    "drv",
		sites:      map[string]string{"site-1": "site-drv"},
		drives:     map[string]string{"drv": "root", "site-drv": "root-site"},
		nodes:      map[string]*graphNode{},
		onceStatus: map[string]int{},
		pageSize:   2,
	}
}

func (g *graphFX) add(n *graphNode) {
	if n.lastMod == "" {
		n.lastMod = "2026-01-02T03:04:05Z"
	}
	if n.size == 0 && len(n.body) > 0 {
		n.size = int64(len(n.body))
	}
	g.nodes[n.id] = n
}

func (g *graphFX) legalTree() *graphFX {
	g.add(&graphNode{id: "root", name: "root", folder: true})
	g.add(&graphNode{id: "root-site", name: "root", folder: true})
	g.add(&graphNode{id: "f1", name: "a.txt", parent: "root", mime: "text/plain", body: []byte("one")})
	g.add(&graphNode{id: "xlsx1", name: "Budget.xlsx", parent: "root", mime: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"})
	g.add(&graphNode{id: "note1", name: "Notebook", parent: "root", pkg: "oneNote"})
	g.add(&graphNode{id: "gone", name: "gone.txt", parent: "root", mime: "text/plain", body: []byte("x"), noContent: true})
	g.add(&graphNode{id: "exp", name: "exp.txt", parent: "root", mime: "text/plain", body: []byte("x"), contentStatus: http.StatusUnauthorized})
	g.add(&graphNode{id: "c2", name: "c.txt", parent: "root", mime: "text/plain", body: []byte("c")})
	g.add(&graphNode{id: "fs", name: "note.txt", parent: "root-site", mime: "text/plain", body: []byte("from-site")})
	g.add(&graphNode{id: "txt1", name: "file.txt", mime: "text/plain", body: []byte("nope")})
	return g
}

func (g *graphFX) json(n *graphNode) map[string]any {
	it := map[string]any{
		"id": n.id, "name": n.name, "size": n.size,
		"lastModifiedDateTime": n.lastMod,
		"parentReference":      map[string]any{"id": n.parent},
	}
	switch {
	case n.folder:
		it["folder"] = map[string]any{}
	case n.pkg != "":
		it["package"] = map[string]any{"type": n.pkg}
	case n.mime != "":
		it["file"] = map[string]any{"mimeType": n.mime}
	default:
		it["file"] = map[string]any{}
	}
	return it
}

func writeGraphJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func readGraphBody(r *http.Request) []byte {
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

func (g *graphFX) kids(parent string) []*graphNode {
	var out []*graphNode
	for _, n := range g.nodes {
		if n.parent == parent {
			out = append(out, n)
		}
	}
	slices.SortFunc(out, func(a, b *graphNode) int { return strings.Compare(a.id, b.id) })
	return out
}

func (g *graphFX) byName(parent, name string) *graphNode {
	for _, n := range g.kids(parent) {
		if n.name == name {
			return n
		}
	}
	return nil
}

func (g *graphFX) lookupRel(itemID, rel string) *graphNode {
	cur := g.nodes[itemID]
	if cur == nil {
		return nil
	}
	if rel == "" {
		return cur
	}
	for _, part := range strings.Split(rel, "/") {
		cur = g.byName(cur.id, part)
		if cur == nil {
			return nil
		}
	}
	return cur
}

func (g *graphFX) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	p := r.URL.Path
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.Method == http.MethodGet {
		if status, ok := g.onceStatus[p]; ok {
			delete(g.onceStatus, p)
			w.WriteHeader(status)
			return
		}
	}
	switch {
	case p == "/me/drive" || p == "/users/me-token-to-replace/drive":
		writeGraphJSON(w, map[string]any{"id": g.meDrive})
		return
	case strings.HasPrefix(p, "/sites/") && strings.HasSuffix(p, "/drive"):
		site := strings.TrimSuffix(strings.TrimPrefix(p, "/sites/"), "/drive")
		if id, ok := g.sites[site]; ok {
			writeGraphJSON(w, map[string]any{"id": id})
			return
		}
	}
	driveID, itemID, rel, tail, isRoot := parseGraphPath(p)
	rootID := g.drives[driveID]
	if isRoot && itemID == "" {
		itemID = rootID
	}
	if tail == "content" {
		g.serveContent(w, r, itemID, rel)
		return
	}
	if tail == "children" && r.Method == http.MethodGet {
		g.serveChildren(w, r, itemID)
		return
	}
	if tail == "children" && r.Method == http.MethodPost {
		var body map[string]any
		_ = json.Unmarshal(readGraphBody(r), &body)
		name, _ := body["name"].(string)
		id := "dir-" + name
		n := &graphNode{id: id, name: name, parent: itemID, folder: true}
		g.add(n)
		writeGraphJSON(w, g.json(n))
		return
	}
	if r.Method == http.MethodDelete && itemID != "" && rel == "" && tail == "" {
		delete(g.nodes, itemID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	n := g.nodes[itemID]
	if rel != "" {
		n = g.lookupRel(itemID, rel)
	}
	if n == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeGraphJSON(w, g.json(n))
}

func (g *graphFX) serveContent(w http.ResponseWriter, r *http.Request, itemID, rel string) {
	n := g.nodes[itemID]
	if rel != "" {
		parent := itemID
		if n == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPut {
			body := readGraphBody(r)
			mime := "text/plain"
			if bytes.Contains(body, []byte("word/document.xml")) {
				mime = adapters.DOCXMediaType
			}
			id := "new-" + rel
			created := &graphNode{id: id, name: rel, parent: parent, mime: mime, body: append([]byte(nil), body...)}
			g.add(created)
			writeGraphJSON(w, g.json(created))
			return
		}
		n = g.lookupRel(itemID, rel)
	}
	if n == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Method == http.MethodPut {
		n.body = readGraphBody(r)
		n.size = int64(len(n.body))
		writeGraphJSON(w, g.json(n))
		return
	}
	if n.noContent {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if n.contentStatus != 0 {
		w.WriteHeader(n.contentStatus)
		return
	}
	if r.URL.Query().Get("direct") == "" && n.id == "redir" {
		http.Redirect(w, r, "/drives/drv/items/redir/content?direct=1", http.StatusFound)
		return
	}
	_, _ = w.Write(n.body)
}

func (g *graphFX) serveChildren(w http.ResponseWriter, r *http.Request, itemID string) {
	kids := g.kids(itemID)
	skip := r.URL.Query().Get("skip") != ""
	page := kids
	next := ""
	if g.pageSize > 0 && len(kids) > g.pageSize {
		if skip {
			page = kids[g.pageSize:]
		} else {
			page = kids[:g.pageSize]
			next = g.base + r.URL.Path + "?skip=1"
		}
	}
	value := make([]any, 0, len(page))
	for _, k := range page {
		value = append(value, g.json(k))
	}
	out := map[string]any{"value": value}
	if next != "" {
		out["@odata.nextLink"] = next
	}
	writeGraphJSON(w, out)
}

func parseGraphPath(p string) (driveID, itemID, rel, tail string, isRoot bool) {
	rest, ok := strings.CutPrefix(p, "/drives/")
	if !ok {
		return "", "", "", "", false
	}
	driveID, rest, _ = strings.Cut(rest, "/")
	switch {
	case rest == "root" || rest == "root/":
		return driveID, "", "", "", true
	case strings.HasPrefix(rest, "root:/"):
		rel, tail = splitRelTail(strings.TrimPrefix(rest, "root:/"))
		return driveID, "", unescapeGraphRel(rel), tail, true
	}
	rest = strings.TrimPrefix(rest, "items/")
	if id, after, ok := strings.Cut(rest, ":/"); ok {
		rel, tail = splitRelTail(after)
		return driveID, id, unescapeGraphRel(rel), tail, false
	}
	if id, extra, ok := strings.Cut(rest, "/"); ok {
		return driveID, id, "", extra, false
	}
	return driveID, rest, "", "", false
}

func splitRelTail(s string) (rel, tail string) {
	if name, extra, ok := strings.Cut(s, ":/"); ok {
		return name, extra
	}
	return s, ""
}

func unescapeGraphRel(rel string) string {
	parts := strings.Split(rel, "/")
	for i, p := range parts {
		if dec, err := url.PathUnescape(p); err == nil {
			parts[i] = dec
		}
	}
	return strings.Join(parts, "/")
}

func mountGraphHTTP(t *testing.T, srv *testhttp.Server, writable bool, members ...vfs.MountSpec) (*vfs.MountSession, *vfs.SessionAuth) {
	t.Helper()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Writable: writable, Auth: vfs.Credential{Token: "tok"},
		Params: map[string]string{vfs.ParamName: "legal"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.GraphFactory{
		ID: vfs.ProviderMicrosoft, Auth: auth, Account: vfs.AccountPersonal, Base: srv.URL, HTTP: srv.Client(),
	}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) == 0 {
		members = []vfs.MountSpec{{
			Profile:  vfs.ProviderMicrosoft,
			ReadOnly: !writable,
			Params:   map[string]string{vfs.ParamName: "legal"},
		}}
	}
	if err := ms.Mount(t.Context(), vfs.Workspace(members...)); err != nil {
		t.Fatal(err)
	}
	return ms, auth
}

func TestGraph_readWriteMkdirTrashRefreshAndErrors(t *testing.T) {
	ctx := t.Context()
	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	fx.base = srv.URL

	ms, auth := mountGraphHTTP(t, srv, true)
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
	for _, n := range []string{"a.txt", "Budget.xlsx", "Notebook", "c.txt"} {
		if !names[n] {
			t.Fatalf("ReadDir = %+v", ents)
		}
	}
	if _, err := ms.ReadDir(ctx, "/workspace/legal/a.txt"); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("file ReadDir: %v", err)
	}
	if _, err := ms.Open(ctx, "/workspace/legal"); err == nil || !errors.Is(err, vfs.ErrIsDir) {
		t.Fatalf("open dir: %v", err)
	}

	if err := ms.WriteFile(ctx, "/workspace/legal/a.txt", []byte("two")); err != nil {
		t.Fatal(err)
	}
	fx.mu.Lock()
	fx.onceStatus["/drives/drv/items/f1/content"] = http.StatusUnauthorized
	fx.mu.Unlock()
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
	if err := ms.WriteFile(ctx, "/workspace/legal/my file.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/legal/my file.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("create space = %q err=%v", got, err)
	}

	if err := ms.Remove(ctx, "/workspace/legal/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/legal/a.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed: %v", err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/legal/Notebook"); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("onenote: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/legal/gone.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("content 404: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/legal/exp.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("content 401: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/legal/huge.bin", make([]byte, vfs.MaxReadFileBytes+1)); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize put: %v", err)
	}
	if err := ms.WriteDocument(ctx, vfs.NewTextDocument("/workspace/legal/note.md", "text/markdown", "utf-8", "# hi\n")); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/legal/note.md")
	if err != nil || !strings.Contains(string(got), "# hi") {
		t.Fatalf("WriteDocument = %q err=%v", got, err)
	}
	if err := adapters.RegisterCommon(vfs.DefaultContentRegistry()); err != nil {
		t.Fatal(err)
	}
	html := "<h1>x</h1>"
	if _, err := ms.Apply(ctx, "/workspace/legal/SPIKE", vfs.Mutation{Content: &html}); err != nil {
		t.Fatal(err)
	}
	st, err = ms.Stat(ctx, "/workspace/legal/SPIKE")
	if err != nil || st.MediaType != adapters.DOCXMediaType {
		t.Fatalf("extensionless HTML graph Stat=%+v err=%v", st, err)
	}
	word, err := ms.ReadText(ctx, "/workspace/legal/SPIKE")
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := vfs.AsRich(word)
	if !ok || len(rd.Blocks()) == 0 || rd.Blocks()[0].Text != "x" {
		t.Fatalf("graph Word IR = %+v ok=%v", word, ok)
	}
	if err := ms.MkdirAll(ctx, "/workspace/legal/Budget.xlsx/nope"); err == nil || !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("mkdir through file: %v", err)
	}

	ro, _ := mountGraphHTTP(t, srv, false)
	got, err = ro.ReadFile(ctx, "/workspace/legal/c.txt")
	if err != nil || string(got) != "c" {
		t.Fatalf("ro read = %q err=%v", got, err)
	}
	if err := ro.WriteFile(ctx, "/workspace/legal/c.txt", []byte("nope")); !errors.Is(err, vfs.ErrReadOnly) {
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
	fx := newGraphFX().legalTree()
	fx.nodes["xlsx1"].body = raw
	fx.nodes["xlsx1"].size = int64(len(raw))
	srv := testhttp.New(t, fx)
	fx.base = srv.URL
	ms, _ := mountGraphHTTP(t, srv, true)

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
	grid, ok := vfs.AsGrid(got)
	if !ok {
		t.Fatalf("reload type %T", got)
	}
	if grid.Sheets()[0].Cells[1][1].Input != "99" && grid.Sheets()[0].Cells[1][1].Value != "99" {
		t.Fatalf("cell = %+v", grid.Sheets()[0].Cells[1][1])
	}
}

func TestGraphAndDrive_writesStayOnMatchingProviders(t *testing.T) {
	ctx := t.Context()
	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	fx.base = srv.URL
	driveAPI := driveTree()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: "gdrive", Writable: true, Auth: vfs.Credential{Token: "gd"},
		Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Writable: true, Auth: vfs.Credential{Token: "tok"},
		Params: map[string]string{vfs.ParamName: "legal"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: driveAPI}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth, Account: vfs.AccountPersonal, Base: srv.URL, HTTP: srv.Client()}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.Workspace(
		vfs.BindingMember(vfs.Binding{Provider: "gdrive", Writable: true, Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"}}),
		vfs.BindingMember(vfs.Binding{Provider: vfs.ProviderMicrosoft, Writable: true, Params: map[string]string{vfs.ParamName: "legal"}}),
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
	if tok, ok := auth.Credential("s", vfs.ProviderMicrosoft); !ok || tok.Token != "tok" {
		t.Fatalf("graph token after drive refresh = %+v ok=%v", tok, ok)
	}
}

func TestGraphFactory_openRequiresFolderTokenAndId(t *testing.T) {
	ctx := t.Context()
	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	fx.base = srv.URL
	auth := vfs.NewSessionAuth()
	_ = auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Auth: vfs.Credential{Token: "tok"},
		Params: map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "drv", vfs.ParamItemID: "txt1"},
	})
	reg := vfs.NewBackendRegistry()
	_ = reg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth, Base: srv.URL, HTTP: srv.Client()})
	ms, err := vfs.NewMountSession("s", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.BindingSpec(vfs.Binding{
		Provider: vfs.ProviderMicrosoft,
		Params:   map[string]string{vfs.ParamName: "legal", vfs.ParamDriveID: "drv", vfs.ParamItemID: "txt1"},
	})); err == nil {
		t.Fatal("file itemId must fail")
	}

	siteAuth := vfs.NewSessionAuth()
	_ = siteAuth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Auth: vfs.Credential{Token: "tok"},
		Params: map[string]string{vfs.ParamName: "lib"},
	})
	siteReg := vfs.NewBackendRegistry()
	_ = siteReg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: siteAuth, Base: srv.URL, HTTP: srv.Client()})
	siteMS, err := vfs.NewMountSession("s", siteReg)
	if err != nil {
		t.Fatal(err)
	}
	if err := siteMS.Mount(ctx, vfs.Workspace(
		vfs.MountSpec{Profile: vfs.ProviderMicrosoft, Params: map[string]string{vfs.ParamName: "lib", vfs.ParamSiteID: "site-1"}},
	)); err != nil {
		t.Fatal(err)
	}
	got, err := siteMS.ReadFile(ctx, "/workspace/lib/note.txt")
	if err != nil || string(got) != "from-site" {
		t.Fatalf("site bind = %q err=%v", got, err)
	}

	if _, err := (vfs.GraphFactory{}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("want factory id")
	}
	if _, err := (vfs.GraphFactory{ID: "msgraph"}).Open(ctx, "s", vfs.MountSpec{}); err == nil {
		t.Fatal("want token")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := (vfs.GraphFactory{ID: "msgraph"}).Open(canceled, "s", vfs.MountSpec{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

func TestGraphFactory_organizationRequiresSiteOrDrive(t *testing.T) {
	ctx := t.Context()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", vfs.Binding{
		Provider: vfs.ProviderMicrosoft, Auth: vfs.Credential{Token: "tok"},
		Params: map[string]string{vfs.ParamName: "lib"},
	}); err != nil {
		t.Fatal(err)
	}
	f := vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth}
	if _, err := f.Open(ctx, "s", vfs.MountSpec{Params: map[string]string{vfs.ParamName: "lib"}}); err == nil || !strings.Contains(err.Error(), "siteId or driveId") {
		t.Fatalf("default organization: %v", err)
	}
	if _, err := f.Open(ctx, "s", vfs.MountSpec{Params: map[string]string{vfs.ParamAccount: "nope"}}); err == nil || !strings.Contains(err.Error(), "not organization or personal") {
		t.Fatalf("bad account: %v", err)
	}

	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	personal := vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth, Account: vfs.AccountPersonal, Base: srv.URL, HTTP: srv.Client()}
	got, err := personal.Open(ctx, "s", vfs.MountSpec{Params: map[string]string{vfs.ParamName: "lib"}})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("personal /me/drive")
	}
}
