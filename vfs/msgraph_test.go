package vfs_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
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
	g.add(&graphNode{id: "redir", name: "moved.txt", parent: "root", mime: "text/plain", body: []byte("moved")})
	return g
}

func (g *graphFX) json(n *graphNode) map[string]any {
	it := map[string]any{
		"id": n.id, "name": n.name, "size": n.size,
		"lastModifiedDateTime": n.lastMod,
		"parentReference":      map[string]any{"id": n.parent},
		"cTag":                 fmt.Sprintf("%x", sha256.Sum256(n.body)),
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
	body, _ := io.ReadAll(r.Body)
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
	case p == "/me/drive":
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
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(n.body))
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

func mustGraph(t *testing.T, srv *httptest.Server, holder *vfs.TokenHolder, account string) vfs.Open {
	t.Helper()
	if holder == nil {
		holder = vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	}
	return vfs.Graph(vfs.NewMicrosoftGraph(holder, srv.URL+"/", srv.Client()), holder, account)
}

func mountGraphHTTP(t *testing.T, srv *httptest.Server, writable bool) (*vfs.MountSession, *vfs.TokenHolder) {
	t.Helper()
	holder := vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	open := mustGraph(t, srv, holder, vfs.AccountPersonal)
	ms, err := vfs.Tree(vfs.At("legal", open)).WithCodecs(adapters.Codecs()...)(t.Context(), "s", vfs.Request{Bindings: []mount.Binding{{
		Provider: mount.ProviderMicrosoft, Writable: writable,
		Params: map[string]string{mount.ParamName: "legal"},
		Auth:   mount.Credential{Token: "tok"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return ms, holder
}

func TestGraph_readWriteMkdirTrashRefreshAndErrors(t *testing.T) {
	ctx := t.Context()
	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	fx.base = srv.URL

	ms, holder := mountGraphHTTP(t, srv, true)

	got, err := ms.Route(ctx, "/workspace/legal/a.txt").
		ReadFile(ctx)
	if err != nil || string(got) != "one" {
		t.Fatalf("read = %q err=%v", got, err)
	}

	ents, err := ms.Route(ctx, "/workspace/legal").
		ReadDir(ctx)
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

	if got, err := ms.Route(ctx, "/workspace/legal/moved.txt").
		ReadFile(ctx); err != nil || string(got) != "moved" {
		t.Fatalf("redirected content = %q err=%v", got, err)
	}

	if _, err := ms.Route(ctx, "/workspace/legal/a.txt").
		ReadDir(ctx); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("file ReadDir: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/legal").
		Open(ctx); err == nil || !errors.Is(err, vfs.ErrIsDir) {
		t.Fatalf("open dir: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/legal/a.txt").
		WriteFile(ctx, []byte("two")); err != nil {
		t.Fatal(err)
	}

	fx.mu.Lock()
	fx.onceStatus["/drives/drv/items/f1/content"] = http.StatusUnauthorized
	fx.mu.Unlock()
	holder.SetRefresh(func(context.Context) (mount.Credential, error) {
		return mount.Credential{Token: "fresh"}, nil
	})

	got, err = ms.Route(ctx, "/workspace/legal/a.txt").
		ReadFile(ctx)
	if err != nil || string(got) != "two" {
		t.Fatalf("refresh read = %q err=%v", got, err)
	}

	if err := ms.Route(ctx, "/workspace/legal/sub/dir").
		MkdirAll(ctx); err != nil {
		t.Fatal(err)
	}

	st, err := ms.Route(ctx, "/workspace/legal/sub/dir").
		Stat(ctx)
	if err != nil || !st.IsDir {
		t.Fatalf("mkdir stat = %+v err=%v", st, err)
	}

	if err := ms.Route(ctx, "/workspace/legal/sub/dir/c.txt").
		WriteFile(ctx, []byte("nested")); err != nil {
		t.Fatal(err)
	}

	got, err = ms.Route(ctx, "/workspace/legal/sub/dir/c.txt").
		ReadFile(ctx)
	if err != nil || string(got) != "nested" {
		t.Fatalf("nested write = %q err=%v", got, err)
	}

	if err := ms.Route(ctx, "/workspace/legal/my file.txt").
		WriteFile(ctx, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	got, err = ms.Route(ctx, "/workspace/legal/my file.txt").
		ReadFile(ctx)
	if err != nil || string(got) != "hello" {
		t.Fatalf("create space = %q err=%v", got, err)
	}

	if err := ms.Route(ctx, "/workspace/legal/a.txt").
		Remove(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Route(ctx, "/workspace/legal/a.txt").
		ReadFile(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("trashed: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/legal/Notebook").
		ReadText(ctx); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("onenote: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/legal/gone.txt").
		ReadFile(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("content 404: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/legal/exp.txt").
		ReadFile(ctx); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("content 401: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/legal/huge.bin").
		WriteFile(ctx, make([]byte, vfs.MaxReadFileBytes+1)); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize put: %v", err)
	}

	if err := ms.WriteDocument(ctx, vfs.NewTextDocument("/workspace/legal/note.md", "text/markdown", "utf-8", "# hi\n")); err != nil {
		t.Fatal(err)
	}

	got, err = ms.Route(ctx, "/workspace/legal/note.md").
		ReadFile(ctx)
	if err != nil || !strings.Contains(string(got), "# hi") {
		t.Fatalf("WriteDocument = %q err=%v", got, err)
	}
	html := "<h1>x</h1>"
	if _, err := ms.Apply(ctx, "/workspace/legal/SPIKE", vfs.Mutation{Content: &html}); err != nil {
		t.Fatal(err)
	}

	if st, err := ms.Route(ctx, "/workspace/legal/SPIKE").
		Stat(ctx); err != nil || st.MediaType != adapters.DOCXMediaType {
		t.Fatalf("extensionless HTML graph Stat=%+v err=%v", st, err)
	}

	word, err := ms.Route(ctx, "/workspace/legal/SPIKE").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd, ok := vfs.AsRich(word)
	if !ok || len(rd.Blocks()) == 0 || rd.Blocks()[0].Text != "x" {
		t.Fatalf("graph Word IR = %+v ok=%v", word, ok)
	}

	if err := ms.Route(ctx, "/workspace/legal/Budget.xlsx/nope").
		MkdirAll(ctx); err == nil || !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("mkdir through file: %v", err)
	}

	ro, _ := mountGraphHTTP(t, srv, false)

	if got, err := ro.Route(ctx, "/workspace/legal/c.txt").
		ReadFile(ctx); err != nil || string(got) != "c" {
		t.Fatalf("ro read = %q err=%v", got, err)
	}

	if err := ro.Route(ctx, "/workspace/legal/c.txt").
		WriteFile(ctx, []byte("nope")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro put: %v", err)
	}
}

func TestGraphAndDrive_writesStayOnMatchingProviders(t *testing.T) {
	ctx := t.Context()
	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	fx.base = srv.URL
	driveAPI := testdrive.Tree()
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("s", mount.Binding{
		Provider: "gdrive", Writable: true, Auth: mount.Credential{Token: "gd"},
		Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.Bind("s", mount.Binding{
		Provider: mount.ProviderMicrosoft, Writable: true, Auth: mount.Credential{Token: "tok"},
		Params: map[string]string{mount.ParamName: "legal"},
	}); err != nil {
		t.Fatal(err)
	}
	driveLive := auth.Holder("s", "gdrive")
	graphLive := auth.Holder("s", mount.ProviderMicrosoft)
	ms, err := vfs.Tree(
		vfs.At("contracts", testdrive.Open(t, driveAPI, driveLive)),
		vfs.At("legal", mustGraph(t, srv, graphLive, vfs.AccountPersonal)),
	)(ctx, "s", vfs.Request{Bindings: []mount.Binding{
		{Provider: "gdrive", Writable: true, Auth: mount.Credential{Token: "gd"}, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"}},
		{Provider: mount.ProviderMicrosoft, Writable: true, Auth: mount.Credential{Token: "tok"}, Params: map[string]string{mount.ParamName: "legal"}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/contracts/new.txt").
		WriteFile(ctx, []byte("drive")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/legal/b.txt").
		WriteFile(ctx, []byte("graph")); err != nil {
		t.Fatal(err)
	}

	got, err := ms.Route(ctx, "/workspace/legal/b.txt").
		ReadFile(ctx)
	if err != nil || string(got) != "graph" {
		t.Fatalf("graph read = %q err=%v", got, err)
	}

	if got, err := ms.Route(ctx, "/workspace/contracts/new.txt").
		ReadFile(ctx); err != nil || string(got) != "drive" {
		t.Fatalf("drive read = %q err=%v", got, err)
	}
	if err := auth.Refresh("s", "gdrive", mount.Credential{Token: "gd2"}); err != nil {
		t.Fatal(err)
	}
	if tok, ok := auth.Credential("s", mount.ProviderMicrosoft); !ok || tok.Token != "tok" {
		t.Fatalf("graph token after drive refresh = %+v ok=%v", tok, ok)
	}
}

// TestGraphFactory_aBindingsParamsChooseTheDriveAndBadOnesFail: a file item
// id, an organization binding without a site or drive, and an unknown account
// fail; a site binding opens that site's library and a personal one /me/drive.
func TestGraphFactory_aBindingsParamsChooseTheDriveAndBadOnesFail(t *testing.T) {
	ctx := t.Context()
	fx := newGraphFX().legalTree()
	srv := testhttp.New(t, fx)
	fx.base = srv.URL
	open := mustGraph(t, srv, nil, "")
	if _, err := open(ctx, "s", mount.Binding{
		Auth:   mount.Credential{Token: "tok"},
		Params: map[string]string{mount.ParamName: "legal", mount.ParamDriveID: "drv", mount.ParamItemID: "txt1"},
	}); err == nil {
		t.Fatal("file itemId must fail")
	}

	siteMS, err := vfs.Tree(vfs.At("lib", open))(ctx, "s", vfs.Request{Bindings: []mount.Binding{{
		Auth:   mount.Credential{Token: "tok"},
		Params: map[string]string{mount.ParamName: "lib", mount.ParamSiteID: "site-1"},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	if got, err := siteMS.Route(ctx, "/workspace/lib/note.txt").
		ReadFile(ctx); err != nil || string(got) != "from-site" {
		t.Fatalf("site bind = %q err=%v", got, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := mustGraph(t, srv, nil, vfs.AccountPersonal)(canceled, "s", mount.Binding{Auth: mount.Credential{Token: "tok"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}

	// The account type picks where a binding without a site or drive opens.
	tok := mount.Binding{Auth: mount.Credential{Token: "tok"}, Params: map[string]string{mount.ParamName: "lib"}}
	if _, err := open(ctx, "s", tok); err == nil || !strings.Contains(err.Error(), "siteId or driveId") {
		t.Fatalf("default organization: %v", err)
	}
	if _, err := open(ctx, "s", mount.Binding{Auth: mount.Credential{Token: "tok"}, Params: map[string]string{mount.ParamAccount: "nope"}}); err == nil || !strings.Contains(err.Error(), "not organization or personal") {
		t.Fatalf("bad account: %v", err)
	}

	if _, err := open(ctx, "s", mount.Binding{Auth: mount.Credential{Token: "tok"}, Params: map[string]string{mount.ParamSiteID: "no-site"}}); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("unknown site: %v", err)
	}
	if _, err := open(ctx, "s", mount.Binding{Auth: mount.Credential{Token: "tok"}, Params: map[string]string{mount.ParamDriveID: "drv", mount.ParamItemID: "missing"}}); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("missing item: %v", err)
	}

	personal := mustGraph(t, srv, nil, vfs.AccountPersonal)
	got, err := personal(ctx, "s", mount.Binding{Auth: mount.Credential{Token: "tok"}, Params: map[string]string{mount.ParamName: "lib"}})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("personal /me/drive")
	}
	fx.mu.Lock()
	fx.meDrive = ""
	fx.mu.Unlock()
	if _, err := personal(ctx, "s", mount.Binding{Auth: mount.Credential{Token: "tok"}}); err == nil || !strings.Contains(err.Error(), "drive id missing") {
		t.Fatalf("drive without an id: %v", err)
	}
}

// TestGraphProvider_returnPaths drives the Graph provider through the real
// REST adapter against a scripted Graph server.
func TestGraphProvider_returnPaths(t *testing.T) {
	ctx := t.Context()
	codecs, err := vfs.NewContentRegistry()
	if err != nil {
		t.Fatal(err)
	}
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
	var vanished atomic.Bool
	srv := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case p == "/drives/drv/root", p == "/drives/drv/items/root":
			writeJSON(w, folder)
		case p == "/drives/drv/items/vanish" && !vanished.Load():
			writeJSON(w, map[string]any{"id": "vanish", "name": "vanish", "folder": map[string]any{}})
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
			writeJSON(w, plain("huge", "huge.bin", "application/octet-stream", "2026-01-02T03:04:05Z", vfs.MaxReadFileBytes+1))
		case p == "/drives/drv/items/huge":
			writeJSON(w, plain("huge", "huge.bin", "application/octet-stream", "2026-01-02T03:04:05Z", vfs.MaxReadFileBytes+1))
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
		case p == "/drives/drv/items/root:/boom.txt":
			w.WriteHeader(http.StatusInternalServerError)
		case p == "/drives/drv/items/root:/elsewhere":
			writeJSON(w, map[string]any{"id": "elsewhere", "name": "elsewhere", "folder": map[string]any{}})
		case p == "/drives/drv/items/elsewhere/children":
			writeJSON(w, map[string]any{"value": []any{}, "@odata.nextLink": "https://elsewhere.example/v1.0/drives/drv/items/elsewhere/children?skip=1"})
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

	holder := vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	sdk := vfs.NewMicrosoftGraph(holder, srv.URL+"/", srv.Client())
	p := openGraphProvider(t, sdk, holder, "root", true)

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
	if err := p.WriteDocument(canceled, "a.txt", vfs.NewTextDocument("a.txt", "text/plain", "utf-8", "x"), codecs); !errors.Is(err, context.Canceled) {
		t.Fatalf("writedoc ctx: %v", err)
	}

	st, err := p.Stat(ctx, ".")
	if err != nil || !st.IsDir {
		t.Fatalf("stat root = %+v err=%v", st, err)
	}
	if _, err := p.Stat(ctx, ".."); err == nil {
		t.Fatal("stat outside the drive root")
	}
	bare, err := p.Stat(ctx, "bare.bin")
	if err != nil || bare.MediaType == "" {
		t.Fatalf("bare stat = %+v err=%v", bare, err)
	}
	if _, err := p.OpenFile(ctx, "bare.bin", os.O_RDONLY, 0); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("bare open: %v", err)
	}
	if _, err := p.OpenDocument(ctx, "bare.bin", nil); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("bare document: %v", err)
	}
	if _, err := p.OpenDocument(ctx, "huge.bin", nil); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("huge document: %v", err)
	}
	if _, err := p.OpenFile(ctx, "a.txt", os.O_WRONLY, 0); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("write open: %v", err)
	}
	ro := openGraphProvider(t, sdk, holder, "root", false)
	if _, err := ro.OpenFile(ctx, "a.txt", os.O_WRONLY, 0); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro open: %v", err)
	}
	if err := ro.Remove(ctx, "a.txt"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro remove: %v", err)
	}
	if err := ro.MkdirAll(ctx, "sub", fs.ModeDir); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro mkdir: %v", err)
	}
	if err := ro.PutFile(ctx, "a.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro put: %v", err)
	}
	if err := ro.WriteDocument(ctx, "a.txt", vfs.NewTextDocument("a.txt", "text/plain", "utf-8", "x"), codecs); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("ro writedoc: %v", err)
	}
	if err := p.Remove(ctx, ""); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("remove root: %v", err)
	}
	if err := p.MkdirAll(ctx, "", 0); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := p.PutFile(ctx, "", bytes.NewReader([]byte("x")), 1); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("put root: %v", err)
	}
	if _, err := p.OpenDocument(ctx, ".", nil); !errors.Is(err, vfs.ErrIsDir) {
		t.Fatalf("opendoc dir: %v", err)
	}
	if err := p.MkdirAll(ctx, "nope", 0); !errors.Is(err, vfs.ErrExist) {
		t.Fatalf("mkdir conflict: %v", err)
	}
	ents, err := p.ReadDir(ctx, ".")
	if err != nil || len(ents) < 2 {
		t.Fatalf("readdir root = %+v err=%v", ents, err)
	}
	if err := p.MkdirAll(ctx, "deny.txt/x", 0); !errors.Is(err, vfs.ErrPermission) {
		t.Fatalf("mkdir through denied: %v", err)
	}
	if err := p.WriteDocument(ctx, "a.txt", nil, codecs); !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("writedoc nil: %v", err)
	}
	if _, err := p.ReadDir(ctx, "missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("readdir missing: %v", err)
	}
	if _, err := p.ReadDir(ctx, "a.txt"); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("readdir file: %v", err)
	}
	if _, err := p.ReadDir(ctx, "locked"); !errors.Is(err, vfs.ErrPermission) {
		t.Fatalf("readdir locked: %v", err)
	}
	if _, err := p.Stat(ctx, "boom.txt"); err == nil || !strings.Contains(err.Error(), "msgraph HTTP 500") {
		t.Fatalf("stat 500: %v", err)
	}
	// A next link to another host would get the bearer token; it is refused.
	if _, err := p.ReadDir(ctx, "elsewhere"); err == nil || !strings.Contains(err.Error(), "is not under") {
		t.Fatalf("foreign next link: %v", err)
	}
	if err := p.Remove(ctx, ".."); err == nil {
		t.Fatal("remove dirty")
	}
	if err := p.Remove(ctx, "missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}
	if err := p.MkdirAll(ctx, "..", 0); err == nil {
		t.Fatal("mkdir dirty")
	}
	if _, err := p.OpenDocument(ctx, "missing", nil); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("opendoc missing: %v", err)
	}
	if _, err := p.OpenDocument(ctx, "gone.txt", nil); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("opendoc gone: %v", err)
	}
	if err := p.PutFile(ctx, "..", bytes.NewReader([]byte("x")), 1); err == nil {
		t.Fatal("put dirty")
	}
	if err := p.PutFile(ctx, "big.bin", bytes.NewReader(nil), int64(vfs.MaxReadFileBytes)+1); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("put oversize: %v", err)
	}
	if err := p.PutFile(ctx, "deny.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, vfs.ErrPermission) {
		t.Fatalf("put deny: %v", err)
	}
	if err := p.PutFile(ctx, "x.bin", iotest.ErrReader(errors.New("read fail")), 0); err == nil {
		t.Fatal("put reader")
	}
	missingRoot := openGraphProvider(t, sdk, holder, "vanish", true)
	vanished.Store(true)
	if err := missingRoot.Validate(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("validate missing: %v", err)
	}
	if _, err := missingRoot.Stat(ctx, "."); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("stat missing root: %v", err)
	}
	if err := missingRoot.MkdirAll(ctx, "sub", 0); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("mkdir missing root: %v", err)
	}
	if err := missingRoot.PutFile(ctx, "sub/a.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("put nested missing root: %v", err)
	}
	if err := missingRoot.PutFile(ctx, "a.txt", bytes.NewReader([]byte("x")), 1); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("put leaf missing root: %v", err)
	}
	if _, err := vfs.Graph(sdk, holder, vfs.AccountPersonal)(ctx, "s", graphBinding("txt1", true)); err == nil || !strings.Contains(err.Error(), "not a folder") {
		t.Fatalf("open on a file: %v", err)
	}

	holder.Set(mount.Credential{Token: "stale"})
	stale, err := p.OpenFile(ctx, "a.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(stale.(io.Reader)); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("refresh missing: %v", err)
	}
	_ = stale.Close()
	holder.Set(mount.Credential{})
	if _, err := p.Stat(ctx, "a.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("ensure valid: %v", err)
	}
}

// graphProvider is the Graph provider's surface: Provider plus its one-shot
// put and document I/O.
type graphProvider interface {
	vfs.Provider
	PutFile(ctx context.Context, name string, r io.Reader, size int64) error
	OpenDocument(ctx context.Context, name string, reg *vfs.ContentRegistry) (vfs.Document, error)
	WriteDocument(ctx context.Context, name string, doc vfs.Document, reg *vfs.ContentRegistry) error
}

func graphBinding(itemID string, writable bool) mount.Binding {
	return mount.Binding{Params: map[string]string{mount.ParamDriveID: "drv", mount.ParamItemID: itemID}, Writable: writable}
}

func openGraphProvider(t *testing.T, api vfs.GraphAPI, holder *vfs.TokenHolder, itemID string, writable bool) graphProvider {
	t.Helper()
	p, err := vfs.Graph(api, holder, vfs.AccountPersonal)(t.Context(), "s", graphBinding(itemID, writable))
	if err != nil {
		t.Fatal(err)
	}
	return p.(graphProvider)
}
