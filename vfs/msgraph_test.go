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
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

// graphNode is one drive item of the Graph fixture.
type graphNode struct {
	id, name, parent, mime, pkg string
	folder                      bool
	size                        int64
	body                        []byte
	// wholeBody makes content ignore Range and answer HTTP 200.
	wholeBody bool
}

// graphFX is an in-memory OneDrive that speaks the Graph driveItem REST API.
type graphFX struct {
	mu       sync.Mutex
	base     string // next links point here
	meDrive  string
	sites    map[string]string // site id → drive id
	drives   map[string]string // drive id → root item id
	nodes    map[string]*graphNode
	fail     map[string]int // "<item id>" or "<item id>/<tail>" → HTTP status of every request
	once     map[string]int // the same, for the next request only
	pageSize int
}

// startGraph serves the Graph fixture:
//
//	drv root     a.txt "one", c.txt "c", Budget.xlsx, Notebook (OneNote),
//	             bare.bin (no MIME type), huge.bin (over the read cap),
//	             whole.txt (ignores Range), lost.txt (content 404),
//	             exp.txt (content 401), deny.txt (403), boom.txt (500),
//	             locked/ (children 403), vanish/
//	drv          file.txt (a file item outside the root)
//	site-1 root  note.txt "from-site"
func startGraph(t *testing.T) (*graphFX, *httptest.Server) {
	t.Helper()
	g := &graphFX{
		meDrive:  "drv",
		sites:    map[string]string{"site-1": "site-drv"},
		drives:   map[string]string{"drv": "root", "site-drv": "root-site"},
		nodes:    map[string]*graphNode{},
		once:     map[string]int{},
		pageSize: 2,
		fail: map[string]int{
			"lost/content": http.StatusNotFound, "exp/content": http.StatusUnauthorized,
			"deny": http.StatusForbidden, "boom": http.StatusInternalServerError,
			"locked/children": http.StatusForbidden,
		},
	}
	text := func(id, name, body string) *graphNode {
		return &graphNode{id: id, name: name, parent: "root", mime: "text/plain", body: []byte(body)}
	}
	for _, n := range []*graphNode{
		{id: "root", folder: true},
		{id: "root-site", folder: true},
		{id: "fs", name: "note.txt", parent: "root-site", mime: "text/plain", body: []byte("from-site")},
		{id: "txt1", name: "file.txt", mime: "text/plain", body: []byte("nope")},
		{id: "xlsx1", name: "Budget.xlsx", parent: "root", mime: adapters.XLSXMediaType},
		{id: "note1", name: "Notebook", parent: "root", pkg: "oneNote"},
		{id: "bare", name: "bare.bin", parent: "root", body: []byte("x")},
		{id: "huge", name: "huge.bin", parent: "root", mime: "application/octet-stream", size: int64(vfs.MaxReadFileBytes) + 1},
		{id: "whole", name: "whole.txt", parent: "root", mime: "text/plain", body: []byte("abc"), wholeBody: true},
		{id: "locked", name: "locked", parent: "root", folder: true},
		{id: "vanish", name: "vanish", parent: "root", folder: true},
		text("f1", "a.txt", "one"), text("c2", "c.txt", "c"), text("lost", "lost.txt", "x"),
		text("exp", "exp.txt", "x"), text("deny", "deny.txt", "x"), text("boom", "boom.txt", "x"),
	} {
		g.add(n)
	}
	srv := testhttp.New(t, g)
	g.base = srv.URL
	return g, srv
}

func (g *graphFX) add(n *graphNode) {
	if n.size == 0 {
		n.size = int64(len(n.body))
	}
	g.nodes[n.id] = n
}

func (g *graphFX) json(n *graphNode) map[string]any {
	it := map[string]any{
		"id": n.id, "name": n.name, "size": n.size,
		"lastModifiedDateTime": "2026-01-02T03:04:05Z",
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

// lookup is the node at rel under itemID, or itemID itself when rel is empty.
func (g *graphFX) lookup(itemID, rel string) *graphNode {
	cur := g.nodes[itemID]
	if rel == "" || cur == nil {
		return cur
	}
	for _, part := range strings.Split(rel, "/") {
		i := slices.IndexFunc(g.kids(cur.id), func(n *graphNode) bool { return n.name == part })
		if i < 0 {
			return nil
		}
		cur = g.kids(cur.id)[i]
	}
	return cur
}

// status is the HTTP status the fixture fails a request on n's tail with,
// or 0.
func (g *graphFX) status(n *graphNode, tail string) int {
	for _, key := range []string{n.id, n.id + "/" + tail} {
		if st, ok := g.once[key]; ok {
			delete(g.once, key)
			return st
		}
		if st := g.fail[key]; st != 0 {
			return st
		}
	}
	return 0
}

func (g *graphFX) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	p := r.URL.Path
	if p == "/me/drive" {
		writeGraphJSON(w, map[string]any{"id": g.meDrive})
		return
	}
	if site, ok := strings.CutPrefix(strings.TrimSuffix(p, "/drive"), "/sites/"); ok {
		if id, ok := g.sites[site]; ok {
			writeGraphJSON(w, map[string]any{"id": id})
			return
		}
	}
	driveID, itemID, rel, tail := parseGraphPath(p)
	if itemID == "" {
		itemID = g.drives[driveID]
	}
	n := g.lookup(itemID, rel)
	if n != nil {
		if st := g.status(n, tail); st != 0 {
			w.WriteHeader(st)
			return
		}
	}
	switch {
	case tail == "content" && r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if n == nil && g.nodes[itemID] != nil {
			n = &graphNode{id: "new-" + itemID + "-" + rel, name: rel, parent: itemID, mime: "text/plain"}
			if bytes.Contains(body, []byte("word/document.xml")) {
				n.mime = adapters.DOCXMediaType
			}
		}
		if n == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		n.body, n.size = body, int64(len(body))
		g.add(n)
		writeGraphJSON(w, g.json(n))
	case n == nil:
		w.WriteHeader(http.StatusNotFound)
	case tail == "content" && n.wholeBody:
		_, _ = w.Write(n.body)
	case tail == "content" && r.URL.Query().Get("direct") == "":
		// Graph answers a content GET with a redirect to the download URL.
		http.Redirect(w, r, p+"?direct=1", http.StatusFound)
	case tail == "content":
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(n.body))
	case tail == "children" && r.Method == http.MethodPost:
		var folder struct{ Name string }
		_ = json.NewDecoder(r.Body).Decode(&folder)
		if g.lookup(n.id, folder.Name) != nil {
			w.WriteHeader(http.StatusConflict)
			return
		}
		created := &graphNode{id: "dir-" + folder.Name, name: folder.Name, parent: n.id, folder: true}
		g.add(created)
		writeGraphJSON(w, g.json(created))
	case tail == "children":
		g.serveChildren(w, r, n.id)
	case r.Method == http.MethodDelete:
		delete(g.nodes, n.id)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeGraphJSON(w, g.json(n))
	}
}

// serveChildren pages the children pageSize at a time with a next link.
func (g *graphFX) serveChildren(w http.ResponseWriter, r *http.Request, itemID string) {
	page, out := g.kids(itemID), map[string]any{}
	if len(page) > g.pageSize {
		if r.URL.Query().Get("skip") != "" {
			page = page[g.pageSize:]
		} else {
			page = page[:g.pageSize]
			out["@odata.nextLink"] = g.base + r.URL.Path + "?skip=1"
		}
	}
	value := make([]any, 0, len(page))
	for _, k := range page {
		value = append(value, g.json(k))
	}
	out["value"] = value
	writeGraphJSON(w, out)
}

// parseGraphPath splits /drives/{d}/root[:/{rel}[:/{tail}]] and
// /drives/{d}/items/{id}[:/{rel}[:/{tail}]|/{tail}]. An empty item id is
// the drive root.
func parseGraphPath(p string) (driveID, itemID, rel, tail string) {
	rest, ok := strings.CutPrefix(p, "/drives/")
	if !ok {
		return "", "", "", ""
	}
	driveID, rest, _ = strings.Cut(rest, "/")
	if after, ok := strings.CutPrefix(rest, "items/"); ok {
		itemID, rest = after, ""
		if i := strings.IndexAny(after, ":/"); i >= 0 {
			itemID, rest = after[:i], after[i:]
		}
	} else {
		rest = strings.TrimPrefix(rest, "root")
	}
	if after, ok := strings.CutPrefix(rest, ":/"); ok {
		rel, tail, _ = strings.Cut(after, ":/")
	} else {
		tail = strings.TrimPrefix(rest, "/")
	}
	return driveID, itemID, rel, tail
}

func mustGraph(srv *httptest.Server, holder *vfs.TokenHolder, account string) vfs.Open {
	if holder == nil {
		holder = vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	}
	return vfs.Graph(vfs.NewMicrosoftGraph(holder, srv.URL+"/", srv.Client()), holder, account)
}

func mountGraphHTTP(t *testing.T, srv *httptest.Server, writable bool) (*vfs.MountSession, *vfs.TokenHolder) {
	t.Helper()
	holder := vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	ms, err := vfs.Tree(vfs.At("legal", mustGraph(srv, holder, vfs.AccountPersonal))).WithCodecs(adapters.Codecs()...)(t.Context(), "s", vfs.Request{Bindings: []mount.Binding{{
		Provider: mount.ProviderMicrosoft, Writable: writable,
		Params: map[string]string{mount.ParamName: "legal"},
		Auth:   mount.Credential{Token: "tok"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return ms, holder
}

// A Graph mount follows content redirects, refreshes a token Graph refuses,
// makes nested folders, writes names with spaces, and writes HTML as Word.
// Content Graph refuses fails with the matching vfs error, and a read-only
// mount reads but does not write.
func TestGraph_readWriteMkdirTrashRefreshAndErrors(t *testing.T) {
	ctx := t.Context()
	fx, srv := startGraph(t)
	ms, holder := mountGraphHTTP(t, srv, true)
	read := func(name string) string {
		t.Helper()
		got, err := ms.Route(ctx, "/workspace/legal/"+name).ReadFile(ctx)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(got)
	}

	// n < 0 reads from the offset to the end of the content.
	tail, err := vfs.NewMicrosoftGraph(holder, srv.URL+"/", srv.Client()).GetContent(ctx, "site-drv", "fs", 5, -1)
	if err != nil {
		t.Fatal(err)
	}
	rest, _ := io.ReadAll(tail)
	_ = tail.Close()
	if string(rest) != "site" {
		t.Fatalf("content from offset 5 = %q", rest)
	}

	if err := ms.Route(ctx, "/workspace/legal/a.txt").WriteFile(ctx, []byte("two")); err != nil {
		t.Fatal(err)
	}
	fx.mu.Lock()
	fx.once["f1/content"] = http.StatusUnauthorized
	fx.mu.Unlock()
	holder.SetRefresh(func(context.Context) (mount.Credential, error) {
		return mount.Credential{Token: "fresh"}, nil
	})
	if got := read("a.txt"); got != "two" {
		t.Fatalf("read after a refresh = %q", got)
	}

	if err := ms.Route(ctx, "/workspace/legal/sub/dir/c.txt").WriteFile(ctx, []byte("nested")); err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/legal/my file.txt").WriteFile(ctx, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, vfs.NewTextDocument("/workspace/legal/note.md", "text/markdown", "utf-8", "# hi\n")); err != nil {
		t.Fatal(err)
	}
	if got := read("sub/dir/c.txt") + "|" + read("my file.txt") + "|" + read("note.md"); got != "nested|hello|# hi\n" {
		t.Fatalf("written files read %q", got)
	}

	html := "<h1>x</h1>"
	if _, err := ms.Apply(ctx, "/workspace/legal/SPIKE", vfs.Mutation{Content: &html}); err != nil {
		t.Fatal(err)
	}
	word, err := ms.Route(ctx, "/workspace/legal/SPIKE").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rd, ok := vfs.AsRich(word); word.MediaType() != adapters.DOCXMediaType || !ok || len(rd.Blocks()) == 0 || rd.Blocks()[0].Text != "x" {
		t.Fatalf("extensionless HTML write = %s %+v", word.MediaType(), word)
	}

	if _, err := ms.Route(ctx, "/workspace/legal/Notebook").ReadText(ctx); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("OneNote: %v", err)
	}
	if _, err := ms.Route(ctx, "/workspace/legal/lost.txt").ReadFile(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("content 404: %v", err)
	}
	if _, err := ms.Route(ctx, "/workspace/legal/exp.txt").ReadFile(ctx); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("content 401: %v", err)
	}
	if err := ms.Route(ctx, "/workspace/legal/big.bin").WriteFile(ctx, make([]byte, vfs.MaxReadFileBytes+1)); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("oversize put: %v", err)
	}
	if err := ms.Route(ctx, "/workspace/legal/Budget.xlsx/nope").MkdirAll(ctx); !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("mkdir through a file: %v", err)
	}

	ro, _ := mountGraphHTTP(t, srv, false)
	if got, err := ro.Route(ctx, "/workspace/legal/c.txt").ReadFile(ctx); err != nil || string(got) != "c" {
		t.Fatalf("read-only read = %q err=%v", got, err)
	}
	if err := ro.Route(ctx, "/workspace/legal/c.txt").WriteFile(ctx, []byte("nope")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("read-only put: %v", err)
	}
}

// TestGraphFactory_aBindingsParamsChooseTheDriveAndBadOnesFail: a file item
// id, an organization binding without a site or drive, and an unknown account
// fail; a site binding opens that site's library and a personal one /me/drive.
func TestGraphFactory_aBindingsParamsChooseTheDriveAndBadOnesFail(t *testing.T) {
	ctx := t.Context()
	fx, srv := startGraph(t)
	open := mustGraph(srv, nil, "")
	personal := mustGraph(srv, nil, vfs.AccountPersonal)
	bind := func(params map[string]string) mount.Binding {
		return mount.Binding{Auth: mount.Credential{Token: "tok"}, Params: params}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	siteMS, err := vfs.Tree(vfs.At("lib", open))(ctx, "s", vfs.Request{Bindings: []mount.Binding{
		bind(map[string]string{mount.ParamName: "lib", mount.ParamSiteID: "site-1"}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := siteMS.Route(ctx, "/workspace/lib/note.txt").ReadFile(ctx); err != nil || string(got) != "from-site" {
		t.Fatalf("site bind = %q err=%v", got, err)
	}
	if p, err := personal(ctx, "s", bind(nil)); err != nil || p == nil {
		t.Fatalf("personal /me/drive = %v err=%v", p, err)
	}

	for name, c := range map[string]struct {
		open vfs.Open
		ctx  context.Context
		b    mount.Binding
		want string
	}{
		"file item":            {open, ctx, bind(map[string]string{mount.ParamDriveID: "drv", mount.ParamItemID: "txt1"}), "not a folder"},
		"default organization": {open, ctx, bind(nil), "siteId or driveId"},
		"bad account":          {open, ctx, bind(map[string]string{mount.ParamAccount: "nope"}), "not organization or personal"},
		"unknown site":         {open, ctx, bind(map[string]string{mount.ParamSiteID: "no-site"}), vfs.ErrNotExist.Error()},
		"missing item":         {open, ctx, bind(map[string]string{mount.ParamDriveID: "drv", mount.ParamItemID: "missing"}), vfs.ErrNotExist.Error()},
		"canceled":             {personal, canceled, bind(nil), context.Canceled.Error()},
	} {
		if _, err := c.open(c.ctx, "s", c.b); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v, want %q", name, err, c.want)
		}
	}

	fx.mu.Lock()
	fx.meDrive = ""
	fx.mu.Unlock()
	if _, err := personal(ctx, "s", bind(nil)); err == nil || !strings.Contains(err.Error(), "drive id missing") {
		t.Fatalf("drive without an id: %v", err)
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

func openGraphProvider(t *testing.T, srv *httptest.Server, holder *vfs.TokenHolder, itemID string, writable bool) graphProvider {
	t.Helper()
	p, err := mustGraph(srv, holder, vfs.AccountPersonal)(t.Context(), "s", mount.Binding{
		Params: map[string]string{mount.ParamDriveID: "drv", mount.ParamItemID: itemID}, Writable: writable,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p.(graphProvider)
}

// TestGraphProvider_returnPaths: every provider call fails with the vfs
// error that matches what Graph answered, a path outside the root, a
// read-only mount, an oversize or unreadable upload, a root that is gone,
// a canceled context and an empty token.
func TestGraphProvider_returnPaths(t *testing.T) {
	ctx := t.Context()
	fx, srv := startGraph(t)
	codecs, err := vfs.NewContentRegistry()
	if err != nil {
		t.Fatal(err)
	}
	holder := vfs.NewTokenHolder(mount.Credential{Token: "tok"})
	p := openGraphProvider(t, srv, holder, "root", true)
	ro := openGraphProvider(t, srv, holder, "root", false)
	gone := openGraphProvider(t, srv, holder, "vanish", true)
	fx.mu.Lock()
	delete(fx.nodes, "vanish")
	fx.mu.Unlock()
	text := vfs.NewTextDocument("a.txt", "text/plain", "utf-8", "x")
	put := func(p graphProvider, name string) func(context.Context) error {
		return func(ctx context.Context) error { return p.PutFile(ctx, name, bytes.NewReader([]byte("x")), 1) }
	}
	open := func(p graphProvider, name string, flag int) func(context.Context) error {
		return func(ctx context.Context) error { _, err := p.OpenFile(ctx, name, flag, 0); return err }
	}
	doc := func(name string) func(context.Context) error {
		return func(ctx context.Context) error { _, err := p.OpenDocument(ctx, name, codecs); return err }
	}
	stat := func(p graphProvider, name string) func(context.Context) error {
		return func(ctx context.Context) error { _, err := p.Stat(ctx, name); return err }
	}
	list := func(name string) func(context.Context) error {
		return func(ctx context.Context) error { _, err := p.ReadDir(ctx, name); return err }
	}
	mkdir := func(p graphProvider, name string) func(context.Context) error {
		return func(ctx context.Context) error { return p.MkdirAll(ctx, name, fs.ModeDir) }
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for i, call := range []func(context.Context) error{
		p.Validate, stat(p, "a.txt"), open(p, "a.txt", os.O_RDONLY), list("."), mkdir(p, "sub"), doc("a.txt"), put(p, "a.txt"),
		func(ctx context.Context) error { return p.Remove(ctx, "a.txt") },
		func(ctx context.Context) error { return p.WriteDocument(ctx, "a.txt", text, codecs) },
	} {
		if err := call(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled call %d = %v", i, err)
		}
	}

	for name, c := range map[string]struct {
		call func(context.Context) error
		want error
	}{
		"open without MIME":     {open(p, "bare.bin", os.O_RDONLY), vfs.ErrNoCodec},
		"document without MIME": {doc("bare.bin"), vfs.ErrNoCodec},
		"document over the cap": {doc("huge.bin"), vfs.ErrTooLarge},
		"document of a folder":  {doc("."), vfs.ErrIsDir},
		"document missing":      {doc("missing"), vfs.ErrNotExist},
		"document content 404":  {doc("lost.txt"), vfs.ErrNotExist},
		"open for write":        {open(p, "a.txt", os.O_WRONLY), vfs.ErrNotSupported},
		"read-only open":        {open(ro, "a.txt", os.O_WRONLY), vfs.ErrReadOnly},
		"read-only remove":      {func(ctx context.Context) error { return ro.Remove(ctx, "a.txt") }, vfs.ErrReadOnly},
		"read-only mkdir":       {mkdir(ro, "sub"), vfs.ErrReadOnly},
		"read-only put":         {put(ro, "a.txt"), vfs.ErrReadOnly},
		"read-only document":    {func(ctx context.Context) error { return ro.WriteDocument(ctx, "a.txt", text, codecs) }, vfs.ErrReadOnly},
		"remove the root":       {func(ctx context.Context) error { return p.Remove(ctx, "") }, vfs.ErrInvalidPath},
		"remove missing":        {func(ctx context.Context) error { return p.Remove(ctx, "missing") }, vfs.ErrNotExist},
		"put the root":          {put(p, ""), vfs.ErrInvalidPath},
		"put denied":            {put(p, "deny.txt"), vfs.ErrPermission},
		"put over the cap":      {func(ctx context.Context) error { return p.PutFile(ctx, "big.bin", nil, int64(vfs.MaxReadFileBytes)+1) }, vfs.ErrTooLarge},
		"mkdir through denied":  {mkdir(p, "deny.txt/x"), vfs.ErrPermission},
		"list missing":          {list("missing"), vfs.ErrNotExist},
		"list a file":           {list("a.txt"), vfs.ErrNotDir},
		"list locked":           {list("locked"), vfs.ErrPermission},
		"write nil document":    {func(ctx context.Context) error { return p.WriteDocument(ctx, "a.txt", nil, codecs) }, vfs.ErrNotTextual},
		"validate a gone root":  {gone.Validate, vfs.ErrNotExist},
		"stat a gone root":      {stat(gone, "."), vfs.ErrNotExist},
		"mkdir in a gone root":  {mkdir(gone, "sub"), vfs.ErrNotExist},
		"put nested, gone root": {put(gone, "sub/a.txt"), vfs.ErrNotExist},
		"put leaf, gone root":   {put(gone, "a.txt"), vfs.ErrNotExist},
	} {
		if err := c.call(ctx); !errors.Is(err, c.want) {
			t.Fatalf("%s = %v, want %v", name, err, c.want)
		}
	}
	for name, call := range map[string]func(context.Context) error{
		"stat outside the root":   stat(p, ".."),
		"remove outside the root": func(ctx context.Context) error { return p.Remove(ctx, "..") },
		"mkdir outside the root":  mkdir(p, ".."),
		"put outside the root":    put(p, ".."),
		"put an unreadable body": func(ctx context.Context) error {
			return p.PutFile(ctx, "x.bin", iotest.ErrReader(errors.New("read fail")), 0)
		},
	} {
		if err := call(ctx); err == nil {
			t.Fatalf("%s succeeded", name)
		}
	}

	if st, err := p.Stat(ctx, "."); err != nil || !st.IsDir {
		t.Fatalf("stat root = %+v err=%v", st, err)
	}
	if st, err := p.Stat(ctx, "bare.bin"); err != nil || st.MediaType == "" {
		t.Fatalf("a file without MIME has no detected type: %+v err=%v", st, err)
	}
	if err := p.MkdirAll(ctx, "", 0); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := p.MkdirAll(ctx, "locked", 0); err != nil {
		t.Fatalf("mkdir of an existing folder: %v", err)
	}
	// Another writer makes the folder between the lookup and the create.
	fx.mu.Lock()
	fx.once["root/children"] = http.StatusConflict
	fx.mu.Unlock()
	if err := p.MkdirAll(ctx, "raced", 0); !errors.Is(err, vfs.ErrExist) {
		t.Fatalf("mkdir conflict: %v", err)
	}
	if _, err := p.Stat(ctx, "boom.txt"); err == nil || !strings.Contains(err.Error(), "msgraph HTTP 500") {
		t.Fatalf("stat 500: %v", err)
	}
	// A next link to another host would get the bearer token; it is refused.
	fx.mu.Lock()
	fx.base = "https://elsewhere.example/v1.0"
	fx.mu.Unlock()
	if _, err := p.ReadDir(ctx, "."); err == nil || !strings.Contains(err.Error(), "is not under") {
		t.Fatalf("foreign next link: %v", err)
	}

	// The server ignores Range and sends the whole body; the read fails
	// instead of returning the wrong bytes.
	whole, err := p.OpenFile(ctx, "whole.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := whole.(io.ReaderAt).ReadAt(make([]byte, 1), 1); err == nil || !strings.Contains(err.Error(), "answered with HTTP 200") {
		t.Fatalf("range ignored: %v", err)
	}
	_ = whole.Close()

	// Graph refuses the token, and the holder has no refresh.
	fx.mu.Lock()
	fx.once["f1"] = http.StatusUnauthorized
	fx.mu.Unlock()
	if _, err := p.Stat(ctx, "a.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("refused token without a refresh: %v", err)
	}
	holder.Set(mount.Credential{})
	if _, err := p.Stat(ctx, "a.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("empty token: %v", err)
	}
}
