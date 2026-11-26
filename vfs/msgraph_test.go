package vfs_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

// graphNode is one drive item of the Graph fixture. A file with no mime
// has a file facet without a type; pkg makes it a package (OneNote).
type graphNode struct {
	id, name, parent, mime, pkg string
	folder                      bool
	size                        int64
	body                        []byte
}

// graphFX is an in-memory OneDrive that speaks the Graph driveItem REST
// API. It pages children one at a time, so every listing of two or more
// follows a next link.
type graphFX struct {
	mu      sync.Mutex
	base    string // next links point here
	meDrive string
	sites   map[string]string // site id → drive id
	drives  map[string]string // drive id → root item id
	nodes   map[string]*graphNode
	// answers answer the next request of "<METHOD> <item id>/<tail>" in
	// place of the fixture.
	answers map[string]func(http.ResponseWriter, *http.Request)
}

// startGraph serves the Graph fixture:
//
//	drv root     a.txt "one"
//	drv          file.txt (a file item outside the root)
//	site-1 root  note.txt "from-site"
func startGraph(t *testing.T) (*graphFX, *httptest.Server) {
	t.Helper()
	g := &graphFX{
		meDrive: "drv",
		sites:   map[string]string{"site-1": "site-drv"},
		drives:  map[string]string{"drv": "root", "site-drv": "root-site"},
		nodes:   map[string]*graphNode{},
		answers: map[string]func(http.ResponseWriter, *http.Request){},
	}
	for _, n := range []*graphNode{
		{id: "root", folder: true},
		{id: "root-site", folder: true},
		{id: "fs", name: "note.txt", parent: "root-site", mime: "text/plain", body: []byte("from-site")},
		{id: "txt1", name: "file.txt", mime: "text/plain", body: []byte("nope")},
		{id: "f1", name: "a.txt", parent: "root", mime: "text/plain", body: []byte("one")},
	} {
		g.put(n)
	}
	srv := testhttp.New(t, g)
	g.base = srv.URL
	return g, srv
}

// put adds or replaces n. Its size is its body's unless it has one.
func (g *graphFX) put(n *graphNode) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.add(n)
}

func (g *graphFX) add(n *graphNode) {
	if n.size == 0 {
		n.size = int64(len(n.body))
	}
	g.nodes[n.id] = n
}

// answer has the next request of method on the item id (and tail) answered
// by fn.
func (g *graphFX) answer(method, id, tail string, fn func(http.ResponseWriter, *http.Request)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.answers[method+" "+id+"/"+tail] = fn
}

// status is an answer with status code and no body.
func status(code int) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
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
		key := r.Method + " " + n.id + "/" + tail
		if fn, ok := g.answers[key]; ok {
			delete(g.answers, key)
			fn(w, r)
			return
		}
	}
	switch {
	case tail == "content" && r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if n == nil && g.nodes[itemID] != nil {
			n = &graphNode{id: "new-" + itemID + "-" + rel, name: rel, parent: itemID, mime: "text/plain"}
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
	case tail == "content" && r.URL.Query().Get("direct") == "":
		http.Redirect(w, r, p+"?direct=1", http.StatusFound)
	case tail == "content":
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(n.body))
	case tail == "children" && r.Method == http.MethodPost:
		var folder struct{ Name string }
		_ = json.NewDecoder(r.Body).Decode(&folder)
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

// serveChildren answers one child per page, with a next link to the rest.
func (g *graphFX) serveChildren(w http.ResponseWriter, r *http.Request, itemID string) {
	kids := g.kids(itemID)
	var skip int
	_, _ = fmt.Sscan(r.URL.Query().Get("skip"), &skip)
	out, value := map[string]any{}, []any{}
	if skip < len(kids) {
		value = append(value, g.json(kids[skip]))
	}
	if skip+1 < len(kids) {
		out["@odata.nextLink"] = fmt.Sprintf("%s%s?skip=%d", g.base, r.URL.Path, skip+1)
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

// mountGraphHTTP mounts the personal drive of srv at /workspace/legal with
// the Word and Excel codecs.
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

// TestGraphFactory_aBindingsParamsChooseTheDriveAndBadOnesFail: a site
// binding opens that site's library and a personal one /me/drive with the
// binding's token. A file item id, an organization binding without a site
// or drive, an unknown account, a site or item Graph does not find, a drive
// with no id and a binding with no token for a factory with no holder fail.
func TestGraphFactory_aBindingsParamsChooseTheDriveAndBadOnesFail(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, srv := startGraph(t)
	noDriveFX, noDriveSrv := startGraph(t)
	noDriveFX.mu.Lock()
	noDriveFX.meDrive = ""
	noDriveFX.mu.Unlock()
	open := mustGraph(srv, nil, "")
	unheld := vfs.Graph(vfs.NewMicrosoftGraph(vfs.NewTokenHolder(mount.Credential{Token: "tok"}), srv.URL+"/", srv.Client()), nil, vfs.AccountPersonal)
	bind := func(params map[string]string) mount.Binding {
		return mount.Binding{Auth: mount.Credential{Token: "tok"}, Params: params}
	}
	siteMS, err := vfs.Tree(vfs.At("lib", open))(ctx, "s", vfs.Request{Bindings: []mount.Binding{
		bind(map[string]string{mount.ParamName: "lib", mount.ParamSiteID: "site-1"}),
	}})
	if err != nil {
		t.Fatal(err)
	}

	site, siteErr := siteMS.Route(ctx, "/workspace/lib/note.txt").ReadFile(ctx)
	personal, personalErr := unheld(ctx, "s", bind(nil))
	refused := map[string]struct {
		open vfs.Open
		b    mount.Binding
		want string
	}{
		"a file item":            {open, bind(map[string]string{mount.ParamDriveID: "drv", mount.ParamItemID: "txt1"}), "not a folder"},
		"a default organization": {open, bind(nil), "siteId or driveId"},
		"a bad account":          {open, bind(map[string]string{mount.ParamAccount: "nope"}), "not organization or personal"},
		"an unknown site":        {open, bind(map[string]string{mount.ParamSiteID: "no-site"}), vfs.ErrNotExist.Error()},
		"a missing item":         {open, bind(map[string]string{mount.ParamDriveID: "drv", mount.ParamItemID: "missing"}), vfs.ErrNotExist.Error()},
		"a drive with no id":     {mustGraph(noDriveSrv, nil, vfs.AccountPersonal), bind(nil), "drive id missing"},
		"no token":               {unheld, mount.Binding{}, "access token required"},
	}

	if siteErr != nil || string(site) != "from-site" {
		t.Fatalf("site binding read = %q %v", site, siteErr)
	}
	if personalErr != nil || personal == nil {
		t.Fatalf("personal binding = %v %v, want /me/drive", personal, personalErr)
	}
	for name, c := range refused {
		if _, err := c.open(ctx, "s", c.b); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
}

// TestGraph_aTokenGraphRefusesRefreshesOnceOrExpires: a token Graph refuses
// refreshes once through the holder and the read succeeds. With no refresh,
// or with no token, the read fails as expired.
func TestGraph_aTokenGraphRefusesRefreshesOnceOrExpires(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		refresh func(context.Context) (mount.Credential, error)
		token   string
		want    error
	}{
		"a refresh reads": {
			refresh: func(context.Context) (mount.Credential, error) { return mount.Credential{Token: "fresh"}, nil },
			token:   "tok",
		},
		"no refresh expires": {token: "tok", want: vfs.ErrAuthExpired},
		"no token expires":   {want: vfs.ErrAuthExpired},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fx, srv := startGraph(t)
			ms, holder := mountGraphHTTP(t, srv, false)
			holder.SetRefresh(c.refresh)
			holder.Set(mount.Credential{Token: c.token})
			fx.answer(http.MethodGet, "f1", "content", status(http.StatusUnauthorized))

			got, err := ms.Route(ctx, "/workspace/legal/a.txt").ReadFile(ctx)

			if !errors.Is(err, c.want) || (c.want == nil && string(got) != "one") {
				t.Fatalf("read = %q %v, want %v", got, err, c.want)
			}
		})
	}
}

// TestGraph_contentGraphCannotServeDoesNotRead: an item with no content
// type or a OneNote package has no codec, a size or a body over the read
// cap is too large, a body cut short ends early, content Graph does not find
// is not found, and a reply that ignores the requested range is refused
// rather than read at the wrong offset.
func TestGraph_contentGraphCannotServeDoesNotRead(t *testing.T) {
	t.Parallel()
	overCap := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.Copy(w, io.LimitReader(zeros{}, int64(vfs.MaxReadFileBytes)+1))
	}
	cut := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "10")
		_, _ = w.Write([]byte("abc"))
	}
	wholeBody := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("abc")) }
	readAt := func(ctx context.Context, r vfs.Route) error {
		f, err := r.Open(ctx)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.(io.ReaderAt).ReadAt(make([]byte, 1), 1)
		return err
	}
	for name, c := range map[string]struct {
		node   graphNode
		answer func(http.ResponseWriter, *http.Request)
		read   func(context.Context, vfs.Route) error
		want   func(error) bool
	}{
		"an item with no type has no codec":    {node: graphNode{body: []byte("x")}, read: routeOps["open"], want: is(vfs.ErrNoCodec)},
		"an item with no type has no document": {node: graphNode{body: []byte("x")}, read: routeOps["text"], want: is(vfs.ErrNoCodec)},
		"a OneNote package has no codec":       {node: graphNode{pkg: "oneNote"}, read: routeOps["text"], want: is(vfs.ErrNoCodec)},
		"a size over the cap is too large": {
			node: graphNode{mime: "text/plain", size: int64(vfs.MaxReadFileBytes) + 1}, read: routeOps["text"], want: is(vfs.ErrTooLarge),
		},
		"a body over the cap is too large": {
			node: graphNode{mime: "text/plain", body: []byte("x")}, answer: overCap, read: routeOps["text"], want: is(vfs.ErrTooLarge),
		},
		"a body cut short ends early": {
			node: graphNode{mime: "text/plain", size: 10}, answer: cut, read: routeOps["text"], want: is(io.ErrUnexpectedEOF),
		},
		"content Graph does not find is not found": {
			node: graphNode{mime: "text/plain", body: []byte("x")}, answer: status(http.StatusNotFound), read: routeOps["text"], want: is(vfs.ErrNotExist),
		},
		"a reply that ignores the range is refused": {
			node: graphNode{mime: "text/plain", body: []byte("abc")}, answer: wholeBody, read: readAt,
			want: func(err error) bool { return err != nil && strings.Contains(err.Error(), "answered with HTTP 200") },
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fx, srv := startGraph(t)
			node := c.node
			node.id, node.name, node.parent = "item", "item.bin", "root"
			fx.put(&node)
			if c.answer != nil {
				fx.answer(http.MethodGet, "item", "content", c.answer)
			}
			ms, _ := mountGraphHTTP(t, srv, false)

			err := c.read(ctx, ms.Route(ctx, "/workspace/legal/item.bin"))

			if !c.want(err) {
				t.Fatalf("read = %v", err)
			}
		})
	}
}

// TestGraph_aFileReadsAsTheTypeGraphReports: a file with no extension
// decodes with the media type Graph reports for it.
func TestGraph_aFileReadsAsTheTypeGraphReports(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx, srv := startGraph(t)
	fx.put(&graphNode{id: "w", name: "SPIKE", parent: "root", mime: adapters.DOCXMediaType, body: wordDocument(t, `<w:p><w:r><w:t>x</w:t></w:r></w:p>`)})
	ms, _ := mountGraphHTTP(t, srv, false)

	doc, err := ms.Route(ctx, "/workspace/legal/SPIKE").ReadText(ctx)

	if err != nil || doc.MediaType() != adapters.DOCXMediaType {
		t.Fatalf("read text = %v %v, want a Word document", doc, err)
	}
	if rd, ok := vfs.AsRich(doc); !ok || len(rd.Blocks()) == 0 || rd.Blocks()[0].Text != "x" {
		t.Fatalf("blocks of %+v, want the paragraph x", doc)
	}
}

// TestGraph_aCallGraphRefusesFailsTheOperation: a read, list, write, upload,
// new folder or removal whose Graph call is refused fails with ErrPermission,
// and a folder that another writer made first is ErrExist.
func TestGraph_aCallGraphRefusesFailsTheOperation(t *testing.T) {
	t.Parallel()
	write := func(ctx context.Context, r vfs.Route) error { return r.WriteFile(ctx, []byte("x")) }
	for name, c := range map[string]struct {
		method, id, tail string
		code             int
		path             string
		op               func(context.Context, vfs.Route) error
	}{
		"a stat":                    {http.MethodGet, "f1", "", http.StatusForbidden, "a.txt", routeOps["stat"]},
		"a stat of the root":        {http.MethodGet, "root", "", http.StatusForbidden, "", routeOps["stat"]},
		"a list":                    {http.MethodGet, "root", "children", http.StatusForbidden, "", routeOps["list"]},
		"a lookup before a write":   {http.MethodGet, "f1", "", http.StatusForbidden, "a.txt", write},
		"an upload over a file":     {http.MethodPut, "f1", "content", http.StatusForbidden, "a.txt", write},
		"the root of a new file":    {http.MethodGet, "root", "", http.StatusForbidden, "new.txt", write},
		"the root of a nested file": {http.MethodGet, "root", "", http.StatusForbidden, "sub/new.txt", write},
		"a lookup before a mkdir":   {http.MethodGet, "f1", "", http.StatusForbidden, "a.txt/x", routeOps["mkdir"]},
		"a new folder":              {http.MethodPost, "root", "children", http.StatusForbidden, "sub", routeOps["mkdir"]},
		"a removal":                 {http.MethodDelete, "f1", "", http.StatusForbidden, "a.txt", routeOps["remove"]},
		"a folder made first":       {http.MethodPost, "root", "children", http.StatusConflict, "sub", routeOps["mkdir"]},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fx, srv := startGraph(t)
			ms, _ := mountGraphHTTP(t, srv, true)
			fx.answer(c.method, c.id, c.tail, status(c.code))
			want := vfs.ErrPermission
			if c.code == http.StatusConflict {
				want = vfs.ErrExist
			}

			err := c.op(ctx, ms.Route(ctx, "/workspace/legal/"+c.path))

			if !errors.Is(err, want) {
				t.Fatalf("err = %v, want %v", err, want)
			}
		})
	}
}

// TestGraph_aNextLinkToAnotherHostIsRefused: a page link off the Graph
// base would get the bearer token, so the listing fails.
func TestGraph_aNextLinkToAnotherHostIsRefused(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx, srv := startGraph(t)
	fx.put(&graphNode{id: "f2", name: "b.txt", parent: "root", mime: "text/plain"})
	fx.mu.Lock()
	fx.base = "https://elsewhere.example/v1.0"
	fx.mu.Unlock()
	ms, _ := mountGraphHTTP(t, srv, false)

	_, err := ms.Route(ctx, "/workspace/legal").ReadDir(ctx)

	if err == nil || !strings.Contains(err.Error(), "is not under") {
		t.Fatalf("ReadDir = %v, want the foreign link refused", err)
	}
}

// TestGraph_contentFromAnOffsetReadsToTheEnd: GetContent with n < 0 reads
// from the offset to the end.
func TestGraph_contentFromAnOffsetReadsToTheEnd(t *testing.T) {
	t.Parallel()
	_, srv := startGraph(t)
	api := vfs.NewMicrosoftGraph(vfs.NewTokenHolder(mount.Credential{Token: "tok"}), srv.URL+"/", srv.Client())

	body, err := api.GetContent(t.Context(), "site-drv", "fs", 5, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	rest, err := io.ReadAll(body)

	if err != nil || string(rest) != "site" {
		t.Fatalf("content from offset 5 = %q %v", rest, err)
	}
}
