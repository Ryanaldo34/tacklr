package brain_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

func treeSession(t *testing.T, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(members...)(context.Background(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

func withParams(open vfs.Open, point string, params map[string]string) vfs.Open {
	return func(ctx context.Context, sessionID string, b vfs.Binding) (vfs.Provider, error) {
		merged := make(map[string]string, len(b.Params)+len(params))
		for k, v := range b.Params {
			merged[k] = v
		}
		for k, v := range params {
			merged[k] = v
		}
		b.Params = merged
		if point != "" {
			b.Point = point
		}
		return open(ctx, sessionID, b)
	}
}

func TestBrainProvider_prefixWriteReadDirRemoveAndIR(t *testing.T) {
	ctx := context.Background()
	ns := brain.MustNamespace("id", uuid.NewString())
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{
			Kind: "Deal", IsParent: true,
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString, Required: true},
			},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ms := treeSession(t, vfs.At("engram", brain.Open(eng, brain.Scope{Namespace: ns})))
	spec, ok := brain.MountForKind(ms.Specs(), "Deal")
	if !ok || spec.Point != brain.DefaultMountPoint || spec.Profile != brain.DefaultProfile {
		t.Fatalf("MountForKind: %+v ok=%v", spec, ok)
	}

	md := []byte("---\ndomain: Deal\nslug: acme\nstage: open\n---\n\nHello Acme.\n")
	if err := ms.WriteFile(ctx, "/workspace/engram/deal/acme.md", md); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/engram/deal/acme.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "Hello Acme.") || !strings.Contains(string(got), "id:") {
		t.Fatalf("read after write (id rewritten):\n%s", got)
	}
	if !strings.Contains(string(got), "stage: open") {
		t.Fatalf("props:\n%s", got)
	}

	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/engram/deal/acme.md")
	if err != nil {
		t.Fatal(err)
	}
	if obj.Kind != "Deal" || obj.Content != "Hello Acme.\n" || obj.Properties["stage"] != "open" {
		t.Fatalf("engine object: %+v", obj)
	}
	if obj.ID == uuid.Nil || obj.Properties[brain.PropSlug] != "acme" {
		t.Fatalf("id/slug: %+v", obj)
	}

	ents, err := ms.ReadDir(ctx, "/workspace/engram/deal")
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 || ents[0].Name != "acme.md" || ents[0].IsDir {
		t.Fatalf("ReadDir files: %+v", ents)
	}
	roots, err := ms.ReadDir(ctx, "/workspace/engram")
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0].Name != "deal" || !roots[0].IsDir {
		t.Fatalf("ReadDir kinds (parents only): %+v", roots)
	}

	rootInfo, err := ms.Stat(ctx, "/workspace/engram")
	if err != nil || !rootInfo.IsDir {
		t.Fatalf("stat mount root: %+v err=%v", rootInfo, err)
	}
	kindInfo, err := ms.Stat(ctx, "/workspace/engram/deal")
	if err != nil || !kindInfo.IsDir || kindInfo.Name != "deal" {
		t.Fatalf("stat kind dir: %+v err=%v", kindInfo, err)
	}
	if _, err := ms.Stat(ctx, "/workspace/engram/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("stat unknown kind: %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/engram/deal"); err != nil {
		t.Fatal(err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/engram"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Open(ctx, "/workspace/engram/deal"); err == nil {
		t.Fatal("open kind dir")
	}
	if err := ms.Remove(ctx, "/workspace/engram/deal"); err == nil {
		t.Fatal("remove kind dir")
	}
	if _, err := ms.ReadDir(ctx, "/workspace/engram/deal/acme.md"); err == nil {
		t.Fatal("ReadDir file")
	}
	if _, err := ms.ReadText(ctx, "/workspace/engram"); err == nil {
		t.Fatal("ReadText mount root")
	}
	if err := ms.WriteDocument(ctx, bareDoc{path: "/workspace/engram/deal/x.md"}); !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("bare WriteDocument: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(cctx, "/workspace/engram/deal/acme.md"); !errors.Is(err, context.Canceled) {
		t.Fatalf("stat cancel: %v", err)
	}
	if _, err := ms.ReadDir(cctx, "/workspace/engram"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir cancel: %v", err)
	}
	if _, err := ms.Open(cctx, "/workspace/engram/deal/acme.md"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancel: %v", err)
	}
	if err := ms.Remove(cctx, "/workspace/engram/deal/acme.md"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Remove cancel: %v", err)
	}
	if err := ms.MkdirAll(cctx, "/workspace/engram/deal"); !errors.Is(err, context.Canceled) {
		t.Fatalf("MkdirAll cancel: %v", err)
	}
	if _, err := ms.ReadText(cctx, "/workspace/engram/deal/acme.md"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadText cancel: %v", err)
	}
	if err := ms.WriteFile(cctx, "/workspace/engram/deal/x.md", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteFile cancel: %v", err)
	}
	if err := ms.WriteDocument(cctx, vfs.NewTextDocument("/workspace/engram/deal/y.md", "text/markdown", "utf-8", "y")); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteDocument cancel: %v", err)
	}

	// Second write of the same path without id keeps Object.ID.
	rewrite := []byte("---\ndomain: Deal\nslug: acme\nstage: won\n---\n\nUpdated Acme.\n")
	if err := ms.WriteFile(ctx, "/workspace/engram/deal/acme.md", rewrite); err != nil {
		t.Fatal(err)
	}
	rewritten, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/engram/deal/acme.md")
	if err != nil || rewritten.ID != obj.ID || !strings.Contains(rewritten.Content, "Updated Acme.") {
		t.Fatalf("overwrite without id: %+v err=%v", rewritten, err)
	}

	renewal := []byte("---\ndomain: Deal\ntitle: Acme Renewal!\nstage: open\n---\n\nRenewal body.\n")
	if err := ms.WriteFile(ctx, "/workspace/engram/deal/acme-renewal.md", renewal); err != nil {
		t.Fatal(err)
	}
	ents, err = ms.ReadDir(ctx, "/workspace/engram/deal")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name)
	}
	if !strings.Contains(strings.Join(names, ","), "acme-renewal.md") {
		t.Fatalf("slug from title: %v", names)
	}

	// Required field missing: fail closed, no leftover object.
	bad := []byte("---\ndomain: Deal\nslug: leftover\n---\n\nno stage\n")
	if err := ms.WriteFile(ctx, "/workspace/engram/deal/leftover.md", bad); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("want required-field error, got %v", err)
	}
	if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/engram/deal/leftover.md"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("leftover object: %v", err)
	}
	ents, err = ms.ReadDir(ctx, "/workspace/engram/deal")
	if err != nil || len(ents) != 2 {
		t.Fatalf("dir after failed write: %+v err=%v", ents, err)
	}

	mismatch := []byte("---\ndomain: Person\nslug: x\nstage: open\n---\n\nnope\n")
	if err := ms.WriteFile(ctx, "/workspace/engram/deal/x.md", mismatch); err == nil || !strings.Contains(err.Error(), "does not match path kind") {
		t.Fatalf("kind mismatch: %v", err)
	}
	if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/engram/deal/x.md"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("mismatch leftover: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/engram/unknown/y.md", []byte("---\ndomain: Unknown\nslug: y\n---\n\nnope\n")); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("unknown kind: %v", err)
	}
	if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/engram/unknown/y.md"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("unknown leftover: %v", err)
	}

	for _, tc := range []struct {
		name, path, want string
		raw              []byte
	}{
		{"unclosed", "/workspace/engram/deal/bad-fence.md", "missing closing", []byte("---\nid: x\n")},
		{"bad yaml", "/workspace/engram/deal/bad-yaml.md", "front matter", []byte("---\n: bad\n---\n")},
		{"not uuid", "/workspace/engram/deal/bad-id.md", "engram id", []byte("---\nid: not-a-uuid\nstage: open\n---\n")},
		{"non-string domain", "/workspace/engram/deal/bad-domain.md", "must be a string", []byte("---\ndomain: [1]\nstage: open\n---\n")},
	} {
		if err := ms.WriteFile(ctx, tc.path, tc.raw); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: got %v, want containing %q", tc.name, err, tc.want)
		}
		if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, tc.path); !errors.Is(err, brain.ErrNotFound) {
			t.Fatalf("%s leftover: %v", tc.name, err)
		}
	}

	// IR replace_lines then Sync updates Content.
	doc, err := ms.ReadText(ctx, "/workspace/engram/deal/acme.md")
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(doc.Text(), "Updated Acme.", "Hello IR.", 1)
	edited := vfs.NewTextDocument("/workspace/engram/deal/acme.md", "text/markdown", "utf-8", body)
	if err := ms.WriteDocument(ctx, edited); err != nil {
		t.Fatal(err)
	}
	obj2, err := eng.Get(ctx, brain.Scope{Namespace: ns}, obj.ID)
	if err != nil || !strings.Contains(obj2.Content, "Hello IR.") {
		t.Fatalf("after IR sync: %+v err=%v", obj2, err)
	}

	if err := ms.Remove(ctx, "/workspace/engram/deal/acme.md"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Get(ctx, brain.Scope{Namespace: ns}, obj.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-delete: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/engram/deal/acme.md"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("read after remove: %v", err)
	}
}

type bareDoc struct{ path string }

func (b bareDoc) Path() string      { return b.path }
func (b bareDoc) MediaType() string { return "text/markdown" }

func TestBrainProvider_rootsLayout(t *testing.T) {
	ctx := context.Background()
	ns := brain.MustNamespace("id", uuid.NewString())
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Person", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	ms := treeSession(t, vfs.At("person", withParams(
		brain.Open(eng, brain.Scope{Namespace: ns}),
		"/workspace/person",
		map[string]string{"mode": "roots", "kind": "Person"},
	)))
	body := []byte("---\ntitle: Sam\n---\n\nBuyer.\n")
	if err := ms.WriteFile(ctx, "/workspace/person/sam.md", body); err != nil {
		t.Fatal(err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/person")
	if err != nil || len(ents) != 1 || ents[0].Name != "sam.md" {
		t.Fatalf("roots listing: %+v err=%v", ents, err)
	}
	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/person/sam.md")
	if err != nil || obj.Kind != "Person" || obj.Content != "Buyer.\n" {
		t.Fatalf("roots object: %+v err=%v", obj, err)
	}
	if _, err := ms.Stat(ctx, "/workspace/person/nested/sam.md"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("roots nested: %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/person/extra"); err == nil {
		t.Fatal("roots mkdir")
	}
	if _, err := ms.ReadDir(ctx, "/workspace/person/sam.md"); err == nil {
		t.Fatal("roots ReadDir file")
	}
	doc, err := ms.ReadText(ctx, "/workspace/person/sam.md")
	if err != nil || !strings.Contains(doc.Text(), "Buyer.") {
		t.Fatalf("roots IR: %v", err)
	}
}

func TestBrainProvider_openCatalogListsKindsInUseAndMkdir(t *testing.T) {
	ctx := context.Background()
	ns := brain.MustNamespace("id", uuid.NewString())
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ms := treeSession(t, vfs.At("engram", brain.Open(eng, brain.Scope{Namespace: ns})))
	if err := ms.MkdirAll(ctx, "/workspace/engram/note"); err != nil {
		t.Fatal(err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/engram/note/nested"); err == nil {
		t.Fatal("arbitrary dirs must fail")
	}
	if err := ms.WriteFile(ctx, "/workspace/engram/note/hello.md", []byte("hello body\n")); err != nil {
		t.Fatal(err)
	}
	kinds, err := eng.KindsWithObjects(ctx, brain.Scope{Namespace: ns})
	if err != nil || len(kinds) != 1 || kinds[0] != "note" {
		t.Fatalf("kinds in use: %v err=%v", kinds, err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/engram")
	if err != nil || len(ents) != 1 || ents[0].Name != "note" || !ents[0].IsDir {
		t.Fatalf("open catalog dirs: %+v err=%v", ents, err)
	}
	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/engram/note/hello.md")
	if err != nil || !strings.Contains(obj.Content, "hello body") {
		t.Fatalf("open write: %+v err=%v", obj, err)
	}
}

func TestBrainOpen_rejectsInvalidConfig(t *testing.T) {
	ctx := context.Background()
	ns := brain.MustNamespace("id", uuid.NewString())
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Note", IsParent: true},
		brain.KindSpec{Kind: "Deal", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	valid := brain.Open(eng, brain.Scope{Namespace: ns})

	cases := []struct {
		name string
		open vfs.Open
		bind vfs.Binding
		want string
	}{
		{
			name: "nil engine",
			open: brain.Open(nil, brain.Scope{Namespace: ns}),
			bind: vfs.Binding{Point: "/workspace/engram"},
			want: "engine is required",
		},
		{
			name: "missing namespace",
			open: brain.Open(eng, brain.Scope{}),
			bind: vfs.Binding{Point: "/workspace/engram"},
			want: "namespace is required",
		},
		{
			name: "bad mode",
			open: valid,
			bind: vfs.Binding{Point: "/workspace/engram", Params: map[string]string{"mode": "other"}},
			want: "mode must be",
		},
		{
			name: "roots without kind",
			open: valid,
			bind: vfs.Binding{Point: "/workspace/person", Params: map[string]string{"mode": "roots"}},
			want: "roots mode requires",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.open(ctx, "", tc.bind)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want containing %q", err, tc.want)
			}
		})
	}

	ms := treeSession(t, vfs.At("person", withParams(valid, "/workspace/person", map[string]string{
		"mode": "roots", "kinds": "Note",
	})))
	if err := ms.WriteFile(ctx, "/workspace/person/hello.md", []byte("---\ntitle: Hello\n---\n\nNote body.\n")); err != nil {
		t.Fatal(err)
	}
	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: ns}, brain.PropVFSPath, "/workspace/person/hello.md")
	if err != nil || obj.Kind != "Note" || !strings.Contains(obj.Content, "Note body.") {
		t.Fatalf("roots kinds= inference: %+v err=%v", obj, err)
	}
	// Empty point defaults to /workspace/engram.
	p, err := valid(ctx, "", vfs.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.Validate(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Validate cancel: %v", err)
	}
	if _, err := p.Stat(cctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat cancel: %v", err)
	}
	if _, err := p.OpenFile(cctx, "note/n.md", 0, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenFile cancel: %v", err)
	}
	if _, err := p.ReadDir(cctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir cancel: %v", err)
	}
	if err := p.Remove(cctx, "note/n.md"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Remove cancel: %v", err)
	}
	if err := p.MkdirAll(cctx, "note", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("MkdirAll cancel: %v", err)
	}
	d, ok := p.(interface {
		PutFile(context.Context, string, io.Reader, int64) error
		OpenDocument(context.Context, string, *vfs.ContentRegistry) (vfs.Document, error)
		WriteDocument(context.Context, string, vfs.Document) error
	})
	if !ok {
		t.Fatal("engram provider missing document I/O")
	}
	if err := d.PutFile(cctx, "note/n.md", strings.NewReader(""), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutFile cancel: %v", err)
	}
	if _, err := d.OpenDocument(cctx, "note/n.md", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenDocument cancel: %v", err)
	}
	if err := d.WriteDocument(cctx, "note/n.md", vfs.NewTextDocument("note/n.md", "text/markdown", "utf-8", "x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteDocument cancel: %v", err)
	}

	// prefix kinds= allow-list: ReadDir lists only the allowed kind after a write.
	ms2 := treeSession(t, vfs.At("engram", withParams(valid, "", map[string]string{
		"mode": "prefix", "kinds": "Note",
	})))
	if err := ms2.WriteFile(ctx, "/workspace/engram/note/n.md", []byte("---\ndomain: Note\nslug: n\n---\n\nallow.\n")); err != nil {
		t.Fatal(err)
	}
	ents, err := ms2.ReadDir(ctx, "/workspace/engram")
	if err != nil || len(ents) != 1 || ents[0].Name != "note" {
		t.Fatalf("kinds= allow-list dirs: %+v err=%v", ents, err)
	}
}
