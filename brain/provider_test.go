package brain_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestBrainProvider_prefixWriteReadDirRemoveAndIR(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
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
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(brain.BrainFactory{
		ID: "brain", Engine: eng, Scope: brain.Scope{Namespace: &ns},
	}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("engram", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{
		Point: "/engram", Profile: "brain",
		Params: map[string]string{"mode": "prefix"},
	}); err != nil {
		t.Fatal(err)
	}

	md := []byte("---\ndomain: Deal\nslug: acme\nstage: open\n---\n\nHello Acme.\n")
	if err := ms.WriteFile(ctx, "/engram/deal/acme.md", md); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(ctx, "/engram/deal/acme.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "Hello Acme.") || !strings.Contains(string(got), "id:") {
		t.Fatalf("read after write (id rewritten):\n%s", got)
	}
	if !strings.Contains(string(got), "stage: open") {
		t.Fatalf("props:\n%s", got)
	}

	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/engram/deal/acme.md")
	if err != nil {
		t.Fatal(err)
	}
	if obj.Kind != "Deal" || obj.Content != "Hello Acme.\n" || obj.Properties["stage"] != "open" {
		t.Fatalf("engine object: %+v", obj)
	}
	if obj.ID == uuid.Nil || obj.Properties[brain.PropSlug] != "acme" {
		t.Fatalf("id/slug: %+v", obj)
	}

	ents, err := ms.ReadDir(ctx, "/engram/deal")
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 || ents[0].Name != "acme.md" || ents[0].IsDir {
		t.Fatalf("ReadDir files: %+v", ents)
	}
	roots, err := ms.ReadDir(ctx, "/engram")
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0].Name != "deal" || !roots[0].IsDir {
		t.Fatalf("ReadDir kinds (parents only): %+v", roots)
	}

	rootInfo, err := ms.Stat(ctx, "/engram")
	if err != nil || !rootInfo.IsDir {
		t.Fatalf("stat mount root: %+v err=%v", rootInfo, err)
	}
	kindInfo, err := ms.Stat(ctx, "/engram/deal")
	if err != nil || !kindInfo.IsDir || kindInfo.Name != "deal" {
		t.Fatalf("stat kind dir: %+v err=%v", kindInfo, err)
	}
	if _, err := ms.Stat(ctx, "/engram/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("stat unknown kind: %v", err)
	}

	// Second write of the same path without id keeps Object.ID.
	rewrite := []byte("---\ndomain: Deal\nslug: acme\nstage: won\n---\n\nUpdated Acme.\n")
	if err := ms.WriteFile(ctx, "/engram/deal/acme.md", rewrite); err != nil {
		t.Fatal(err)
	}
	rewritten, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/engram/deal/acme.md")
	if err != nil || rewritten.ID != obj.ID || !strings.Contains(rewritten.Content, "Updated Acme.") {
		t.Fatalf("overwrite without id: %+v err=%v", rewritten, err)
	}

	renewal := []byte("---\ndomain: Deal\ntitle: Acme Renewal!\nstage: open\n---\n\nRenewal body.\n")
	if err := ms.WriteFile(ctx, "/engram/deal/acme-renewal.md", renewal); err != nil {
		t.Fatal(err)
	}
	ents, err = ms.ReadDir(ctx, "/engram/deal")
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
	if err := ms.WriteFile(ctx, "/engram/deal/leftover.md", bad); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("want required-field error, got %v", err)
	}
	if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/engram/deal/leftover.md"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("leftover object: %v", err)
	}
	ents, err = ms.ReadDir(ctx, "/engram/deal")
	if err != nil || len(ents) != 2 {
		t.Fatalf("dir after failed write: %+v err=%v", ents, err)
	}

	mismatch := []byte("---\ndomain: Person\nslug: x\nstage: open\n---\n\nnope\n")
	if err := ms.WriteFile(ctx, "/engram/deal/x.md", mismatch); err == nil || !strings.Contains(err.Error(), "does not match path kind") {
		t.Fatalf("kind mismatch: %v", err)
	}
	if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/engram/deal/x.md"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("mismatch leftover: %v", err)
	}
	if err := ms.WriteFile(ctx, "/engram/unknown/y.md", []byte("---\ndomain: Unknown\nslug: y\n---\n\nnope\n")); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("unknown kind: %v", err)
	}
	if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/engram/unknown/y.md"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("unknown leftover: %v", err)
	}

	for _, tc := range []struct {
		name, path, want string
		raw              []byte
	}{
		{"unclosed", "/engram/deal/bad-fence.md", "missing closing", []byte("---\nid: x\n")},
		{"bad yaml", "/engram/deal/bad-yaml.md", "front matter", []byte("---\n: bad\n---\n")},
		{"not uuid", "/engram/deal/bad-id.md", "engram id", []byte("---\nid: not-a-uuid\nstage: open\n---\n")},
		{"non-string domain", "/engram/deal/bad-domain.md", "must be a string", []byte("---\ndomain: [1]\nstage: open\n---\n")},
	} {
		if err := ms.WriteFile(ctx, tc.path, tc.raw); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: got %v, want containing %q", tc.name, err, tc.want)
		}
		if _, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, tc.path); !errors.Is(err, brain.ErrNotFound) {
			t.Fatalf("%s leftover: %v", tc.name, err)
		}
	}

	// IR replace_lines then Sync updates Content.
	doc, err := ms.ReadText(ctx, "/engram/deal/acme.md")
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(doc.Text(), "Updated Acme.", "Hello IR.", 1)
	edited := vfs.NewTextDocument("/engram/deal/acme.md", "text/markdown", "utf-8", body)
	if err := ms.WriteDocument(ctx, edited); err != nil {
		t.Fatal(err)
	}
	if err := ms.Sync(ctx, "/engram/deal/acme.md"); err != nil {
		t.Fatal(err)
	}
	obj2, err := eng.Get(ctx, brain.Scope{Namespace: &ns}, obj.ID)
	if err != nil || !strings.Contains(obj2.Content, "Hello IR.") {
		t.Fatalf("after IR sync: %+v err=%v", obj2, err)
	}

	if err := ms.Remove(ctx, "/engram/deal/acme.md"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Get(ctx, brain.Scope{Namespace: &ns}, obj.ID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("soft-delete: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/engram/deal/acme.md"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("read after remove: %v", err)
	}
}

func TestBrainProvider_rootsLayout(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Person", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(brain.BrainFactory{
		Engine: eng, Scope: brain.Scope{Namespace: &ns},
	}); err != nil {
		t.Fatal(err)
	}
	if reg.HasProfile("brain") == false {
		t.Fatal("default profile")
	}
	ms := vfs.NewMountSession("roots", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{
		Point: "/person", Profile: "brain",
		Params: map[string]string{"mode": "roots", "kind": "Person"},
	}); err != nil {
		t.Fatal(err)
	}
	body := []byte("---\ntitle: Sam\n---\n\nBuyer.\n")
	if err := ms.WriteFile(ctx, "/person/sam.md", body); err != nil {
		t.Fatal(err)
	}
	ents, err := ms.ReadDir(ctx, "/person")
	if err != nil || len(ents) != 1 || ents[0].Name != "sam.md" {
		t.Fatalf("roots listing: %+v err=%v", ents, err)
	}
	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/person/sam.md")
	if err != nil || obj.Kind != "Person" || obj.Content != "Buyer.\n" {
		t.Fatalf("roots object: %+v err=%v", obj, err)
	}
}

func TestBrainProvider_openCatalogListsKindsInUseAndMkdir(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(brain.BrainFactory{
		Engine: eng, Scope: brain.Scope{Namespace: &ns},
	}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("open", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/engram", Profile: "brain"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.MkdirAll(ctx, "/engram/note"); err != nil {
		t.Fatal(err)
	}
	if err := ms.MkdirAll(ctx, "/engram/note/nested"); err == nil {
		t.Fatal("arbitrary dirs must fail")
	}
	if err := ms.WriteFile(ctx, "/engram/note/hello.md", []byte("hello body\n")); err != nil {
		t.Fatal(err)
	}
	kinds, err := eng.KindsWithObjects(ctx, brain.Scope{Namespace: &ns})
	if err != nil || len(kinds) != 1 || kinds[0] != "note" {
		t.Fatalf("kinds in use: %v err=%v", kinds, err)
	}
	ents, err := ms.ReadDir(ctx, "/engram")
	if err != nil || len(ents) != 1 || ents[0].Name != "note" || !ents[0].IsDir {
		t.Fatalf("open catalog dirs: %+v err=%v", ents, err)
	}
	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/engram/note/hello.md")
	if err != nil || !strings.Contains(obj.Content, "hello body") {
		t.Fatalf("open write: %+v err=%v", obj, err)
	}
}

func TestBrainFactory_openRejectsInvalidConfig(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(
		brain.KindSpec{Kind: "Note", IsParent: true},
		brain.KindSpec{Kind: "Deal", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	valid := brain.BrainFactory{Engine: eng, Scope: brain.Scope{Namespace: &ns}}

	cases := []struct {
		name string
		f    brain.BrainFactory
		spec vfs.MountSpec
		want string
	}{
		{
			name: "nil engine",
			f:    brain.BrainFactory{Scope: brain.Scope{Namespace: &ns}},
			spec: vfs.MountSpec{Point: "/engram"},
			want: "engine is required",
		},
		{
			name: "missing namespace",
			f:    brain.BrainFactory{Engine: eng, Scope: brain.Scope{}},
			spec: vfs.MountSpec{Point: "/engram"},
			want: "namespace is required",
		},
		{
			name: "bad mode",
			f:    valid,
			spec: vfs.MountSpec{Point: "/engram", Params: map[string]string{"mode": "other"}},
			want: "mode must be",
		},
		{
			name: "roots without kind",
			f:    valid,
			spec: vfs.MountSpec{Point: "/person", Params: map[string]string{"mode": "roots"}},
			want: "roots mode requires",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.f.Open(ctx, "", tc.spec)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want containing %q", err, tc.want)
			}
		})
	}

	// kinds= singleton infers roots kind= (no kind= param).
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(valid); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("factory", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{
		Point: "/person", Profile: "brain",
		Params: map[string]string{"mode": "roots", "kinds": "Note"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/person/hello.md", []byte("---\ntitle: Hello\n---\n\nNote body.\n")); err != nil {
		t.Fatal(err)
	}
	obj, err := eng.GetByProperty(ctx, brain.Scope{Namespace: &ns}, brain.PropVFSPath, "/person/hello.md")
	if err != nil || obj.Kind != "Note" || !strings.Contains(obj.Content, "Note body.") {
		t.Fatalf("roots kinds= inference: %+v err=%v", obj, err)
	}

	// prefix kinds= allow-list: ReadDir lists only the allowed kind after a write.
	ms2 := vfs.NewMountSession("kinds-allow", reg)
	if err := ms2.Mount(ctx, vfs.MountSpec{
		Point: "/engram", Profile: "brain",
		Params: map[string]string{"mode": "prefix", "kinds": "Note"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ms2.WriteFile(ctx, "/engram/note/n.md", []byte("---\ndomain: Note\nslug: n\n---\n\nallow.\n")); err != nil {
		t.Fatal(err)
	}
	ents, err := ms2.ReadDir(ctx, "/engram")
	if err != nil || len(ents) != 1 || ents[0].Name != "note" {
		t.Fatalf("kinds= allow-list dirs: %+v err=%v", ents, err)
	}
}
