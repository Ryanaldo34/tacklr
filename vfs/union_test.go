package vfs_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/vfs"
)

func writeUnionTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func unionSession(t *testing.T, dirs ...string) *vfs.MountSession {
	t.Helper()
	opens := make([]vfs.Open, 0, len(dirs))
	for _, host := range dirs {
		if err := os.MkdirAll(host, 0o755); err != nil {
			t.Fatal(err)
		}
		opens = append(opens, vfs.Local(host))
	}
	ms, err := vfs.Tree(vfs.At("skills", vfs.Union(opens...)).ReadOnly())(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// A union lists and reads the files of all its members as one read-only
// member.
func TestMountSession_unionMergesBackends(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b := t.TempDir(), t.TempDir()
	writeUnionTree(t, a, map[string]string{"alpha/SKILL.md": "---\nname: alpha\ndescription: A\n---\n\nA"})
	writeUnionTree(t, b, map[string]string{"zeta/SKILL.md": "---\nname: zeta\ndescription: Z\n---\n\nZ"})

	ms, err := vfs.Tree(vfs.At("skills", vfs.Union(vfs.Local(a), vfs.Local(b))).ReadOnly())(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	specs := ms.Specs()
	if len(specs) != 1 || specs[0].Point != "/workspace/skills" {
		t.Fatalf("Specs = %+v", specs)
	}
	mem := specs[0]
	if mem.Profile != "skills" || !mem.ReadOnly || mem.NotIndexed {
		t.Fatalf("skills member = %+v", mem)
	}
	if spec, err := ms.SpecAt("/workspace/skills/alpha/SKILL.md"); err != nil || spec.Point != "/workspace/skills" || spec.Profile != "skills" || !spec.ReadOnly || spec.NotIndexed {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}

	ents, err := ms.Route(ctx, "/workspace/skills").
		ReadDir(ctx)
	if err != nil || len(ents) != 2 || ents[0].Name != "alpha" || ents[1].Name != "zeta" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}

	got, err := ms.Route(ctx, "/workspace/skills/alpha/SKILL.md").
		ReadFile(ctx)
	if err != nil || string(got) != "---\nname: alpha\ndescription: A\n---\n\nA" {
		t.Fatalf("ReadFile alpha = %q err=%v", got, err)
	}

	if got, err := ms.Route(ctx, "/workspace/skills/zeta/SKILL.md").
		ReadFile(ctx); err != nil || string(got) != "---\nname: zeta\ndescription: Z\n---\n\nZ" {
		t.Fatalf("ReadFile zeta = %q err=%v", got, err)
	}

	text, err := ms.Route(ctx, "/workspace/skills/alpha/SKILL.md").
		ReadText(ctx)
	if err != nil || text.Path() != "/workspace/skills/alpha/SKILL.md" {
		t.Fatalf("ReadText: %+v err=%v", text, err)
	}

	if ents, err := ms.Route(ctx, "/workspace/skills/alpha").
		ReadDir(ctx); err != nil || len(ents) != 1 || ents[0].Name != "SKILL.md" {
		t.Fatalf("ReadDir child = %+v err=%v", ents, err)
	}

	st, err := ms.Route(ctx, "/workspace/skills").
		Stat(ctx)
	if err != nil || !st.IsDir {
		t.Fatalf("Stat root = %+v err=%v", st, err)
	}

	if st, err := ms.Route(ctx, "/workspace/skills/alpha/SKILL.md").
		Stat(ctx); err != nil || st.IsDir {
		t.Fatalf("Stat file = %+v err=%v", st, err)
	}

	if _, err := ms.Route(ctx, "/workspace/skills/alpha/SKILL.md").
		OpenDocument(ctx); err != nil {
		t.Fatalf("OpenDocument local = %v", err)
	}

	if err := ms.WriteDocument(ctx, text); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteDocument = %v", err)
	}

	if err := ms.Route(ctx, "/workspace/skills/new.md").
		WriteFile(ctx, []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteFile = %v", err)
	}

	if err := ms.Route(ctx, "/workspace/skills/alpha/SKILL.md").
		Remove(ctx); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Remove = %v", err)
	}

	if err := ms.Route(ctx, "/workspace/skills/new").
		MkdirAll(ctx); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("MkdirAll = %v", err)
	}
}

// A name that two union members both have is ambiguous: at mount, and when
// a member gains it after the mount.
func TestMountSession_unionNameInTwoMembersIsAmbiguous(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, c, d := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	writeUnionTree(t, a, map[string]string{"alpha/SKILL.md": "a"})
	writeUnionTree(t, b, map[string]string{"zeta/SKILL.md": "b"})
	writeUnionTree(t, c, map[string]string{"dup/SKILL.md": "c"})
	writeUnionTree(t, d, map[string]string{"dup/SKILL.md": "d"})
	ms := unionSession(t, a, b)
	writeUnionTree(t, b, map[string]string{"alpha/SKILL.md": "late"})

	_, atMount := vfs.Tree(vfs.At("skills", vfs.Union(vfs.Local(c), vfs.Local(d))))(ctx, t.Name(), vfs.Request{})
	_, readDirErr := ms.Route(ctx, "/workspace/skills").ReadDir(ctx)
	_, statErr := ms.Route(ctx, "/workspace/skills/alpha/SKILL.md").Stat(ctx)

	for name, err := range map[string]error{"mount": atMount, "ReadDir": readDirErr, "Stat": statErr} {
		if !errors.Is(err, vfs.ErrAmbiguous) {
			t.Fatalf("%s = %v, want ErrAmbiguous", name, err)
		}
	}
}

// A union needs members, each with an open; it fails when a member open
// fails and leaves out a member that opens no provider.
func TestUnion_constructErrors(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	if _, err := vfs.Union()(ctx, t.Name(), mount.Binding{}); err == nil {
		t.Fatal("empty Union")
	}
	if _, err := vfs.Union(nil)(ctx, t.Name(), mount.Binding{}); err == nil {
		t.Fatal("nil member open")
	}
	boom := vfs.Open(func(context.Context, string, mount.Binding) (vfs.Provider, error) {
		return nil, errors.New("open failed")
	})
	if _, err := vfs.Union(boom)(ctx, t.Name(), mount.Binding{}); err == nil {
		t.Fatal("member open error")
	}
	skip := vfs.Open(func(context.Context, string, mount.Binding) (vfs.Provider, error) {
		return nil, nil
	})
	p, err := vfs.Union(vfs.Local(t.TempDir()), skip)(ctx, t.Name(), mount.Binding{})
	if err != nil || p == nil {
		t.Fatalf("nil member skipped: p=%v err=%v", p, err)
	}
}

// A name no member has is not found, the union root is no file, and a
// canceled context stops every call.
func TestMountSession_unionMissingAndCanceled(t *testing.T) {
	t.Parallel()
	ms := unionSession(t, t.TempDir())
	ctx := t.Context()

	if _, err := ms.Route(ctx, "/workspace/skills/nope").
		Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("Stat = %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/skills/nope").
		ReadDir(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadDir = %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/skills").
		Open(ctx); err == nil {
		t.Fatal("open root")
	}

	if _, err := ms.Route(ctx, "/workspace/skills/nope.md").
		OpenDocument(ctx); err == nil {
		t.Fatal("OpenDocument missing")
	}

	if _, err := ms.Route(ctx, "/workspace/skills/nope").
		Open(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("Open missing = %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/skills").
		ReadText(ctx); err == nil {
		t.Fatal("ReadText root")
	}

	if _, err := ms.Route(ctx, "/workspace/skills/nope").
		ReadText(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadText missing = %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := ms.Route(canceled, "/workspace/skills").
		Stat(canceled); err == nil {
		t.Fatal("Stat canceled")
	}

	if _, err := ms.Route(canceled, "/workspace/skills").
		ReadDir(canceled); err == nil {
		t.Fatal("ReadDir canceled")
	}

	if _, err := ms.Route(canceled, "/workspace/skills/x").
		Open(canceled); err == nil {
		t.Fatal("Open canceled")
	}

	if _, err := ms.Route(canceled, "/workspace/skills/x").
		ReadText(canceled); err == nil {
		t.Fatal("ReadText canceled")
	}

	if _, err := vfs.Tree(vfs.At("skills", vfs.Union(vfs.Local(t.TempDir()))))(canceled, "union-cancel", vfs.Request{}); err == nil {
		t.Fatal("Tree canceled")
	}
}

// The union provider refuses every write and a name that leaves its root.
func TestUnion_providerRejectsWritesAndInvalidPaths(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p, err := vfs.Union(vfs.Local(t.TempDir()))(ctx, t.Name(), mount.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Remove(ctx, "x"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Remove = %v", err)
	}
	if err := p.MkdirAll(ctx, "x", 0o755); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("MkdirAll = %v", err)
	}
	if _, err := p.OpenFile(ctx, "x", os.O_WRONLY, 0); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("OpenFile write = %v", err)
	}
	if _, err := p.Stat(ctx, ".."); err == nil {
		t.Fatal("Stat invalid")
	}
	if _, err := p.OpenFile(ctx, "..", os.O_RDONLY, 0); err == nil {
		t.Fatal("OpenFile invalid")
	}
	if _, err := p.ReadDir(ctx, ".."); err == nil {
		t.Fatal("ReadDir invalid")
	}
	docs := p.(interface {
		OpenDocument(ctx context.Context, name string, reg *vfs.ContentRegistry) (vfs.Document, error)
	})
	if _, err := docs.OpenDocument(ctx, "..", nil); !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("OpenDocument invalid = %v", err)
	}
}

// TestUnion_aMemberFailureFailsTheUnion: a member root that is gone or that
// the user cannot list fails the mount. A first name that a member cannot
// stat, for example a symlink out of its root, fails the lookup. A member with
// no document I/O has no documents, and a union that the tree mounts
// writable still refuses a document write.
func TestUnion_aMemberFailureFailsTheUnion(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	gone := filepath.Join(t.TempDir(), "gone")
	if err := os.Mkdir(gone, 0o750); err != nil {
		t.Fatal(err)
	}
	goneRoot, err := vfs.NewLocalProvider(gone)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	locked := t.TempDir()
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o750) })
	linked := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(linked, "out")); err != nil {
		t.Fatal(err)
	}
	host := func(context.Context, string, mount.Binding) (vfs.Provider, error) {
		return hostProvider{open: opens(hostFile{})}, nil
	}
	tree := func(member vfs.Open) (*vfs.MountSession, error) {
		return vfs.Tree(vfs.At("skills", vfs.Union(member)))(ctx, t.Name(), vfs.Request{})
	}
	prebuilt := func(p vfs.Provider) vfs.Open {
		return func(context.Context, string, mount.Binding) (vfs.Provider, error) { return p, nil }
	}
	links, err := tree(vfs.Local(linked))
	if err != nil {
		t.Fatal(err)
	}
	hosted, err := tree(host)
	if err != nil {
		t.Fatal(err)
	}

	_, goneErr := tree(prebuilt(goneRoot))
	_, lockedErr := tree(vfs.Local(locked))
	_, linkErr := links.Route(ctx, "/workspace/skills/out/x").Stat(ctx)
	_, hostDocErr := hosted.Route(ctx, "/workspace/skills/f").OpenDocument(ctx)
	writeErr := links.WriteDocument(ctx, vfs.NewTextDocument("/workspace/skills/new.md", "text/markdown", "", "x"))

	if !errors.Is(goneErr, fs.ErrNotExist) {
		t.Fatalf("mount with a gone member root = %v", goneErr)
	}
	if !errors.Is(lockedErr, vfs.ErrPermission) {
		t.Fatalf("mount with a member root the user cannot list = %v", lockedErr)
	}
	if !errors.Is(linkErr, vfs.ErrInvalidPath) {
		t.Fatalf("stat through a symlink out of a member = %v", linkErr)
	}
	if !errors.Is(hostDocErr, vfs.ErrNotSupported) {
		t.Fatalf("document of a member with no document I/O = %v", hostDocErr)
	}
	if !errors.Is(writeErr, vfs.ErrReadOnly) {
		t.Fatalf("document write to a writable union = %v", writeErr)
	}
}
