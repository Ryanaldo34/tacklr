package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/builtins"
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
		opens = append(opens, builtins.Local(host))
	}
	ms, err := vfs.Tree(vfs.At("skills", vfs.Union(opens...)).ReadOnly())(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

func TestMountSession_unionMergesBackends(t *testing.T) {
	ctx := t.Context()
	a, b := t.TempDir(), t.TempDir()
	writeUnionTree(t, a, map[string]string{"alpha/SKILL.md": "---\nname: alpha\ndescription: A\n---\n\nA"})
	writeUnionTree(t, b, map[string]string{"zeta/SKILL.md": "---\nname: zeta\ndescription: Z\n---\n\nZ"})

	ms, err := vfs.Tree(vfs.At("skills", vfs.Union(builtins.Local(a), builtins.Local(b))).ReadOnly())(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	specs := ms.Specs()
	if len(specs) != 1 || specs[0].Point != vfs.WorkspacePoint || len(specs[0].Members) != 1 {
		t.Fatalf("Specs = %+v", specs)
	}
	mem := specs[0].Members[0]
	if mem.Profile != "skills" || !mem.ReadOnly || mem.IndexPolicy != "none" {
		t.Fatalf("skills member = %+v", mem)
	}
	if spec, err := ms.SpecAt("/workspace/skills/alpha/SKILL.md"); err != nil || spec.Point != "/workspace/skills" || spec.Profile != "skills" || !spec.ReadOnly || spec.IndexPolicy != "none" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}
	ents, err := ms.ReadDir(ctx, "/workspace/skills")
	if err != nil || len(ents) != 2 || ents[0].Name != "alpha" || ents[1].Name != "zeta" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/skills/alpha/SKILL.md")
	if err != nil || string(got) != "---\nname: alpha\ndescription: A\n---\n\nA" {
		t.Fatalf("ReadFile alpha = %q err=%v", got, err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/skills/zeta/SKILL.md")
	if err != nil || string(got) != "---\nname: zeta\ndescription: Z\n---\n\nZ" {
		t.Fatalf("ReadFile zeta = %q err=%v", got, err)
	}
	text, err := ms.ReadText(ctx, "/workspace/skills/alpha/SKILL.md")
	if err != nil || text.Path() != "/workspace/skills/alpha/SKILL.md" {
		t.Fatalf("ReadText: %+v err=%v", text, err)
	}
	ents, err = ms.ReadDir(ctx, "/workspace/skills/alpha")
	if err != nil || len(ents) != 1 || ents[0].Name != "SKILL.md" {
		t.Fatalf("ReadDir child = %+v err=%v", ents, err)
	}
	st, err := ms.Stat(ctx, "/workspace/skills")
	if err != nil || !st.IsDir {
		t.Fatalf("Stat root = %+v err=%v", st, err)
	}
	if st, err = ms.Stat(ctx, "/workspace/skills/alpha/SKILL.md"); err != nil || st.IsDir {
		t.Fatalf("Stat file = %+v err=%v", st, err)
	}
	if err := ms.WriteDocument(ctx, text); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteDocument = %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/skills/new.md", []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteFile = %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/skills/alpha/SKILL.md"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Remove = %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/skills/new"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("MkdirAll = %v", err)
	}
}

func TestMountSession_unionNameCollisionAtMount(t *testing.T) {
	c, d := t.TempDir(), t.TempDir()
	writeUnionTree(t, c, map[string]string{"dup/SKILL.md": "c"})
	writeUnionTree(t, d, map[string]string{"dup/SKILL.md": "d"})
	_, err := vfs.Tree(vfs.At("skills", vfs.Union(builtins.Local(c), builtins.Local(d))))(t.Context(), t.Name(), vfs.Request{})
	if !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("err = %v", err)
	}
}

func TestMountSession_unionNameCollisionAfterMount(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	writeUnionTree(t, a, map[string]string{"alpha/SKILL.md": "a"})
	writeUnionTree(t, b, map[string]string{"zeta/SKILL.md": "b"})
	ms := unionSession(t, a, b)
	writeUnionTree(t, b, map[string]string{"alpha/SKILL.md": "late"})
	if _, err := ms.ReadDir(t.Context(), "/workspace/skills"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("ReadDir = %v", err)
	}
	if _, err := ms.Stat(t.Context(), "/workspace/skills/alpha/SKILL.md"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("Stat = %v", err)
	}
}

func TestUnion_constructErrors(t *testing.T) {
	ctx := t.Context()
	if _, err := vfs.Union()(ctx, t.Name(), vfs.Binding{}); err == nil {
		t.Fatal("empty Union")
	}
	if _, err := vfs.Union(nil)(ctx, t.Name(), vfs.Binding{}); err == nil {
		t.Fatal("nil member open")
	}
	boom := vfs.Open(func(context.Context, string, vfs.Binding) (vfs.Provider, error) {
		return nil, errors.New("open failed")
	})
	if _, err := vfs.Union(boom)(ctx, t.Name(), vfs.Binding{}); err == nil {
		t.Fatal("member open error")
	}
}

func TestMountSession_unionMissingAndCanceled(t *testing.T) {
	ms := unionSession(t, t.TempDir())
	ctx := t.Context()
	if _, err := ms.Stat(ctx, "/workspace/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("Stat = %v", err)
	}
	if _, err := ms.ReadDir(ctx, "/workspace/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadDir = %v", err)
	}
	if _, err := ms.Open(ctx, "/workspace/skills"); err == nil {
		t.Fatal("open root")
	}
	if _, err := ms.Open(ctx, "/workspace/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("Open missing = %v", err)
	}
	if _, err := ms.ReadText(ctx, "/workspace/skills"); err == nil {
		t.Fatal("ReadText root")
	}
	if _, err := ms.ReadText(ctx, "/workspace/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadText missing = %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(canceled, "/workspace/skills"); err == nil {
		t.Fatal("Stat canceled")
	}
	if _, err := ms.ReadDir(canceled, "/workspace/skills"); err == nil {
		t.Fatal("ReadDir canceled")
	}
	if _, err := ms.Open(canceled, "/workspace/skills/x"); err == nil {
		t.Fatal("Open canceled")
	}
	if _, err := ms.ReadText(canceled, "/workspace/skills/x"); err == nil {
		t.Fatal("ReadText canceled")
	}
	if _, err := vfs.Tree(vfs.At("skills", vfs.Union(builtins.Local(t.TempDir()))))(canceled, "union-cancel", vfs.Request{}); err == nil {
		t.Fatal("Tree canceled")
	}
}

func TestMountSession_workAndSkills(t *testing.T) {
	ctx := t.Context()
	work, pack := t.TempDir(), t.TempDir()
	writeUnionTree(t, pack, map[string]string{"alpha/SKILL.md": "---\nname: alpha\ndescription: A\n---\n\nA"})
	ms, err := vfs.Tree(
		vfs.At("work", builtins.Local(work)),
		vfs.At("skills", vfs.Union(builtins.Local(pack))).ReadOnly(),
	)(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	spec, err := ms.SpecAt("/workspace/skills/alpha/SKILL.md")
	if err != nil || spec.Point != "/workspace/skills" || !spec.ReadOnly || spec.IndexPolicy != "none" {
		t.Fatalf("SpecAt /workspace/skills: %+v err=%v", spec, err)
	}
	got, err := ms.ReadFile(ctx, "/workspace/skills/alpha/SKILL.md")
	if err != nil || !strings.Contains(string(got), "name: alpha") {
		t.Fatalf("ReadFile = %q err=%v", got, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/note.txt", []byte("ok")); err != nil {
		t.Fatal(err)
	}
}
