package vfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func unionSession(t *testing.T, hosts map[string]string, members []vfs.MountSpec) *vfs.MountSession {
	t.Helper()
	reg := vfs.NewBackendRegistry()
	for id, host := range hosts {
		if err := os.MkdirAll(host, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(vfs.LocalFactory{ID: id, Base: host}); err != nil {
			t.Fatal(err)
		}
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.Mount(t.Context(), vfs.MountSpec{
		Point: "/skills", Profile: "skills", IndexPolicy: "none", Members: members,
	}); err != nil {
		t.Fatal(err)
	}
	return ms
}

func TestMountSession_unionMergesBackends(t *testing.T) {
	ctx := t.Context()
	a, b := t.TempDir(), t.TempDir()
	writeUnionTree(t, a, map[string]string{"alpha/SKILL.md": "---\nname: alpha\ndescription: A\n---\n\nA"})
	writeUnionTree(t, b, map[string]string{"zeta/SKILL.md": "---\nname: zeta\ndescription: Z\n---\n\nZ"})

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "a", Base: a}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.LocalFactory{ID: "b", Base: b}); err != nil {
		t.Fatal(err)
	}
	spec := vfs.MountSpec{
		Point: "/skills", Profile: "skills", IndexPolicy: "none",
		Members: []vfs.MountSpec{{Profile: "a"}, {Profile: "b"}},
	}
	if err := vfs.CheckMount(ctx, reg, t.Name(), spec); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.Mount(ctx, spec); err != nil {
		t.Fatal(err)
	}

	if specs := ms.Specs(); len(specs) != 1 || specs[0].Point != "/skills" || !specs[0].ReadOnly || len(specs[0].Members) != 2 {
		t.Fatalf("Specs = %+v", specs)
	}
	if spec, err := ms.SpecAt("/skills/alpha/SKILL.md"); err != nil || spec.Profile != "skills" {
		t.Fatalf("SpecAt: %+v err=%v", spec, err)
	}
	ents, err := ms.ReadDir(ctx, "/skills")
	if err != nil || len(ents) != 2 || ents[0].Name != "alpha" || ents[1].Name != "zeta" {
		t.Fatalf("ReadDir = %+v err=%v", ents, err)
	}
	got, err := ms.ReadFile(ctx, "/skills/alpha/SKILL.md")
	if err != nil || string(got) != "---\nname: alpha\ndescription: A\n---\n\nA" {
		t.Fatalf("ReadFile alpha = %q err=%v", got, err)
	}
	got, err = ms.ReadFile(ctx, "/skills/zeta/SKILL.md")
	if err != nil || string(got) != "---\nname: zeta\ndescription: Z\n---\n\nZ" {
		t.Fatalf("ReadFile zeta = %q err=%v", got, err)
	}
	text, err := ms.ReadText(ctx, "/skills/alpha/SKILL.md")
	if err != nil || text.Path() != "/skills/alpha/SKILL.md" {
		t.Fatalf("ReadText: %+v err=%v", text, err)
	}
	ents, err = ms.ReadDir(ctx, "/skills/alpha")
	if err != nil || len(ents) != 1 || ents[0].Name != "SKILL.md" {
		t.Fatalf("ReadDir child = %+v err=%v", ents, err)
	}
	st, err := ms.Stat(ctx, "/skills")
	if err != nil || !st.IsDir {
		t.Fatalf("Stat root = %+v err=%v", st, err)
	}
	if st, err = ms.Stat(ctx, "/skills/alpha/SKILL.md"); err != nil || st.IsDir {
		t.Fatalf("Stat file = %+v err=%v", st, err)
	}
	if err := ms.WriteDocument(ctx, text); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteDocument = %v", err)
	}
	if err := ms.WriteFile(ctx, "/skills/new.md", []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteFile = %v", err)
	}
	if err := ms.Remove(ctx, "/skills/alpha/SKILL.md"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Remove = %v", err)
	}
	if err := ms.MkdirAll(ctx, "/skills/new"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("MkdirAll = %v", err)
	}
}

func TestMountSession_unionNameCollisionAtMount(t *testing.T) {
	c, d := t.TempDir(), t.TempDir()
	writeUnionTree(t, c, map[string]string{"dup/SKILL.md": "c"})
	writeUnionTree(t, d, map[string]string{"dup/SKILL.md": "d"})
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "c", Base: c}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.LocalFactory{ID: "d", Base: d}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.Mount(t.Context(), vfs.MountSpec{
		Point: "/skills", Profile: "skills",
		Members: []vfs.MountSpec{{Profile: "c"}, {Profile: "d"}},
	}); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("err = %v", err)
	}
}

func TestMountSession_unionNameCollisionAfterMount(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	writeUnionTree(t, a, map[string]string{"alpha/SKILL.md": "a"})
	writeUnionTree(t, b, map[string]string{"zeta/SKILL.md": "b"})
	ms := unionSession(t, map[string]string{"a": a, "b": b}, []vfs.MountSpec{{Profile: "a"}, {Profile: "b"}})
	writeUnionTree(t, b, map[string]string{"alpha/SKILL.md": "late"})
	if _, err := ms.ReadDir(t.Context(), "/skills"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("ReadDir = %v", err)
	}
	if _, err := ms.Stat(t.Context(), "/skills/alpha/SKILL.md"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("Stat = %v", err)
	}
}

func TestMountSession_unionConstructErrors(t *testing.T) {
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "a", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	t.Cleanup(func() { _ = ms.Close() })
	ctx := t.Context()

	t.Run("empty profile", func(t *testing.T) {
		if err := ms.Mount(ctx, vfs.MountSpec{
			Point: "/skills", Members: []vfs.MountSpec{{Profile: "a"}},
		}); !errors.Is(err, vfs.ErrInvalidProvider) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("member point", func(t *testing.T) {
		if err := ms.Mount(ctx, vfs.MountSpec{
			Point: "/skills", Profile: "skills",
			Members: []vfs.MountSpec{{Profile: "a", Point: "/hidden"}},
		}); err == nil || !strings.Contains(err.Error(), "member point") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("member profile", func(t *testing.T) {
		if err := ms.Mount(ctx, vfs.MountSpec{
			Point: "/skills", Profile: "skills",
			Members: []vfs.MountSpec{{}},
		}); err == nil || !strings.Contains(err.Error(), "profile required") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unknown member", func(t *testing.T) {
		if err := ms.Mount(ctx, vfs.MountSpec{
			Point: "/skills", Profile: "skills",
			Members: []vfs.MountSpec{{Profile: "missing"}},
		}); !errors.Is(err, vfs.ErrUnknownProfile) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("nested union", func(t *testing.T) {
		if err := ms.Mount(ctx, vfs.MountSpec{
			Point: "/skills", Profile: "skills",
			Members: []vfs.MountSpec{{Profile: "a", Members: []vfs.MountSpec{{Profile: "a"}}}},
		}); err == nil || !strings.Contains(err.Error(), "nested union") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestMountSession_unionMissingAndCanceled(t *testing.T) {
	ms := unionSession(t, map[string]string{"a": t.TempDir()}, []vfs.MountSpec{{Profile: "a"}})
	ctx := t.Context()
	if _, err := ms.Stat(ctx, "/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("Stat = %v", err)
	}
	if _, err := ms.ReadDir(ctx, "/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadDir = %v", err)
	}
	if _, err := ms.Open(ctx, "/skills"); err == nil {
		t.Fatal("open root")
	}
	if _, err := ms.Open(ctx, "/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("Open missing = %v", err)
	}
	if _, err := ms.ReadText(ctx, "/skills"); err == nil {
		t.Fatal("ReadText root")
	}
	if _, err := ms.ReadText(ctx, "/skills/nope"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("ReadText missing = %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(canceled, "/skills"); err == nil {
		t.Fatal("Stat canceled")
	}
	if _, err := ms.ReadDir(canceled, "/skills"); err == nil {
		t.Fatal("ReadDir canceled")
	}
	if _, err := ms.Open(canceled, "/skills/x"); err == nil {
		t.Fatal("Open canceled")
	}
	if _, err := ms.ReadText(canceled, "/skills/x"); err == nil {
		t.Fatal("ReadText canceled")
	}
	if err := vfs.MustNewMountSession("union-cancel", vfs.NewBackendRegistry()).Mount(canceled, vfs.MountSpec{
		Point: "/skills", Profile: "skills", Members: []vfs.MountSpec{{Profile: "a"}},
	}); err == nil {
		t.Fatal("Mount canceled")
	}
}

func TestMountSession_attachesSkillsFromFactory(t *testing.T) {
	ctx := t.Context()
	work, pack := t.TempDir(), t.TempDir()
	writeUnionTree(t, pack, map[string]string{"alpha/SKILL.md": "---\nname: alpha\ndescription: A\n---\n\nA"})
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "work", Base: work}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.LocalFactory{ID: "pack", Base: pack, Skills: "."}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.MustNewMountSession(t.Name(), reg)
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "work"}); err != nil {
		t.Fatal(err)
	}
	spec, err := ms.SpecAt("/skills/alpha/SKILL.md")
	if err != nil || spec.Point != vfs.SkillsPoint || !spec.ReadOnly || spec.IndexPolicy != "none" {
		t.Fatalf("SpecAt /skills: %+v err=%v", spec, err)
	}
	got, err := ms.ReadFile(ctx, "/skills/alpha/SKILL.md")
	if err != nil || !strings.Contains(string(got), "name: alpha") {
		t.Fatalf("ReadFile = %q err=%v", got, err)
	}
	other := t.TempDir()
	if err := reg.Register(vfs.LocalFactory{ID: "other", Base: other}); err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/other", Profile: "other"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ReadFile(ctx, "/skills/alpha/SKILL.md"); err != nil {
		t.Fatalf("skills after remount: %v", err)
	}
}

func TestSkillSource_memberSpecs(t *testing.T) {
	if _, ok := (vfs.LocalFactory{ID: "x", Base: t.TempDir()}).SkillMember(); ok {
		t.Fatal("empty Skills")
	}
	spec, ok := (vfs.LocalFactory{ID: "x", Skills: "."}).SkillMember()
	if !ok || spec.Profile != "x" || spec.Params != nil {
		t.Fatalf("local . = %+v ok=%v", spec, ok)
	}
	spec, ok = (vfs.LocalFactory{ID: "x", Skills: "pack"}).SkillMember()
	if !ok || spec.Params["subpath"] != "pack" {
		t.Fatalf("local pack = %+v", spec)
	}
	if _, ok := (vfs.S3Factory{ID: "s"}).SkillMember(); ok {
		t.Fatal("empty S3 Skills")
	}
	spec, ok = (vfs.S3Factory{ID: "s", Skills: "org/"}).SkillMember()
	if !ok || spec.Params["prefix"] != "org" {
		t.Fatalf("s3 prefix = %+v", spec)
	}
	spec, ok = (vfs.S3Factory{ID: "s", Skills: "."}).SkillMember()
	if !ok || spec.Params != nil {
		t.Fatalf("s3 . = %+v", spec)
	}
}
