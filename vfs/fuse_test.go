package vfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fuseAvailable() bool {
	if _, err := os.Stat("/dev/fuse"); err == nil {
		return true
	}
	ents, err := os.ReadDir("/dev")
	if err != nil {
		return false
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "macfuse") {
			return true
		}
	}
	return false
}

func TestFuseMount_hostSeesDirtyText(t *testing.T) {
	if !fuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ctx := t.Context()
	reg := NewBackendRegistry()
	if err := reg.Register(LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := NewMountSession(t.Name(), reg)
	if err := ms.Mount(ctx, MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/note.md", []byte("old secret\n")); err != nil {
		t.Fatal(err)
	}
	doc, err := ms.ReadText(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	const want = "# Title\n\nnew phrase lives here\n"
	doc.SetText(want)
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/pic.bin", []byte{0x89, 'P', 'N', 'G', 1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	ents, err := os.ReadDir(dir)
	if err != nil || len(ents) != 1 || ents[0].Name() != "work" || !ents[0].IsDir() {
		t.Fatalf("host readdir: %+v err=%v", ents, err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "work", "note.md"))
	if err != nil || string(got) != want {
		t.Fatalf("host read dirty IR: %q err=%v", got, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "work", "pic.bin"))
	if err != nil || len(raw) != 8 || raw[1] != 'P' {
		t.Fatalf("host binary: %x err=%v", raw, err)
	}
}
