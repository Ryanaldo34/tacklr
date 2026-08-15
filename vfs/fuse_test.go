package vfs

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestFuseMount_hostSeesDirtyText(t *testing.T) {
	if !FuseAvailable() {
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
	if err := ms.MkdirAll(ctx, "/work/subdir"); err != nil {
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
	if got := ms.HostDir(); got != dir {
		t.Fatalf("HostDir = %q want %q", got, dir)
	}

	sessEnts, err := ms.ReadDir(ctx, "/work")
	if err != nil {
		t.Fatal(err)
	}
	hostEnts, err := os.ReadDir(filepath.Join(dir, "work"))
	if err != nil {
		t.Fatal(err)
	}
	wantNames := map[string]bool{}
	for _, e := range sessEnts {
		wantNames[e.Name] = e.IsDir
	}
	if len(hostEnts) != len(sessEnts) {
		t.Fatalf("host readdir len=%d session=%d host=%+v sess=%+v", len(hostEnts), len(sessEnts), hostEnts, sessEnts)
	}
	for _, e := range hostEnts {
		isDir, ok := wantNames[e.Name()]
		if !ok || isDir != e.IsDir() {
			t.Fatalf("host entry %q IsDir=%v match=%v session=%+v", e.Name(), e.IsDir(), ok, sessEnts)
		}
	}

	got, err := os.ReadFile(filepath.Join(dir, "work", "note.md"))
	if err != nil || string(got) != want {
		t.Fatalf("host read dirty IR: %q err=%v", got, err)
	}
	st, err := os.Stat(filepath.Join(dir, "work", "note.md"))
	if err != nil {
		t.Fatal(err)
	}
	dirty, err := ms.ReadText(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(len(dirty.Text())) {
		t.Fatalf("host stat size=%d want %d", st.Size(), len(dirty.Text()))
	}
	raw, err := os.ReadFile(filepath.Join(dir, "work", "pic.bin"))
	if err != nil || len(raw) != 8 || raw[1] != 'P' {
		t.Fatalf("host binary: %x err=%v", raw, err)
	}

	if _, err := exec.LookPath("rg"); err == nil {
		out, err := exec.Command("rg", "-F", "new phrase lives here", dir).CombinedOutput()
		if err != nil {
			t.Fatalf("rg dirty phrase: %v out=%s", err, out)
		}
		if !strings.Contains(string(out), "new phrase lives here") {
			t.Fatalf("rg missed dirty phrase: %s", out)
		}
	}
}

func TestFuseMount_plaintextWritableProjectedEROFS(t *testing.T) {
	if !FuseAvailable() {
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
	if err := ms.WriteFile(ctx, "/work/note.txt", []byte("old\n")); err != nil {
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

	work := filepath.Join(dir, "work")
	if err := os.MkdirAll(filepath.Join(work, "d"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(work, "d", "a.txt"), []byte("hi from host\n"), 0o644); err != nil {
		t.Fatalf("write plaintext: %v", err)
	}
	got, err := ms.ReadText(ctx, "/work/d/a.txt")
	if err != nil || got.Text() != "hi from host\n" {
		t.Fatalf("session after host write: %q err=%v", textOr(got), err)
	}

	f, err := os.OpenFile(filepath.Join(work, "note.txt"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open plaintext: %v", err)
	}
	if _, err := f.Write([]byte("appended\n")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadText(ctx, "/work/note.txt")
	if err != nil || !strings.Contains(got.Text(), "appended") {
		t.Fatalf("append visible: %q err=%v", textOr(got), err)
	}

	if err := os.WriteFile(filepath.Join(work, "pic.bin"), []byte("nope"), 0o644); err == nil {
		t.Fatal("binary write: want EROFS")
	} else if !errors.Is(err, syscall.EROFS) && !errors.Is(err, os.ErrPermission) &&
		!strings.Contains(err.Error(), "read-only") && !strings.Contains(err.Error(), "EROFS") {
		t.Fatalf("binary write err = %v", err)
	}

	if err := os.Remove(filepath.Join(work, "d", "a.txt")); err != nil {
		t.Fatalf("rm: %v", err)
	}
	if _, err := ms.Stat(ctx, "/work/d/a.txt"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("stat after rm: %v", err)
	}

	if _, err := exec.LookPath("rg"); err == nil {
		out, err := exec.Command("rg", "-F", "appended", dir).CombinedOutput()
		if err != nil || !strings.Contains(string(out), "appended") {
			t.Fatalf("rg after host append: %v out=%s", err, out)
		}
	}
}

func textOr(t Textual) string {
	if t == nil {
		return ""
	}
	return t.Text()
}

func TestFuseMount_rejectsMultiSegmentPoint(t *testing.T) {
	ctx := t.Context()
	reg := NewBackendRegistry()
	if err := reg.Register(LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := NewMountSession(t.Name(), reg)
	if err := ms.Mount(ctx, MountSpec{Point: "/tmp/tacklr", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	err := ms.FuseMount(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "/tmp/tacklr") {
		t.Fatalf("want multi-segment error naming the point, got %v", err)
	}
}
