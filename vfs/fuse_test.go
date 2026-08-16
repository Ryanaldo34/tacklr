package vfs

import (
	"context"
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
	ms := MustNewMountSession(t.Name(), reg)
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
	ms := MustNewMountSession(t.Name(), reg)
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

	if _, err := exec.LookPath("rg"); err == nil {
		out, err := exec.Command("rg", "-F", "appended", dir).CombinedOutput()
		if err != nil || !strings.Contains(string(out), "appended") {
			t.Fatalf("rg after host append: %v out=%s", err, out)
		}
	}

	if err := os.Remove(filepath.Join(work, "d", "a.txt")); err != nil {
		t.Fatalf("rm: %v", err)
	}
	if _, err := ms.Stat(ctx, "/work/d/a.txt"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("stat after rm: %v", err)
	}
	if err := os.Remove(filepath.Join(work, "d")); err != nil {
		t.Fatalf("rmdir: %v", err)
	}
	if _, err := ms.Stat(ctx, "/work/d"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("stat after rmdir: %v", err)
	}

	if err := os.Rename(filepath.Join(work, "note.txt"), filepath.Join(work, "renamed.txt")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	got, err = ms.ReadText(ctx, "/work/renamed.txt")
	if err != nil || !strings.Contains(got.Text(), "appended") {
		t.Fatalf("rename visible: %q err=%v", textOr(got), err)
	}
	if _, err := ms.Stat(ctx, "/work/note.txt"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("old name after rename: %v", err)
	}

	tf, err := os.OpenFile(filepath.Join(work, "renamed.txt"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := tf.Truncate(4); err != nil {
		t.Fatalf("handle truncate: %v", err)
	}
	if err := tf.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = ms.ReadText(ctx, "/work/renamed.txt")
	if err != nil || got.Text() != "old\n" {
		t.Fatalf("truncate body: %q err=%v", textOr(got), err)
	}

	if _, err := os.OpenFile(work, os.O_WRONLY, 0); err == nil {
		t.Fatal("open dir for write")
	}

	wf, err := os.OpenFile(filepath.Join(work, "renamed.txt"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.Write([]byte("sync")); err != nil {
		t.Fatal(err)
	}
	if err := wf.Sync(); err != nil {
		t.Fatalf("fsync: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Create(filepath.Join(dir, "root.txt")); err == nil {
		t.Fatal("create at fuse root: want EPERM")
	}
	if err := os.Mkdir(filepath.Join(dir, "rootdir"), 0o755); err == nil {
		t.Fatal("mkdir at fuse root: want EPERM")
	}

	if err := ms.FuseMount(dir); err != nil {
		t.Fatalf("remount same dir: %v", err)
	}
	dir2 := t.TempDir()
	if err := ms.FuseMount(dir2); err != nil {
		t.Fatalf("remount other dir: %v", err)
	}
	if got := ms.HostDir(); got != dir2 {
		t.Fatalf("HostDir after remount = %q want %q", got, dir2)
	}
	got, err = ms.ReadText(ctx, "/work/renamed.txt")
	if err != nil || !strings.Contains(got.Text(), "sync") {
		t.Fatalf("session after remount: %q err=%v", textOr(got), err)
	}
	ents, err := os.ReadDir(filepath.Join(dir2, "work"))
	if err != nil {
		t.Fatalf("host readdir after remount: %v", err)
	}
	var sawRenamed bool
	for _, e := range ents {
		if e.Name() == "renamed.txt" {
			sawRenamed = true
			break
		}
	}
	if !sawRenamed {
		t.Fatalf("host remount missing renamed.txt: %v", ents)
	}
}

// TestFuseMount_projectedTextualReadOnly: a non-identity codec still projects
// ReadText to the kernel (cat/rg); kernel writes stay EROFS.
func TestFuseMount_projectedTextualReadOnly(t *testing.T) {
	if !FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	const media = "application/x-fuse-projected"
	extMediaTypes[".proj"] = media
	t.Cleanup(func() { delete(extMediaTypes, ".proj") })
	if err := defaultContentRegistry.Register(fuseProjectedCodec{}); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	reg := NewBackendRegistry()
	if err := reg.Register(LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := MustNewMountSession(t.Name(), reg)
	if err := ms.Mount(ctx, MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/doc.proj", []byte("container-bytes")); err != nil {
		t.Fatal(err)
	}
	doc, err := ms.ReadText(ctx, "/work/doc.proj")
	if err != nil || doc.Text() != "EXTRACTED:container-bytes" {
		t.Fatalf("ReadText projection: %q err=%v", textOr(doc), err)
	}

	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	host := filepath.Join(dir, "work", "doc.proj")
	st, err := os.Stat(host)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(len("EXTRACTED:container-bytes")) {
		t.Fatalf("host size=%d want extracted plaintext", st.Size())
	}
	if st.Mode().Perm()&0200 != 0 {
		t.Fatalf("projected file should be read-only, mode=%v", st.Mode())
	}
	got, err := os.ReadFile(host)
	if err != nil || string(got) != "EXTRACTED:container-bytes" {
		t.Fatalf("host cat projection: %q err=%v", got, err)
	}
	if err := os.WriteFile(host, []byte("nope"), 0o644); err == nil {
		t.Fatal("projected write: want EROFS")
	} else if !errors.Is(err, syscall.EROFS) && !errors.Is(err, os.ErrPermission) &&
		!strings.Contains(err.Error(), "read-only") && !strings.Contains(err.Error(), "EROFS") {
		t.Fatalf("projected write err = %v", err)
	}
	if _, err := exec.LookPath("rg"); err == nil {
		out, err := exec.Command("rg", "-F", "EXTRACTED:container-bytes", dir).CombinedOutput()
		if err != nil || !strings.Contains(string(out), "EXTRACTED:container-bytes") {
			t.Fatalf("rg projection: %v out=%s", err, out)
		}
	}
}

type fuseProjectedCodec struct{}

func (fuseProjectedCodec) MediaTypes() []string {
	return []string{"application/x-fuse-projected"}
}

func (fuseProjectedCodec) Decode(_ context.Context, p, mt string, data []byte) (Document, error) {
	return NewTextDocument(p, mt, "utf-8", "EXTRACTED:"+string(data)), nil
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
	ms := MustNewMountSession(t.Name(), reg)
	if err := ms.Mount(ctx, MountSpec{Point: "/tmp/tacklr", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	err := ms.FuseMount(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "/tmp/tacklr") {
		t.Fatalf("want multi-segment error naming the point, got %v", err)
	}
	if err := ms.FuseMount(""); err == nil {
		t.Fatal("empty FuseMount dir")
	}
	if FuseAvailable() {
		empty := MustNewMountSession("empty-tree", reg)
		dir := t.TempDir()
		if err := empty.FuseMount(dir); err != nil {
			t.Fatalf("empty specs FuseMount: %v", err)
		}
		_ = empty.Close()
	}
}
