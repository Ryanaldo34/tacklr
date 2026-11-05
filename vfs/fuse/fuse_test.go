package fuse_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
)

// TestMount_hostSeesDirtyText: the host lists what the session lists and
// reads each file as the session reads it: text as edited, a Word file as
// its projected text, and binary bytes as stored.
func TestMount_hostSeesDirtyText(t *testing.T) {
	ctx := t.Context()
	ms := mustTree(t, vfs.At("work", vfs.Local(t.TempDir())))

	if err := ms.Route(ctx, "/workspace/work/note.md").
		WriteFile(ctx, []byte("old secret\n")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/subdir").
		MkdirAll(ctx); err != nil {
		t.Fatal(err)
	}

	doc, err := ms.Route(ctx, "/workspace/work/note.md").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const want = "# Title\n\nnew phrase lives here\n"
	if err := doc.SetText(want); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/pic.bin").
		WriteFile(ctx, []byte{0x89, 'P', 'N', 'G', 1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}

	word := "a word phrase"
	if _, err := ms.Apply(ctx, "/workspace/work/doc.docx", vfs.Mutation{MediaType: adapters.DOCXMediaType, Content: &word}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	mustMount(t, ms, dir)

	sessEnts, err := ms.Route(ctx, "/workspace/work").
		ReadDir(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hostEnts, err := os.ReadDir(filepath.Join(dir, "workspace", "work"))
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

	got, err := os.ReadFile(filepath.Join(dir, "workspace", "work", "note.md"))
	if err != nil || string(got) != want {
		t.Fatalf("host read dirty IR: %q err=%v", got, err)
	}
	st, err := os.Stat(filepath.Join(dir, "workspace", "work", "note.md"))
	if err != nil {
		t.Fatal(err)
	}

	dirty, err := ms.Route(ctx, "/workspace/work/note.md").
		ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(len(dirty.Text())) {
		t.Fatalf("host stat size=%d want %d", st.Size(), len(dirty.Text()))
	}
	projected, err := os.ReadFile(filepath.Join(dir, "workspace", "work", "doc.docx"))
	if err != nil || !strings.Contains(string(projected), word) {
		t.Fatalf("host read of a Word file: %q err=%v", projected, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "workspace", "work", "pic.bin"))
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

func TestMount_plaintextWritableProjectedEROFS(t *testing.T) {
	ctx := t.Context()
	ms := mustTree(t, vfs.At("work", vfs.Local(t.TempDir())))

	if err := ms.Route(ctx, "/workspace/work/note.txt").
		WriteFile(ctx, []byte("old\n")); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/pic.bin").
		WriteFile(ctx, []byte{0x89, 'P', 'N', 'G', 1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	mustMount(t, ms, dir)

	work := filepath.Join(dir, "workspace", "work")
	if err := os.MkdirAll(filepath.Join(work, "d"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(work, "d", "a.txt"), []byte("hi from host\n"), 0o644); err != nil {
		t.Fatalf("write plaintext: %v", err)
	}

	got, err := ms.Route(ctx, "/workspace/work/d/a.txt").
		ReadText(ctx)
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

	got, err = ms.Route(ctx, "/workspace/work/note.txt").
		ReadText(ctx)
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

	if _, err := ms.Route(ctx, "/workspace/work/d/a.txt").
		Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("stat after rm: %v", err)
	}

	if err := os.Remove(filepath.Join(work, "d")); err != nil {
		t.Fatalf("rmdir: %v", err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/d").
		Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("stat after rmdir: %v", err)
	}

	if err := os.Rename(filepath.Join(work, "note.txt"), filepath.Join(work, "renamed.txt")); err != nil {
		t.Fatalf("rename: %v", err)
	}

	got, err = ms.Route(ctx, "/workspace/work/renamed.txt").
		ReadText(ctx)
	if err != nil || !strings.Contains(got.Text(), "appended") {
		t.Fatalf("rename visible: %q err=%v", textOr(got), err)
	}

	if _, err := ms.Route(ctx, "/workspace/work/note.txt").
		Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
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

	got, err = ms.Route(ctx, "/workspace/work/renamed.txt").
		ReadText(ctx)
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

	dir2 := t.TempDir()
	mustMount(t, ms, dir2)

	if got, err := ms.Route(ctx, "/workspace/work/renamed.txt").
		ReadText(ctx); err != nil || !strings.Contains(got.Text(), "sync") {
		t.Fatalf("session after a second mount: %q err=%v", textOr(got), err)
	}
	ents, err := os.ReadDir(filepath.Join(dir2, "workspace", "work"))
	if err != nil {
		t.Fatalf("host readdir of the second mount: %v", err)
	}
	var sawRenamed bool
	for _, e := range ents {
		if e.Name() == "renamed.txt" {
			sawRenamed = true
			break
		}
	}
	if !sawRenamed {
		t.Fatalf("second mount missing renamed.txt: %v", ents)
	}
}

func textOr(t vfs.Textual) string {
	if t == nil {
		return ""
	}
	return t.Text()
}

func TestMount_requiresADirectory(t *testing.T) {
	ms := mustTree(t)

	_, err := fuse.Mount(ms, "")

	if err == nil {
		t.Fatal("want an error for an empty directory")
	}
}

func mustTree(t *testing.T, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	// The Word and Excel codecs, as a host that reads them registers.
	ms, err := vfs.Tree(members...).WithCodecs(adapters.Codecs()...)(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func mustMount(t *testing.T, ms *vfs.MountSession, dir string) {
	t.Helper()
	srv, err := fuse.Mount(ms, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
}
