package fuse_test

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ryanaldo34/tacklr/internal/faultproxy"
	"github.com/ryanaldo34/tacklr/internal/s3test"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
	"github.com/ryanaldo34/tacklr/vfs/awss3"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// The FUSE tests run one at a time, and none forks while it holds a file
// open in a mount. This process serves each mount, and Mount, Close and the
// rg checks fork a child. At exec the child closes the files it inherited,
// and the kernel sends FLUSH for each open file in a mount. The forking
// thread waits for the exec, so with one CPU (-cpu=1) nothing answers, and
// in parallel another test's open file meets this test's fork.

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

// TestMount_plaintextWritableProjectedEROFS: the host writes, appends,
// removes, renames, truncates and syncs plain text through the mount and the
// session sees each change; a binary write fails with EROFS, the mount root
// refuses new entries, and a second mount shows the same tree.
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

// TestMount_needsADirectoryItCanMountAndAnIdleMountToClose: Mount fails
// without a directory or on one the user cannot write, and Close fails
// while a host process holds a directory open in the mount, then succeeds.
func TestMount_needsADirectoryItCanMountAndAnIdleMountToClose(t *testing.T) {
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ms := mustTree(t, vfs.At("work", vfs.Local(work)))
	readOnly := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(readOnly, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o755) })
	_, noDirErr := fuse.Mount(ms, "")
	_, readOnlyErr := fuse.Mount(ms, readOnly)
	srv, err := fuse.Mount(ms, filepath.Join(t.TempDir(), "mnt"))
	if err != nil {
		t.Fatal(err)
	}
	// An open directory keeps the mount busy. An open file would too, but
	// the fork that unmounts sends FLUSH for it, which one CPU cannot answer.
	f, err := os.Open(filepath.Join(srv.Dir(), "workspace", "work"))
	if err != nil {
		t.Fatal(err)
	}

	busyErr := srv.Close()
	_ = f.Close()
	idleErr := srv.Close()

	if noDirErr == nil || readOnlyErr == nil {
		t.Errorf("mount without a directory = %v, on a directory the user cannot write = %v, want errors", noDirErr, readOnlyErr)
	}
	if busyErr == nil || idleErr != nil {
		t.Errorf("close while a directory is open = %v, then once idle = %v, want an error and then success", busyErr, idleErr)
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

// hostBackend is a host's own in-memory provider. A file it writes exists
// after the write as an empty binary, except vanish.bin. It lists nothing,
// and it fails the opens and decodes its names ask for.
type hostBackend struct {
	mu      sync.Mutex
	written map[string]bool
}

func (*hostBackend) Validate(context.Context) error { return nil }

func (h *hostBackend) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case name == "":
		return vfs.FileInfo{Name: ".", Mode: fs.ModeDir | 0o755, IsDir: true}, nil
	case name == "noread.bin" || name == "noopen.bin" || name == "nodoc.bin" || h.written[name]:
		return vfs.FileInfo{Name: name, Mode: 0o644, MediaType: "application/octet-stream"}, nil
	}
	return vfs.FileInfo{}, vfs.ErrNotExist
}

func (h *hostBackend) OpenFile(_ context.Context, name string, flag int, _ fs.FileMode) (vfs.File, error) {
	if flag&os.O_CREATE != 0 {
		h.mu.Lock()
		h.written[name] = name != "vanish.bin"
		h.mu.Unlock()
	}
	if name == "noopen.bin" {
		return nil, vfs.ErrPermission
	}
	return hostFile{}, nil
}

func (*hostBackend) ReadDir(context.Context, string) ([]vfs.DirEntry, error) {
	return nil, errors.New("host: list failed")
}
func (*hostBackend) Remove(context.Context, string) error                { return vfs.ErrReadOnly }
func (*hostBackend) MkdirAll(context.Context, string, fs.FileMode) error { return nil }

func (*hostBackend) OpenDocument(_ context.Context, name string, _ *vfs.ContentRegistry) (vfs.Document, error) {
	if name == "nodoc.bin" || name == "half.bin" {
		return nil, errors.New("host: decode failed")
	}
	return nil, vfs.ErrNoCodec
}

func (*hostBackend) WriteDocument(context.Context, string, vfs.Document, *vfs.ContentRegistry) error {
	return vfs.ErrReadOnly
}

// hostFile is an open host file that can only be closed and stat'd.
type hostFile struct{}

func (hostFile) Close() error                { return nil }
func (hostFile) Stat() (vfs.FileInfo, error) { return vfs.FileInfo{Mode: 0o644}, nil }

// TestMount_aFailingBackendReachesTheHostAsAnErrno: each failure of a local
// disk, an S3 server, a host backend or a read-only member reaches the host
// process as the errno that names it, and the calls that can still succeed
// do.
func TestMount_aFailingBackendReachesTheHostAsAnErrno(t *testing.T) {
	ctx := t.Context()
	work, ro := t.TempDir(), t.TempDir()
	for name, body := range map[string]string{"a.txt": "old\n", "b.bin": "\x89PNG\x01", "empty.bin": "", "locked.txt": "secret\n", "p.txt": "p\n", "q.txt": "q\n", "c.txt": "c\n", "big.bin": strings.Repeat("\x00", vfs.MaxReadFileBytes+1)} {
		if err := os.WriteFile(filepath.Join(work, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(ro, "r.txt"), []byte("r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(work, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	chmod(t, filepath.Join(work, "locked.txt"), 0)
	client, bucket := s3test.Start(t)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: new("pic.bin"), Body: strings.NewReader("\x89PNG\x01\x02\x03")}); err != nil {
		t.Fatal(err)
	}
	proxy, proxied := s3test.Proxy(t)
	host := &hostBackend{written: map[string]bool{}}
	ms := mustTree(t,
		vfs.At("work", vfs.Local(work)),
		vfs.At("ro", vfs.Local(ro)).ReadOnly(),
		vfs.At("s3", vfs.S3(awss3.API{Client: proxied}, bucket)),
		vfs.At("host", func(context.Context, string, mount.Binding) (vfs.Provider, error) { return host, nil }),
	)
	dir := t.TempDir()
	mustMount(t, ms, dir)
	at := func(name string) string { return filepath.Join(dir, "workspace", name) }
	readFile := func(name string) func() error {
		return func() error {
			_, err := os.ReadFile(at(name))
			return err
		}
	}
	truncate := func(name string, size int64) func() error {
		return func() error { return os.Truncate(at(name), size) }
	}
	// handle opens name and runs use on the handle; the error is use's, or
	// Close's when use succeeds.
	handle := func(name string, flag int, use func(*os.File) error) func() error {
		return func() error {
			f, err := os.OpenFile(at(name), flag, 0)
			if err != nil {
				return err
			}
			err = use(f)
			return errors.Join(err, f.Close())
		}
	}
	sub, err := os.Open(at("work/sub"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	for _, tc := range []struct {
		name string
		act  func() error
		want syscall.Errno // 0: the call succeeds
	}{
		{"read of a file the disk refuses", readFile("work/locked.txt"), syscall.EACCES},
		{"stat of a file over the read limit", func() error { _, err := os.Stat(at("work/big.bin")); return err }, syscall.EFBIG},
		{"stat of a name the tree rejects", func() error { _, err := os.Stat(at(`work/a\b`)); return err }, syscall.EINVAL},
		{"stat of an empty binary", func() error { _, err := os.Stat(at("work/empty.bin")); return err }, 0},
		{"stat of a directory removed under its handle", func() error {
			if err := os.Remove(filepath.Join(work, "sub")); err != nil {
				return err
			}
			_, err := sub.Stat()
			return err
		}, syscall.ENOENT},
		{"create of a projected file", func() error { _, err := os.Create(at("work/new.docx")); return err }, syscall.EROFS},
		{"create in a read-only member", func() error { _, err := os.Create(at("ro/new.txt")); return err }, syscall.EROFS},
		{"mkdir in a read-only member", func() error { return os.Mkdir(at("ro/d"), 0o755) }, syscall.EROFS},
		{"remove at the root", func() error { return os.Remove(filepath.Join(dir, "workspace")) }, syscall.EPERM},
		{"rename at the root", func() error { return os.Rename(filepath.Join(dir, "workspace"), filepath.Join(dir, "other")) }, syscall.EPERM},
		{"rename into a read-only member", func() error { return os.Rename(at("work/c.txt"), at("ro/c.txt")) }, syscall.EROFS},
		{"rename of a file the host cannot open", func() error { return os.Rename(at("host/noopen.bin"), at("host/moved.bin")) }, syscall.EACCES},
		{"truncate of a binary", truncate("work/b.bin", 1), syscall.EROFS},
		{"truncate over the read limit", truncate("work/a.txt", vfs.MaxReadFileBytes+1), syscall.EFBIG},
		{"truncate in a read-only member", truncate("ro/r.txt", 1), syscall.EROFS},
		{"truncate of a text file", truncate("work/a.txt", 2), 0},
		{"time change of a text file", func() error { return os.Chtimes(at("work/a.txt"), time.Now(), time.Now()) }, 0},
		{"write past the read limit", handle("work/a.txt", os.O_WRONLY, func(f *os.File) error {
			_, err := f.WriteAt([]byte("x"), vfs.MaxReadFileBytes+1)
			return err
		}), syscall.EFBIG},
		{"close of a write the disk refuses", handle("work/p.txt", os.O_WRONLY, func(f *os.File) error {
			chmod(t, filepath.Join(work, "p.txt"), 0o444)
			_, err := f.Write([]byte("new"))
			return err
		}), syscall.EACCES},
		{"handle truncate over the read limit", handle("work/c.txt", os.O_RDWR, func(f *os.File) error { return f.Truncate(vfs.MaxReadFileBytes + 1) }), syscall.EFBIG},
		{"handle truncate that grows the file", handle("work/c.txt", os.O_RDWR, func(f *os.File) error { return f.Truncate(8) }), 0},
		{"handle truncate the disk refuses", handle("work/q.txt", os.O_RDWR, func(f *os.File) error {
			chmod(t, filepath.Join(work, "q.txt"), 0o444)
			return f.Truncate(1)
		}), syscall.EACCES},
		{"read of a binary whose ranged get fails", func() error {
			proxy.Set(faultproxy.Rule{Match: func(r *http.Request) bool { return r.Header.Get("Range") != "" }, Fault: faultproxy.Answer(http.StatusInternalServerError, "")})
			defer proxy.Clear()
			return readFile("s3/pic.bin")()
		}, syscall.EIO},
		{"list of a host directory that fails", func() error { _, err := os.ReadDir(at("host")); return err }, syscall.EIO},
		{"read of a host file without ReadAt", readFile("host/noread.bin"), syscall.EIO},
		{"read of a host file it cannot open", readFile("host/noopen.bin"), syscall.EACCES},
		{"read of a host file it cannot decode", readFile("host/nodoc.bin"), syscall.EIO},
		{"create of a host file that is gone after the write", func() error { _, err := os.Create(at("host/vanish.bin")); return err }, syscall.ENOENT},
		{"create of a host file it cannot decode", func() error {
			f, err := os.OpenFile(at("host/half.bin"), os.O_CREATE|os.O_WRONLY, 0o644)
			if err == nil {
				_ = f.Close()
			}
			return err
		}, syscall.EIO},
		{"mkdir of a host directory that is gone after it", func() error { return os.Mkdir(at("host/madedir"), 0o755) }, syscall.ENOENT},
	} {
		err := tc.act()

		if tc.want == 0 && err != nil || tc.want != 0 && !errors.Is(err, tc.want) {
			t.Errorf("%s = %v, want errno %d (%v)", tc.name, err, tc.want, tc.want)
		}
	}
}

// chmod sets path's mode, and gives the owner its rights back when t ends
// so the temporary directory can be removed.
func chmod(t *testing.T, path string, mode fs.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o755) })
}
