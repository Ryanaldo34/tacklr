package vfs_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// A symlink reports the version and size of its target, under its own name.
func TestLocal_aSymlinkReportsItsTargetsVersion(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(dir)))(ctx, "sess-local-version", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/w/a.txt").WriteFile(ctx, []byte("target")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}

	target, err := ms.Route(ctx, "/workspace/w/a.txt").Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	link, err := ms.Route(ctx, "/workspace/w/link.txt").Stat(ctx)

	if err != nil || link.Name != "link.txt" || link.Version != target.Version || link.Size != target.Size {
		t.Fatalf("symlink stat = %+v, target %+v, err=%v", link, target, err)
	}
}

// TestSessionLocal_givesEachSessionItsOwnDirectory: a session, and its
// specialist (a child id has slashes), each write into a directory of
// their own under the base, and neither sees the other's file.
func TestSessionLocal_givesEachSessionItsOwnDirectory(t *testing.T) {
	t.Parallel()
	// Arrange
	ctx, base := t.Context(), t.TempDir()
	tree := vfs.Tree(vfs.At("work", vfs.SessionLocal(base)))
	write := func(session, name string) *vfs.MountSession {
		t.Helper()
		ms, err := tree(ctx, session, vfs.Request{})
		if err != nil {
			t.Fatal(err)
		}
		if err := ms.Route(ctx, "/workspace/work/"+name).WriteFile(ctx, []byte(session)); err != nil {
			t.Fatal(err)
		}
		return ms
	}

	// Act
	parent := write("s1", "parent.txt")
	child := write("s1/w/researcher/c1", "child.txt")

	// Assert
	dirs, err := os.ReadDir(base)
	if err != nil || len(dirs) != 2 {
		t.Fatalf("base holds %v (%v), want one directory per session", dirs, err)
	}
	if _, err := parent.Route(ctx, "/workspace/work/child.txt").Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("the parent stats its child's file: %v, want not found", err)
	}
	if _, err := child.Route(ctx, "/workspace/work/parent.txt").Stat(ctx); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("the child stats its parent's file: %v, want not found", err)
	}
}

// TestLocal_diskFailuresBecomeVFSErrors: each failure of the disk under a
// local mount becomes the vfs error the agent can act on, and an error with
// no vfs match keeps its cause but not the host path. The failures are real:
// a file where a directory must be, a directory where a file must be, a
// symlink loop, a symlink out of the root, a file the user cannot read, a
// file over the cap, a name too long and a closed handle.
func TestLocal_diskFailuresBecomeVFSErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for name, body := range map[string]string{"f.txt": "x", "locked.txt": "x", "README": "x"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"locked.txt", "README"} {
		if err := os.Chmod(filepath.Join(dir, name), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("loop", filepath.Join(dir, "loop")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, "out")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "big.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(dir, "big.txt"), int64(vfs.MaxReadFileBytes)+1); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(dir)))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	route := func(name string) vfs.Route { return ms.Route(t.Context(), "/workspace/w/"+name) }
	stat := func(name string) func(context.Context) error {
		return func(ctx context.Context) error { _, err := route(name).Stat(ctx); return err }
	}
	openDoc := func(name string) func(context.Context) error {
		return func(ctx context.Context) error { _, err := route(name).OpenDocument(ctx); return err }
	}
	list := func(name string) func(context.Context) error {
		return func(ctx context.Context) error { _, err := route(name).ReadDir(ctx); return err }
	}
	longName := strings.Repeat("n", 300)

	for name, tc := range map[string]struct {
		call func(context.Context) error
		want func(error) bool
	}{
		"a path through a file is not a directory": {stat("f.txt/x"), is(vfs.ErrNotDir)},
		"a list of a file is not a directory":      {list("f.txt"), is(vfs.ErrNotDir)},
		"a write to a directory is a directory": {
			func(ctx context.Context) error { return route("sub").WriteFile(ctx, []byte("x")) }, is(vfs.ErrIsDir),
		},
		"a document under a file is not a directory": {
			func(ctx context.Context) error {
				return ms.WriteDocument(ctx, vfs.NewTextDocument("/workspace/w/f.txt/sub/x.txt", "text/plain", "", "x"))
			},
			is(vfs.ErrNotDir),
		},
		"a list through a symlink out of the root is invalid": {list("out"), is(vfs.ErrInvalidPath)},
		"a document the user cannot read is refused":          {openDoc("locked.txt"), is(vfs.ErrPermission)},
		"a document over the cap is too large":                {openDoc("big.txt"), is(vfs.ErrTooLarge)},
		"a symlink loop fails without the host path":          {stat("loop"), hidesHost(dir, nil)},
		"a name too long keeps its cause without the host path": {
			stat(longName), hidesHost(dir, syscall.ENAMETOOLONG),
		},
		"a closed handle cannot stat": {
			func(ctx context.Context) error {
				f, err := route("f.txt").Open(ctx)
				if err != nil {
					return err
				}
				_ = f.Close()
				_, err = f.Stat()
				return err
			},
			is(fs.ErrClosed),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tc.call(t.Context())

			if !tc.want(err) {
				t.Fatalf("err = %v", err)
			}
		})
	}

	t.Run("a file the user cannot read has no sniffed type", func(t *testing.T) {
		t.Parallel()

		st, err := route("README").Stat(t.Context())

		if err != nil || st.MediaType != "application/octet-stream" {
			t.Fatalf("stat = %+v err=%v", st, err)
		}
	})
}

// TestLocal_aWrappedProviderRefusesBadNamesAndFailedUploads: a host that
// wraps the local provider calls it with names the mount session never
// sends. A name that leaves the root or cancels to the root is invalid, a
// canceled context stops the one-shot put and the document I/O, and an
// upload whose reader fails returns that error.
func TestLocal_aWrappedProviderRefusesBadNamesAndFailedUploads(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p, err := vfs.NewLocalProvider(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	local := p.(backendProvider)
	reg, err := vfs.NewContentRegistry()
	if err != nil {
		t.Fatal(err)
	}
	doc := vfs.NewTextDocument("x.txt", "text/plain", "", "x")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	errUpload := errors.New("upload broke")

	invalid := map[string]error{
		"stat ..":          func() error { _, err := local.Stat(ctx, ".."); return err }(),
		"stat a/..":        func() error { _, err := local.Stat(ctx, "a/.."); return err }(),
		"open ..":          func() error { _, err := local.OpenFile(ctx, "..", os.O_RDONLY, 0); return err }(),
		"put ..":           local.PutFile(ctx, "..", strings.NewReader("x"), 1),
		"open document ..": func() error { _, err := local.OpenDocument(ctx, "..", reg); return err }(),
		"list ..":          func() error { _, err := local.ReadDir(ctx, ".."); return err }(),
		"remove ..":        local.Remove(ctx, ".."),
		"mkdir ..":         local.MkdirAll(ctx, "..", 0o750),
	}
	stopped := map[string]error{
		"put":            local.PutFile(canceled, "x.txt", strings.NewReader("x"), 1),
		"open document":  func() error { _, err := local.OpenDocument(canceled, "x.txt", reg); return err }(),
		"write document": local.WriteDocument(canceled, "x.txt", doc, reg),
	}
	uploadErr := local.PutFile(ctx, "up.txt", iotest.ErrReader(errUpload), 4)

	for name, err := range invalid {
		if !errors.Is(err, vfs.ErrInvalidPath) {
			t.Errorf("%s = %v, want ErrInvalidPath", name, err)
		}
	}
	for name, err := range stopped {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("canceled %s = %v", name, err)
		}
	}
	if !errors.Is(uploadErr, errUpload) {
		t.Errorf("failed upload = %v, want %v", uploadErr, errUpload)
	}
}

// is reports whether an error is target.
func is(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}

// hidesHost reports whether an error is cause (any error when cause is nil)
// and does not show the host directory dir.
func hidesHost(dir string, cause error) func(error) bool {
	return func(err error) bool {
		return err != nil && (cause == nil || errors.Is(err, cause)) && !strings.Contains(err.Error(), dir)
	}
}

// TestLocal_noPathOrSymlinkReachesOutsideItsRoot: a symlink under a local
// root that points outside it (as the agent's shell can make, by an
// absolute or a relative target, to a file or to a directory) is refused
// for every read and write, and a mount whose subpath is such a symlink
// does not open. Nothing outside the root changes.
func TestLocal_noPathOrSymlinkReachesOutsideItsRoot(t *testing.T) {
	t.Parallel()
	// Arrange
	outside, dir := t.TempDir(), t.TempDir()
	secret := filepath.Join(outside, "secret")
	if err := os.WriteFile(secret, []byte("host"), 0o600); err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(dir, secret)
	if err != nil {
		t.Fatal(err)
	}
	for link, target := range map[string]string{"abs": secret, "rel": rel, "dir": outside} {
		if err := os.Symlink(target, filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
	}
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(dir)))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	route := func(name string) vfs.Route { return ms.Route(t.Context(), "/workspace/w/"+name) }

	// Act
	refused := map[string]error{
		"a write through an absolute link":  route("abs").WriteFile(t.Context(), []byte("x")),
		"a write through a relative link":   route("rel").WriteFile(t.Context(), []byte("x")),
		"a new file in a linked directory":  route("dir/new").WriteFile(t.Context(), []byte("x")),
		"a directory in a linked directory": route("dir/sub").MkdirAll(t.Context()),
		"a removal in a linked directory":   route("dir/secret").Remove(t.Context()),
	}
	_, refused["a read through a link"] = route("abs").ReadFile(t.Context())
	_, refused["a list of a linked directory"] = route("dir").ReadDir(t.Context())
	_, refused["a mount whose subpath is a link"] = vfs.Local(dir)(t.Context(), "s", mount.Binding{Params: map[string]string{"subpath": "dir"}})
	left, _ := os.ReadFile(secret)
	entries, _ := os.ReadDir(outside)

	// Assert
	for name, err := range refused {
		if !errors.Is(err, vfs.ErrInvalidPath) {
			t.Errorf("%s: %v; want it refused as an invalid path", name, err)
		}
	}
	if string(left) != "host" || len(entries) != 1 {
		t.Fatalf("outside the root: secret %q and %d entries; want it unchanged", left, len(entries))
	}
}
