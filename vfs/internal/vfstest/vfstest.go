// Package vfstest is the contract every built-in vfs backend must meet. The
// backend tests in package vfs run it, so a new backend or provider method
// cannot ship without the behavior the others have. It is internal: only
// the vfs tree can import it.
//
// Each case gets a fresh Backend and asserts outcomes through the mount
// session only: what a caller reading and writing files observes.
package vfstest

import (
	"bytes"
	"errors"
	"io"
	"path"
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// Backend is one fresh, writable mount for a contract case.
type Backend struct {
	Session *vfs.MountSession
	// Root is a virtual directory the case may write under. It holds no
	// files the contract writes.
	Root string
	// ReadsAfterRemove reports what a handle opened before Remove reads. A
	// local file keeps its open inode, so the handle reads the old content.
	// A remote object downloads on the first Read, which then finds the
	// object gone (ErrNotExist).
	ReadsAfterRemove bool
}

// Run runs the contract. newBackend returns a fresh backend for each case.
func Run(t *testing.T, newBackend func(t *testing.T) Backend) {
	t.Run("a missing path is not found", func(t *testing.T) {
		ctx, b := t.Context(), newBackend(t)
		missing := path.Join(b.Root, "missing.txt")

		_, statErr := b.Session.Route(ctx, missing).Stat(ctx)
		_, openErr := b.Session.Route(ctx, missing).Open(ctx)

		if !errors.Is(statErr, vfs.ErrNotExist) || !errors.Is(openErr, vfs.ErrNotExist) {
			t.Fatalf("stat = %v, open = %v, want ErrNotExist", statErr, openErr)
		}
	})

	t.Run("a written file reads back with its size, type, version and listing", func(t *testing.T) {
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "note.txt")
		mustWrite(t, b, name, []byte("hello contract\n"))

		got, readErr := b.Session.Route(ctx, name).ReadFile(ctx)
		st, statErr := b.Session.Route(ctx, name).Stat(ctx)
		ents, listErr := b.Session.Route(ctx, b.Root).ReadDir(ctx)

		if readErr != nil || string(got) != "hello contract\n" {
			t.Fatalf("read = %q %v", got, readErr)
		}
		if statErr != nil || st.IsDir || st.Size != int64(len(got)) || st.MediaType != "text/plain" || st.Version == "" {
			t.Fatalf("stat = %+v %v, want a %d-byte text/plain file with a version", st, statErr, len(got))
		}
		if listErr != nil || !slices.ContainsFunc(ents, func(e vfs.DirEntry) bool { return e.Name == "note.txt" && !e.IsDir }) {
			t.Fatalf("listing = %+v %v, want note.txt", ents, listErr)
		}
	})

	t.Run("a version stays the same until the content changes", func(t *testing.T) {
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "v.txt")
		version := func() string {
			t.Helper()
			st, err := b.Session.Route(ctx, name).Stat(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return st.Version
		}
		mustWrite(t, b, name, []byte("first"))

		before, again := version(), version()
		mustWrite(t, b, name, []byte("second, longer"))
		after := version()

		if before == "" || again != before {
			t.Fatalf("version of unchanged content = %q, then %q", before, again)
		}
		if after == "" || after == before {
			t.Fatalf("version after a change = %q, before %q", after, before)
		}
	})

	t.Run("a large file reads at offsets past the read-ahead window and streams", func(t *testing.T) {
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "big.bin")
		want := pattern(5 << 19)
		mustWrite(t, b, name, want)

		f, err := b.Session.Route(ctx, name).Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		ra, ok := f.(io.ReaderAt)
		if !ok {
			t.Fatal("read handle has no ReadAt")
		}
		at := make([]byte, 64)
		off := len(want) - 100
		n, atErr := ra.ReadAt(at, int64(off))
		streamed, readErr := io.ReadAll(reader(t, f))

		if atErr != nil || !bytes.Equal(at[:n], want[off:off+64]) {
			t.Fatalf("ReadAt(%d) = %d bytes, err=%v", off, n, atErr)
		}
		if readErr != nil || !bytes.Equal(streamed, want) {
			t.Fatalf("streamed read = %d bytes, err=%v", len(streamed), readErr)
		}
	})

	t.Run("a directory lists its children and is no file", func(t *testing.T) {
		ctx, b := t.Context(), newBackend(t)
		dir := path.Join(b.Root, "folder")
		if err := b.Session.Route(ctx, dir).MkdirAll(ctx); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, b, path.Join(dir, "child.txt"), []byte("child"))

		st, statErr := b.Session.Route(ctx, dir).Stat(ctx)
		_, openErr := b.Session.Route(ctx, dir).Open(ctx)
		ents, listErr := b.Session.Route(ctx, dir).ReadDir(ctx)

		if statErr != nil || !st.IsDir || st.Version != "" {
			t.Fatalf("stat = %+v %v, want a directory without a version", st, statErr)
		}
		if !errors.Is(openErr, vfs.ErrIsDir) {
			t.Fatalf("open = %v, want ErrIsDir", openErr)
		}
		if listErr != nil || len(ents) != 1 || ents[0].Name != "child.txt" || ents[0].IsDir {
			t.Fatalf("listing = %+v %v, want [child.txt]", ents, listErr)
		}
	})

	t.Run("a removed file is not found, and a handle opened before reads as the backend promises", func(t *testing.T) {
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "gone.txt")
		mustWrite(t, b, name, []byte("soon gone"))
		f, err := b.Session.Route(ctx, name).Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })

		removeErr := b.Session.Route(ctx, name).Remove(ctx)
		_, statErr := b.Session.Route(ctx, name).Stat(ctx)
		got, readErr := io.ReadAll(reader(t, f))

		if removeErr != nil || !errors.Is(statErr, vfs.ErrNotExist) {
			t.Fatalf("remove = %v, then stat = %v, want ErrNotExist", removeErr, statErr)
		}
		if b.ReadsAfterRemove && (readErr != nil || string(got) != "soon gone") {
			t.Fatalf("read of an open handle after remove = %q %v, want the old content", got, readErr)
		}
		if !b.ReadsAfterRemove && !errors.Is(readErr, vfs.ErrNotExist) {
			t.Fatalf("read of an open handle after remove = %q %v, want ErrNotExist", got, readErr)
		}
	})
}

// reader is f's streaming read side; every read handle has one.
func reader(t *testing.T, f vfs.File) io.Reader {
	t.Helper()
	r, ok := f.(io.Reader)
	if !ok {
		t.Fatal("read handle has no Read")
	}
	return r
}

func mustWrite(t *testing.T, b Backend, name string, data []byte) {
	t.Helper()
	if err := b.Session.Route(t.Context(), name).WriteFile(t.Context(), data); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// pattern is n bytes that differ from one offset to the next, so a read
// from the wrong offset never matches.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i ^ i>>8 ^ i>>16) //nolint:gosec // G115: truncation to a byte is the point
	}
	return b
}
