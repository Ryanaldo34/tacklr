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

// Run runs the contract. newBackend returns a fresh backend for each case,
// and the cases run in parallel, so newBackend must be safe to call from
// many goroutines.
func Run(t *testing.T, newBackend func(t *testing.T) Backend) {
	t.Run("a missing path is not found", func(t *testing.T) {
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		route := b.Session.Route(ctx, path.Join(b.Root, "missing.txt"))

		_, statErr := route.Stat(ctx)
		_, openErr := route.Open(ctx)
		_, textErr := route.ReadText(ctx)
		_, listErr := route.ReadDir(ctx)
		removeErr := route.Remove(ctx)

		for name, err := range map[string]error{"stat": statErr, "open": openErr, "read text": textErr, "list": listErr, "remove": removeErr} {
			if !errors.Is(err, vfs.ErrNotExist) {
				t.Errorf("%s = %v, want ErrNotExist", name, err)
			}
		}
	})

	t.Run("a written file reads back with its size, type, version and listing", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
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
		t.Parallel()
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
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		dir := path.Join(b.Root, "folder")
		mustMkdir(t, b, dir)
		mustWrite(t, b, path.Join(dir, "child.txt"), []byte("child"))

		st, statErr := b.Session.Route(ctx, dir).Stat(ctx)
		_, openErr := b.Session.Route(ctx, dir).Open(ctx)
		_, textErr := b.Session.Route(ctx, dir).ReadText(ctx)
		ents, listErr := b.Session.Route(ctx, dir).ReadDir(ctx)

		if statErr != nil || !st.IsDir || st.Version != "" {
			t.Fatalf("stat = %+v %v, want a directory without a version", st, statErr)
		}
		if !errors.Is(openErr, vfs.ErrIsDir) || !errors.Is(textErr, vfs.ErrIsDir) {
			t.Fatalf("open = %v, read text = %v, want ErrIsDir", openErr, textErr)
		}
		if listErr != nil || len(ents) != 1 || ents[0].Name != "child.txt" || ents[0].IsDir {
			t.Fatalf("listing = %+v %v, want [child.txt]", ents, listErr)
		}
	})

	t.Run("a text file reads back as a document at its path", func(t *testing.T) {
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "lines.txt")
		mustWrite(t, b, name, []byte("one\ntwo\n"))

		doc, err := b.Session.Route(ctx, name).ReadText(ctx)

		if err != nil || doc.Text() != "one\ntwo\n" || doc.Path() != name || doc.MediaType() != "text/plain" {
			t.Fatalf("read text = %+v %v, want the text/plain document at %s", doc, err, name)
		}
	})

	t.Run("an overwrite replaces the content", func(t *testing.T) {
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "over")
		mustWrite(t, b, name, []byte("first, and longer"))
		mustWrite(t, b, name, []byte("second"))

		got, readErr := b.Session.Route(ctx, name).ReadFile(ctx)
		st, statErr := b.Session.Route(ctx, name).Stat(ctx)

		if readErr != nil || string(got) != "second" || statErr != nil || st.Size != int64(len("second")) {
			t.Fatalf("read = %q %v, stat = %+v %v, want the second write", got, readErr, st, statErr)
		}
	})

	t.Run("an empty file reads back empty", func(t *testing.T) {
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "empty.txt")
		mustWrite(t, b, name, nil)

		got, readErr := b.Session.Route(ctx, name).ReadFile(ctx)
		st, statErr := b.Session.Route(ctx, name).Stat(ctx)

		if readErr != nil || len(got) != 0 || statErr != nil || st.IsDir || st.Size != 0 {
			t.Fatalf("read = %q %v, stat = %+v %v, want an empty file", got, readErr, st, statErr)
		}
	})

	t.Run("nested directories make at once, list as directories and make again", func(t *testing.T) {
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		outer, inner := path.Join(b.Root, "outer"), path.Join(b.Root, "outer", "inner")
		mustMkdir(t, b, inner)
		mustWrite(t, b, path.Join(inner, "leaf.txt"), []byte("leaf"))

		ents, listErr := b.Session.Route(ctx, outer).ReadDir(ctx)
		st, statErr := b.Session.Route(ctx, inner).Stat(ctx)
		againErr := b.Session.Route(ctx, inner).MkdirAll(ctx)
		leaf, leafErr := b.Session.Route(ctx, path.Join(inner, "leaf.txt")).ReadFile(ctx)

		if listErr != nil || len(ents) != 1 || ents[0].Name != "inner" || !ents[0].IsDir {
			t.Fatalf("listing of outer = %+v %v, want the inner directory", ents, listErr)
		}
		if statErr != nil || !st.IsDir || againErr != nil {
			t.Fatalf("stat of inner = %+v %v, mkdir again = %v, want an existing directory", st, statErr, againErr)
		}
		if leafErr != nil || string(leaf) != "leaf" {
			t.Fatalf("nested read = %q %v", leaf, leafErr)
		}
	})

	t.Run("an empty directory removes", func(t *testing.T) {
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		dir := path.Join(b.Root, "bare")
		mustMkdir(t, b, dir)

		removeErr := b.Session.Route(ctx, dir).Remove(ctx)
		_, statErr := b.Session.Route(ctx, dir).Stat(ctx)

		if removeErr != nil || !errors.Is(statErr, vfs.ErrNotExist) {
			t.Fatalf("remove = %v, then stat = %v, want the directory gone", removeErr, statErr)
		}
	})

	t.Run("a file is no directory to list or to make a directory in", func(t *testing.T) {
		t.Parallel()
		ctx, b := t.Context(), newBackend(t)
		name := path.Join(b.Root, "plain.txt")
		mustWrite(t, b, name, []byte("plain"))

		_, listErr := b.Session.Route(ctx, name).ReadDir(ctx)
		mkdirErr := b.Session.Route(ctx, path.Join(name, "sub")).MkdirAll(ctx)

		if !errors.Is(listErr, vfs.ErrNotDir) || !errors.Is(mkdirErr, vfs.ErrNotDir) {
			t.Fatalf("list = %v, mkdir under it = %v, want ErrNotDir", listErr, mkdirErr)
		}
	})

	t.Run("a removed file is not found, and a handle opened before reads as the backend promises", func(t *testing.T) {
		t.Parallel()
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

func mustMkdir(t *testing.T, b Backend, name string) {
	t.Helper()
	if err := b.Session.Route(t.Context(), name).MkdirAll(t.Context()); err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
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
