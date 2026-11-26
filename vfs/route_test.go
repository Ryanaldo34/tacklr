package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// hostFile is the handle of a host's own provider with only the methods every
// File has: Close and Stat. A host adds Read or Write when its backend can.
type hostFile struct {
	info vfs.FileInfo
	err  error
}

func (hostFile) Close() error                  { return nil }
func (f hostFile) Stat() (vfs.FileInfo, error) { return f.info, f.err }

// hostReader is a host handle that streams r.
type hostReader struct {
	hostFile
	io.Reader
}

// hostWriter is a host handle that writes to w.
type hostWriter struct {
	hostFile
	io.Writer
}

// hostFS is a host's own backend as a host plugs it in through Tree: its
// files by name. It opens a file as its handle, whatever the flags, and it
// has no one-shot PutFile and no document I/O.
type hostFS map[string]vfs.File

func (hostFS) Validate(context.Context) error { return nil }

func (h hostFS) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	f, ok := h[name]
	if !ok {
		return vfs.FileInfo{}, vfs.ErrNotExist
	}
	return f.Stat()
}

func (h hostFS) OpenFile(_ context.Context, name string, _ int, _ fs.FileMode) (vfs.File, error) {
	f, ok := h[name]
	if !ok {
		return nil, vfs.ErrNotExist
	}
	return f, nil
}

func (h hostFS) ReadDir(context.Context, string) ([]vfs.DirEntry, error) {
	var out []vfs.DirEntry
	for _, name := range slices.Sorted(maps.Keys(h)) {
		out = append(out, vfs.DirEntry{Name: name})
	}
	return out, nil
}

func (hostFS) Remove(context.Context, string) error                { return nil }
func (hostFS) MkdirAll(context.Context, string, fs.FileMode) error { return nil }

// hostOpen opens h as a host's Open func does.
func hostOpen(h hostFS) vfs.Open {
	return func(context.Context, string, mount.Binding) (vfs.Provider, error) { return h, nil }
}

// mountHost mounts h at /workspace/host.
func mountHost(t *testing.T, h hostFS) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(vfs.At("host", hostOpen(h)))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// zeros reads zero bytes without end.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// TestRoute_readFileFromAHostHandle: ReadFile reads a host handle of unknown
// size to its end, refuses a handle that cannot read and a stream over the
// cap, and returns the error of a stream that fails.
func TestRoute_readFileFromAHostHandle(t *testing.T) {
	t.Parallel()
	errStream := errors.New("stream broke")
	unsized := hostFile{info: vfs.FileInfo{Size: -1}}
	notReadable := func(err error) bool { return err != nil && strings.Contains(err.Error(), "not readable") }
	for name, tc := range map[string]struct {
		file    vfs.File
		want    string
		wantErr func(error) bool
	}{
		"a stream of unknown size reads to its end": {
			file: hostReader{unsized, strings.NewReader("streamed")}, want: "streamed",
		},
		"a sized handle with no Read is not readable":       {file: hostFile{info: vfs.FileInfo{Size: 4}}, wantErr: notReadable},
		"a handle with no Stat and no Read is not readable": {file: hostFile{err: vfs.ErrNotSupported}, wantErr: notReadable},
		"a stream that fails returns its error": {
			file:    hostReader{unsized, iotest.ErrReader(errStream)},
			wantErr: func(err error) bool { return errors.Is(err, errStream) },
		},
		"a stream over the cap is too large": {
			file:    hostReader{unsized, zeros{}},
			wantErr: func(err error) bool { return errors.Is(err, vfs.ErrTooLarge) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			ms := mountHost(t, hostFS{"f": tc.file})

			got, err := ms.Route(ctx, "/workspace/host/f").ReadFile(ctx)

			if tc.wantErr != nil && !tc.wantErr(err) {
				t.Fatalf("ReadFile = %q, %v", got, err)
			}
			if tc.wantErr == nil && (err != nil || string(got) != tc.want) {
				t.Fatalf("ReadFile = %q, %v, want %q", got, err, tc.want)
			}
		})
	}
}

// TestRoute_writeFileToAHostHandle: a provider with no PutFile gets the bytes
// through the handle it opens for write. An empty write needs no Write, a
// handle with no Write is read-only, and a failed open fails the write.
func TestRoute_writeFileToAHostHandle(t *testing.T) {
	t.Parallel()
	t.Run("a handle with Write gets the bytes", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		var written bytes.Buffer
		ms := mountHost(t, hostFS{"f": hostWriter{Writer: &written}})

		err := ms.Route(ctx, "/workspace/host/f").WriteFile(ctx, []byte("written"))

		if err != nil || written.String() != "written" {
			t.Fatalf("WriteFile = %v, handle got %q", err, written.String())
		}
	})
	for name, tc := range map[string]struct {
		file    string
		data    string
		wantErr error
	}{
		"an empty write needs no Write":       {file: "f"},
		"a handle with no Write is read-only": {file: "f", data: "x", wantErr: vfs.ErrReadOnly},
		"a failed open fails the write":       {file: "missing", data: "x", wantErr: vfs.ErrNotExist},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			ms := mountHost(t, hostFS{"f": hostFile{}})

			err := ms.Route(ctx, "/workspace/host/"+tc.file).WriteFile(ctx, []byte(tc.data))

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("WriteFile = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestRoute_aProviderWithNoDocumentIOHasNoDocuments: OpenDocument of a file
// on a provider that does not decode documents is not supported.
func TestRoute_aProviderWithNoDocumentIOHasNoDocuments(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms := mountHost(t, hostFS{"f": hostFile{}})

	_, err := ms.Route(ctx, "/workspace/host/f").OpenDocument(ctx)

	if !errors.Is(err, vfs.ErrNotSupported) {
		t.Fatalf("OpenDocument = %v, want ErrNotSupported", err)
	}
}

// routeOps calls each Route method and returns its error.
var routeOps = map[string]func(context.Context, vfs.Route) error{
	"stat":     func(ctx context.Context, r vfs.Route) error { _, err := r.Stat(ctx); return err },
	"open":     func(ctx context.Context, r vfs.Route) error { _, err := r.Open(ctx); return err },
	"read":     func(ctx context.Context, r vfs.Route) error { _, err := r.ReadFile(ctx); return err },
	"document": func(ctx context.Context, r vfs.Route) error { _, err := r.OpenDocument(ctx); return err },
	"text":     func(ctx context.Context, r vfs.Route) error { _, err := r.ReadText(ctx); return err },
	"list":     func(ctx context.Context, r vfs.Route) error { _, err := r.ReadDir(ctx); return err },
	"write":    func(ctx context.Context, r vfs.Route) error { return r.WriteFile(ctx, []byte("x")) },
	"remove":   func(ctx context.Context, r vfs.Route) error { return r.Remove(ctx) },
	"mkdir":    func(ctx context.Context, r vfs.Route) error { return r.MkdirAll(ctx) },
}

// TestRoute_refusesACallBeforeItReachesTheProvider: a Route refuses, for
// every provider and before the provider sees it, a call whose context
// ended, a path that is not a clean absolute path or that no mount holds, a
// write, removal or new directory on a read-only mount, and a call on the
// mount root that a directory cannot take.
func TestRoute_refusesACallBeforeItReachesTheProvider(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	base := t.TempDir()
	ms, err := vfs.Tree(
		vfs.At("work", vfs.Local(base)),
		vfs.At("ro", vfs.Local(base)).ReadOnly(),
	)(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "f.txt"), []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	all := slices.Collect(maps.Keys(routeOps))
	for _, c := range []struct {
		name string
		ctx  context.Context
		path string
		ops  []string
		want error
	}{
		{"an ended context", canceled, "/workspace/work/f.txt", all, context.Canceled},
		{"an empty path", ctx, "", all, vfs.ErrInvalidPath},
		{"a relative path", ctx, "workspace/work/f.txt", all, vfs.ErrInvalidPath},
		{"a path with a NUL", ctx, "/workspace/work/f\x00.txt", all, vfs.ErrInvalidPath},
		{"a path no mount holds", ctx, "/elsewhere/f.txt", all, vfs.ErrNotMounted},
		{"a change on a read-only mount", ctx, "/workspace/ro/f.txt", []string{"write", "remove"}, vfs.ErrReadOnly},
		{"a directory on a read-only mount", ctx, "/workspace/ro/new", []string{"mkdir"}, vfs.ErrReadOnly},
		{"a removal of the mount root", ctx, "/workspace/work", []string{"remove"}, vfs.ErrInvalidPath},
		{"a file call on the mount root", ctx, "/workspace/work", []string{"open", "read", "document", "text", "write"}, vfs.ErrIsDir},
		{"a file call on the workspace", ctx, "/workspace", []string{"open", "write"}, vfs.ErrIsDir},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			for _, op := range c.ops {
				err := routeOps[op](c.ctx, ms.Route(c.ctx, c.path))

				if !errors.Is(err, c.want) {
					t.Errorf("%s = %v, want %v", op, err, c.want)
				}
			}
		})
	}
	t.Run("nothing on disk changed", func(t *testing.T) {
		got, err := os.ReadFile(filepath.Join(base, "f.txt"))
		entries, _ := os.ReadDir(base)
		if err != nil || string(got) != "kept" || len(entries) != 1 {
			t.Fatalf("f.txt = %q %v, %d entries, want it alone and unchanged", got, err, len(entries))
		}
	})
}

// TestRoute_aWriteOverTheCapIsTooLarge: a write over MaxReadFileBytes does
// not reach the provider.
func TestRoute_aWriteOverTheCapIsTooLarge(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms := mountHost(t, hostFS{})

	err := ms.Route(ctx, "/workspace/host/big").WriteFile(ctx, make([]byte, vfs.MaxReadFileBytes+1))

	if !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("WriteFile = %v, want ErrTooLarge", err)
	}
}

// TestRoute_mkdirOfTheMountRootIsDone: the mount root exists, so making it
// succeeds.
func TestRoute_mkdirOfTheMountRootIsDone(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms := mountHost(t, hostFS{})

	err := ms.Route(ctx, "/workspace/host").MkdirAll(ctx)

	if err != nil {
		t.Fatalf("MkdirAll = %v", err)
	}
}

// imageCodec is a host codec that decodes PNG files to an image document,
// which has no text form.
type imageCodec struct{}

func (imageCodec) MediaTypes() []string { return []string{"image/png"} }
func (imageCodec) Decode(_ context.Context, path, _ string, _ []byte) (vfs.Document, error) {
	return imageDocument{path: path}, nil
}

// imageDocument is a document with no text form, as a host image codec
// returns it.
type imageDocument struct{ path string }

func (d imageDocument) Path() string    { return d.path }
func (imageDocument) MediaType() string { return "image/png" }

// TestRoute_readTextRefusesAHostDocumentWithNoText: a host codec decodes a
// file to a document with no text form, and ReadText refuses it.
func TestRoute_readTextRefusesAHostDocumentWithNoText(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pic.png"), []byte("\x89PNG\r\n\x1a\nbody"), 0o600); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(dir))).WithCodecs(imageCodec{})(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	_, textErr := ms.Route(ctx, "/workspace/w/pic.png").ReadText(ctx)

	if !errors.Is(textErr, vfs.ErrNotTextual) {
		t.Fatalf("ReadText = %v, want ErrNotTextual", textErr)
	}
}
