package vfs_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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

// hostProvider is a host's own backend, as a host plugs it in through Tree.
// It opens every name with open, and it has no one-shot PutFile and no
// document I/O.
type hostProvider struct {
	open func(flag int) (vfs.File, error)
}

func (hostProvider) Validate(context.Context) error { return nil }
func (hostProvider) Stat(context.Context, string) (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: "f", MediaType: "application/octet-stream"}, nil
}
func (p hostProvider) OpenFile(_ context.Context, _ string, flag int, _ fs.FileMode) (vfs.File, error) {
	return p.open(flag)
}
func (hostProvider) ReadDir(context.Context, string) ([]vfs.DirEntry, error) { return nil, nil }
func (hostProvider) Remove(context.Context, string) error                    { return nil }
func (hostProvider) MkdirAll(context.Context, string, fs.FileMode) error     { return nil }

// mountHost mounts a hostProvider at /workspace/host.
func mountHost(t *testing.T, open func(flag int) (vfs.File, error)) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(vfs.At("host", func(context.Context, string, mount.Binding) (vfs.Provider, error) {
		return hostProvider{open: open}, nil
	}))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// opens returns an open func that always returns f.
func opens(f vfs.File) func(int) (vfs.File, error) {
	return func(int) (vfs.File, error) { return f, nil }
}

// zeros reads zero bytes without end.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// TestRoute_readFileFromAHostHandle: ReadFile reads a host handle of unknown
// size to its end, refuses a handle that cannot read and a stream over the
// cap, and returns the error of a stream that fails. ContentRev hashes the
// bytes of a provider with no document I/O and fails when its open fails.
func TestRoute_readFileFromAHostHandle(t *testing.T) {
	t.Parallel()
	errStream := errors.New("stream broke")
	unsized := hostFile{info: vfs.FileInfo{Size: -1}}
	for name, tc := range map[string]struct {
		file    vfs.File
		want    string
		wantErr func(error) bool
	}{
		"a stream of unknown size reads to its end": {
			file: hostReader{unsized, strings.NewReader("streamed")}, want: "streamed",
		},
		"a sized handle with no Read is not readable": {
			file:    hostFile{info: vfs.FileInfo{Size: 4}},
			wantErr: func(err error) bool { return err != nil && strings.Contains(err.Error(), "not readable") },
		},
		"a handle with no Stat and no Read is not readable": {
			file:    hostFile{err: vfs.ErrNotSupported},
			wantErr: func(err error) bool { return err != nil && strings.Contains(err.Error(), "not readable") },
		},
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
			ms := mountHost(t, opens(tc.file))

			got, err := ms.Route(ctx, "/workspace/host/f").ReadFile(ctx)

			if tc.wantErr != nil && !tc.wantErr(err) {
				t.Fatalf("ReadFile = %q, %v", got, err)
			}
			if tc.wantErr == nil && (err != nil || string(got) != tc.want) {
				t.Fatalf("ReadFile = %q, %v, want %q", got, err, tc.want)
			}
		})
	}

	t.Run("ContentRev hashes the bytes or fails with the open", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		streams := mountHost(t, func(int) (vfs.File, error) { return hostReader{unsized, strings.NewReader("rev")}, nil })
		denies := mountHost(t, func(int) (vfs.File, error) { return nil, vfs.ErrPermission })

		rev, revErr := streams.ContentRev(ctx, "/workspace/host/f")
		_, deniedErr := denies.ContentRev(ctx, "/workspace/host/f")

		if sum := sha256.Sum256([]byte("rev")); revErr != nil || rev.Hash != hex.EncodeToString(sum[:]) {
			t.Fatalf("ContentRev = %+v, %v", rev, revErr)
		}
		if !errors.Is(deniedErr, vfs.ErrPermission) {
			t.Fatalf("ContentRev of a refused open = %v, want ErrPermission", deniedErr)
		}
	})
}

// TestRoute_writeFileToAHostHandle: a provider with no PutFile gets the bytes
// through the handle it opens for write. An empty write needs no Write, a
// handle with no Write is read-only, and a failed open fails the write. Such
// a provider has no document I/O.
func TestRoute_writeFileToAHostHandle(t *testing.T) {
	t.Parallel()
	t.Run("a handle with Write gets the bytes", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		var written bytes.Buffer
		ms := mountHost(t, opens(hostWriter{Writer: &written}))

		err := ms.Route(ctx, "/workspace/host/f").WriteFile(ctx, []byte("written"))

		if err != nil || written.String() != "written" {
			t.Fatalf("WriteFile = %v, handle got %q", err, written.String())
		}
	})
	for name, tc := range map[string]struct {
		open    func(int) (vfs.File, error)
		data    string
		wantErr error
	}{
		"an empty write needs no Write":       {open: opens(hostFile{})},
		"a handle with no Write is read-only": {open: opens(hostFile{}), data: "x", wantErr: vfs.ErrReadOnly},
		"a failed open fails the write": {
			open: func(int) (vfs.File, error) { return nil, vfs.ErrPermission }, data: "x", wantErr: vfs.ErrPermission,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			ms := mountHost(t, tc.open)

			err := ms.Route(ctx, "/workspace/host/f").WriteFile(ctx, []byte(tc.data))

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("WriteFile = %v, want %v", err, tc.wantErr)
			}
		})
	}
	t.Run("there is no document I/O", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		ms := mountHost(t, opens(hostFile{}))

		_, openErr := ms.Route(ctx, "/workspace/host/f").OpenDocument(ctx)
		writeErr := ms.Route(ctx, "/workspace/host/f").WriteDocument(ctx, vfs.NewTextDocument("/workspace/host/f", "text/plain", "", "x"))

		if !errors.Is(openErr, vfs.ErrNotSupported) || !errors.Is(writeErr, vfs.ErrNotSupported) {
			t.Fatalf("OpenDocument = %v, WriteDocument = %v, want ErrNotSupported", openErr, writeErr)
		}
	})
}

// imageCodec is a host codec that decodes PNG files to an image document,
// which has no text form.
type imageCodec struct{}

func (imageCodec) MediaTypes() []string { return []string{"image/png"} }
func (imageCodec) Decode(_ context.Context, path, _ string, _ []byte) (vfs.Document, error) {
	return imageDocument{path: path}, nil
}

// hostText is a host's own Textual. It is not the vfs IR.
type hostText struct{ *vfs.IR }

// TestRoute_aHostDocumentWithNoTextHasARawRev: a host codec decodes a file to
// a document with no text form. ReadText refuses it, and ContentRev hashes
// the raw bytes instead. The rev of a host's own Textual is the hash of its
// text.
func TestRoute_aHostDocumentWithNoTextHasARawRev(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	png := []byte("\x89PNG\r\n\x1a\nbody")
	if err := os.WriteFile(filepath.Join(dir, "pic.png"), png, 0o600); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(dir))).WithCodecs(imageCodec{})(ctx, t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}

	_, textErr := ms.Route(ctx, "/workspace/w/pic.png").ReadText(ctx)
	rev, revErr := ms.ContentRev(ctx, "/workspace/w/pic.png")
	_, missingErr := ms.ContentRev(ctx, "/workspace/w/missing.png")
	token := vfs.ContentToken(hostText{vfs.NewTextDocument("/h", "text/plain", "", "host text")})

	if !errors.Is(textErr, vfs.ErrNotTextual) {
		t.Fatalf("ReadText = %v, want ErrNotTextual", textErr)
	}
	if sum := sha256.Sum256(png); revErr != nil || rev.Hash != hex.EncodeToString(sum[:]) {
		t.Fatalf("ContentRev = %+v, %v, want the hash of the raw bytes", rev, revErr)
	}
	if !errors.Is(missingErr, vfs.ErrNotExist) {
		t.Fatalf("ContentRev of a missing file = %v, want ErrNotExist", missingErr)
	}
	if token != vfs.ContentHash("host text") {
		t.Fatalf("ContentToken of a host Textual = %q, want the hash of its text", token)
	}
}
