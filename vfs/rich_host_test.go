package vfs_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// Host media types for the host codecs below.
const (
	richFrozenType = "application/x-rich-frozen"
	richOpaqueType = "application/x-rich-opaque"
	richPlainType  = "application/x-rich-plain"
)

var errRichFrozen = errors.New("host: the document is frozen")

// richHostFS is a host's own read-only backend. Each file has the media type,
// the Stat size and the reader that the host gives it.
type richHostFS map[string]richHostFile

type richHostFile struct {
	mediaType string
	// size is the Stat size; -1 when the host cannot tell.
	size int64
	// open is the content; nil gives a handle that cannot read.
	open func() io.Reader
	// conflict makes each write of the file fail its compare-and-swap.
	conflict bool
}

// richHostText is a host file of known size with body as its content.
func richHostText(mediaType, body string) richHostFile {
	return richHostFile{mediaType: mediaType, size: int64(len(body)), open: func() io.Reader { return strings.NewReader(body) }}
}

func (h richHostFS) Validate(context.Context) error { return nil }

func (h richHostFS) Stat(_ context.Context, name string) (vfs.FileInfo, error) {
	f, ok := h[name]
	if !ok {
		return vfs.FileInfo{}, vfs.ErrNotExist
	}
	return vfs.FileInfo{Name: name, Size: f.size, Mode: 0o444, MediaType: f.mediaType}, nil
}

func (h richHostFS) OpenFile(ctx context.Context, name string, _ int, _ fs.FileMode) (vfs.File, error) {
	fi, err := h.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	handle := richHostHandle{info: fi}
	if open := h[name].open; open != nil {
		return richHostReader{richHostHandle: handle, Reader: open()}, nil
	}
	return handle, nil
}

func (h richHostFS) ReadDir(context.Context, string) ([]vfs.DirEntry, error) {
	return nil, vfs.ErrNotSupported
}

func (h richHostFS) Remove(context.Context, string) error { return vfs.ErrReadOnly }

func (h richHostFS) MkdirAll(context.Context, string, fs.FileMode) error { return vfs.ErrReadOnly }

// OpenDocument decodes the file with the session's codecs.
func (h richHostFS) OpenDocument(ctx context.Context, name string, reg *vfs.ContentRegistry) (vfs.Document, error) {
	f, ok := h[name]
	if !ok || f.open == nil {
		return nil, vfs.ErrNotExist
	}
	data, err := io.ReadAll(f.open())
	if err != nil {
		return nil, err
	}
	return reg.Decode(ctx, name, f.mediaType, data)
}

func (h richHostFS) WriteDocument(_ context.Context, name string, _ vfs.Document, _ *vfs.ContentRegistry) error {
	if h[name].conflict {
		return vfs.ErrConflict
	}
	return vfs.ErrReadOnly
}

type richHostHandle struct{ info vfs.FileInfo }

func (f richHostHandle) Close() error                { return nil }
func (f richHostHandle) Stat() (vfs.FileInfo, error) { return f.info, nil }

type richHostReader struct {
	richHostHandle
	io.Reader
}

// richFrozenCodec is a host identity codec. Its text cannot change, its
// lines cannot be read, and its one block has no line span.
type richFrozenCodec struct{}

func (richFrozenCodec) MediaTypes() []string { return []string{richFrozenType} }
func (richFrozenCodec) Identity()            {}
func (richFrozenCodec) Decode(_ context.Context, path, mediaType string, data []byte) (vfs.Document, error) {
	return richFrozenText{vfs.NewTextDocument(path, mediaType, "utf-8", string(data))}, nil
}

type richFrozenText struct{ *vfs.IR }

func (richFrozenText) SetText(string) error             { return errRichFrozen }
func (richFrozenText) Lines(int, int) ([]string, error) { return nil, errRichFrozen }
func (richFrozenText) Blocks() []vfs.Block {
	return []vfs.Block{{ID: "loose", Kind: vfs.BlockKindParagraph}}
}

// richOpaqueCodec is a host codec of a document with no text (an image).
type richOpaqueCodec struct{}

type richOpaqueDoc struct{ path string }

func (d richOpaqueDoc) Path() string      { return d.path }
func (d richOpaqueDoc) MediaType() string { return richOpaqueType }

func (richOpaqueCodec) MediaTypes() []string { return []string{richOpaqueType} }
func (richOpaqueCodec) Decode(_ context.Context, path, _ string, _ []byte) (vfs.Document, error) {
	return richOpaqueDoc{path: path}, nil
}
func (richOpaqueCodec) Create(path, _ string, _ vfs.Mutation) (vfs.Document, error) {
	return richOpaqueDoc{path: path}, nil
}

// richPlainCodec is a host projected codec whose checkout is plain text: it
// is neither a grid nor a block tree.
type richPlainCodec struct{}

func (richPlainCodec) MediaTypes() []string { return []string{richPlainType} }
func (richPlainCodec) Decode(_ context.Context, path, mediaType string, data []byte) (vfs.Document, error) {
	return vfs.NewTextDocument(path, mediaType, "utf-8", string(data)), nil
}

// richHostSession mounts host at /workspace/host next to members, with the
// host codecs and codecs.
func richHostSession(t *testing.T, host richHostFS, members []vfs.Member, codecs ...vfs.Codec) *vfs.MountSession {
	t.Helper()
	members = append(members, vfs.At("host", func(context.Context, string, mount.Binding) (vfs.Provider, error) {
		return host, nil
	}))
	codecs = append(codecs, richFrozenCodec{}, richOpaqueCodec{}, richPlainCodec{})
	ms, err := vfs.Tree(members...).WithCodecs(codecs...)(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// richLineReader repeats line forever.
type richLineReader struct {
	line []byte
	off  int
}

func (r *richLineReader) Read(p []byte) (int, error) {
	n := copy(p, r.line[r.off:])
	r.off = (r.off + n) % len(r.line)
	return n, nil
}

// TestRoute_readLinesStreamsAHostFile: ReadLines streams a host file of
// unknown size. It returns a full window before the end, the last lines at
// the end of the file, and the read error of the host reader. It refuses a
// start past the end, a line that is too long, a scan past MaxLineScanBytes
// and a handle that cannot read. A host text whose lines cannot be read
// gives its error.
func TestRoute_readLinesStreamsAHostFile(t *testing.T) {
	t.Parallel()
	errHostRead := errors.New("host: read failed")
	stream := func(open func() io.Reader) richHostFile {
		return richHostFile{mediaType: "application/octet-stream", size: -1, open: open}
	}
	text := func(body string) func() io.Reader {
		return func() io.Reader { return strings.NewReader(body) }
	}
	ms := richHostSession(t, richHostFS{
		"lines":  stream(text("a\nb\nc\n")),
		"tail":   stream(text("a\nb")),
		"broken": stream(func() io.Reader { return io.MultiReader(strings.NewReader("a\nb\n"), iotest.ErrReader(errHostRead)) }),
		"long":   stream(text(strings.Repeat("y", vfs.MaxLineBytes+1) + "\n")),
		"endless": stream(func() io.Reader {
			return &richLineReader{line: []byte(strings.Repeat("z", vfs.MaxLineBytes-1) + "\n")}
		}),
		"closed": {mediaType: "application/octet-stream", size: -1},
		"frozen": richHostText(richFrozenType, "one\ntwo\n"),
	}, nil)
	cases := []struct {
		name       string
		file       string
		start, end int
		want       []string
		wantEOF    bool
		wantErr    error
		wantErrMsg string
	}{
		{name: "a full window before the end", file: "lines", start: 1, end: 3, want: []string{"a", "b"}},
		{name: "the last lines at the end", file: "tail", start: 2, end: 9, want: []string{"b"}, wantEOF: true},
		{name: "a start past the end", file: "lines", start: 9, end: 10, wantErr: vfs.ErrLineOutOfRange},
		{name: "the read error of the host reader", file: "broken", start: 1, end: 9, wantErr: errHostRead},
		{name: "a line that is too long", file: "long", start: 1, end: 2, wantErr: vfs.ErrLineTooLong},
		{name: "a scan past the byte cap", file: "endless", start: 1000, end: 1001, wantErr: vfs.ErrTooLarge},
		{name: "a handle that cannot read", file: "closed", start: 1, end: 2, wantErrMsg: "not readable"},
		{name: "a host text whose lines cannot be read", file: "frozen", start: 1, end: 2, wantErr: errRichFrozen},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			win, err := ms.Route(t.Context(), "/workspace/host/"+tc.file).ReadLines(t.Context(), tc.start, tc.end)

			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ReadLines err = %v, want %v", err, tc.wantErr)
				}
			case tc.wantErrMsg != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Fatalf("ReadLines err = %v, want %q", err, tc.wantErrMsg)
				}
			case err != nil || strings.Join(win.Lines, "|") != strings.Join(tc.want, "|") || win.EOF != tc.wantEOF:
				t.Fatalf("ReadLines = %+v err=%v, want lines %q eof=%v", win, err, tc.want, tc.wantEOF)
			}
		})
	}
}
