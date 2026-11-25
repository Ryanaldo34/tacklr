package vfs_test

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

// filesSession is a work mount with a note, a binary file, a Word document
// and a document that does not decode, and a read-only mount.
func filesSession(t *testing.T) (*vfs.MountSession, vfs.Files) {
	t.Helper()
	dir, ro := t.TempDir(), t.TempDir()
	for name, body := range map[string]string{
		"note.txt":    "hello\n",
		"blob.bin":    "a\x00b\x00c",
		"broken.docx": "not a zip",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	ms, err := vfs.Tree(
		vfs.At("work", vfs.Local(dir)),
		vfs.At("ro", vfs.Local(ro)).ReadOnly(),
	).WithCodecs(adapters.Codecs()...)(t.Context(), t.Name(), vfs.Request{})
	require.NoError(t, err)
	spec := "<h1>Title</h1><p>text</p>"
	_, err = ms.Apply(t.Context(), "/workspace/work/Spec.docx", vfs.Mutation{Content: &spec})
	require.NoError(t, err)
	return ms, vfs.NewFiles(ms)
}

func readAll(t *testing.T, o *vfs.OpenFile) string {
	t.Helper()
	buf := make([]byte, o.Size()+8)
	n, err := o.ReadAt(buf, 0)
	require.ErrorIs(t, err, io.EOF)
	return string(buf[:n])
}

func TestFiles_writesPlainTextAtOffsetsAndKeepsItOnClose(t *testing.T) {
	ms, files := filesSession(t)
	ctx := t.Context()

	created, err := files.Open(ctx, "/workspace/work/new.txt", vfs.OpenMode{Write: true, Create: true})
	require.NoError(t, err)
	_, err = created.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	_, err = created.WriteAt([]byte("J"), 0)
	require.NoError(t, err)
	require.NoError(t, created.Close(ctx))
	appended, err := files.Open(ctx, "/workspace/work/new.txt", vfs.OpenMode{Append: true})
	require.NoError(t, err)
	_, err = appended.WriteAt([]byte("!"), 0)
	require.NoError(t, err)
	require.NoError(t, appended.Sync(ctx))
	require.NoError(t, appended.Truncate(3))
	require.NoError(t, appended.Truncate(5))
	require.NoError(t, appended.Close(ctx))
	truncated, err := files.Open(ctx, "/workspace/work/note.txt", vfs.OpenMode{Truncate: true})
	require.NoError(t, err)
	require.NoError(t, truncated.Close(ctx))

	data, err := ms.Route(ctx, "/workspace/work/new.txt").ReadFile(ctx)
	require.NoError(t, err)
	require.Equal(t, "Jel\x00\x00", string(data))
	note, err := ms.Route(ctx, "/workspace/work/note.txt").ReadFile(ctx)
	require.NoError(t, err)
	require.Empty(t, note)
}

func TestFiles_readsADocumentAsItsProjectedText(t *testing.T) {
	_, files := filesSession(t)
	ctx := t.Context()

	st, err := files.Stat(ctx, "/workspace/work/Spec.docx")
	require.NoError(t, err)
	doc, err := files.Open(ctx, "/workspace/work/Spec.docx", vfs.OpenMode{})
	require.NoError(t, err)

	head := make([]byte, 2)
	_, headErr := doc.ReadAt(head, 0)
	_, pastEndErr := doc.ReadAt(head, st.Size)
	text := readAll(t, doc)
	require.Contains(t, text, "Title")
	require.Equal(t, int64(len(text)), st.Size)
	require.NoError(t, headErr)
	require.Equal(t, text[:2], string(head))
	require.ErrorIs(t, pastEndErr, io.EOF)
	require.NoError(t, doc.Close(ctx))
}

func TestFiles_readsABinaryFileAtOffsets(t *testing.T) {
	_, files := filesSession(t)
	ctx := t.Context()
	bin, err := files.Open(ctx, "/workspace/work/blob.bin", vfs.OpenMode{})
	require.NoError(t, err)

	buf := make([]byte, 3)
	n, err := bin.ReadAt(buf, 2)

	require.NoError(t, err)
	require.Equal(t, "b\x00c", string(buf[:n]))
	require.Equal(t, int64(5), bin.Size())
	require.ErrorIs(t, bin.Truncate(1), vfs.ErrReadOnly)
	_, err = bin.WriteAt([]byte("x"), 0)
	require.ErrorIs(t, err, vfs.ErrReadOnly)
	require.NoError(t, bin.Close(ctx))
}

func TestFiles_makesMovesListsAndRemovesEntries(t *testing.T) {
	ms, files := filesSession(t)
	ctx := t.Context()

	require.NoError(t, files.Mkdir(ctx, "/workspace/work/a/b"))
	require.NoError(t, files.Rename(ctx, "/workspace/work/note.txt", "/workspace/work/a/b/note.txt"))
	entries, err := files.ReadDir(ctx, "/workspace/work/a/b")
	require.NoError(t, err)
	require.NoError(t, files.Remove(ctx, "/workspace/work/blob.bin"))

	require.Equal(t, []string{"note.txt"}, []string{entries[0].Name})
	moved, err := ms.Route(ctx, "/workspace/work/a/b/note.txt").ReadFile(ctx)
	require.NoError(t, err)
	require.Equal(t, "hello\n", string(moved))
	_, err = files.Stat(ctx, "/workspace/work/blob.bin")
	require.ErrorIs(t, err, vfs.ErrNotExist)
}

// Each case is one refusal of Files or OpenFile.
func TestFiles_refusesWhatTheViewCannotDo(t *testing.T) {
	_, files := filesSession(t)
	ctx := t.Context()
	open := func(p string, mode vfs.OpenMode) error {
		_, err := files.Open(ctx, p, mode)
		return err
	}
	cases := []struct {
		name string
		err  error
		want error
	}{
		{"a new entry in the root", files.Mkdir(ctx, "/x"), vfs.ErrPermission},
		{"removing a root entry", files.Remove(ctx, "/workspace"), vfs.ErrPermission},
		{"a move out of the root", files.Rename(ctx, "/workspace", "/workspace/work/x"), vfs.ErrPermission},
		{"a move of a missing file", files.Rename(ctx, "/workspace/work/none", "/workspace/work/x"), vfs.ErrNotExist},
		{"a move into a read-only mount", files.Rename(ctx, "/workspace/work/note.txt", "/workspace/ro/note.txt"), vfs.ErrReadOnly},
		{"a file created beside the mounts", open("/x.txt", vfs.OpenMode{Create: true}), vfs.ErrNotMounted},
		{"a new document", open("/workspace/work/new.docx", vfs.OpenMode{Create: true}), vfs.ErrReadOnly},
		{"a file created in a read-only mount", open("/workspace/ro/x.txt", vfs.OpenMode{Create: true}), vfs.ErrReadOnly},
		{"a missing file", open("/workspace/work/none", vfs.OpenMode{}), vfs.ErrNotExist},
		{"a directory", open("/workspace/work", vfs.OpenMode{}), vfs.ErrIsDir},
		{"a write to a document", open("/workspace/work/Spec.docx", vfs.OpenMode{Write: true}), vfs.ErrReadOnly},
		{"a document that does not decode", open("/workspace/work/broken.docx", vfs.OpenMode{}), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.want == nil {
				require.Error(t, c.err)
				return
			}
			require.ErrorIs(t, c.err, c.want)
		})
	}
}

func TestFiles_refusesAFileLargerThanTheLimit(t *testing.T) {
	_, files := filesSession(t)
	ctx := t.Context()
	note, err := files.Open(ctx, "/workspace/work/note.txt", vfs.OpenMode{Write: true})
	require.NoError(t, err)

	_, writeErr := note.WriteAt([]byte("x"), vfs.MaxReadFileBytes)
	truncateErr := note.Truncate(vfs.MaxReadFileBytes + 1)

	require.ErrorIs(t, writeErr, vfs.ErrTooLarge)
	require.ErrorIs(t, truncateErr, vfs.ErrTooLarge)
	require.NoError(t, note.Close(ctx))
}

func TestFiles_reportsAChangeItCannotWriteBack(t *testing.T) {
	_, files := filesSession(t)
	ctx := t.Context()
	note, err := files.Open(ctx, "/workspace/work/gone.txt", vfs.OpenMode{Create: true})
	require.NoError(t, err)
	_, err = note.WriteAt([]byte("x"), 0)
	require.NoError(t, err)
	require.NoError(t, files.Remove(ctx, "/workspace/work/gone.txt"))
	require.NoError(t, files.Mkdir(ctx, "/workspace/work/gone.txt"))

	err = note.Close(ctx)

	require.Error(t, err)
}

// TestFiles_refusesAHostFileItCannotReadAtOffsets: a binary host file that
// only streams cannot be opened for ranged reads, and one the host cannot
// read fails with the host's error.
func TestFiles_refusesAHostFileItCannotReadAtOffsets(t *testing.T) {
	t.Parallel()
	ms := richHostSession(t, richHostFS{
		"stream": {mediaType: "application/octet-stream", size: 3, open: func() io.Reader { return strings.NewReader("abc") }},
		"sealed": {mediaType: "application/octet-stream", size: 3},
	}, nil)
	files := vfs.NewFiles(ms)

	_, streamErr := files.Open(t.Context(), "/workspace/host/stream", vfs.OpenMode{})
	_, sealedErr := files.Open(t.Context(), "/workspace/host/sealed", vfs.OpenMode{})

	require.ErrorIs(t, streamErr, vfs.ErrNotSupported)
	require.Error(t, sealedErr)
	require.NotErrorIs(t, sealedErr, vfs.ErrNotSupported)
}
