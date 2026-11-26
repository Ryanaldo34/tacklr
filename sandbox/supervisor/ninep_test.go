package supervisor

import (
	"archive/zip"
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

// serve9P serves the tree of a local directory, mounted as work with a Word
// document in it, over 9P, and returns the directory and the root of a 9P
// client of it.
func serve9P(t *testing.T) (string, p9.File) {
	t.Helper()
	dir := t.TempDir()
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(dir))).WithCodecs(adapters.Codecs()...)(t.Context(), t.Name(), vfs.Request{})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Spec.docx"), specDocument(t), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "names.txt"), []byte("b\na\n"), 0o600))
	server, conn := net.Pipe()
	served := make(chan struct{})
	go func() {
		defer close(served)
		serveWorkspace(t.Context(), vfs.NewFiles(ms), server)
	}()
	client, err := p9.NewClient(conn)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		<-served
	})
	root, err := client.Attach("")
	require.NoError(t, err)
	return dir, root
}

// specDocument is a Word document of a Title heading and a text paragraph.
func specDocument(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("word/document.xml")
	require.NoError(t, err)
	_, err = io.WriteString(w, `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>`+
		`<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>Title</w:t></w:r></w:p><w:p><w:r><w:t>text</w:t></w:r></w:p></w:body></w:document>`)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// walk walks from f to names and returns the new fid.
func walk(t *testing.T, f p9.File, names ...string) p9.File {
	t.Helper()
	_, file, err := f.Walk(names)
	require.NoError(t, err)
	return file
}

// read opens f for reading and returns all of it.
func read(t *testing.T, f p9.File) string {
	t.Helper()
	_, _, err := f.Open(p9.ReadOnly)
	require.NoError(t, err)
	buf := make([]byte, 4096)
	n, err := f.ReadAt(buf, 0)
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	return string(buf[:n])
}

func TestWorkspaceReadsAFileAndListsADirectory(t *testing.T) {
	_, root := serve9P(t)

	names := read(t, walk(t, root, "work", "names.txt"))
	document := read(t, walk(t, root, "work", "Spec.docx"))
	_, _, attr, attrErr := walk(t, root, "work", "names.txt").GetAttr(p9.AttrMask{Size: true})
	dir := walk(t, root, "work")
	_, _, openErr := dir.Open(p9.ReadOnly)
	entries, readdirErr := dir.Readdir(0, 4096)
	_, dirReadErr := dir.ReadAt(make([]byte, 1), 0)
	stat, statErr := root.StatFS()

	require.Equal(t, "b\na\n", names)
	require.Contains(t, document, "Title")
	require.NoError(t, attrErr)
	require.Equal(t, uint64(4), attr.Size)
	require.Equal(t, uint32(guestUID), uint32(attr.UID))
	require.NoError(t, openErr)
	require.NoError(t, readdirErr)
	var listed []string
	for _, e := range entries {
		listed = append(listed, e.Name)
	}
	require.ElementsMatch(t, []string{"Spec.docx", "names.txt"}, listed)
	require.ErrorIs(t, dirReadErr, linux.EISDIR)
	require.NoError(t, statErr)
	require.Equal(t, uint32(v9fsMagic), stat.Type)
}

func TestWorkspaceWritesBackAFileThatItsFidsShare(t *testing.T) {
	dir, root := serve9P(t)
	created, _, _, err := walk(t, root, "work").Create("new.txt", p9.WriteOnly, 0o644, guestUID, guestGID)
	require.NoError(t, err)
	_, err = created.WriteAt([]byte("hello world"), 0)
	require.NoError(t, err)
	again := walk(t, root, "work", "new.txt")
	_, _, err = again.Open(p9.ReadWrite)
	require.NoError(t, err)

	_, writeErr := again.WriteAt([]byte("HELLO"), 0)
	truncateErr := walk(t, root, "work", "new.txt").SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: 5})
	_, _, attr, attrErr := walk(t, root, "work", "new.txt").GetAttr(p9.AttrMask{Size: true})
	head := make([]byte, 2)
	_, headErr := again.ReadAt(head, 0)
	syncErr := again.FSync()
	timesErr := again.SetAttr(p9.SetAttrMask{MTime: true}, p9.SetAttr{MTimeSeconds: 1})
	require.NoError(t, created.Close())
	require.NoError(t, again.Close())

	require.NoError(t, writeErr)
	require.NoError(t, truncateErr)
	require.NoError(t, attrErr)
	require.Equal(t, uint64(5), attr.Size)
	require.NoError(t, headErr)
	require.Equal(t, "HE", string(head))
	require.NoError(t, syncErr)
	require.NoError(t, timesErr)
	written, err := os.ReadFile(filepath.Join(dir, "new.txt"))
	require.NoError(t, err)
	require.Equal(t, "HELLO", string(written))
}

func TestWorkspaceMakesMovesAndRemovesFiles(t *testing.T) {
	dir, root := serve9P(t)
	work := walk(t, root, "work")

	_, mkdirErr := work.Mkdir("out", 0o755, guestUID, guestGID)
	renameErr := work.RenameAt("names.txt", walk(t, root, "work", "out"), "moved.txt")
	removeErr := walk(t, root, "work").UnlinkAt("Spec.docx", 0)

	require.NoError(t, mkdirErr)
	require.NoError(t, renameErr)
	require.NoError(t, removeErr)
	moved, err := os.ReadFile(filepath.Join(dir, "out", "moved.txt"))
	require.NoError(t, err)
	require.Equal(t, "b\na\n", string(moved))
	require.NoFileExists(t, filepath.Join(dir, "names.txt"))
	require.NoFileExists(t, filepath.Join(dir, "Spec.docx"))
}

func TestWorkspaceRefusesWhatTheTreeCannotDo(t *testing.T) {
	dir, root := serve9P(t)
	work := walk(t, root, "work")

	_, _, missingErr := root.Walk([]string{"work", "missing.txt"})
	_, _, documentErr := walk(t, root, "work", "Spec.docx").Open(p9.WriteOnly)
	documentTruncateErr := walk(t, root, "work", "Spec.docx").SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{})
	rootFile, _, _, rootCreateErr := walk(t, root).Create("top.txt", p9.WriteOnly, 0o644, guestUID, guestGID)
	_, rootMkdirErr := root.Mkdir("top", 0o755, guestUID, guestGID)
	renameErr := work.RenameAt("missing.txt", walk(t, root, "work"), "other.txt")
	unlinkErr := work.UnlinkAt("missing.txt", 0)
	_, symlinkErr := work.Symlink("names.txt", "link", guestUID, guestGID)
	reader := walk(t, root, "work", "names.txt")
	_, _, err := reader.Open(p9.ReadOnly)
	require.NoError(t, err)
	status, lockErr := (&node{}).Lock(1, p9.WriteLock, 0, 0, 0, "guest")
	syncErr := reader.FSync()
	gone := walk(t, root, "work", "names.txt")
	require.NoError(t, os.Remove(filepath.Join(dir, "names.txt")))
	_, _, goneAttr, goneAttrErr := gone.GetAttr(p9.AttrMask{Size: true})
	_, _, goneOpenErr := gone.Open(p9.ReadOnly)

	require.ErrorIs(t, missingErr, linux.ENOENT)
	require.ErrorIs(t, documentErr, linux.EROFS)
	require.ErrorIs(t, documentTruncateErr, linux.EROFS)
	require.Nil(t, rootFile)
	require.ErrorIs(t, rootCreateErr, linux.ENOENT)
	require.ErrorIs(t, rootMkdirErr, linux.ENOTSUP)
	require.ErrorIs(t, renameErr, linux.ENOENT)
	require.ErrorIs(t, unlinkErr, linux.ENOENT)
	require.ErrorIs(t, symlinkErr, linux.ENOSYS)
	require.NoError(t, lockErr)
	require.Equal(t, p9.LockStatusOK, status)
	require.NoError(t, syncErr)
	require.Zero(t, goneAttr)
	require.ErrorIs(t, goneAttrErr, linux.ENOENT)
	require.ErrorIs(t, goneOpenErr, linux.ENOENT)
}

func TestErrnoOfAnErrorTheTreeDoesNotNameIsEIO(t *testing.T) {
	require.Equal(t, linux.EIO, errno(io.ErrUnexpectedEOF))
	require.Equal(t, linux.ENOTSUP, errno(vfs.ErrNotSupported))
}

func TestWorkspaceOfATreeWithNoMountsIsAnEmptyDirectory(t *testing.T) {
	ms, err := vfs.Tree()(t.Context(), t.Name(), vfs.Request{})
	require.NoError(t, err)
	server, conn := net.Pipe()
	go serveWorkspace(t.Context(), vfs.NewFiles(ms), server)
	client, err := p9.NewClient(conn)
	require.NoError(t, err)
	defer client.Close()
	root, err := client.Attach("")
	require.NoError(t, err)

	qid, _, attr, attrErr := root.GetAttr(p9.AttrMask{Mode: true})
	_, _, openErr := root.Open(p9.ReadOnly)
	entries, readdirErr := root.Readdir(0, 4096)

	require.NoError(t, attrErr)
	require.Equal(t, p9.TypeDir, qid.Type)
	require.True(t, attr.Mode.IsDir())
	require.NoError(t, openErr)
	require.NoError(t, readdirErr)
	require.Empty(t, entries)
}
