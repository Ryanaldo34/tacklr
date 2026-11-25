package supervisor

import (
	"context"
	"errors"
	"io"
	"path"
	"sync"

	"github.com/hugelgupf/p9/fsimpl/readdir"
	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"

	"github.com/ryanaldo34/tacklr/vfs"
)

// workspaceRoot is where the guest's /workspace starts in the vfs: the
// guest's /workspace/work/a.txt is /workspace/work/a.txt of the tree.
const workspaceRoot = "/workspace"

// guestUID and guestGID own every file of /workspace in the guest: the
// agent user that runs scripts.
const (
	guestUID = 1000
	guestGID = 1000
)

// v9fsMagic is the file system type that statfs reports for a 9P mount.
const v9fsMagic = 0x01021997

// errnos are the errnos of the vfs errors, in the order they are checked.
// Any other error is EIO.
var errnos = []struct {
	err   error
	errno linux.Errno
}{
	{vfs.ErrNotExist, linux.ENOENT},
	{vfs.ErrNotMounted, linux.ENOENT},
	{vfs.ErrReadOnly, linux.EROFS},
	{vfs.ErrPermission, linux.EACCES},
	{vfs.ErrExist, linux.EEXIST},
	{vfs.ErrIsDir, linux.EISDIR},
	{vfs.ErrNotDir, linux.ENOTDIR},
	{vfs.ErrTooLarge, linux.EFBIG},
	{vfs.ErrInvalidPath, linux.EINVAL},
	{vfs.ErrNotSupported, linux.ENOTSUP},
}

// errno is the errno of a vfs error, or nil.
func errno(err error) error {
	if err == nil {
		return nil
	}
	for _, e := range errnos {
		if errors.Is(err, e.err) {
			return e.errno
		}
	}
	return linux.EIO
}

// workspace serves one guest's /workspace over 9P2000.L from its session's
// tree, through vfs.Files, so the guest reaches only the tree. Every file
// is the agent user's. The fids that write one file share one open file:
// a truncate and the writes of each fid change the same bytes, and the
// last close writes them back.
type workspace struct {
	ctx   context.Context
	files vfs.Files

	// mu guards paths and writers.
	mu      sync.Mutex
	paths   map[string]uint64
	writers map[string]*writer
}

// writer is the open file that the writing fids of one path share.
type writer struct {
	file *vfs.OpenFile
	refs int
}

// newWorkspace serves files. ctx is the context of each vfs call.
func newWorkspace(ctx context.Context, files vfs.Files) *workspace {
	return &workspace{ctx: ctx, files: files, paths: map[string]uint64{}, writers: map[string]*writer{}}
}

// Attach returns the root of /workspace.
func (w *workspace) Attach() (p9.File, error) {
	return &node{w: w, path: "/"}, nil
}

// qid is the QID of the file at p: one path number for each path.
func (w *workspace) qid(p string, dir bool) p9.QID {
	w.mu.Lock()
	defer w.mu.Unlock()
	id, ok := w.paths[p]
	if !ok {
		id = uint64(len(w.paths)) + 1
		w.paths[p] = id
	}
	if dir {
		return p9.QID{Type: p9.TypeDir, Path: id}
	}
	return p9.QID{Type: p9.TypeRegular, Path: id}
}

// stat is the vfs stat of the guest path p, with the size of its writer
// when one is open. The root is a directory, also in a tree with no
// mounts.
func (w *workspace) stat(p string) (vfs.FileInfo, error) {
	if p == "/" {
		return vfs.FileInfo{Name: "/", IsDir: true}, nil
	}
	st, err := w.files.Stat(w.ctx, path.Join(workspaceRoot, p))
	if err != nil {
		return vfs.FileInfo{}, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if open := w.writers[p]; open != nil {
		st.Size = open.file.Size()
	}
	return st, nil
}

// openWriter opens the writer of p, or takes another reference on it.
func (w *workspace) openWriter(p string, create bool) (*writer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if open := w.writers[p]; open != nil {
		open.refs++
		return open, nil
	}
	file, err := w.files.Open(w.ctx, path.Join(workspaceRoot, p), vfs.OpenMode{Write: true, Create: create})
	if err != nil {
		return nil, err
	}
	open := &writer{file: file, refs: 1}
	w.writers[p] = open
	return open, nil
}

// release drops a reference on the writer of p. The last one writes the
// file back.
func (w *workspace) release(p string, open *writer) error {
	w.mu.Lock()
	open.refs--
	last := open.refs == 0
	if last {
		delete(w.writers, p)
	}
	w.mu.Unlock()
	if !last {
		return nil
	}
	return open.file.Close(w.ctx)
}

// node is one fid of /workspace: a path, and what the fid opened.
type node struct {
	templatefs.NotImplementedFile
	templatefs.NoopRenamed

	w    *workspace
	path string

	read    *vfs.OpenFile
	write   *writer
	entries []vfs.DirEntry
}

var _ p9.File = (*node)(nil)

// child is the guest path of name in n.
func (n *node) child(name string) string {
	return path.Join(n.path, name)
}

func (n *node) Walk(names []string) ([]p9.QID, p9.File, error) {
	p := n.path
	qids := make([]p9.QID, 0, len(names))
	for _, name := range names {
		p = path.Join(p, name)
		st, err := n.w.stat(p)
		if err != nil {
			return nil, nil, errno(err)
		}
		qids = append(qids, n.w.qid(p, st.IsDir))
	}
	return qids, &node{w: n.w, path: p}, nil
}

func (n *node) StatFS() (p9.FSStat, error) {
	return p9.FSStat{Type: v9fsMagic, BlockSize: 4096, Blocks: 1 << 30, BlocksFree: 1 << 30, BlocksAvailable: 1 << 30, Files: 1 << 30, FilesFree: 1 << 30, NameLength: 255}, nil
}

func (n *node) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	st, err := n.w.stat(n.path)
	if err != nil {
		return p9.QID{}, p9.AttrMask{}, p9.Attr{}, errno(err)
	}
	attr := p9.Attr{
		Mode:      p9.ModeRegular | 0o644,
		UID:       guestUID,
		GID:       guestGID,
		NLink:     1,
		Size:      uint64(max(st.Size, 0)),
		BlockSize: 4096,
		Blocks:    uint64(max(st.Size, 0)+511) / 512,
	}
	if st.IsDir {
		attr.Mode, attr.NLink, attr.Size, attr.Blocks = p9.ModeDirectory|0o755, 2, 0, 0
	}
	if !st.ModTime.IsZero() {
		sec, nsec := uint64(max(st.ModTime.Unix(), 0)), uint64(st.ModTime.Nanosecond()) //nolint:gosec // a nanosecond is not negative
		attr.MTimeSeconds, attr.MTimeNanoSeconds = sec, nsec
		attr.ATimeSeconds, attr.ATimeNanoSeconds = sec, nsec
		attr.CTimeSeconds, attr.CTimeNanoSeconds = sec, nsec
	}
	valid := p9.AttrMask{Mode: true, NLink: true, UID: true, GID: true, Size: true, Blocks: true, ATime: true, MTime: true, CTime: true}
	return n.w.qid(n.path, st.IsDir), valid, attr, nil
}

// SetAttr sets the size. It keeps the other attributes as they are: the
// tree has no owners, modes or times to set.
func (n *node) SetAttr(valid p9.SetAttrMask, attr p9.SetAttr) error {
	if !valid.Size {
		return nil
	}
	size := int64(min(attr.Size, vfs.MaxReadFileBytes+1)) //nolint:gosec // capped above MaxReadFileBytes
	open, err := n.w.openWriter(n.path, false)
	if err != nil {
		return errno(err)
	}
	return errno(errors.Join(open.file.Truncate(size), n.w.release(n.path, open)))
}

func (n *node) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	st, err := n.w.stat(n.path)
	if err != nil {
		return p9.QID{}, 0, errno(err)
	}
	qid := n.w.qid(n.path, st.IsDir)
	switch {
	case st.IsDir:
		n.entries, err = n.w.files.ReadDir(n.w.ctx, path.Join(workspaceRoot, n.path))
		if n.path == "/" && errors.Is(err, vfs.ErrNotMounted) {
			n.entries, err = nil, nil
		}
	case mode.Mode() == p9.ReadOnly:
		n.read, err = n.w.files.Open(n.w.ctx, path.Join(workspaceRoot, n.path), vfs.OpenMode{})
	default:
		n.write, err = n.w.openWriter(n.path, false)
	}
	return qid, 0, errno(err)
}

// ReadAt reads from the fid's open file. A directory has no bytes.
func (n *node) ReadAt(p []byte, offset int64) (int, error) {
	file := n.read
	if n.write != nil {
		file = n.write.file
	}
	if file == nil {
		return 0, linux.EISDIR
	}
	count, err := file.ReadAt(p, offset)
	if errors.Is(err, io.EOF) {
		return count, io.EOF
	}
	return count, errno(err)
}

// WriteAt writes to the fid's writer: the p9 package refuses writes to a
// fid that is not open for writing.
func (n *node) WriteAt(p []byte, offset int64) (int, error) {
	count, err := n.write.file.WriteAt(p, offset)
	return count, errno(err)
}

func (n *node) FSync() error {
	if n.write == nil {
		return nil
	}
	return errno(n.write.file.Sync(n.w.ctx))
}

// Lock grants every lock: the guest is the only client of its workspace.
func (n *node) Lock(int, p9.LockType, p9.LockFlags, uint64, uint64, string) (p9.LockStatus, error) {
	return p9.LockStatusOK, nil
}

func (n *node) Create(name string, _ p9.OpenFlags, _ p9.FileMode, _ p9.UID, _ p9.GID) (p9.File, p9.QID, uint32, error) {
	p := n.child(name)
	open, err := n.w.openWriter(p, true)
	if err != nil {
		return nil, p9.QID{}, 0, errno(err)
	}
	return &node{w: n.w, path: p, write: open}, n.w.qid(p, false), 0, nil
}

func (n *node) Mkdir(name string, _ p9.FileMode, _ p9.UID, _ p9.GID) (p9.QID, error) {
	p := n.child(name)
	if err := n.w.files.Mkdir(n.w.ctx, path.Join(workspaceRoot, p)); err != nil {
		return p9.QID{}, errno(err)
	}
	return n.w.qid(p, true), nil
}

// RenameAt moves oldName of n to newName of newDir, another node of this
// workspace: the p9 package passes only the server's own files.
func (n *node) RenameAt(oldName string, newDir p9.File, newName string) error {
	to, _ := newDir.(*node)
	return errno(n.w.files.Rename(n.w.ctx, path.Join(workspaceRoot, n.child(oldName)), path.Join(workspaceRoot, to.child(newName))))
}

func (n *node) UnlinkAt(name string, _ uint32) error {
	return errno(n.w.files.Remove(n.w.ctx, path.Join(workspaceRoot, n.child(name))))
}

func (n *node) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	names := make([]string, 0, len(n.entries))
	qids := make(map[string]p9.QID, len(n.entries))
	for _, e := range n.entries {
		names = append(names, e.Name)
		qids[e.Name] = n.w.qid(n.child(e.Name), e.IsDir)
	}
	return readdir.Readdir(offset, count, names, qids)
}

// Close closes what the fid opened. Closing the last writer of a file
// writes it back.
func (n *node) Close() error {
	switch {
	case n.read != nil:
		return errno(n.read.Close(n.w.ctx))
	case n.write != nil:
		return errno(n.w.release(n.path, n.write))
	}
	return nil
}

// serveWorkspace serves files over 9P on conn until conn ends, then closes
// every file the guest left open, which writes it back.
func serveWorkspace(ctx context.Context, files vfs.Files, conn io.ReadWriteCloser) {
	_ = p9.NewServer(newWorkspace(ctx, files)).Handle(conn, conn)
}
