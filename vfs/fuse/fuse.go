// Package fuse shows a vfs.MountSession as a directory on the host kernel, so
// host processes (run_command) see the session's virtual tree.
//
// A file whose text a codec can read shows that text: size and Read use it.
// Kernel writes persist through WriteFile only for identity bodies
// (ContentRegistry.Identity); projected types (Word, Docs, Sheets) are read-only on
// the kernel (EROFS). Other files show their bytes through io.ReaderAt.
package fuse

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/ryanaldo34/tacklr/vfs"
)

// Server is a live kernel mount of one MountSession.
type Server struct {
	srv *gofuse.Server
	dir string
}

// Mount shows ms at dir. It creates dir when it is missing and takes down a
// mount a crashed process left there.
func Mount(ms *vfs.MountSession, dir string) (*Server, error) {
	if dir == "" {
		return nil, errors.New("fuse: mount directory required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	_ = syscall.Unmount(dir, 0)
	zero := time.Duration(0)
	owner := currentOwner()
	srv, err := fusefs.Mount(dir, &node{sess: ms, path: "/"}, &fusefs.Options{
		MountOptions:    gofuse.MountOptions{FsName: "tacklr", Name: "tacklr"},
		UID:             owner.Uid,
		GID:             owner.Gid,
		EntryTimeout:    &zero,
		AttrTimeout:     &zero,
		NegativeTimeout: &zero,
	})
	if err != nil {
		return nil, err
	}
	return &Server{srv: srv, dir: dir}, nil
}

// Dir is the host directory the tree is mounted at. Harness tool results,
// errors and checkpoints must never show it.
func (s *Server) Dir() string { return s.dir }

// Close unmounts the tree and removes its directory.
func (s *Server) Close() error {
	if err := s.srv.Unmount(); err != nil {
		return err
	}
	return os.Remove(s.dir)
}

type node struct {
	fusefs.Inode
	sess *vfs.MountSession
	path string
}

func (n *node) childPath(name string) string {
	if n.path == "/" {
		return "/" + name
	}
	return path.Join(n.path, name)
}

func (n *node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	p := n.childPath(name)
	st, err := n.stat(ctx, p)
	if err != nil {
		return nil, toErrno(err)
	}
	child := n.NewInode(ctx, &node{sess: n.sess, path: p}, stable(st))
	fillAttr(&out.Attr, st, n.sess.Codecs())
	return child, 0
}

func (n *node) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	ents, err := n.sess.Route(ctx, n.path).ReadDir(ctx)
	if err != nil {
		return nil, toErrno(err)
	}
	list := make([]gofuse.DirEntry, 0, len(ents))
	for _, e := range ents {
		mode := uint32(gofuse.S_IFREG)
		if e.IsDir {
			mode = uint32(gofuse.S_IFDIR)
		}
		list = append(list, gofuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return fusefs.NewListDirStream(list), 0
}

func (n *node) Getattr(ctx context.Context, f fusefs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	if fh, ok := f.(*file); ok {
		return fh.getattr(out)
	}
	st, err := n.stat(ctx, n.path)
	if err != nil {
		return toErrno(err)
	}
	fillAttr(&out.Attr, st, n.sess.Codecs())
	return 0
}

func (n *node) Open(ctx context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	st, err := n.stat(ctx, n.path)
	if err != nil {
		return nil, 0, toErrno(err)
	}
	if st.IsDir {
		return nil, 0, syscall.EISDIR
	}
	wantWrite := flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_TRUNC) != 0
	if wantWrite && !n.sess.Codecs().Identity(st.MediaType) {
		return nil, 0, syscall.EROFS
	}
	f, errno := openFile(ctx, n.sess, n.path, flags&syscall.O_TRUNC != 0, flags&syscall.O_APPEND != 0)
	if errno != 0 {
		return nil, 0, errno
	}
	return f, 0, 0
}

func (n *node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*fusefs.Inode, fusefs.FileHandle, uint32, syscall.Errno) {
	if n.path == "/" {
		return nil, nil, 0, syscall.EPERM
	}
	p := n.childPath(name)
	if n.sess.Codecs().Projected(vfs.DetectMediaType(name, nil)) {
		return nil, nil, 0, syscall.EROFS
	}

	if err := n.sess.Route(ctx, p).
		WriteFile(ctx, nil); err != nil {
		return nil, nil, 0, toErrno(err)
	}

	st, err := n.stat(ctx, p)
	if err != nil {
		return nil, nil, 0, toErrno(err)
	}
	fh, errno := openFile(ctx, n.sess, p, flags&syscall.O_TRUNC != 0, flags&syscall.O_APPEND != 0)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	child := n.NewInode(ctx, &node{sess: n.sess, path: p}, stable(st))
	fillAttr(&out.Attr, st, n.sess.Codecs())
	return child, fh, 0, 0
}

func (n *node) Mkdir(ctx context.Context, name string, _ uint32, out *gofuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	if n.path == "/" {
		return nil, syscall.EPERM
	}
	p := n.childPath(name)

	if err := n.sess.Route(ctx, p).
		MkdirAll(ctx); err != nil {
		return nil, toErrno(err)
	}

	st, err := n.stat(ctx, p)
	if err != nil {
		return nil, toErrno(err)
	}
	child := n.NewInode(ctx, &node{sess: n.sess, path: p}, stable(st))
	fillAttr(&out.Attr, st, n.sess.Codecs())
	return child, 0
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.path == "/" {
		return syscall.EPERM
	}
	return toErrno(n.sess.Route(ctx, n.childPath(name)).Remove(ctx))
}

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.Unlink(ctx, name)
}

func (n *node) Rename(ctx context.Context, name string, newParent fusefs.InodeEmbedder, newName string, _ uint32) syscall.Errno {
	np := newParent.(*node) //nolint:errcheck // every inode of the tree is a *node
	if n.path == "/" || np.path == "/" {
		return syscall.EPERM
	}
	src := n.childPath(name)
	dst := np.childPath(newName)

	data, err := n.sess.Route(ctx, src).ReadFile(ctx)
	if err != nil {
		return toErrno(err)
	}

	if err := n.sess.Route(ctx, dst).
		WriteFile(ctx, data); err != nil {
		return toErrno(err)
	}

	return toErrno(n.sess.Route(ctx, src).Remove(ctx))
}

func (n *node) Setattr(ctx context.Context, f fusefs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if fh, ok := f.(*file); ok {
		return fh.setattr(ctx, in, out)
	}
	sz, ok := in.GetSize()
	if !ok {
		return n.Getattr(ctx, nil, out)
	}
	st, err := n.stat(ctx, n.path)
	if err != nil {
		return toErrno(err)
	}
	if !n.sess.Codecs().Identity(st.MediaType) {
		return syscall.EROFS
	}
	if sz > uint64(vfs.MaxReadFileBytes) {
		return syscall.EFBIG
	}
	body, err := plaintext(ctx, n.sess, n.path)
	if err != nil {
		return toErrno(err)
	}
	b := []byte(body)
	nsize := int(sz)
	if nsize < len(b) {
		b = b[:nsize]
	}

	if err := n.sess.Route(ctx, n.path).
		WriteFile(ctx, b); err != nil {
		return toErrno(err)
	}

	st.Size = int64(len(b))
	fillAttr(&out.Attr, st, n.sess.Codecs())
	return 0
}

func (n *node) stat(ctx context.Context, virtualPath string) (vfs.FileInfo, error) {
	st, err := n.sess.Route(ctx, virtualPath).Stat(ctx)
	if err != nil {
		return vfs.FileInfo{}, err
	}
	if st.IsDir {
		return st, nil
	}
	if st.Size == 0 && !n.sess.Codecs().Identity(st.MediaType) {
		return st, nil
	}

	t, err := n.sess.Route(ctx, virtualPath).ReadText(ctx)
	if err == nil {
		st.Size = int64(len(t.Text()))
		return st, nil
	}
	if errors.Is(err, vfs.ErrNoCodec) || errors.Is(err, vfs.ErrNotTextual) {
		return st, nil
	}
	return vfs.FileInfo{}, err
}

// file is one kernel open. Text is a plaintext snapshot; binaries keep
// ReaderAt. The kernel sends writes and truncates only to a handle opened
// for writing, and Open refuses those on a file that is not plain text.
type file struct {
	mu      sync.Mutex
	sess    *vfs.MountSession
	path    string
	body    []byte
	bin     vfs.File
	ra      io.ReaderAt
	dirty   bool
	oappend bool
}

// openFile opens virtualPath, a file (callers stat it first). trunc implies
// a write: Open counts O_TRUNC as one, and Create always writes.
func openFile(ctx context.Context, sess *vfs.MountSession, virtualPath string, trunc, oappend bool) (*file, syscall.Errno) {
	if trunc {
		return &file{sess: sess, path: virtualPath, oappend: oappend}, 0
	}
	plain, err := plaintext(ctx, sess, virtualPath)
	if err == nil {
		return &file{sess: sess, path: virtualPath, body: []byte(plain), oappend: oappend}, 0
	}
	if !errors.Is(err, vfs.ErrNoCodec) && !errors.Is(err, vfs.ErrNotTextual) && !errors.Is(err, vfs.ErrNotExist) {
		return nil, toErrno(err)
	}

	h, err := sess.Route(ctx, virtualPath).Open(ctx)
	if err != nil {
		return nil, toErrno(err)
	}
	ra, ok := h.(io.ReaderAt)
	if !ok {
		_ = h.Close()
		return nil, syscall.EIO
	}
	return &file{sess: sess, path: virtualPath, bin: h, ra: ra}, 0
}

func plaintext(ctx context.Context, sess *vfs.MountSession, virtualPath string) (string, error) {
	t, err := sess.Route(ctx, virtualPath).ReadText(ctx)
	if err != nil {
		return "", err
	}
	return t.Text(), nil
}

// Read reads at off. The kernel never sends a negative offset.
func (f *file) Read(_ context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ra != nil {
		n, err := f.ra.ReadAt(dest, off)
		if err != nil && err != io.EOF {
			return nil, toErrno(err)
		}
		return gofuse.ReadResultData(dest[:n]), 0
	}
	if off >= int64(len(f.body)) {
		return gofuse.ReadResultData(nil), 0
	}
	n := copy(dest, f.body[off:])
	return gofuse.ReadResultData(dest[:n]), 0
}

func (f *file) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.oappend {
		off = int64(len(f.body))
	}
	end := int(off) + len(data)
	if end > vfs.MaxReadFileBytes || off > int64(vfs.MaxReadFileBytes) {
		return 0, syscall.EFBIG
	}
	if end > len(f.body) {
		next := make([]byte, end)
		copy(next, f.body)
		f.body = next
	}
	copy(f.body[off:], data)
	f.dirty = true
	return uint32(len(data)), 0 //nolint:gosec // G115: Write is capped at vfs.MaxReadFileBytes
}

func (f *file) Flush(ctx context.Context) syscall.Errno {
	return f.persist(ctx)
}

func (f *file) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	return f.persist(ctx)
}

func (f *file) persist(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirty {
		return 0
	}

	if err := f.sess.Route(ctx, f.path).
		WriteFile(ctx, f.body); err != nil {
		return toErrno(err)
	}

	f.dirty = false
	return 0
}

func (f *file) setattr(ctx context.Context, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		f.mu.Lock()
		if sz > uint64(vfs.MaxReadFileBytes) {
			f.mu.Unlock()
			return syscall.EFBIG
		}
		if int(sz) < len(f.body) {
			f.body = f.body[:sz]
		} else if int(sz) > len(f.body) {
			next := make([]byte, sz)
			copy(next, f.body)
			f.body = next
		}
		f.dirty = true
		f.mu.Unlock()
		if errno := f.persist(ctx); errno != 0 {
			return errno
		}
	}
	return f.getattr(out)
}

func (f *file) getattr(out *gofuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := vfs.FileInfo{Name: path.Base(f.path), Size: int64(len(f.body)), MediaType: "text/plain"}
	if f.ra != nil {
		if fi, err := f.bin.Stat(); err == nil {
			st = fi
		}
	}
	fillAttr(&out.Attr, st, f.sess.Codecs())
	if f.ra == nil {
		out.Size = uint64(len(f.body))
	}
	return 0
}

func (f *file) Release(ctx context.Context) syscall.Errno {
	errno := f.persist(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bin != nil {
		_ = f.bin.Close()
		f.bin = nil
		f.ra = nil
	}
	return errno
}

func stable(st vfs.FileInfo) fusefs.StableAttr {
	mode := uint32(gofuse.S_IFREG)
	if st.IsDir {
		mode = gofuse.S_IFDIR
	}
	return fusefs.StableAttr{Mode: mode}
}

func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, vfs.ErrNotExist), errors.Is(err, vfs.ErrNotMounted):
		return syscall.ENOENT
	case errors.Is(err, vfs.ErrReadOnly):
		return syscall.EROFS
	case errors.Is(err, vfs.ErrPermission):
		return syscall.EACCES
	case errors.Is(err, vfs.ErrExist):
		return syscall.EEXIST
	case errors.Is(err, vfs.ErrTooLarge):
		return syscall.EFBIG
	case errors.Is(err, vfs.ErrInvalidPath):
		return syscall.EINVAL
	default:
		return syscall.EIO
	}
}

func fillAttr(out *gofuse.Attr, st vfs.FileInfo, codecs *vfs.ContentRegistry) {
	if st.IsDir {
		out.Mode = gofuse.S_IFDIR | 0755
	} else if codecs.Identity(st.MediaType) {
		out.Mode = gofuse.S_IFREG | 0644
		if st.Size > 0 {
			out.Size = uint64(st.Size)
		}
	} else {
		out.Mode = gofuse.S_IFREG | 0444
		if st.Size > 0 {
			out.Size = uint64(st.Size)
		}
	}
	if u := st.ModTime.Unix(); u > 0 {
		out.Mtime = uint64(u)
	}
	out.Owner = currentOwner()
}

func currentOwner() gofuse.Owner {
	return gofuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())} //nolint:gosec // G115: Linux uid/gid fit uint32
}

var (
	_ fusefs.NodeLookuper  = (*node)(nil)
	_ fusefs.NodeReaddirer = (*node)(nil)
	_ fusefs.NodeGetattrer = (*node)(nil)
	_ fusefs.NodeOpener    = (*node)(nil)
	_ fusefs.NodeCreater   = (*node)(nil)
	_ fusefs.NodeMkdirer   = (*node)(nil)
	_ fusefs.NodeUnlinker  = (*node)(nil)
	_ fusefs.NodeRmdirer   = (*node)(nil)
	_ fusefs.NodeRenamer   = (*node)(nil)
	_ fusefs.NodeSetattrer = (*node)(nil)
	_ fusefs.FileReader    = (*file)(nil)
	_ fusefs.FileWriter    = (*file)(nil)
	_ fusefs.FileFlusher   = (*file)(nil)
	_ fusefs.FileFsyncer   = (*file)(nil)
	_ fusefs.FileReleaser  = (*file)(nil)
)
