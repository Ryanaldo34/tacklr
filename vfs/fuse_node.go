package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// FuseAvailable reports whether this process can mount a FUSE tree.
// Probes /dev/fuse and /dev/macfuse* only (not /dev/osxfuse*).
func FuseAvailable() bool {
	if _, err := os.Stat("/dev/fuse"); err == nil {
		return true
	}
	matches, err := filepath.Glob("/dev/macfuse*")
	return err == nil && len(matches) > 0
}

// FuseMount projects the session as a host tree at dir.
// If ReadText succeeds (Textual), the kernel sees that plaintext: size and
// Read use the projection. Kernel writes stay EROFS unless kernelWritable
// (IdentityCodec). Otherwise Open uses Stat + ReaderAt (binaries).
// session.Mount attaches a provider; FuseMount is the host kernel mount.
// Every live Specs() point must be a single path segment (/work, /engram).
func (m *MountSession) FuseMount(dir string) error {
	if dir == "" {
		return errors.New("vfs: fuse mountpoint required")
	}
	for _, spec := range m.Specs() {
		name := strings.TrimPrefix(spec.Point, "/")
		if name == "" || strings.Contains(name, "/") {
			return fmt.Errorf("vfs: fuse requires single-segment mount points (got %q); use /work and /engram", spec.Point)
		}
	}
	m.mu.Lock()
	if m.fuse != nil && m.hostDir == dir {
		m.mu.Unlock()
		return nil
	}
	old := m.fuse
	m.fuse = nil
	m.hostDir = ""
	m.mu.Unlock()
	if old != nil {
		_ = old.Unmount()
	}
	zero := time.Duration(0)
	owner := currentOwner()
	srv, err := fusefs.Mount(dir, &fuseNode{sess: m, path: "/"}, &fusefs.Options{
		MountOptions:    gofuse.MountOptions{FsName: "tacklr", Name: "tacklr"},
		UID:             owner.Uid,
		GID:             owner.Gid,
		EntryTimeout:    &zero,
		AttrTimeout:     &zero,
		NegativeTimeout: &zero,
	})
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.fuse = srv
	m.hostDir = dir
	m.mu.Unlock()
	return nil
}

// Close unmounts the host FUSE tree.
func (m *MountSession) Close() error {
	m.mu.Lock()
	srv := m.fuse
	m.fuse = nil
	m.hostDir = ""
	m.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Unmount()
}

type fuseNode struct {
	fusefs.Inode
	sess *MountSession
	path string
}

func (n *fuseNode) childPath(name string) string {
	if n.path == "/" {
		return "/" + name
	}
	return path.Join(n.path, name)
}

func (n *fuseNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	p := n.childPath(name)
	st, err := n.stat(ctx, p)
	if err != nil {
		return nil, fuseErrno(err)
	}
	child := n.NewInode(ctx, &fuseNode{sess: n.sess, path: p}, fuseStable(st))
	fillFuseAttr(&out.Attr, st)
	return child, 0
}

func (n *fuseNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	var ents []DirEntry
	var err error
	if n.path == "/" {
		specs := n.sess.Specs()
		ents = make([]DirEntry, 0, len(specs))
		for _, spec := range specs {
			name := strings.TrimPrefix(spec.Point, "/")
			if name == "" || strings.Contains(name, "/") {
				continue
			}
			ents = append(ents, DirEntry{Name: name, IsDir: true})
		}
	} else {
		ents, err = n.sess.ReadDir(ctx, n.path)
	}
	if err != nil {
		return nil, fuseErrno(err)
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

func (n *fuseNode) Getattr(ctx context.Context, f fusefs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	if fh, ok := f.(*fuseFile); ok {
		return fh.getattr(out)
	}
	st, err := n.stat(ctx, n.path)
	if err != nil {
		return fuseErrno(err)
	}
	fillFuseAttr(&out.Attr, st)
	return 0
}

func (n *fuseNode) Open(ctx context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	st, err := n.stat(ctx, n.path)
	if err != nil {
		return nil, 0, fuseErrno(err)
	}
	if st.IsDir {
		return nil, 0, syscall.EISDIR
	}
	wantWrite := flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_TRUNC) != 0
	if wantWrite && !kernelWritableFile(st) {
		return nil, 0, syscall.EROFS
	}
	f, errno := openFuseFile(ctx, n.sess, n.path, st, wantWrite, flags&syscall.O_TRUNC != 0, flags&syscall.O_APPEND != 0)
	if errno != 0 {
		return nil, 0, errno
	}
	return f, 0, 0
}

func (n *fuseNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*fusefs.Inode, fusefs.FileHandle, uint32, syscall.Errno) {
	if n.path == "/" {
		return nil, nil, 0, syscall.EPERM
	}
	p := n.childPath(name)
	if !kernelCreateOK(name) {
		return nil, nil, 0, syscall.EROFS
	}
	if err := n.sess.WriteFile(ctx, p, nil); err != nil {
		return nil, nil, 0, fuseErrno(err)
	}
	st, err := n.stat(ctx, p)
	if err != nil {
		return nil, nil, 0, fuseErrno(err)
	}
	fh, errno := openFuseFile(ctx, n.sess, p, st, true, flags&syscall.O_TRUNC != 0, flags&syscall.O_APPEND != 0)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	child := n.NewInode(ctx, &fuseNode{sess: n.sess, path: p}, fuseStable(st))
	fillFuseAttr(&out.Attr, st)
	return child, fh, 0, 0
}

func (n *fuseNode) Mkdir(ctx context.Context, name string, _ uint32, out *gofuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	if n.path == "/" {
		return nil, syscall.EPERM
	}
	p := n.childPath(name)
	if err := n.sess.MkdirAll(ctx, p); err != nil {
		return nil, fuseErrno(err)
	}
	st, err := n.stat(ctx, p)
	if err != nil {
		return nil, fuseErrno(err)
	}
	child := n.NewInode(ctx, &fuseNode{sess: n.sess, path: p}, fuseStable(st))
	fillFuseAttr(&out.Attr, st)
	return child, 0
}

func (n *fuseNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.path == "/" {
		return syscall.EPERM
	}
	return fuseErrno(n.sess.Remove(ctx, n.childPath(name)))
}

func (n *fuseNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.Unlink(ctx, name)
}

func (n *fuseNode) Rename(ctx context.Context, name string, newParent fusefs.InodeEmbedder, newName string, _ uint32) syscall.Errno {
	np, ok := newParent.(*fuseNode)
	if !ok {
		return syscall.EIO
	}
	if n.path == "/" || np.path == "/" {
		return syscall.EPERM
	}
	src := n.childPath(name)
	dst := np.childPath(newName)
	data, err := n.sess.ReadFile(ctx, src)
	if err != nil {
		return fuseErrno(err)
	}
	if err := n.sess.WriteFile(ctx, dst, data); err != nil {
		return fuseErrno(err)
	}
	return fuseErrno(n.sess.Remove(ctx, src))
}

func (n *fuseNode) Setattr(ctx context.Context, f fusefs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if fh, ok := f.(*fuseFile); ok {
		return fh.setattr(ctx, in, out)
	}
	sz, ok := in.GetSize()
	if !ok {
		return n.Getattr(ctx, nil, out)
	}
	st, err := n.stat(ctx, n.path)
	if err != nil {
		return fuseErrno(err)
	}
	if !kernelWritableFile(st) {
		return syscall.EROFS
	}
	if sz > uint64(MaxReadFileBytes) {
		return syscall.EFBIG
	}
	body, err := fusePlaintext(ctx, n.sess, n.path)
	if err != nil {
		return fuseErrno(err)
	}
	b := []byte(body)
	nsize := int(sz)
	if nsize < len(b) {
		b = b[:nsize]
	}
	if err := n.sess.WriteFile(ctx, n.path, b); err != nil {
		return fuseErrno(err)
	}
	st.Size = int64(len(b))
	fillFuseAttr(&out.Attr, st)
	return 0
}

func (n *fuseNode) stat(ctx context.Context, virtualPath string) (FileInfo, error) {
	if virtualPath == "/" {
		return FileInfo{Name: "/", IsDir: true}, nil
	}
	st, err := n.sess.Stat(ctx, virtualPath)
	if err != nil {
		return FileInfo{}, err
	}
	if st.IsDir {
		return st, nil
	}
	if st.Size == 0 && !kernelWritable(st.MediaType) {
		return st, nil
	}
	t, err := n.sess.ReadText(ctx, virtualPath)
	if err == nil {
		st.Size = int64(len(t.Text()))
		return st, nil
	}
	if errors.Is(err, ErrNoCodec) || errors.Is(err, ErrNotTextual) {
		return st, nil
	}
	return FileInfo{}, err
}

// fuseFile is one kernel open. Text is a plaintext snapshot; binaries keep ReaderAt.
type fuseFile struct {
	mu       sync.Mutex
	sess     *MountSession
	path     string
	body     []byte
	bin      File
	ra       io.ReaderAt
	dirty    bool
	writable bool
	oappend  bool
}

func openFuseFile(ctx context.Context, sess *MountSession, virtualPath string, st FileInfo, writable, trunc, oappend bool) (*fuseFile, syscall.Errno) {
	if st.IsDir {
		return nil, syscall.EISDIR
	}
	if !trunc {
		plain, err := fusePlaintext(ctx, sess, virtualPath)
		if err == nil {
			return &fuseFile{sess: sess, path: virtualPath, body: []byte(plain), writable: writable, oappend: oappend}, 0
		}
		if !errors.Is(err, ErrNoCodec) && !errors.Is(err, ErrNotTextual) && !errors.Is(err, ErrNotExist) {
			return nil, fuseErrno(err)
		}
	} else if writable {
		return &fuseFile{sess: sess, path: virtualPath, writable: true, oappend: oappend}, 0
	}
	h, err := sess.Open(ctx, virtualPath)
	if err != nil {
		return nil, fuseErrno(err)
	}
	ra, ok := h.(io.ReaderAt)
	if !ok {
		_ = h.Close()
		return nil, syscall.EIO
	}
	return &fuseFile{bin: h, ra: ra}, 0
}

func fusePlaintext(ctx context.Context, sess *MountSession, virtualPath string) (string, error) {
	t, err := sess.ReadText(ctx, virtualPath)
	if err != nil {
		return "", err
	}
	return t.Text(), nil
}

func (f *fuseFile) Read(_ context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	if off < 0 {
		return nil, syscall.EINVAL
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ra != nil {
		n, err := f.ra.ReadAt(dest, off)
		if err != nil && err != io.EOF {
			return nil, fuseErrno(err)
		}
		return gofuse.ReadResultData(dest[:n]), 0
	}
	if off >= int64(len(f.body)) {
		return gofuse.ReadResultData(nil), 0
	}
	n := copy(dest, f.body[off:])
	return gofuse.ReadResultData(dest[:n]), 0
}

func (f *fuseFile) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.writable {
		return 0, syscall.EROFS
	}
	if f.oappend {
		off = int64(len(f.body))
	}
	if off < 0 {
		return 0, syscall.EINVAL
	}
	end := int(off) + len(data)
	if end > MaxReadFileBytes || off > int64(MaxReadFileBytes) {
		return 0, syscall.EFBIG
	}
	if end > len(f.body) {
		next := make([]byte, end)
		copy(next, f.body)
		f.body = next
	}
	copy(f.body[off:], data)
	f.dirty = true
	return uint32(len(data)), 0 //nolint:gosec // G115: Write is capped at MaxReadFileBytes
}

func (f *fuseFile) Flush(ctx context.Context) syscall.Errno {
	return f.persist(ctx)
}

func (f *fuseFile) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	return f.persist(ctx)
}

func (f *fuseFile) persist(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirty {
		return 0
	}
	if err := f.sess.WriteFile(ctx, f.path, f.body); err != nil {
		return fuseErrno(err)
	}
	f.dirty = false
	return 0
}

func (f *fuseFile) setattr(ctx context.Context, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		f.mu.Lock()
		if !f.writable {
			f.mu.Unlock()
			return syscall.EROFS
		}
		if sz > uint64(MaxReadFileBytes) {
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

func (f *fuseFile) getattr(out *gofuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := FileInfo{Name: path.Base(f.path), Size: int64(len(f.body)), MediaType: "text/plain"}
	if f.ra != nil {
		if fi, err := f.bin.Stat(); err == nil {
			st = fi
		}
	} else if f.writable {
		st.MediaType = "text/plain"
	}
	fillFuseAttr(&out.Attr, st)
	if f.ra == nil {
		out.Size = uint64(len(f.body))
	}
	return 0
}

func (f *fuseFile) Release(ctx context.Context) syscall.Errno {
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

func fuseStable(st FileInfo) fusefs.StableAttr {
	mode := uint32(gofuse.S_IFREG)
	if st.IsDir {
		mode = gofuse.S_IFDIR
	}
	return fusefs.StableAttr{Mode: mode}
}

func fuseErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, ErrNotExist), errors.Is(err, ErrNotMounted):
		return syscall.ENOENT
	case errors.Is(err, ErrReadOnly):
		return syscall.EROFS
	case errors.Is(err, ErrPermission):
		return syscall.EACCES
	case errors.Is(err, ErrExist):
		return syscall.EEXIST
	case errors.Is(err, ErrTooLarge):
		return syscall.EFBIG
	case errors.Is(err, ErrInvalidPath):
		return syscall.EINVAL
	default:
		return syscall.EIO
	}
}

func fillFuseAttr(out *gofuse.Attr, st FileInfo) {
	if st.IsDir {
		out.Mode = gofuse.S_IFDIR | 0755
	} else if kernelWritableFile(st) {
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
	u, g := os.Getuid(), os.Getgid()
	if u < 0 {
		u = 0
	}
	if g < 0 {
		g = 0
	}
	return gofuse.Owner{Uid: uint32(u), Gid: uint32(g)} //nolint:gosec // G115: POSIX uid/gid fit uint32
}

var (
	_ fusefs.NodeLookuper  = (*fuseNode)(nil)
	_ fusefs.NodeReaddirer = (*fuseNode)(nil)
	_ fusefs.NodeGetattrer = (*fuseNode)(nil)
	_ fusefs.NodeOpener    = (*fuseNode)(nil)
	_ fusefs.NodeCreater   = (*fuseNode)(nil)
	_ fusefs.NodeMkdirer   = (*fuseNode)(nil)
	_ fusefs.NodeUnlinker  = (*fuseNode)(nil)
	_ fusefs.NodeRmdirer   = (*fuseNode)(nil)
	_ fusefs.NodeRenamer   = (*fuseNode)(nil)
	_ fusefs.NodeSetattrer = (*fuseNode)(nil)
	_ fusefs.FileReader    = (*fuseFile)(nil)
	_ fusefs.FileWriter    = (*fuseFile)(nil)
	_ fusefs.FileFlusher   = (*fuseFile)(nil)
	_ fusefs.FileFsyncer   = (*fuseFile)(nil)
	_ fusefs.FileReleaser  = (*fuseFile)(nil)
)
