package vfs

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"syscall"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// FuseMount projects the session as a read-only host tree at dir.
// Textual files appear as ReadText (dirty IR plaintext). Binaries use Stat + ReadAt.
// session.Mount attaches a provider; FuseMount is the host kernel mount.
func (m *MountSession) FuseMount(dir string) error {
	if dir == "" {
		return errors.New("vfs: fuse mountpoint required")
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
	srv, err := fusefs.Mount(dir, &fuseNode{sess: m, path: "/"}, &fusefs.Options{
		MountOptions: gofuse.MountOptions{FsName: "tacklr", Name: "tacklr"},
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

func (n *fuseNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	p := n.path
	if p == "/" {
		p = "/" + name
	} else {
		p = path.Join(p, name)
	}
	st, err := n.stat(ctx, p)
	if err != nil {
		return nil, fuseErrno(err)
	}
	mode := uint32(gofuse.S_IFREG)
	if st.IsDir {
		mode = gofuse.S_IFDIR
	}
	child := n.NewInode(ctx, &fuseNode{sess: n.sess, path: p}, fusefs.StableAttr{Mode: mode})
	fillFuseAttr(&out.Attr, st)
	return child, 0
}

func (n *fuseNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	var ents []DirEntry
	var err error
	if n.path == "/" {
		for _, spec := range n.sess.Specs() {
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
			mode = gofuse.S_IFDIR
		}
		list = append(list, gofuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return fusefs.NewListDirStream(list), 0
}

func (n *fuseNode) Getattr(ctx context.Context, _ fusefs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	st, err := n.stat(ctx, n.path)
	if err != nil {
		return fuseErrno(err)
	}
	fillFuseAttr(&out.Attr, st)
	return 0
}

func (n *fuseNode) Open(_ context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return &fuseFile{sess: n.sess, path: n.path}, 0, 0
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

type fuseFile struct {
	sess *MountSession
	path string
}

func (f *fuseFile) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	if off < 0 {
		return nil, syscall.EINVAL
	}
	if t, err := f.sess.ReadText(ctx, f.path); err == nil {
		body := t.Text()
		if off >= int64(len(body)) {
			return gofuse.ReadResultData(nil), 0
		}
		n := copy(dest, body[off:])
		return gofuse.ReadResultData(dest[:n]), 0
	} else if !errors.Is(err, ErrNoCodec) && !errors.Is(err, ErrNotTextual) {
		return nil, fuseErrno(err)
	}
	h, err := f.sess.Open(ctx, f.path)
	if err != nil {
		return nil, fuseErrno(err)
	}
	defer h.Close()
	ra, ok := h.(io.ReaderAt)
	if !ok {
		return nil, syscall.EIO
	}
	n, err := ra.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, fuseErrno(err)
	}
	return gofuse.ReadResultData(dest[:n]), 0
}

func fuseErrno(err error) syscall.Errno {
	if errors.Is(err, ErrNotExist) || errors.Is(err, ErrNotMounted) {
		return syscall.ENOENT
	}
	return syscall.EIO
}

func fillFuseAttr(out *gofuse.Attr, st FileInfo) {
	if st.IsDir {
		out.Mode = gofuse.S_IFDIR | 0555
	} else {
		out.Mode = gofuse.S_IFREG | 0444
		out.Size = uint64(st.Size)
	}
	if !st.ModTime.IsZero() {
		out.Mtime = uint64(st.ModTime.Unix())
	}
}

var (
	_ fusefs.NodeLookuper  = (*fuseNode)(nil)
	_ fusefs.NodeReaddirer = (*fuseNode)(nil)
	_ fusefs.NodeGetattrer = (*fuseNode)(nil)
	_ fusefs.NodeOpener    = (*fuseNode)(nil)
	_ fusefs.FileReader    = (*fuseFile)(nil)
)
