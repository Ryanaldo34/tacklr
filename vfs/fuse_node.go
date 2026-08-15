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

// FuseMount projects the session as a read-only host tree at dir.
// Textual files appear as ReadText (provider IR plaintext). Binaries use Stat + ReadAt.
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
	srv, err := fusefs.Mount(dir, &fuseNode{sess: m, path: "/"}, &fusefs.Options{
		MountOptions:    gofuse.MountOptions{FsName: "tacklr", Name: "tacklr"},
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

func (n *fuseNode) Open(ctx context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	f, errno := openFuseFile(ctx, n.sess, n.path)
	if errno != 0 {
		return nil, 0, errno
	}
	return f, 0, 0
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
	// Binaries: Stat size is the FUSE size. Do not ReadText (that would ReadFile the body).
	if st.MediaType != "" && !IsTextLike(st.MediaType) {
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
	body string
	bin  File
	ra   io.ReaderAt
}

func openFuseFile(ctx context.Context, sess *MountSession, virtualPath string) (*fuseFile, syscall.Errno) {
	st, err := sess.Stat(ctx, virtualPath)
	if err != nil {
		return nil, fuseErrno(err)
	}
	if st.IsDir {
		return nil, syscall.EISDIR
	}
	if st.MediaType == "" || IsTextLike(st.MediaType) {
		body, err := fusePlaintext(ctx, sess, virtualPath)
		if err == nil {
			return &fuseFile{body: body}, 0
		}
		if !errors.Is(err, ErrNoCodec) && !errors.Is(err, ErrNotTextual) {
			return nil, fuseErrno(err)
		}
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

func (f *fuseFile) Release(_ context.Context) syscall.Errno {
	if f.bin != nil {
		_ = f.bin.Close()
		f.bin = nil
		f.ra = nil
	}
	return 0
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
		if st.Size > 0 {
			out.Size = uint64(st.Size)
		}
	}
	if u := st.ModTime.Unix(); u > 0 {
		out.Mtime = uint64(u)
	}
}

var (
	_ fusefs.NodeLookuper  = (*fuseNode)(nil)
	_ fusefs.NodeReaddirer = (*fuseNode)(nil)
	_ fusefs.NodeGetattrer = (*fuseNode)(nil)
	_ fusefs.NodeOpener    = (*fuseNode)(nil)
	_ fusefs.FileReader    = (*fuseFile)(nil)
	_ fusefs.FileReleaser  = (*fuseFile)(nil)
)
