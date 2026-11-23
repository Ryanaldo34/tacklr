package sandbox

import (
	"context"
	"errors"
	"io"
	"path"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/vfs"
)

// workspaceRoot is where a worker's paths start in the vfs: the worker's
// /work/a.txt is /workspace/work/a.txt.
const workspaceRoot = "/workspace"

// maxRead is the most bytes one read reply carries, so the reply fits in a
// frame.
const maxRead = wire.MaxBody / 2

// errnos are the errno names of the vfs errors, in the order they are
// checked. Any other error is EIO.
var errnos = []struct {
	err  error
	name string
}{
	{vfs.ErrNotExist, "ENOENT"},
	{vfs.ErrNotMounted, "ENOENT"},
	{vfs.ErrReadOnly, "EROFS"},
	{vfs.ErrPermission, "EACCES"},
	{vfs.ErrExist, "EEXIST"},
	{vfs.ErrIsDir, "EISDIR"},
	{vfs.ErrNotDir, "ENOTDIR"},
	{vfs.ErrTooLarge, "EFBIG"},
	{vfs.ErrInvalidPath, "EINVAL"},
	{errBadHandle, "EBADF"},
}

// errBadHandle is the error of a request for a handle the run did not open.
var errBadHandle = errors.New("sandbox: no open file has this handle")

func errno(err error) string {
	for _, e := range errnos {
		if errors.Is(err, e.err) {
			return e.name
		}
	}
	return "EIO"
}

// server serves the file requests of one run from its workspace, one at a
// time and in order. The worker is not trusted: a request reaches only the
// workspace, through vfs.Files, and only the files this run opened.
type server struct {
	files vfs.Files
	open  map[uint64]*vfs.OpenFile
	next  uint64
}

// serve answers each file frame of requests with reply, until requests
// closes or a frame is not a valid request. Then it closes the files the
// run left open, which writes their changes back, and returns the error of
// the frame and of those closes.
func (s *server) serve(ctx context.Context, requests <-chan []byte, reply func(wire.FSReply, []byte)) (err error) {
	defer func() {
		for _, open := range s.open {
			err = errors.Join(err, open.Close(ctx))
		}
	}()
	for body := range requests {
		request, data, err := wire.ParseFS(body)
		if err != nil {
			return err
		}
		answer, bytes, err := s.do(ctx, request, data)
		answer.ID = request.ID
		if err != nil {
			answer = wire.FSReply{ID: request.ID, Error: errno(err)}
		}
		reply(answer, bytes)
	}
	return nil
}

func (s *server) do(ctx context.Context, r wire.FSRequest, data []byte) (wire.FSReply, []byte, error) {
	p := path.Join(workspaceRoot, r.Path)
	switch r.Op {
	case "stat":
		st, err := s.files.Stat(ctx, p)
		return wire.FSReply{IsDir: st.IsDir, Size: uint64(max(st.Size, 0)), Modified: uint64(max(st.ModTime.UnixNano(), 0))}, nil, err
	case "read_dir":
		entries, err := s.files.ReadDir(ctx, p)
		reply := wire.FSReply{Entries: make([]wire.FSEntry, 0, len(entries))}
		for _, e := range entries {
			reply.Entries = append(reply.Entries, wire.FSEntry{Name: e.Name, IsDir: e.IsDir})
		}
		return reply, nil, err
	case "mkdir":
		return wire.FSReply{}, nil, s.files.Mkdir(ctx, p)
	case "remove":
		return wire.FSReply{}, nil, s.files.Remove(ctx, p)
	case "rename":
		return wire.FSReply{}, nil, s.files.Rename(ctx, path.Join(workspaceRoot, r.From), path.Join(workspaceRoot, r.To))
	case "open":
		open, err := s.files.Open(ctx, p, vfs.OpenMode{Write: r.Write, Create: r.Create, Truncate: r.Truncate, Append: r.Append})
		if err != nil {
			return wire.FSReply{}, nil, err
		}
		s.next++
		s.open[s.next] = open
		return wire.FSReply{Handle: s.next, Size: uint64(open.Size())}, nil, nil //nolint:gosec // a size is not negative
	}
	open, ok := s.open[r.Handle]
	if !ok {
		return wire.FSReply{}, nil, errBadHandle
	}
	switch r.Op {
	case "read":
		buf := make([]byte, min(r.Length, maxRead))
		n, err := open.ReadAt(buf, int64(min(r.Offset, vfs.MaxReadFileBytes))) //nolint:gosec // capped at MaxReadFileBytes
		if errors.Is(err, io.EOF) {
			err = nil
		}
		return wire.FSReply{}, buf[:n], err
	case "write":
		_, err := open.WriteAt(data, int64(min(r.Offset, vfs.MaxReadFileBytes))) //nolint:gosec // capped at MaxReadFileBytes
		return wire.FSReply{Size: uint64(open.Size())}, nil, err                 //nolint:gosec // a size is not negative
	case "truncate":
		err := open.Truncate(int64(min(r.Size, vfs.MaxReadFileBytes+1))) //nolint:gosec // capped above MaxReadFileBytes
		return wire.FSReply{Size: uint64(open.Size())}, nil, err         //nolint:gosec // a size is not negative
	case "close":
		delete(s.open, r.Handle)
		return wire.FSReply{}, nil, open.Close(ctx)
	}
	return wire.FSReply{}, nil, vfs.ErrNotSupported
}
