package vfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
)

// readAhead is the least ReadAt fetches per range request, so sequential
// offset reads (FUSE) cost one request per window instead of one per call.
const readAhead = 1 << 20

// fetchRange opens n bytes of a remote object from off. n < 0 reads to the
// end; callers never pass n == 0.
type fetchRange func(ctx context.Context, off, n int64) (io.ReadCloser, error)

// remoteFile is a read handle on a remote object. Read streams one body,
// opened on the first call. ReadAt serves offset reads from a read-ahead
// window filled by range requests and does not move the Read position.
// FUSE may call ReadAt from several goroutines, so mu guards all state.
type remoteFile struct {
	ctx   context.Context
	info  FileInfo
	fetch fetchRange

	mu     sync.Mutex
	body   io.ReadCloser
	win    []byte
	winOff int64
}

func newRemoteFile(ctx context.Context, info FileInfo, fetch fetchRange) *remoteFile {
	return &remoteFile{ctx: ctx, info: info, fetch: fetch}
}

func (f *remoteFile) Stat() (FileInfo, error) { return f.info, nil }

func (f *remoteFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.body == nil {
		body, err := f.fetch(f.ctx, 0, -1)
		if err != nil {
			return 0, err
		}
		f.body = body
	}
	return f.body.Read(p)
}

func (f *remoteFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("vfs: negative offset %d", off)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for n < len(p) {
		at := off + int64(n)
		if at >= f.info.Size {
			return n, io.EOF
		}
		if at < f.winOff || at >= f.winOff+int64(len(f.win)) {
			if err := f.fill(at, int64(len(p)-n)); err != nil {
				return n, err
			}
		}
		n += copy(p[n:], f.win[at-f.winOff:])
	}
	return n, nil
}

// fill replaces the window with at least want bytes (and at least
// readAhead) from off, capped at the end of the object.
func (f *remoteFile) fill(off, want int64) error {
	size := min(max(want, readAhead), f.info.Size-off)
	f.win = f.win[:0]
	body, err := f.fetch(f.ctx, off, size)
	if err != nil {
		return err
	}
	defer body.Close()
	buf := slices.Grow(f.win, int(size))[:size]
	if _, err := io.ReadFull(body, buf); err != nil {
		return err
	}
	f.win, f.winOff = buf, off
	return nil
}

func (f *remoteFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.body == nil {
		return nil
	}
	return f.body.Close()
}

// rangeHeader is the HTTP Range value for n bytes from off; "" asks for the
// whole object.
func rangeHeader(off, n int64) string {
	switch {
	case n < 0 && off == 0:
		return ""
	case n < 0:
		return fmt.Sprintf("bytes=%d-", off)
	default:
		return fmt.Sprintf("bytes=%d-%d", off, off+n-1)
	}
}

// rangedBody returns resp's body, which must be the range asked for.
func rangedBody(resp *http.Response, off, n int64) (io.ReadCloser, error) {
	if rangeHeader(off, n) != "" && resp.StatusCode != http.StatusPartialContent {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("vfs: range %s answered with HTTP %d", rangeHeader(off, n), resp.StatusCode)
	}
	return resp.Body, nil
}
