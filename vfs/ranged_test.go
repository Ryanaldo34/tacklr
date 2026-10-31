package vfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

// fetchFrom serves ranges of data and counts the requests.
func fetchFrom(data []byte, requests *atomic.Int32) fetchRange {
	return func(_ context.Context, off, n int64) (io.ReadCloser, error) {
		requests.Add(1)
		end := int64(len(data))
		if n >= 0 {
			end = min(off+n, end)
		}
		return io.NopCloser(bytes.NewReader(data[off:end])), nil
	}
}

func TestRemoteFile_offsetReadsShareTheReadAheadWindowAndReadStreamsTheObject(t *testing.T) {
	data := make([]byte, 3<<20+7)
	for i := range data {
		data[i] = byte(i ^ i>>8 ^ i>>16)
	}
	var requests atomic.Int32
	f := newRemoteFile(t.Context(), FileInfo{Size: int64(len(data))}, fetchFrom(data, &requests))

	// Sequential small reads inside one window cost one request.
	for off := 0; off < readAhead; off += 4096 {
		buf := make([]byte, 4096)
		if n, err := f.ReadAt(buf, int64(off)); err != nil || !bytes.Equal(buf[:n], data[off:off+n]) {
			t.Fatalf("ReadAt(%d) = %d bytes, err=%v", off, n, err)
		}
	}
	windowRequests := requests.Load()
	var wg sync.WaitGroup
	for _, s := range []struct{ off, n int }{{readAhead - 3, 10}, {100, 3 << 19}, {len(data) / 2, 64}} {
		wg.Go(func() {
			buf := make([]byte, s.n)
			if n, err := f.ReadAt(buf, int64(s.off)); err != nil || !bytes.Equal(buf[:n], data[s.off:s.off+s.n]) {
				t.Errorf("ReadAt(%d, %d) = %d bytes, err=%v", s.off, s.n, n, err)
			}
		})
	}
	wg.Wait()
	tail := make([]byte, 10)
	n, tailErr := f.ReadAt(tail, int64(len(data)-5))
	_, negErr := f.ReadAt(tail, -1)
	streamed, readErr := io.ReadAll(f)

	if windowRequests != 1 {
		t.Errorf("%d requests for reads inside one window, want 1", windowRequests)
	}
	if !errors.Is(tailErr, io.EOF) || !bytes.Equal(tail[:n], data[len(data)-5:]) {
		t.Errorf("ReadAt past the end = %d bytes, err=%v", n, tailErr)
	}
	if negErr == nil {
		t.Error("ReadAt at a negative offset succeeded")
	}
	if readErr != nil || !bytes.Equal(streamed, data) || f.Close() != nil {
		t.Errorf("streaming Read = %d bytes, err=%v", len(streamed), readErr)
	}
}

func TestRemoteFile_readsFailWhenTheBodyIsShortOrTheFetchFails(t *testing.T) {
	var requests atomic.Int32
	short := newRemoteFile(t.Context(), FileInfo{Size: 10}, fetchFrom([]byte("abcd"), &requests))
	down := newRemoteFile(t.Context(), FileInfo{Size: 10}, func(context.Context, int64, int64) (io.ReadCloser, error) {
		return nil, ErrAuthExpired
	})

	_, shortErr := short.ReadAt(make([]byte, 10), 0)
	_, downAtErr := down.ReadAt(make([]byte, 1), 0)
	_, downErr := down.Read(make([]byte, 1))

	if !errors.Is(shortErr, io.ErrUnexpectedEOF) {
		t.Errorf("ReadAt over a short body = %v, want ErrUnexpectedEOF", shortErr)
	}
	if !errors.Is(downAtErr, ErrAuthExpired) || !errors.Is(downErr, ErrAuthExpired) {
		t.Errorf("reads after a failed fetch = %v, %v, want the fetch error", downAtErr, downErr)
	}
	if down.Close() != nil {
		t.Error("Close of a never-opened handle failed")
	}
}
