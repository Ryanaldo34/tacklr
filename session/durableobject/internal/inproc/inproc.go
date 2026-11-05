// Package inproc serves an http.Handler in this process, without a
// network: the response streams through a pipe as the handler writes it.
// Package dotest delivers requests to objects with it, and package
// cloudflare serves the Worker's requests with it.
package inproc

import (
	"context"
	"io"
	"net/http"
	"sync"
)

// Serve runs h on req and returns its response once the handler set its
// status. The handler's request ends when ctx does or when the caller
// closes the body; once ctx ends, a read of the body is ctx's cause.
func Serve(ctx context.Context, h http.Handler, req *http.Request) *http.Response {
	ctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	stop := context.AfterFunc(ctx, func() { _ = pw.CloseWithError(context.Cause(ctx)) })
	w := &writer{header: http.Header{}, body: pw, ready: make(chan struct{})}
	go func() {
		defer stop()
		h.ServeHTTP(w, req.Clone(ctx))
		w.commit(http.StatusOK)
		// A handler that returned because ctx ended did not finish.
		_ = pw.CloseWithError(context.Cause(ctx))
	}()
	<-w.ready
	return &http.Response{
		Status:        http.StatusText(w.status),
		StatusCode:    w.status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        w.header,
		ContentLength: -1,
		Body:          body{PipeReader: pr, cancel: cancel},
		Request:       req,
	}
}

// writer is an http.ResponseWriter into a pipe: the response is ready once
// its status is set.
type writer struct {
	header http.Header
	body   *io.PipeWriter
	once   sync.Once
	status int
	ready  chan struct{}
}

func (w *writer) Header() http.Header { return w.header }

func (w *writer) WriteHeader(status int) { w.commit(status) }

func (w *writer) Write(b []byte) (int, error) {
	w.commit(http.StatusOK)
	return w.body.Write(b)
}

// Flush implements http.Flusher. The pipe holds nothing: a write returns
// once the reader has it.
func (w *writer) Flush() { w.commit(http.StatusOK) }

func (w *writer) commit(status int) {
	w.once.Do(func() {
		w.status = status
		close(w.ready)
	})
}

// body is a response body that ends its request when closed.
type body struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (b body) Close() error {
	b.cancel()
	return b.PipeReader.Close()
}
