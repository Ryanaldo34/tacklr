//go:build js && wasm

package cloudflare

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"syscall/js"

	"github.com/ryanaldo34/tacklr/session/durableobject/internal/inproc"
)

var (
	jsHeaders         = js.Global().Get("Headers")
	jsAbortController = js.Global().Get("AbortController")
)

// stubTransport sends each request through one Durable Object stub's
// fetch, as Go's own js transport sends through the global fetch.
type stubTransport struct{ stub js.Value }

func (t stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	init := jsObject.New()
	init.Set("method", req.Method)
	headers := jsHeaders.New()
	for k, vs := range req.Header {
		for _, v := range vs {
			headers.Call("append", k, v)
		}
	}
	init.Set("headers", headers)
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(b) > 0 {
			init.Set("body", bytesToJS(b))
		}
	}
	abort := jsAbortController.New()
	init.Set("signal", abort.Get("signal"))
	p, err := try(func() js.Value { return t.stub.Call("fetch", req.URL.String(), init) })
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(req.Context(), func() { abort.Call("abort") })
	res, err := await(p)
	if err != nil {
		stop()
		if ctxErr := req.Context().Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("cloudflare: object fetch: %w", err)
	}
	header := http.Header{}
	entries := res.Get("headers").Call("entries")
	for {
		next := entries.Call("next")
		if next.Get("done").Bool() {
			break
		}
		pair := next.Get("value")
		header.Add(pair.Index(0).String(), pair.Index(1).String())
	}
	var body io.ReadCloser = http.NoBody
	if b := res.Get("body"); !b.IsNull() && !b.IsUndefined() {
		body = &streamReader{reader: b.Call("getReader"), stop: stop}
	} else {
		stop()
	}
	code := res.Get("status").Int()
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", code, http.StatusText(code)),
		StatusCode:    code,
		Header:        header,
		ContentLength: -1,
		Body:          body,
		Request:       req,
	}, nil
}

// streamReader reads a ReadableStream's default reader.
type streamReader struct {
	reader  js.Value
	stop    func() bool
	pending []byte
	err     error
}

var errReaderClosed = errors.New("cloudflare: body closed")

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		res, err := await(r.reader.Call("read"))
		switch {
		case err != nil:
			r.err = err
		case res.Get("done").Bool():
			r.err = io.EOF
		default:
			r.pending = bytesToGo(res.Get("value"))
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *streamReader) Close() error {
	r.stop()
	if r.err == nil {
		r.err = errReaderClosed
		_, _ = try(func() js.Value { return r.reader.Call("cancel") })
	}
	return nil
}

// serve runs h on the shim's request ({method, url, headers, body}) and is
// a Promise of the shim's response: {status, headers, read, cancel}, whose
// read is a Promise of the body's next bytes, or null at its end. done runs
// once the body ended or was cancelled.
func serve(h http.Handler, r js.Value, done func()) js.Value {
	return promise(func() (any, error) {
		req, err := request(r)
		if err != nil {
			done()
			return nil, err
		}
		return response(inproc.Serve(context.Background(), h, req), done), nil //nolint:bodyclose // response closes it when the stream ends
	})
}

// request is the shim's request ({method, url, headers, body}) as Go's.
func request(r js.Value) (*http.Request, error) {
	req, err := http.NewRequest(r.Get("method").String(), r.Get("url").String(), bytes.NewReader(bytesToGo(r.Get("body"))))
	if err != nil {
		return nil, err
	}
	headers := r.Get("headers")
	for i := range headers.Length() {
		pair := headers.Index(i)
		req.Header.Add(pair.Index(0).String(), pair.Index(1).String())
	}
	return req, nil
}

// response is resp for the shim, which streams its body.
func response(resp *http.Response, done func()) js.Value {
	headers := jsArray.New()
	for k, vs := range resp.Header {
		for _, v := range vs {
			headers.Call("push", jsArray.New(k, v))
		}
	}
	var (
		read, cancel js.Func
		end          sync.Once
	)
	finish := func() {
		end.Do(func() {
			_ = resp.Body.Close()
			read.Release()
			cancel.Release()
			done()
		})
	}
	buf := make([]byte, 64<<10)
	read = js.FuncOf(func(js.Value, []js.Value) any {
		return promise(func() (any, error) {
			n, err := resp.Body.Read(buf)
			for n == 0 && err == nil {
				n, err = resp.Body.Read(buf)
			}
			switch {
			case n > 0:
				return bytesToJS(buf[:n]), nil
			case errors.Is(err, io.EOF):
				finish()
				return js.Null(), nil
			default:
				finish()
				return nil, err
			}
		})
	})
	cancel = js.FuncOf(func(js.Value, []js.Value) any {
		finish()
		return nil
	})
	o := jsObject.New()
	o.Set("status", resp.StatusCode)
	o.Set("headers", headers)
	o.Set("read", read)
	o.Set("cancel", cancel)
	return o
}
