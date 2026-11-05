// Package faultproxy is an HTTP proxy that live tests put in front of a
// backend (a container such as Helix, an S3 server or Azurite) to fail its
// traffic at the network. With no rule set, the proxy forwards every request.
// A rule makes it refuse, reset, cut a response in the middle of its body,
// hang, or answer with a status and body of its own, for every request or
// for the requests that match.
package faultproxy

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
)

// Proxy forwards to one backend and applies the rule that is set.
type Proxy struct {
	srv    *httptest.Server
	target *url.URL
	rule   atomic.Pointer[Rule]
}

// Rule chooses the requests that get a fault.
type Rule struct {
	// Match chooses the requests. Nil matches every request. Match can read
	// the request body: the proxy gives the body again to the backend.
	Match func(*http.Request) bool
	Fault Fault
	// Once applies the fault to one request only, then clears the rule.
	Once bool
}

// Fault is what the proxy does with a matched request.
type Fault struct {
	kind   kind
	status int
	body   string
	cut    int64
}

type kind int

const (
	answer kind = iota
	refuse
	reset
	cut
	hang
)

// Answer answers with status and body, and does not forward the request.
func Answer(status int, body string) Fault { return Fault{kind: answer, status: status, body: body} }

// Refuse closes the connection before it answers. The client reads EOF.
func Refuse() Fault { return Fault{kind: refuse} }

// Reset closes the connection with a TCP reset before it answers. The client
// reads "connection reset by peer".
func Reset() Fault { return Fault{kind: reset} }

// Cut forwards the request, sends the response headers and the first n bytes
// of the body, then closes the connection. The client reads an unexpected EOF
// in the middle of the body.
func Cut(n int64) Fault { return Fault{kind: cut, cut: n} }

// Hang holds the request and does not answer until the client stops waiting.
func Hang() Fault { return Fault{kind: hang} }

// Start starts a proxy in front of target, the backend's base URL. The proxy
// stops when t ends.
func Start(t testing.TB, target *url.URL) *Proxy {
	t.Helper()
	p := &Proxy{target: target}
	p.srv = httptest.NewServer(p)
	t.Cleanup(p.srv.Close)
	return p
}

// URL is the base URL a client uses in place of the backend's.
func (p *Proxy) URL() string { return p.srv.URL }

// Set replaces the rule.
func (p *Proxy) Set(r Rule) { p.rule.Store(&r) }

// Clear removes the rule: the proxy forwards every request again.
func (p *Proxy) Clear() { p.rule.Store(nil) }

// ServeHTTP applies the rule to r, or forwards it.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A body the client does not send in full fails at the backend.
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	rule := p.rule.Load()
	if rule == nil || !p.claim(rule, r, body) {
		p.forward(w, r, -1)
		return
	}
	switch f := rule.Fault; f.kind {
	case answer:
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, f.body)
	case refuse, reset:
		// httptest serves HTTP/1.1, where Hijack does not fail.
		conn, _, _ := http.NewResponseController(w).Hijack()
		if tcp, ok := conn.(*net.TCPConn); ok && f.kind == reset {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	case cut:
		p.forward(w, r, f.cut)
	case hang:
		<-r.Context().Done()
	}
}

// claim reports whether rule matches r, and takes a Once rule so that no
// other request gets it. It gives the body back to r for the backend.
func (p *Proxy) claim(rule *Rule, r *http.Request, body []byte) bool {
	matched := rule.Match == nil || rule.Match(r)
	r.Body = io.NopCloser(bytes.NewReader(body))
	return matched && (!rule.Once || p.rule.CompareAndSwap(rule, nil))
}

// forward sends r to the backend. With limit >= 0 it sends only limit bytes
// of the response body, then aborts the connection.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, limit int64) {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(p.target)
			// Signed requests (S3, Azure) sign the Host header the client
			// sent.
			pr.Out.Host = pr.In.Host
		},
		FlushInterval: -1,
		ErrorLog:      log.New(io.Discard, "", 0),
	}
	if limit >= 0 {
		rp.ModifyResponse = func(res *http.Response) error {
			res.Body = &cutBody{ReadCloser: res.Body, left: limit}
			return nil
		}
	}
	rp.ServeHTTP(w, r)
}

// errCut ends a body that Cut stops in the middle.
var errCut = errors.New("faultproxy: body cut")

// cutBody reads left bytes, then fails, so that the reverse proxy aborts the
// connection after it flushes them.
type cutBody struct {
	io.ReadCloser
	left int64
}

func (b *cutBody) Read(buf []byte) (int, error) {
	if b.left <= 0 {
		return 0, errCut
	}
	n, err := b.ReadCloser.Read(buf[:min(int64(len(buf)), b.left)])
	b.left -= int64(n)
	return n, err
}
