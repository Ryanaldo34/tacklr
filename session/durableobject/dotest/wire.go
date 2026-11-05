package dotest

import (
	"context"
	"net/http"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/internal/inproc"
)

// NewWire is New, but every call to an object, from a client or from
// another object, goes over the object protocol (durableobject.Handler and
// durableobject.Remote), as it does on a platform where objects are
// remote. Evict also breaks the object's open streams, as a platform that
// takes an object away does.
func NewWire(cfg durableobject.Config) *Platform {
	p := New(cfg)
	p.wire = true
	return p
}

// remote is id's object over the object protocol.
func (p *Platform) remote(id session.ID) durableobject.Ref {
	return durableobject.Remote(&http.Client{Transport: objectTransport{p: p, id: id}})
}

// objectTransport delivers each request to one object's Handler in this
// process.
type objectTransport struct {
	p  *Platform
	id session.ID
}

func (t objectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	h := t.p.host(t.id)
	// An evicted object's streams break.
	ctx, cancel := context.WithCancelCause(req.Context())
	context.AfterFunc(h.ctx, func() { cancel(errStopped) })
	return inproc.Serve(ctx, durableobject.Handler(h.obj), req), nil
}
