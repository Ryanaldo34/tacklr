//go:build js && wasm

package cloudflare

import (
	"net/http"
	"syscall/js"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
)

// host is one object's durableobject.Host over the shim's Host: its SQLite
// storage and its alarm. Storage calls are synchronous; the alarm calls
// wait on their promise.
type host struct {
	js       js.Value
	platform platform
}

// Get implements durableobject.Storage.
func (h host) Get(key string) ([]byte, bool, error) {
	v, err := try(func() js.Value { return h.js.Call("get", key) })
	if err != nil || v.IsNull() {
		return nil, false, err
	}
	return bytesToGo(v), true, nil
}

// Scan implements durableobject.Storage.
func (h host) Scan(prefix, after string, limit int) ([]durableobject.Entry, error) {
	v, err := try(func() js.Value { return h.js.Call("scan", prefix, after, limit) })
	if err != nil {
		return nil, err
	}
	out := make([]durableobject.Entry, v.Length())
	for i := range out {
		e := v.Index(i)
		out[i] = durableobject.Entry{Key: e.Index(0).String(), Value: bytesToGo(e.Index(1))}
	}
	return out, nil
}

// Write implements durableobject.Storage: one SQLite transaction.
func (h host) Write(ops ...durableobject.Op) error {
	arr := jsArray.New(len(ops))
	for i, op := range ops {
		o := jsObject.New()
		o.Set("key", op.Key)
		if op.Delete {
			o.Set("delete", true)
		} else {
			o.Set("value", bytesToJS(op.Value))
		}
		arr.SetIndex(i, o)
	}
	_, err := try(func() js.Value { return h.js.Call("write", arr) })
	return err
}

// SetAlarm implements durableobject.Host.
func (h host) SetAlarm(t time.Time) error {
	p, err := try(func() js.Value {
		if t.IsZero() {
			return h.js.Call("deleteAlarm")
		}
		return h.js.Call("setAlarm", float64(t.UnixMilli()))
	})
	if err != nil {
		return err
	}
	_, err = await(p)
	return err
}

// Object implements durableobject.Platform.
func (h host) Object(id session.ID) durableobject.Ref { return h.platform.Object(id) }

// platform reaches session objects through their Durable Object namespace.
type platform struct{ ns js.Value }

// Object implements durableobject.Platform: id's object, over the object
// protocol through its stub.
func (p platform) Object(id session.ID) durableobject.Ref {
	stub := p.ns.Call("get", p.ns.Call("idFromName", string(id)))
	return durableobject.Remote(&http.Client{Transport: stubTransport{stub: stub}})
}
