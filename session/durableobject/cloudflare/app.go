//go:build js && wasm

package cloudflare

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"syscall/js"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
)

// App is a host's Worker: its session objects and its own routes.
type App struct {
	// Binding is the env name of the session objects' Durable Object
	// namespace (wrangler's durable_objects binding).
	Binding string
	// Config is what every session object of the isolate runs with. It is
	// called once per isolate, on the first request.
	Config func(Env) (durableobject.Config, error)
	// Front is the Worker's own routes, over a session.Client of the
	// objects. It is called once per isolate; nil serves nothing.
	Front func(Env, session.Client) (http.Handler, error)
	// MaxIdleObjects bounds the idle objects the isolate keeps in memory
	// (durableobject.NewRegistry; zero: 64).
	MaxIdleObjects int
}

// Env is the Worker's environment: its vars, secrets and bindings.
type Env struct{ v js.Value }

// String is the var or secret name, or "" when there is none.
func (e Env) String(name string) string {
	v := e.v.Get(name)
	if v.Type() != js.TypeString {
		return ""
	}
	return v.String()
}

// Serve serves app to shim.js and blocks. Call it from main.
func Serve(app App) {
	if app.Binding == "" || app.Config == nil {
		panic("cloudflare: App needs a Binding and a Config")
	}
	w := &worker{app: app, objects: durableobject.NewRegistry(app.MaxIdleObjects)}
	js.Global().Set("__tacklr", map[string]any{
		"object": js.FuncOf(w.object),
		"alarm":  js.FuncOf(w.alarm),
		"front":  js.FuncOf(w.front),
	})
	select {}
}

// worker is one isolate's Go half: what it was set up with, and its live
// objects.
type worker struct {
	app     App
	objects *durableobject.Registry

	once     sync.Once
	err      error
	platform platform
	cfg      durableobject.Config
	routes   http.Handler
}

// setup builds what the isolate serves from env, once.
func (w *worker) setup(env js.Value) error {
	w.once.Do(func() {
		ns := env.Get(w.app.Binding)
		if ns.Type() != js.TypeObject {
			w.err = fmt.Errorf("cloudflare: env has no Durable Object namespace %q", w.app.Binding)
			return
		}
		w.platform = platform{ns: ns}
		e := Env{v: env}
		if w.cfg, w.err = w.app.Config(e); w.err != nil {
			return
		}
		if w.cfg.Sealer == nil {
			w.err = errors.New("cloudflare: Config needs a Sealer")
			return
		}
		if w.app.Front != nil {
			w.routes, w.err = w.app.Front(e, durableobject.NewClient(w.platform))
		}
	})
	return w.err
}

// acquire is the object of the shim's host, for one request.
func (w *worker) acquire(h js.Value) (*durableobject.Object, func()) {
	return w.objects.Acquire(h.Get("id").String(), h.Get("instance").Int(), func() *durableobject.Object {
		return durableobject.NewObject(host{js: h, platform: w.platform}, w.cfg)
	})
}

// object(host, env, request) serves a request to an object over the object
// protocol.
func (w *worker) object(_ js.Value, args []js.Value) any {
	h, env, req := args[0], args[1], args[2]
	if err := w.setup(env); err != nil {
		return rejected(err)
	}
	obj, release := w.acquire(h)
	return serve(durableobject.Handler(obj), req, release)
}

// alarm(host, env) runs an object's alarm, then waits until the object
// runs nothing, which keeps it alive while the step the alarm restarted
// runs.
func (w *worker) alarm(_ js.Value, args []js.Value) any {
	h, env := args[0], args[1]
	if err := w.setup(env); err != nil {
		return rejected(err)
	}
	return promise(func() (any, error) {
		obj, release := w.acquire(h)
		defer release()
		ctx := context.Background()
		if err := obj.Alarm(ctx); err != nil {
			return nil, err
		}
		return nil, obj.Idle(ctx)
	})
}

// front(env, request) serves the Worker's own routes.
func (w *worker) front(_ js.Value, args []js.Value) any {
	env, req := args[0], args[1]
	if err := w.setup(env); err != nil {
		return rejected(err)
	}
	routes := w.routes
	if routes == nil {
		routes = http.NotFoundHandler()
	}
	return serve(routes, req, func() {})
}

// rejected is a Promise rejected with err.
func rejected(err error) js.Value {
	return jsPromise.Call("reject", jsError.New(err.Error()))
}
