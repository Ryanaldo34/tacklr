//go:build js && wasm

package cloudflare

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"syscall/js"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/telemetry/otlpjson"
)

// App is a Worker: its session objects, and the host's own routes over a
// client of them.
type App struct {
	// Sessions is the env name of the session objects' Durable Object
	// namespace (wrangler's durable_objects binding).
	Sessions string
	// Config is what every session object of the isolate runs with. It is
	// called once per isolate, on the first request.
	Config func(Env) (durableobject.Config, error)
	// Handler is the host's routes over a session.Client of the session
	// objects: the Worker's every request from outside. It is called once
	// per isolate. A response body streams, so a route can follow a
	// session's events.
	Handler func(Env, *durableobject.Client) (http.Handler, error)
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
	if app.Sessions == "" || app.Config == nil || app.Handler == nil {
		panic("cloudflare: App needs Sessions, Config and Handler")
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
	// tp is the tracer provider when the env names an OTLP endpoint.
	tp *sdktrace.TracerProvider
}

// setup builds what the isolate serves from env, once.
func (w *worker) setup(env js.Value) error {
	w.once.Do(func() { w.err = w.build(env) })
	return w.err
}

func (w *worker) build(env js.Value) error {
	ns := env.Get(w.app.Sessions)
	if ns.Type() != js.TypeObject {
		return fmt.Errorf("cloudflare: env has no Durable Object namespace %q", w.app.Sessions)
	}
	w.platform = platform{ns: ns}
	e := Env{v: env}
	// Telemetry first, as a host process sets it up: OTLP/HTTP in JSON,
	// which needs no exporter code too large for a Worker.
	if endpoint := e.String("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
		exporter := otlpjson.New(endpoint, otlpjson.Headers(e.String("OTEL_EXPORTER_OTLP_HEADERS")), nil)
		w.tp = otlpjson.Install(exporter, cmp.Or(e.String("OTEL_SERVICE_NAME"), "tacklr"))
	}
	var err error
	if w.cfg, err = w.app.Config(e); err != nil {
		return err
	}
	if w.cfg.Sealer == nil {
		return errors.New("cloudflare: Config needs a Sealer")
	}
	w.routes, err = w.app.Handler(e, durableobject.NewClient(w.platform))
	return err
}

// acquire is the object of the shim's host, for one request. Its release
// sends the object's spans once it runs nothing: the platform may stop an
// idle object, and the export's fetch keeps it alive until it is sent.
func (w *worker) acquire(h js.Value) (*durableobject.Object, func()) {
	obj, release := w.objects.Acquire(h.Get("id").String(), h.Get("instance").Int(), func() *durableobject.Object {
		return durableobject.NewObject(host{js: h, platform: w.platform}, w.cfg)
	})
	return obj, func() {
		release()
		go func() {
			if obj.Idle(context.Background()) == nil {
				w.flush()
			}
		}()
	}
}

// flush sends the spans the isolate holds, when it traces.
func (w *worker) flush() {
	if w.tp != nil {
		_ = w.tp.ForceFlush(context.Background())
	}
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
		err := obj.Idle(ctx)
		w.flush()
		return nil, err
	})
}

// front(env, request) serves a request to the Worker with the host's
// routes.
func (w *worker) front(_ js.Value, args []js.Value) any {
	env, req := args[0], args[1]
	if err := w.setup(env); err != nil {
		return rejected(err)
	}
	return serve(w.routes, req, func() {})
}

// rejected is a Promise rejected with err.
func rejected(err error) js.Value {
	return jsPromise.Call("reject", jsError.New(err.Error()))
}
