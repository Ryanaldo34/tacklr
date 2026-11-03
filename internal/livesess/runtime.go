package livesess

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/worker"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

var traceOnce sync.Once

func trace(t testing.TB) {
	t.Helper()
	traceOnce.Do(func() {
		if _, err := telemetry.Init(context.Background(), telemetry.Config{}); err != nil {
			t.Fatal(err)
		}
		// As a host does: Temporal tracing needs its replay-safe tracer.
		if err := telemetry.ReinstallTracer(context.Background(), func(opts ...sdktrace.TracerProviderOption) (oteltrace.TracerProvider, func(context.Context) error) {
			tp := temporalotel.NewReplaySafeTracerProvider(opts...)
			return tp, tp.Shutdown
		}); err != nil {
			t.Fatal(err)
		}
	})
}

// Runtime starts a worker against the shared Temporal Docker server.
func Runtime(t testing.TB, agent tacklr.AgentOptions) session.Client {
	t.Helper()
	trace(t)
	plugin, err := temporalotel.NewPlugin(temporalotel.PluginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := temporal.Dial(client.Options{HostPort: temporaldocker.HostPort(t), Plugins: []client.Plugin{plugin}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	tq := "tacklr-" + uuid.NewString()
	store, log, secrets := session.NewMemoryStore(), session.NewMemoryEventLog(), session.NewMemorySecretStorage()
	rt := temporal.Open(c, temporal.Config{TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets, DisableStreams: true})
	w := worker.New(c, worker.Config{Agent: agent, TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets, DisableStreams: true})
	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		_ = w.Run(stop)
		close(done)
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})
	return rt
}
