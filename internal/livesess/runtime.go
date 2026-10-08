package livesess

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/telemetry"
)

var traceOnce sync.Once

func trace(t testing.TB) {
	t.Helper()
	traceOnce.Do(func() {
		if _, err := telemetry.Init(context.Background(), telemetry.Config{}); err != nil {
			t.Fatal(err)
		}
	})
}

// Runtime starts a worker against the shared Temporal Docker server.
func Runtime(t testing.TB, agent tacklr.AgentOptions) session.Runtime {
	t.Helper()
	trace(t)
	c, err := temporal.Dial(client.Options{HostPort: temporaldocker.HostPort(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cfg := temporal.Config{
		Agent:          agent,
		TaskQueue:      "tacklr-" + uuid.NewString(),
		Snapshots:      session.NewMemorySnapshot(),
		Fallback:       session.NewMemoryEventLog(),
		Secrets:        session.NewMemorySecretStorage(),
		DisableStreams: true,
	}
	rt := temporal.Open(c, cfg)
	w := rt.StartWorker()
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
