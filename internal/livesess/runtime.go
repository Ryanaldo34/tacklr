// Package livesess runs the session runtime on the live stack for tests: a
// Temporal worker on the shared Temporal server, over the Postgres session
// stores.
package livesess

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/worker"
	"github.com/ryanaldo34/tacklr/telemetry"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
	sessionpg "github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/telemetry/export"
	"github.com/ryanaldo34/tacklr/workspace"
)

var traceOnce sync.Once

// Spans records every span the test binary's sessions end, through the
// standard SDK provider and the W3C propagator a host installs
// (export.Init).
var Spans = tracetest.NewSpanRecorder()

func trace(t testing.TB) {
	t.Helper()
	traceOnce.Do(func() {
		if _, err := export.Init(context.Background(), export.Config{}); err != nil {
			t.Fatal(err)
		}
		telemetry.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(Spans)))
	})
}

// stores are the durable session stores in a Postgres schema of the test's
// own, as a production host runs them: the events go through the Postgres
// EventLog, since the live stack turns Workflow Streams off.
func stores(t testing.TB) (session.Store, session.EventLog, session.SecretStorage) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	// As a host does: each statement is a span of the turn that runs it.
	sessionpg.Configure(cfg)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db, err := sessionpg.New(pool, sessionpg.Options{Schema: pgtest.Schema(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	log := db.EventLog()
	t.Cleanup(log.Close)
	return db.Store(), log, db.Secrets(sealer)
}

// Runtime starts a worker against the shared Temporal Docker server, on the
// Postgres session stores, with ws as the agent's workspace. As a host
// serving ACP clients does, it connects MCP servers with mcp.Client unless
// agent names another client.
func Runtime(t testing.TB, agent tacklr.AgentOptions, ws workspace.Config) session.Client {
	t.Helper()
	if agent.MCP == nil {
		agent.MCP = mcp.Client{}
	}
	trace(t)
	c, err := temporal.Dial(client.Options{HostPort: temporaldocker.HostPort(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	tq := "tacklr-" + uuid.NewString()
	store, log, secrets := stores(t)
	// As a host does, the worker's live endpoint is served for clients to
	// dial: the deltas of its model calls stream there, never stored.
	key := []byte("live-" + tq)
	var live http.Handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { live.ServeHTTP(w, r) }))
	t.Cleanup(srv.Close)
	rt := temporal.Open(c, temporal.Config{TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets, DisableStreams: true, Live: temporal.Live{Key: key}})
	w := worker.New(c, worker.Config{Agent: agent, Workspace: ws, TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets, DisableStreams: true, Live: worker.Live{URL: srv.URL, Key: key}})
	live = w.LiveHandler()
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

// Down is a session client whose Temporal connection is closed, as a host
// whose Temporal cluster is gone: each call to it fails.
func Down(t testing.TB) session.Client {
	t.Helper()
	c, err := temporal.Dial(client.Options{HostPort: temporaldocker.HostPort(t)})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	return temporal.Open(c, temporal.Config{Store: session.NewMemoryStore(), Secrets: session.NewMemorySecretStorage(), DisableStreams: true})
}
