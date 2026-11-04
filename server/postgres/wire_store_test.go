package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/server"
	wirepg "github.com/ryanaldo34/tacklr/server/postgres"
)

var (
	wireOnce     sync.Once
	wirePool     *pgxpool.Pool
	wireErr      error
	wireStartErr string
)

// TestWireStore_putGetDelete is the real-Postgres outcome for protocol
// wire envelopes (durable session/load storage separate from harness checkpoints).
func TestWireStore_putGetDelete(t *testing.T) {
	ctx := context.Background()
	conn := wireConn(t)
	ws := wirepg.NewWireStore(conn, "")

	payload, _ := json.Marshal(map[string]any{
		"cwd":          "/proj",
		"configValues": map[string]string{"agent": "default"},
		"mcpServers":   []any{},
	})
	if err := ws.Put(ctx, "sess-1", payload); err != nil {
		t.Fatal(err)
	}

	got, err := ws.Get(ctx, "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(got, &env); err != nil {
		t.Fatal(err)
	}
	if env["cwd"] != "/proj" {
		t.Fatalf("cwd = %v", env["cwd"])
	}

	payload2, _ := json.Marshal(map[string]any{"cwd": "/other"})
	if err := ws.Put(ctx, "sess-1", payload2); err != nil {
		t.Fatal(err)
	}
	got, err = ws.Get(ctx, "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got, &env); err != nil {
		t.Fatal(err)
	}
	if env["cwd"] != "/other" {
		t.Fatalf("overwrite cwd = %v", env["cwd"])
	}

	_, err = ws.Get(ctx, "missing")
	if !errors.Is(err, server.ErrSessionNotFound) {
		t.Fatalf("missing: %v", err)
	}

	if err := ws.Delete(ctx, "sess-1"); err != nil {
		t.Fatal(err)
	}
	_, err = ws.Get(ctx, "sess-1")
	if !errors.Is(err, server.ErrSessionNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

// TestWireStore_acpLoadAfterRestart: create via protocol, new protocol
// with the same Postgres wire store, load + prompt succeed.
func TestWireStore_acpLoadAfterRestart(t *testing.T) {
	conn := wireConn(t)
	wire := wirepg.NewWireStore(conn, "acp")

	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok", IsComplete: true}
	})

	r1 := acptest.NewRuntime(t, strategy, tacklr.AgentOptions{})
	s1 := acptest.NewServer(t, r1, wire)
	rec1 := s1.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/proj"}}`)
	sessionID, _ := acptest.Result(t, rec1)["sessionId"].(string)
	if sessionID == "" {
		t.Fatal("missing sessionId")
	}

	s2 := acptest.NewServer(t, r1, wire)
	rec2 := s2.RPC(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"` + sessionID + `","cwd":"/proj"}}`)
	if acptest.Result(t, rec2)["sessionId"] != sessionID {
		t.Fatalf("load: %s", rec2.Body.String())
	}

	rec3 := s2.RPC(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`)
	var endTurn bool
	for _, f := range acptest.Frames(t, rec3.Body) {
		if res, ok := f["result"].(map[string]any); ok && res["stopReason"] == "end_turn" {
			endTurn = true
		}
		if f["error"] != nil {
			t.Fatalf("prompt: %v", f["error"])
		}
	}
	if !endTurn {
		t.Fatalf("expected end_turn, body=%s", rec3.Body.String())
	}
}

func wireConn(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	wireOnce.Do(func() {
		ctr, err := postgres.Run(ctx, pgtest.Image,
			postgres.WithDatabase("wire"),
			postgres.WithUsername("wire"),
			postgres.WithPassword("wire"),
			postgres.BasicWaitStrategies(),
			postgres.WithSQLDriver("pgx"),
		)
		if err != nil {
			wireStartErr = err.Error()
			return
		}
		url, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			wireErr = err
			_ = ctr.Terminate(ctx)
			return
		}
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			wireErr = err
			return
		}
		if wireErr = wirepg.NewWireStore(pool, "acp").Setup(ctx); wireErr == nil {
			wirePool = pool
		}
	})
	if wireStartErr != "" {
		t.Fatalf("postgres: %s", wireStartErr)
	}
	if wireErr != nil {
		t.Fatal(wireErr)
	}
	return wirePool
}
