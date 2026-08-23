package server

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
)

const wirePgImage = "tacklr-pg-brain:test"

var (
	wireOnce sync.Once
	wireURL  string
	wireErr  error
	wireSkip string
)

// TestPostgresWireStore_putGetDelete is the real-Postgres outcome for protocol
// wire envelopes (durable session/load storage separate from harness checkpoints).
func TestPostgresWireStore_putGetDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres wire store integration in -short mode")
	}
	ctx := context.Background()
	conn := wireConn(t)
	ws := NewPostgresWireStore(conn, "")

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
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing: %v", err)
	}

	if err := ws.Delete(ctx, "sess-1"); err != nil {
		t.Fatal(err)
	}
	_, err = ws.Get(ctx, "sess-1")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

// TestPostgresWireStore_acpLoadAfterRestart: create via protocol, new protocol
// with the same Postgres wire store, load + prompt succeed.
func TestPostgresWireStore_acpLoadAfterRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres wire store integration in -short mode")
	}
	conn := wireConn(t)
	wire := NewPostgresWireStore(conn, "acp")

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}

	r1 := newTestKernel(t, strategy, durable.AgentSpec{})
	s1 := newACPTestServerWithWire(t, r1, wire)
	rec1 := s1.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/proj"}}`)
	sessionID, _ := acpRPCResult(t, rec1)["sessionId"].(string)
	if sessionID == "" {
		t.Fatal("missing sessionId")
	}

	r2 := newTestKernel(t, strategy, durable.AgentSpec{})
	s2 := newACPTestServerWithWire(t, r2, wire)
	rec2 := s2.rpc(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"` + sessionID + `","cwd":"/proj"}}`)
	if acpRPCResult(t, rec2)["sessionId"] != sessionID {
		t.Fatalf("load: %s", rec2.Body.String())
	}

	rec3 := s2.rpc(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`)
	var endTurn bool
	for _, f := range parseACPFrames(t, rec3.Body) {
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

func wireConn(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	wireOnce.Do(func() {
		_, thisFile, _, ok := runtime.Caller(0)
		if !ok {
			wireErr = errors.New("runtime.Caller failed")
			return
		}
		schema := filepath.Join(filepath.Dir(thisFile), "..", "stores", "testdata", "session_schema.sql")
		ctr, err := postgres.Run(ctx, wirePgImage,
			postgres.WithDatabase("wire"),
			postgres.WithUsername("wire"),
			postgres.WithPassword("wire"),
			postgres.WithInitScripts(schema),
			postgres.BasicWaitStrategies(),
			postgres.WithSQLDriver("pgx"),
		)
		if err != nil {
			wireSkip = err.Error() + " (make brain-pg-image)"
			return
		}
		url, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			wireErr = err
			_ = ctr.Terminate(ctx)
			return
		}
		wireURL = url
	})
	if wireSkip != "" {
		t.Skipf("postgres unavailable: %s", wireSkip)
	}
	if wireErr != nil {
		t.Fatal(wireErr)
	}
	conn, err := pgx.Connect(ctx, wireURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn
}
