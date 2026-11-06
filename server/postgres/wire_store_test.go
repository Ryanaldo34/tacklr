package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	wirepg "github.com/ryanaldo34/tacklr/server/postgres"
	"github.com/ryanaldo34/tacklr/session"
)

// setup makes the wire table once for the test binary: each wire store
// shares public.protocol_wire_session, and two CREATE TABLE IF NOT EXISTS
// statements at the same time can conflict.
var setup sync.Once

// wireStore is a wire store on the shared Postgres, set up.
func wireStore(t *testing.T, protocol string) *wirepg.WireStore {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ws := wirepg.NewWireStore(pool, protocol)
	setup.Do(func() { err = ws.Setup(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

// TestWireStore_putGetDelete: a put envelope reads back, a second put
// replaces it, an empty envelope reads back as an empty object, and after
// a delete, as for an id never put, it is not found; a store whose pool is
// closed cannot set up its table or delete.
func TestWireStore_putGetDelete(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ws := wireStore(t, "")
	closed, err := pgxpool.New(ctx, pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	cwd := func(raw []byte, err error) string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		var env struct{ CWD string }
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		return env.CWD
	}

	if err := ws.Put(ctx, "sess-1", []byte(`{"cwd":"/proj"}`)); err != nil {
		t.Fatal(err)
	}
	first := cwd(ws.Get(ctx, "sess-1"))
	if err := ws.Put(ctx, "sess-1", []byte(`{"cwd":"/other"}`)); err != nil {
		t.Fatal(err)
	}
	second := cwd(ws.Get(ctx, "sess-1"))
	if err := ws.Put(ctx, "sess-empty", nil); err != nil {
		t.Fatal(err)
	}
	empty, emptyErr := ws.Get(ctx, "sess-empty")
	_, missing := ws.Get(ctx, "missing")
	if err := ws.Delete(ctx, "sess-1"); err != nil {
		t.Fatal(err)
	}
	_, deleted := ws.Get(ctx, "sess-1")
	setupErr := wirepg.NewWireStore(closed, "").Setup(ctx)
	deleteErr := wirepg.NewWireStore(closed, "").Delete(ctx, "sess-empty")

	if first != "/proj" || second != "/other" || emptyErr != nil || string(empty) != "{}" {
		t.Fatalf("cwd %q, then %q, empty envelope %q %v; want /proj, then /other, then {}", first, second, empty, emptyErr)
	}
	if setupErr == nil || deleteErr == nil {
		t.Fatalf("on a closed pool: setup = %v, delete = %v; want both to fail", setupErr, deleteErr)
	}
	if !errors.Is(missing, server.ErrSessionNotFound) || !errors.Is(deleted, server.ErrSessionNotFound) {
		t.Fatalf("missing = %v, deleted = %v; want not found", missing, deleted)
	}
}

// TestWireStore_acpLoadAfterRestart: a new ACP server on the same Postgres
// wire store loads a session another one made, and prompts it; the stored
// envelope holds what ACP owns of the session, its cwd and owner, and none
// of what the session owns (its MCP servers and their credentials).
func TestWireStore_acpLoadAfterRestart(t *testing.T) {
	t.Parallel()
	wire := wireStore(t, "acp")
	r := acptest.NewRuntime(t, testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok", IsComplete: true}
	}), tacklr.AgentOptions{})
	sessionID := acptest.SessionID(t, acptest.Connect(t, acptest.Listen(t, acptest.Server(r, wire)), `{"protocolVersion":1}`).Call("session/new", `{"cwd":"/proj",`+
		`"mcpServers":[{"type":"http","name":"api","url":"http://127.0.0.1:1/mcp","headers":[{"name":"Authorization","value":"Bearer never-store"}]}]}`))

	restarted := acptest.Connect(t, acptest.Listen(t, acptest.Server(r, wire)), `{"protocolVersion":1}`)
	loaded := acptest.Result(t, restarted.Call("session/load", `{"sessionId":"`+sessionID+`","cwd":"/proj"}`))
	prompted, _ := restarted.Say(sessionID, "hi")
	raw, err := wire.Get(t.Context(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}

	if stop := acptest.StopReason(prompted); loaded["sessionId"] != sessionID || stop != "end_turn" {
		t.Fatalf("load = %v, prompt stopReason = %v; want the session, then end_turn", loaded, stop)
	}
	want := map[string]any{"cwd": "/proj", "owner": "anonymous"}
	if !reflect.DeepEqual(envelope, want) {
		t.Fatalf("envelope = %v, want %v", envelope, want)
	}
}
