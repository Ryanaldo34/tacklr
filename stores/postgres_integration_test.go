package stores

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ryanaldo34/tacklr/streaming"
)

const sessionPgImage = "tacklr-pg-brain:test"

var (
	sessionOnce sync.Once
	sessionURL  string
	sessionErr  error
	sessionSkip string
)

// TestPostgresStore_liveSaveLoad is the real-Postgres outcome for session
// checkpoint persistence (save, overwrite, load, missing).
func TestPostgresStore_liveSaveLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres store integration in -short mode")
	}
	ctx := context.Background()
	conn := sessionConn(t)
	store := NewPostgresStore(conn)

	cp, err := NewCheckpoint(
		[]*streaming.Message{
			{Role: streaming.RoleUser, Content: "hi"},
			{Role: streaming.RoleAssistant, Content: "yo"},
		},
		map[string]PendingToolCall{
			"c1": {ToolCall: &streaming.ToolCall{ID: "c1", Name: "search"}, InterruptActive: false},
		},
		map[string]any{"k": "v"},
		map[string]any{"p": 1},
		map[string]any{"r": 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSession(ctx, "s1", *cp); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.ContextWindow) != 2 || loaded.ContextWindow[0].Content != "hi" {
		t.Fatalf("load: %+v", loaded.ContextWindow)
	}
	if loaded.State.PendingToolCalls["c1"].ToolCall == nil || loaded.State.PendingToolCalls["c1"].ToolCall.Name != "search" {
		t.Fatalf("pending: %+v", loaded.State.PendingToolCalls)
	}
	if loaded.State.RuntimeState["k"] != "v" {
		t.Fatalf("runtime: %+v", loaded.State.RuntimeState)
	}

	// Overwrite
	cp2, err := NewCheckpoint(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "v2"}},
		nil, map[string]any{"n": float64(1)}, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSession(ctx, "s1", *cp2); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.LoadSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.ContextWindow) != 1 || loaded.ContextWindow[0].Content != "v2" {
		t.Fatalf("overwrite: %+v", loaded.ContextWindow)
	}

	_, err = store.LoadSession(ctx, "missing")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing: %v", err)
	}

	// Valid JSONB that does not decode into checkpoint fields.
	_, err = conn.Exec(ctx, `INSERT INTO public.session (session_id, context_window, state)
		VALUES ('bad-cw', '1', '{}')
		ON CONFLICT (session_id) DO UPDATE SET context_window = EXCLUDED.context_window`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSession(ctx, "bad-cw"); err == nil {
		t.Fatal("want context_window decode error")
	}
	_, err = conn.Exec(ctx, `INSERT INTO public.session (session_id, context_window, state)
		VALUES ('bad-st', '[]', '1')
		ON CONFLICT (session_id) DO UPDATE SET state = EXCLUDED.state`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSession(ctx, "bad-st"); err == nil {
		t.Fatal("want state decode error")
	}

	// Closed connection surfaces store errors.
	_ = conn.Close(ctx)
	if err := store.SaveSession(ctx, "x", *cp); err == nil {
		t.Fatal("save on closed conn")
	}
	if _, err := store.LoadSession(ctx, "s1"); err == nil {
		t.Fatal("load on closed conn")
	}
}

func sessionConn(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	sessionOnce.Do(func() {
		_, thisFile, _, ok := runtime.Caller(0)
		if !ok {
			sessionErr = errors.New("runtime.Caller failed")
			return
		}
		schema := filepath.Join(filepath.Dir(thisFile), "testdata", "session_schema.sql")
		ctr, err := postgres.Run(ctx, sessionPgImage,
			postgres.WithDatabase("sessions"),
			postgres.WithUsername("sessions"),
			postgres.WithPassword("sessions"),
			postgres.WithInitScripts(schema),
			postgres.BasicWaitStrategies(),
			postgres.WithSQLDriver("pgx"),
		)
		if err != nil {
			sessionSkip = err.Error() + " (make brain-pg-image)"
			return
		}
		url, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			sessionErr = err
			_ = ctr.Terminate(ctx)
			return
		}
		sessionURL = url
	})
	if sessionSkip != "" {
		t.Skipf("postgres unavailable: %s", sessionSkip)
	}
	if sessionErr != nil {
		t.Fatal(sessionErr)
	}
	conn, err := pgx.Connect(ctx, sessionURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn
}
