package temporal_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// unreachable is a real Temporal client for a server that is not there.
func unreachable(t *testing.T) client.Client {
	t.Helper()
	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestOpen_needsAClientAStoreAndSecrets: Open panics without any of them.
func TestOpen_needsAClientAStoreAndSecrets(t *testing.T) {
	t.Parallel()
	// Arrange
	c, store, secrets := unreachable(t), session.NewMemoryStore(), session.NewMemorySecretStorage()
	for name, tc := range map[string]struct {
		c   client.Client
		cfg temporal.Config
	}{
		"no client":  {nil, temporal.Config{Store: store, Secrets: secrets}},
		"no store":   {c, temporal.Config{Secrets: secrets}},
		"no secrets": {c, temporal.Config{Store: store}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Act and assert
			defer func() {
				if recover() == nil {
					t.Fatal("want a panic")
				}
			}()
			temporal.Open(tc.c, tc.cfg)
		})
	}
}

// TestClient_failsWithTheErrorOfASecretStoreOrServerThatIsDown: credentials
// that the secret store cannot keep stop the prompt or resume that brought
// them. A server that does not answer fails a call, and ends a stream, with
// its own error, not as a missing session.
func TestClient_failsWithTheErrorOfASecretStoreOrServerThatIsDown(t *testing.T) {
	t.Parallel()
	// Arrange
	pool, err := pgxpool.New(t.Context(), "postgres://tacklr@127.0.0.1:1/tacklr")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	db, err := postgres.New(pool, postgres.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := session.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	rt := temporal.Open(unreachable(t), temporal.Config{Store: session.NewMemoryStore(), Secrets: db.Secrets(sealer), DisableStreams: true})
	streams := temporal.Open(unreachable(t), temporal.Config{Store: session.NewMemoryStore(), Secrets: session.NewMemorySecretStorage()})
	auth := session.AuthContext{Bindings: []mount.Binding{{Provider: "gdrive", Auth: mount.Credential{Token: "tok"}}}}

	// Act
	promptErr := rt.Prompt(t.Context(), "s", session.Prompt{Text: "x", Auth: auth})
	resumeErr := rt.Resume(t.Context(), "s", session.Resume{Auth: auth})
	_, statusErr := rt.Status(t.Context(), "s")
	cancelErr := rt.Cancel(t.Context(), "s")
	_, createErr := rt.CreateSession(t.Context(), session.CreateSession{})
	sub, err := streams.Subscribe(t.Context(), "s", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range sub.Events() {
	}

	// Assert
	if promptErr == nil || resumeErr == nil {
		t.Fatalf("prompt %v, resume %v; want both to fail on the secret store", promptErr, resumeErr)
	}
	for name, err := range map[string]error{"create": createErr, "status": statusErr, "cancel": cancelErr, "stream": sub.Err()} {
		if err == nil || !strings.Contains(err.Error(), "connection refused") {
			t.Errorf("%s = %v; want the server's own error", name, err)
		}
	}
}

// TestClient_withStreamsOffReadsTheFallbackLog: with streams off, Head and
// Subscribe read the fallback log that the workers write: an error event
// carries its error.
func TestClient_withStreamsOffReadsTheFallbackLog(t *testing.T) {
	t.Parallel()
	// Arrange
	log := session.NewMemoryEventLog()
	if err := log.Append(t.Context(), "s", session.TopicEvents, session.Event{Type: session.EventError, Fail: "model broke"}); err != nil {
		t.Fatal(err)
	}
	rt := temporal.Open(unreachable(t), temporal.Config{Store: session.NewMemoryStore(), Secrets: session.NewMemorySecretStorage(), Fallback: log, DisableStreams: true})

	// Act
	head, headErr := rt.Head(t.Context(), "s")
	sub, subErr := rt.Subscribe(t.Context(), "s", 0)
	if subErr != nil {
		t.Fatal(subErr)
	}
	defer func() { _ = sub.Close() }()
	ev := <-sub.Events()

	// Assert
	if head != 1 || headErr != nil || ev.Error == nil || ev.Error.Error() != "model broke" {
		t.Fatalf("head %d %v, first event %+v; want the fallback log's head and its first event with its error", head, headErr, ev)
	}
}
