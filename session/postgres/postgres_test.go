package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/session/storetest"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// testDB is the session tables, set up in a schema of their own, over a
// pool of the shared container.
func testDB(t *testing.T) (*postgres.DB, *pgxpool.Pool, string) {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema := pgtest.Schema(t)
	db, err := postgres.New(pool, postgres.Options{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return db, pool, schema
}

func sealer(t *testing.T) *session.AESGCM {
	t.Helper()
	s, err := session.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPostgresStores_meetTheContract(t *testing.T) {
	storetest.Store(t, func(t *testing.T) session.Store {
		db, _, _ := testDB(t)
		return db.Store()
	})
	storetest.Secrets(t, func(t *testing.T) session.SecretStorage {
		db, _, _ := testDB(t)
		return db.Secrets(sealer(t))
	})
	storetest.EventLog(t, func(t *testing.T) session.EventLog {
		db, _, _ := testDB(t)
		log := db.EventLog()
		t.Cleanup(log.Close)
		return log
	})
}

// A user's token is sealed to its session: the row holds no plaintext, and
// the same row read as another session's does not open.
func TestSecrets_sealATokenToItsSession(t *testing.T) {
	db, pool, schema := testDB(t)
	secrets := db.Secrets(sealer(t))
	ctx := t.Context()
	bag := session.Secrets{Auth: session.AuthContext{Bindings: []mount.Binding{{
		Provider: mount.ProviderGoogleDrive, Auth: mount.Credential{Token: "user-refresh-token"},
	}}}}
	if err := secrets.Put(ctx, "alice", bag); err != nil {
		t.Fatal(err)
	}
	var sealed []byte
	table := schema + ".session_secrets"
	if err := pool.QueryRow(ctx, `SELECT sealed FROM `+table+` WHERE session_id = 'alice'`).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO `+table+` (session_id, sealed) VALUES ('mallory', $1)`, sealed); err != nil {
		t.Fatal(err)
	}
	_, swapped := secrets.Get(ctx, "mallory")
	own, ownErr := secrets.Get(ctx, "alice")

	if bytes.Contains(sealed, []byte("user-refresh-token")) {
		t.Fatal("the database row holds the token in plaintext")
	}
	if swapped == nil || ownErr != nil || own.Auth.Bindings[0].Auth.Token != "user-refresh-token" {
		t.Fatalf("another session's row %v, own %+v %v; want it refused and the owner's token back", swapped, own, ownErr)
	}
}

// failingSealer is a host KMS that is down, or returns what is not a bag.
type failingSealer struct{ plain []byte }

func (f failingSealer) Seal(_ context.Context, plaintext, _ []byte) ([]byte, error) {
	if f.plain != nil {
		return plaintext, nil
	}
	return nil, errors.New("kms unavailable")
}

func (f failingSealer) Open(context.Context, []byte, []byte) ([]byte, error) { return f.plain, nil }

func TestSecrets_aSealerThatFailsFailsTheCall(t *testing.T) {
	db, _, _ := testDB(t)
	ctx := t.Context()
	bag := session.Secrets{Auth: session.AuthContext{Bindings: []mount.Binding{{Provider: "gdrive", Auth: mount.Credential{Token: "t"}}}}}
	sealErr := db.Secrets(failingSealer{}).Put(ctx, "s", bag)
	garbled := db.Secrets(failingSealer{plain: []byte("not json")})
	if err := garbled.Put(ctx, "s", bag); err != nil {
		t.Fatal(err)
	}
	_, decodeErr := garbled.Get(ctx, "s")

	if sealErr == nil || !strings.Contains(sealErr.Error(), "kms unavailable") || decodeErr == nil {
		t.Fatalf("seal %v, decode %v; want both to fail the call", sealErr, decodeErr)
	}
}

// Every call names its session and fails cleanly when the database is gone.
func TestStores_failEveryCallOnAClosedPool(t *testing.T) {
	db, pool, _ := testDB(t)
	_, badSchema := postgres.New(pool, postgres.Options{Schema: "x;drop"})
	pool.Close()
	ctx := t.Context()
	store, secrets, log := db.Store(), db.Secrets(sealer(t)), db.EventLog()
	defer log.Close()
	bag := session.Secrets{Auth: session.AuthContext{Bindings: []mount.Binding{{Provider: "gdrive", Auth: mount.Credential{Token: "t"}}}}}
	_, saveErr := store.Save(ctx, "s", []byte("cp"), "")
	_, _, loadErr := store.Load(ctx, "s")
	_, revErr := store.Revision(ctx, "s")
	_, outcomesErr := store.Outcomes(ctx, "s", []string{"k"})
	_, getErr := secrets.Get(ctx, "s")
	_, headErr := log.Head(ctx, "s")
	errs := map[string]error{
		"setup": db.Setup(ctx), "save": saveErr, "load": loadErr, "revision": revErr,
		"put outcome": store.PutOutcome(ctx, "s", "k", session.Outcome{}), "outcomes": outcomesErr,
		"delete": store.Delete(ctx, "s"), "put secrets": secrets.Put(ctx, "s", bag), "get secrets": getErr,
		"delete secrets": secrets.Delete(ctx, "s"), "append": log.Append(ctx, "s", session.TopicEvents, session.Event{}),
		"head": headErr, "close session": log.CloseSession(ctx, "s"),
	}
	sub, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	events, err := log.Subscribe(sub, "s", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}

	if _, err := postgres.New(pool, postgres.Options{}); badSchema == nil || err != nil {
		t.Errorf("schema not an identifier %v, default schema %v; want the first refused", badSchema, err)
	}
	for name, err := range errs {
		if err == nil {
			t.Errorf("%s on a closed pool succeeded", name)
		}
	}
}

// The listener reconnects after its connection dies and wakes its tails, so
// an event appended while it was not listening still arrives; Close ends
// every tail, and a Subscribe after Close ends at once.
func TestEventLog_aListenerThatLosesItsConnectionCatchesUp(t *testing.T) {
	db, _, _ := testDB(t)
	log := db.EventLog()
	ctx := t.Context()
	events, err := log.Subscribe(ctx, "s", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(ctx, "s", session.TopicEvents, session.Event{Content: "before"}); err != nil {
		t.Fatal(err)
	}
	first := <-events
	if _, err := pgtest.Admin(t).Exec(ctx, `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE query LIKE 'LISTEN %' AND pid <> pg_backend_pid()`); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(ctx, "s", session.TopicEvents, session.Event{Content: "while reconnecting"}); err != nil {
		t.Fatal(err)
	}
	var second session.Event
	select {
	case second = <-events:
	case <-time.After(15 * time.Second):
		t.Fatal("the event appended while the listener reconnected never arrived")
	}
	log.Close()
	_, open := <-events
	after, err := log.Subscribe(ctx, "s", 0)
	if err != nil {
		t.Fatal(err)
	}
	var replayed int
	for range after {
		replayed++
	}
	if first.Content != "before" || second.Content != "while reconnecting" || open {
		t.Fatalf("got %q then %q, open after close %v; want both events and a closed tail", first.Content, second.Content, open)
	}
	if replayed != 2 {
		t.Fatalf("a subscribe after close replayed %d events; want what the log holds, then the end", replayed)
	}
}

// A log longer than one read replays in order across reads.
func TestEventLog_aLongLogReplaysInOrder(t *testing.T) {
	db, _, _ := testDB(t)
	log := db.EventLog()
	t.Cleanup(log.Close)
	ctx := t.Context()
	for n := range 300 {
		if err := log.Append(ctx, "long", session.TopicEvents, session.Event{Content: strconv.Itoa(n)}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := log.Subscribe(ctx, "long", 0)
	if err != nil {
		t.Fatal(err)
	}

	for n := range 300 {
		select {
		case ev := <-events:
			if ev.Content != strconv.Itoa(n) {
				t.Fatalf("event %d is %q; want every event once, in order", n, ev.Content)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("got %d of 300 events", n)
		}
	}
}
