package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
)

// postgresDB is the session tables in a schema of their own, in the shared
// Postgres container.
func postgresDB(t *testing.T) (*postgres.DB, *pgxpool.Pool, string) {
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

// dropTable drops one of the session tables, so the database fails each
// query of that table.
func dropTable(t *testing.T, pool *pgxpool.Pool, schema, table string) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), "DROP TABLE "+schema+"."+table); err != nil {
		t.Fatal(err)
	}
}

// testHarness is session id's harness, built by a plain harness; builds counts
// the builds.
func testHarness(t *testing.T, id session.ID, builds *atomic.Int32) harnessSpec {
	t.Helper()
	model := testkit.HTTPModel(t, nil)
	return harnessSpec{ID: id, Key: "k", Build: func(ctx context.Context) (*tacklr.Harness, *fuse.Server, error) {
		builds.Add(1)
		h, err := tacklr.NewHarness(ctx, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})
		return h, nil, err
	}}
}

func say(text string) tacklr.CommitIn {
	return tacklr.CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: text}}}
}

func events() chan session.Event {
	out := make(chan session.Event, 64)
	go func() {
		for range out {
		}
	}()
	return out
}

// TestHarnesses_aStepFailsWhenItsHarnessOrStoreDoes: a harness that cannot
// be built, a run that fails, a call body that is gone at commit, and each
// store query the database fails, fail their step.
func TestHarnesses_aStepFailsWhenItsHarnessOrStoreDoes(t *testing.T) {
	// Arrange
	type step = func(*harnessCache, session.Store, harnessSpec) error
	boom := errors.New("boom")
	read := func(ws *harnessCache, store session.Store, w harnessSpec) error {
		return ws.Read(t.Context(), store, w, func(*tacklr.Harness) error { return nil })
	}
	commit := func(ws *harnessCache, store session.Store, w harnessSpec) error {
		_, err := ws.Commit(t.Context(), store, w, "", say("hi"), events())
		return err
	}
	call := message.ToolCall{ID: "c", CallID: "c", Name: "nope"}
	runCall := func(c tacklr.Call) step {
		return func(ws *harnessCache, store session.Store, w harnessSpec) error {
			return ws.Read(t.Context(), store, w, func(h *tacklr.Harness) error {
				_, err := adapter.RunCall(t.Context(), store, w.ID, h, c, events())
				return err
			})
		}
	}
	ran := tacklr.Call{Tool: call, Done: true}
	commitGone := func(ws *harnessCache, store session.Store, w harnessSpec) error {
		round := &tacklr.Round{Reply: tacklr.Reply{Calls: []message.ToolCall{call}}, Calls: []tacklr.Call{ran}}
		_, err := ws.Commit(t.Context(), store, w, "", tacklr.CommitIn{Round: round}, events())
		return err
	}
	memory := func(*testing.T) session.Store { return session.NewMemoryStore() }
	// without is a Postgres store whose database no longer has table; saved
	// commits a first prompt before the table goes.
	without := func(table string, saved bool) func(*testing.T) session.Store {
		return func(t *testing.T) session.Store {
			db, pool, schema := postgresDB(t)
			store := db.Store()
			if saved {
				var builds atomic.Int32
				if _, err := newHarnessCache(time.Minute, 0).Commit(t.Context(), store, testHarness(t, "s", &builds), "", say("before"), events()); err != nil {
					t.Fatal(err)
				}
			}
			dropTable(t, pool, schema, table)
			return store
		}
	}
	// unreadable holds a checkpoint without its version row.
	unreadable := func(t *testing.T) session.Store {
		store := session.NewMemoryStore()
		if _, err := store.Save(t.Context(), "s", []session.CheckpointChange{{CheckpointEntry: session.CheckpointEntry{Kind: "state", Key: "k", Value: []byte(`"v"`)}}}, ""); err != nil {
			t.Fatal(err)
		}
		return store
	}
	for name, tc := range map[string]struct {
		store func(*testing.T) session.Store
		build error
		steps []step
	}{
		"a harness that cannot be built":                 {store: memory, build: boom, steps: []step{read}},
		"a run that fails":                               {store: memory, steps: []step{runCall(tacklr.Call{Tool: call, Answer: []byte(`{}`)})}},
		"a call body that is gone at commit":             {store: memory, steps: []step{commitGone}},
		"a snapshot that does not decode":                {store: unreadable, steps: []step{read}},
		"a revision the store cannot read":               {store: without("session_checkpoints", false), steps: []step{read, commit}},
		"a snapshot the store cannot load":               {store: without("session_checkpoint_rows", true), steps: []step{read}},
		"a call body the store cannot keep or read back": {store: without("session_calls", false), steps: []step{runCall(tacklr.NewCall(call, 0)), runCall(ran)}},
	} {
		t.Run(name, func(t *testing.T) {
			store := tc.store(t)
			var builds atomic.Int32
			w := testHarness(t, "s", &builds)
			if tc.build != nil {
				w.Build = func(context.Context) (*tacklr.Harness, *fuse.Server, error) { return nil, nil, tc.build }
			}

			// Act and assert
			for n, step := range tc.steps {
				if err := step(newHarnessCache(time.Minute, 0), store, w); err == nil {
					t.Fatalf("step %d succeeded, want its failure", n)
				}
			}
		})
	}
}

// TestHarnesses_aFailedCommitDropsItsHarness: a Commit whose save fails may
// leave the harness ahead of the store, so the next step builds a new
// harness, which commits.
func TestHarnesses_aFailedCommitDropsItsHarness(t *testing.T) {
	// Arrange
	db, pool, schema := postgresDB(t)
	store := db.Store()
	var builds atomic.Int32
	w := testHarness(t, "s", &builds)
	ws := newHarnessCache(time.Minute, 0)
	dropTable(t, pool, schema, "session_checkpoint_rows")

	// Act
	_, failed := ws.Commit(t.Context(), store, w, "", say("hi"), events())
	if err := db.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err := ws.Commit(t.Context(), store, w, "", say("hi"), events())

	// Assert
	if failed == nil || err != nil || builds.Load() != 2 {
		t.Fatalf("commits = %v then %v after %d builds, want a failure, then a new harness that commits", failed, err, builds.Load())
	}
}

// TestHarnesses_aStaleHarnessAStepStillHoldsIsReplaced: a step the turn
// moved past (a cancelled tool that has not returned) still reads the
// harness when another worker commits. The next step builds a new harness
// at the new revision.
func TestHarnesses_aStaleHarnessAStepStillHoldsIsReplaced(t *testing.T) {
	// Arrange
	store := session.NewMemoryStore()
	var here, there atomic.Int32
	w, other := testHarness(t, "s", &here), testHarness(t, "s", &there)
	ws := newHarnessCache(time.Minute, 0)
	rev, err := ws.Commit(t.Context(), store, w, "", say("one"), events())
	if err != nil {
		t.Fatal(err)
	}
	reading, release := make(chan struct{}), make(chan struct{})
	go func() {
		_ = ws.Read(t.Context(), store, w, func(*tacklr.Harness) error {
			close(reading)
			<-release
			return nil
		})
	}()
	<-reading
	if _, err := newHarnessCache(time.Minute, 0).Commit(t.Context(), store, other, rev, say("two"), events()); err != nil {
		t.Fatal(err)
	}

	// Act
	err = ws.Read(t.Context(), store, w, func(*tacklr.Harness) error { return nil })
	close(release)

	// Assert
	if err != nil || here.Load() != 2 {
		t.Fatalf("read = %v after %d builds, want a new harness built at the new revision", err, here.Load())
	}
}

// TestHarnesses_pastItsLimitTheHarnessUnusedLongestCloses: a worker at its
// harness limit closes the harness unused longest to build a new one. The
// next step of that session builds it again.
func TestHarnesses_pastItsLimitTheHarnessUnusedLongestCloses(t *testing.T) {
	// Arrange
	store := session.NewMemoryStore()
	var aBuilds, bBuilds atomic.Int32
	a, b := testHarness(t, "a", &aBuilds), testHarness(t, "b", &bBuilds)
	ws := newHarnessCache(time.Minute, 1)
	read := func(w harnessSpec) error {
		return ws.Read(t.Context(), store, w, func(*tacklr.Harness) error { return nil })
	}

	// Act
	err := errors.Join(read(a), read(b), read(b), read(a))

	// Assert
	if err != nil || aBuilds.Load() != 2 || bBuilds.Load() != 1 {
		t.Fatalf("reads = %v with a built %d and b built %d times, want a closed for b and built again", err, aBuilds.Load(), bBuilds.Load())
	}
}
