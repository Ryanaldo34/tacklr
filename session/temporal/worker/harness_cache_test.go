package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
)

// failingStore is a session.Store whose named operations fail. unreadable
// returns a checkpoint that does not decode.
type failingStore struct {
	*session.MemoryStore
	revision, load, save, put, calls error
	unreadable                       bool
}

func (s *failingStore) PutCall(ctx context.Context, id session.ID, key string, body []byte) error {
	if s.put != nil {
		return s.put
	}
	return s.MemoryStore.PutCall(ctx, id, key, body)
}

func (s *failingStore) Calls(ctx context.Context, id session.ID, keys []string) (map[string][]byte, error) {
	if s.calls != nil {
		return nil, s.calls
	}
	return s.MemoryStore.Calls(ctx, id, keys)
}

func (s *failingStore) Revision(ctx context.Context, id session.ID) (session.Revision, error) {
	if s.revision != nil {
		return "", s.revision
	}
	return s.MemoryStore.Revision(ctx, id)
}

func (s *failingStore) Load(ctx context.Context, id session.ID) ([]session.CheckpointEntry, session.Revision, error) {
	if s.load != nil {
		return nil, "", s.load
	}
	cp, rev, err := s.MemoryStore.Load(ctx, id)
	if s.unreadable {
		cp = nil
	}
	return cp, rev, err
}

func (s *failingStore) Save(ctx context.Context, id session.ID, cp []session.CheckpointChange, expected session.Revision) (session.Revision, error) {
	if s.save != nil {
		return "", s.save
	}
	return s.MemoryStore.Save(ctx, id, cp, expected)
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

// Each failure fails its step.
func TestHarnesses_aStepFailsWhenItsHarnessOrStoreDoes(t *testing.T) {
	boom := errors.New("boom")
	read := func(ws *harnessCache, store session.Store, w harnessSpec) error {
		return ws.Read(t.Context(), store, w, func(*tacklr.Harness) error { return nil })
	}
	commit := func(ws *harnessCache, store session.Store, w harnessSpec) error {
		_, err := ws.Commit(t.Context(), store, w, "", say("hi"), events())
		return err
	}
	call := message.ToolCall{ID: "c", CallID: "c", Name: "nope"}
	runCall := func(c tacklr.Call) func(*harnessCache, session.Store, harnessSpec) error {
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
	for name, tc := range map[string]struct {
		store *failingStore
		saved bool
		build error
		step  func(*harnessCache, session.Store, harnessSpec) error
	}{
		"a harness that cannot be built":             {store: &failingStore{}, build: boom, step: read},
		"a revision the store cannot read":           {store: &failingStore{revision: boom}, step: read},
		"a revision the store cannot read at commit": {store: &failingStore{revision: boom}, step: commit},
		"a snapshot the store cannot load":           {store: &failingStore{load: boom}, saved: true, step: read},
		"a snapshot that does not decode":            {store: &failingStore{unreadable: true}, saved: true, step: read},
		"a call body the store cannot keep":          {store: &failingStore{put: boom}, step: runCall(tacklr.NewCall(call, 0))},
		"a call body the store cannot read back":     {store: &failingStore{calls: boom}, step: runCall(ran)},
		"a call body that is gone at commit":         {store: &failingStore{}, step: commitGone},
		"a run that fails":                           {store: &failingStore{}, step: runCall(tacklr.Call{Tool: call, Answer: []byte(`{}`)})},
	} {
		t.Run(name, func(t *testing.T) {
			tc.store.MemoryStore = session.NewMemoryStore()
			var builds atomic.Int32
			if tc.saved {
				if _, err := newHarnessCache(time.Minute, 0).Commit(t.Context(), tc.store.MemoryStore, testHarness(t, "s", &builds), "", say("before"), events()); err != nil {
					t.Fatal(err)
				}
			}
			w := testHarness(t, "s", &builds)
			if tc.build != nil {
				w.Build = func(context.Context) (*tacklr.Harness, *fuse.Server, error) { return nil, nil, tc.build }
			}

			err := tc.step(newHarnessCache(time.Minute, 0), tc.store, w)

			if err == nil {
				t.Fatal("step succeeded, want its failure")
			}
		})
	}
}

// A Commit whose save fails may leave the harness ahead of the store, so the
// next step builds a new harness.
func TestHarnesses_aFailedCommitDropsItsHarness(t *testing.T) {
	store := &failingStore{MemoryStore: session.NewMemoryStore(), save: errors.New("boom")}
	var builds atomic.Int32
	w := testHarness(t, "s", &builds)
	ws := newHarnessCache(time.Minute, 0)

	_, failed := ws.Commit(t.Context(), store, w, "", say("hi"), events())
	store.save = nil
	_, err := ws.Commit(t.Context(), store, w, "", say("hi"), events())

	if failed == nil || err != nil || builds.Load() != 2 {
		t.Fatalf("commits = %v then %v after %d builds, want a failure, then a new harness that commits", failed, err, builds.Load())
	}
}

// A step the turn moved past (a cancelled tool that has not returned) still
// reads the harness when another worker commits. The next step does not wait
// for it: it builds a new harness at the new revision.
func TestHarnesses_aStaleHarnessAStepStillHoldsIsReplaced(t *testing.T) {
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

	err = ws.Read(t.Context(), store, w, func(*tacklr.Harness) error { return nil })
	close(release)

	if err != nil || here.Load() != 2 {
		t.Fatalf("read = %v after %d builds, want a new harness built at the new revision", err, here.Load())
	}
}

// A worker at its harness limit closes the harness unused longest to build a
// new one; the next step of that session builds it again.
func TestHarnesses_pastItsLimitTheHarnessUnusedLongestCloses(t *testing.T) {
	store := session.NewMemoryStore()
	var aBuilds, bBuilds atomic.Int32
	a, b := testHarness(t, "a", &aBuilds), testHarness(t, "b", &bBuilds)
	ws := newHarnessCache(time.Minute, 1)
	read := func(w harnessSpec) error {
		return ws.Read(t.Context(), store, w, func(*tacklr.Harness) error { return nil })
	}

	err := errors.Join(read(a), read(b), read(b), read(a))

	if err != nil || aBuilds.Load() != 2 || bBuilds.Load() != 1 {
		t.Fatalf("reads = %v with a built %d and b built %d times, want a closed for b and built again", err, aBuilds.Load(), bBuilds.Load())
	}
}
