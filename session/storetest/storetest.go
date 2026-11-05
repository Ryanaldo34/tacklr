// Package storetest is the contract every session Store, SecretStorage and
// EventLog must meet. Run it from a store's own tests (the memory stores and
// session/postgres do), so a new store cannot ship without the same
// compare-and-swap, outcome, secret and event-order rules.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// Store runs the Store contract against a fresh store from newStore.
func Store(t *testing.T, newStore func(t *testing.T) session.Store) {
	t.Run("a save applies its changes on the revision of the last load, and one of two saves from it wins", func(t *testing.T) {
		ctx, store, id := t.Context(), newStore(t), session.ID("s")
		_, noID := store.Save(ctx, "", []session.CheckpointChange{put("a", "", "a")}, "")
		before, beforeErr := store.Revision(ctx, id)
		_, _, unknown := store.Load(ctx, id)
		rev, err := store.Save(ctx, id, []session.CheckpointChange{put("message", "0", "a"), put("message", "1", "b"), put("state", "k/1", "v")}, "")
		if err != nil {
			t.Fatal(err)
		}
		_, again := store.Save(ctx, id, []session.CheckpointChange{put("message", "0", "x")}, "")
		_, foreign := store.Save(ctx, id, []session.CheckpointChange{put("message", "0", "x")}, "not-a-revision")
		saves := map[string][]session.CheckpointChange{
			"left":  {put("message", "1", "left"), remove("state", "k/1")},
			"right": {put("message", "2", "right")},
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		var winner string
		var lost int
		for name, changes := range saves {
			wg.Go(func() {
				_, err := store.Save(ctx, id, changes, rev)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					winner = name
				case errors.Is(err, session.ErrStaleCheckpoint):
					lost++
				}
			})
		}
		wg.Wait()
		rows, loaded, loadErr := store.Load(ctx, id)
		current, _ := store.Revision(ctx, id)
		want := map[string]map[session.CheckpointRow]string{
			"left":  {{Kind: "message", Key: "0"}: "a", {Kind: "message", Key: "1"}: "left"},
			"right": {{Kind: "message", Key: "0"}: "a", {Kind: "message", Key: "1"}: "b", {Kind: "message", Key: "2"}: "right", {Kind: "state", Key: "k/1"}: "v"},
		}[winner]

		if noID == nil || before != "" || beforeErr != nil || !errors.Is(unknown, session.ErrNotFound) {
			t.Fatalf("save without id %v, revision before a save %q %v, load before %v; want an error, no revision, not found", noID, before, beforeErr, unknown)
		}
		if !errors.Is(again, session.ErrStaleCheckpoint) || !errors.Is(foreign, session.ErrStaleCheckpoint) {
			t.Fatalf("a second first save %v, a save from a revision the store never issued %v; want both stale", again, foreign)
		}
		if winner == "" || lost != 1 || loadErr != nil || !maps.Equal(rowMap(rows), want) || loaded != current || loaded == rev {
			t.Fatalf("winner %q, lost %d, loaded %v at %s (current %s, base %s) %v; want the base with only the winner's changes, at a new current revision", winner, lost, rowMap(rows), loaded, current, rev, loadErr)
		}
	})

	t.Run("a call's body is replaced by a later put, a clear drops calls only, and delete removes the session", func(t *testing.T) {
		ctx, store, id := t.Context(), newStore(t), session.ID("s")
		if _, err := store.Save(ctx, id, []session.CheckpointChange{put("message", "0", "cp")}, ""); err != nil {
			t.Fatal(err)
		}
		for _, body := range []string{"first", "second"} {
			if err := store.PutCall(ctx, id, "call", []byte(body)); err != nil {
				t.Fatal(err)
			}
		}
		got, err := store.Calls(ctx, id, []string{"call", "unknown"})
		none, noneErr := store.Calls(ctx, id, nil)
		if err := store.ClearCalls(ctx, id); err != nil {
			t.Fatal(err)
		}
		cleared, _ := store.Calls(ctx, id, []string{"call"})
		_, _, keptErr := store.Load(ctx, id)
		if err := store.PutCall(ctx, id, "next", []byte("next")); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
		_, _, deleted := store.Load(ctx, id)
		rev, _ := store.Revision(ctx, id)
		gone, _ := store.Calls(ctx, id, []string{"next"})

		if err != nil || len(got) != 1 || string(got["call"]) != "second" {
			t.Fatalf("calls %q %v; want the later body only", got, err)
		}
		if len(none) != 0 || noneErr != nil {
			t.Fatalf("calls of no keys %v %v; want none", none, noneErr)
		}
		if len(cleared) != 0 || keptErr != nil {
			t.Fatalf("after a clear: calls %v, load %v; want no calls and the snapshot kept", cleared, keptErr)
		}
		if !errors.Is(deleted, session.ErrNotFound) || rev != "" || len(gone) != 0 {
			t.Fatalf("after delete: load %v, revision %q, calls %v; want nothing left", deleted, rev, gone)
		}
	})
}

// Secrets runs the SecretStorage contract against a fresh storage from
// newSecrets.
func Secrets(t *testing.T, newSecrets func(t *testing.T) session.SecretStorage) {
	t.Run("a bag with bindings replaces the session's, and nothing aliases it", func(t *testing.T) {
		ctx, s, id := t.Context(), newSecrets(t), session.ID("sess")
		expires := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
		bag := func(token string) session.Secrets {
			return session.Secrets{Auth: session.AuthContext{Bindings: []mount.Binding{{
				Provider: mount.ProviderGoogleDrive,
				Params:   map[string]string{mount.ParamName: "docs"},
				Auth:     mount.Credential{Token: token, ExpiresAt: expires},
			}}}}
		}
		noID := s.Put(ctx, "", bag("x"))
		unknown, unknownErr := s.Get(ctx, id)
		in := bag("tok-1")
		if err := s.Put(ctx, id, in); err != nil {
			t.Fatal(err)
		}
		in.Auth.Bindings[0].Auth.Token = "mutated"
		in.Auth.Bindings[0].Params[mount.ParamName] = "other"
		first, err := s.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		first.Auth.Bindings[0].Params[mount.ParamName] = "from-get"
		again, _ := s.Get(ctx, id)
		if err := s.Put(ctx, id, bag("tok-2")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, id, session.Secrets{Auth: session.AuthContext{Drop: []string{"docs"}}}); err != nil {
			t.Fatal(err)
		}
		replaced, _ := s.Get(ctx, id)
		if err := s.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
		deleted, deletedErr := s.Get(ctx, id)

		if noID == nil || len(unknown.Auth.Bindings) != 0 || unknownErr != nil {
			t.Fatalf("put without id %v, unknown session %+v %v; want an error, then an empty bag", noID, unknown, unknownErr)
		}
		b := again.Auth.Bindings[0]
		if b.Auth.Token != "tok-1" || b.Params[mount.ParamName] != "docs" || !b.Auth.ExpiresAt.Equal(expires) {
			t.Fatalf("get after put %+v; want the bag as put, unchanged by edits to the input or an earlier get", b)
		}
		if replaced.Auth.Bindings[0].Auth.Token != "tok-2" {
			t.Fatalf("after a replace and a drop-only put %+v; want the replacing bag", replaced)
		}
		if len(deleted.Auth.Bindings) != 0 || deletedErr != nil {
			t.Fatalf("after delete %+v %v; want an empty bag", deleted, deletedErr)
		}
	})
}

// EventLog runs the EventLog contract against a fresh log from newLog.
func EventLog(t *testing.T, newLog func(t *testing.T) session.EventLog) {
	// The cancelled subscriber never reads: more events than it buffers keep
	// it waiting to send, and Append goes on without it.
	t.Run("a subscriber replays past its offset without retries, then tails until the session closes", func(t *testing.T) {
		ctx, log, id := t.Context(), newLog(t), session.ID("s")
		head, headErr := log.Head(ctx, "unknown")
		for _, topic := range []string{session.TopicEvents, session.TopicRetry, session.TopicEvents} {
			appendEvent(t, log, id, topic, session.Event{Content: topic})
		}
		live, err := log.Subscribe(ctx, id, 1)
		if err != nil {
			t.Fatal(err)
		}
		cctx, cancel := context.WithCancel(ctx)
		cancelled, err := log.Subscribe(cctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		replayed := receive(t, live, 1)
		for n := range 70 {
			appendEvent(t, log, id, session.TopicEvents, session.Event{Content: fmt.Sprint(n)})
		}
		appendEvent(t, log, id, session.TopicEvents, session.FailureEvent(session.ErrModelRefused))
		tailed := receive(t, live, 71)
		atHead, _ := log.Head(ctx, id)
		cancel()
		drain(t, cancelled)
		if err := log.CloseSession(ctx, id); err != nil {
			t.Fatal(err)
		}
		drain(t, live)
		reused, _ := log.Head(ctx, id)
		unknownClose := log.CloseSession(ctx, "unknown")

		if head != 0 || headErr != nil || replayed[0].Content != session.TopicEvents {
			t.Fatalf("head of an unknown session %d %v, replayed %+v; want 0, then the event past the offset", head, headErr, replayed)
		}
		want := make([]string, 70)
		for n := range want {
			want[n] = fmt.Sprint(n)
		}
		if got := contents(tailed[:70]); !slices.Equal(got, want) {
			t.Fatalf("tailed %v; want every appended event once, in order", got)
		}
		if failure := tailed[70]; !errors.Is(failure.FailKind.Err(failure.Fail), session.ErrModelRefused) {
			t.Fatalf("failure event %+v; want the refusal it carried", failure)
		}
		if atHead != 74 || reused != 0 || unknownClose != nil {
			t.Fatalf("head %d, after close %d, closing an unknown session %v; want every append counted (retries too), then a fresh log", atHead, reused, unknownClose)
		}
	})

	t.Run("concurrent appends are numbered without gaps, and a late subscriber gets the rest in order", func(t *testing.T) {
		ctx, log, id := t.Context(), newLog(t), session.ID("c")
		var wg sync.WaitGroup
		for n := range 40 {
			wg.Go(func() { appendEvent(t, log, id, session.TopicEvents, session.Event{Content: fmt.Sprint(n)}) })
		}
		wg.Wait()
		head, _ := log.Head(ctx, id)
		all, err := log.Subscribe(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		concurrent := contents(receive(t, all, 40))
		late, err := log.Subscribe(ctx, id, head)
		if err != nil {
			t.Fatal(err)
		}
		for n := range 10 {
			appendEvent(t, log, id, session.TopicEvents, session.Event{Content: fmt.Sprint("late ", n)})
		}
		later := contents(receive(t, late, 10))

		slices.Sort(concurrent)
		if head != 40 || len(slices.Compact(concurrent)) != 40 {
			t.Fatalf("head %d, distinct events %d; want 40 and 40", head, len(concurrent))
		}
		for n, c := range later {
			if c != fmt.Sprint("late ", n) {
				t.Fatalf("late subscriber got %v; want the later events once, in order", later)
			}
		}
	})

	t.Run("a trim drops the events through its offset and keeps the numbering", func(t *testing.T) {
		ctx, log, id := t.Context(), newLog(t), session.ID("t")
		for n := range 5 {
			appendEvent(t, log, id, session.TopicEvents, session.Event{Content: fmt.Sprint(n + 1)})
		}
		if err := log.Trim(ctx, id, 3); err != nil {
			t.Fatal(err)
		}
		unknownTrim := log.Trim(ctx, "unknown", 3)
		appendEvent(t, log, id, session.TopicEvents, session.Event{Content: "6"})
		head, _ := log.Head(ctx, id)
		fromStart, err := log.Subscribe(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		kept := contents(receive(t, fromStart, 3))
		fromFive, err := log.Subscribe(ctx, id, 5)
		if err != nil {
			t.Fatal(err)
		}
		after := contents(receive(t, fromFive, 1))

		if head != 6 || unknownTrim != nil || !slices.Equal(kept, []string{"4", "5", "6"}) || !slices.Equal(after, []string{"6"}) {
			t.Fatalf("head %d, trim of an unknown session %v, from the start %v, after 5 %v; want 6, nil, [4 5 6], [6]", head, unknownTrim, kept, after)
		}
	})
}

func put(kind, key, value string) session.CheckpointChange {
	return session.CheckpointChange{CheckpointEntry: session.CheckpointEntry{Kind: kind, Key: key, Value: []byte(value)}}
}

func remove(kind, key string) session.CheckpointChange {
	return session.CheckpointChange{CheckpointEntry: session.CheckpointEntry{Kind: kind, Key: key}, Delete: true}
}

func rowMap(rows []session.CheckpointEntry) map[session.CheckpointRow]string {
	out := make(map[session.CheckpointRow]string, len(rows))
	for _, r := range rows {
		out[session.CheckpointRow{Kind: r.Kind, Key: r.Key}] = string(r.Value)
	}
	return out
}

func appendEvent(t *testing.T, log session.EventLog, id session.ID, topic string, ev session.Event) {
	t.Helper()
	if err := log.Append(t.Context(), id, topic, ev); err != nil {
		t.Error(err)
	}
}

// receive reads n events from ch, failing the test when they do not come.
func receive(t *testing.T, ch <-chan session.Event, n int) []session.Event {
	t.Helper()
	out := make([]session.Event, 0, n)
	timeout := time.After(20 * time.Second)
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("subscription closed after %d of %d events", len(out), n)
			}
			out = append(out, ev)
		case <-timeout:
			t.Fatalf("got %d of %d events", len(out), n)
		}
	}
	return out
}

// drain waits for ch to close, failing the test when it does not.
func drain(t *testing.T, ch <-chan session.Event) {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("subscription did not close")
		}
	}
}

func contents(evs []session.Event) []string {
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = ev.Content
	}
	return out
}
