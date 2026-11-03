package session

import (
	"errors"
	"sync"
	"testing"
)

// checkpointOf is a checkpoint holding s, so a test can tell saves apart.
func checkpointOf(t *testing.T, s string) []byte {
	t.Helper()
	return []byte(s)
}

func holds(cp []byte) string { return string(cp) }

// TestMemorySnapshot_keepsASessionsStateByCompareAndSwap: a save needs a
// session id and the revision of the last load; of two saves from one
// revision one wins; a call's outcome record is replaced by a later Put of
// its key, and an unknown key is absent; Delete removes the snapshot and
// the records.
func TestMemorySnapshot_keepsASessionsStateByCompareAndSwap(t *testing.T) {
	store := NewMemoryStore()
	id := ID("s")
	_, noID := store.Save(t.Context(), "", checkpointOf(t, "a"), "")
	empty, emptyErr := store.Revision(t.Context(), id)
	rev, err := store.Save(t.Context(), id, checkpointOf(t, "base"), "")
	if err != nil {
		t.Fatal(err)
	}
	_, stale := store.Save(t.Context(), id, checkpointOf(t, "b"), "")
	var wg sync.WaitGroup
	var mu sync.Mutex
	var wins, lost int
	for _, name := range []string{"left", "right"} {
		wg.Go(func() {
			_, err := store.Save(t.Context(), id, checkpointOf(t, name), rev)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
			} else if errors.Is(err, ErrStaleCheckpoint) {
				lost++
			}
		})
	}
	wg.Wait()
	snap, loaded, loadErr := store.Load(t.Context(), id)
	current, _ := store.Revision(t.Context(), id)
	for _, rec := range []string{"first", "second"} {
		if err := store.PutOutcome(t.Context(), id, "call", Outcome{Record: []byte(rec)}); err != nil {
			t.Fatal(err)
		}
	}
	records, _ := store.Outcomes(t.Context(), id, []string{"call", "unknown"})
	if err := store.Delete(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	_, _, deleted := store.Load(t.Context(), id)
	gone, _ := store.Outcomes(t.Context(), id, []string{"call"})

	if noID == nil || empty != "" || emptyErr != nil || !errors.Is(stale, ErrStaleCheckpoint) {
		t.Fatalf("no id %v, revision before a save %q %v, stale %v; want errors for both saves and no revision", noID, empty, emptyErr, stale)
	}
	if wins != 1 || lost != 1 || loadErr != nil || (holds(snap) != "left" && holds(snap) != "right") || loaded != current {
		t.Fatalf("wins %d lost %d, loaded %q at %s (current %s) %v; want one winner loaded at the current revision", wins, lost, holds(snap), loaded, current, loadErr)
	}
	if string(records["call"].Record) != "second" || len(records) != 1 || !errors.Is(deleted, ErrNotFound) || len(gone) != 0 {
		t.Fatalf("records %q, after delete %v %q; want the later record only, then nothing", records, deleted, gone)
	}
}
