package session

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

// checkpointOf is a checkpoint holding s, so a test can tell saves apart.
func checkpointOf(t *testing.T, s string) tacklr.SessionCheckpoint {
	t.Helper()
	var cp tacklr.SessionCheckpoint
	if err := json.Unmarshal([]byte(strconv.Quote(s)), &cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func holds(cp tacklr.SessionCheckpoint) string {
	raw, _ := json.Marshal(cp)
	s, _ := strconv.Unquote(string(raw))
	return s
}

func TestMemorySnapshot_revisionRejectsStaleSave(t *testing.T) {
	store := NewMemorySnapshot()
	id := SessionID("s")
	rev, err := store.Save(t.Context(), id, checkpointOf(t, "a"), "")
	if err != nil || rev == "" {
		t.Fatal(err)
	}
	if _, err := store.Save(t.Context(), id, checkpointOf(t, "b"), ""); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("stale: %v", err)
	}
	rev2, err := store.Save(t.Context(), id, checkpointOf(t, "b"), rev)
	if err != nil {
		t.Fatal(err)
	}
	snap, got, err := store.Load(t.Context(), id)
	if err != nil || holds(snap) != "b" || got != rev2 {
		t.Fatalf("load %+v rev %q err %v", snap, got, err)
	}
	if err := store.Delete(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(t.Context(), id); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("deleted: %v", err)
	}
}

func TestMemorySnapshot_concurrentSaveOneWins(t *testing.T) {
	store := NewMemorySnapshot()
	id := SessionID("s")
	rev, err := store.Save(t.Context(), id, checkpointOf(t, "base"), "")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var wins, stale int
	for _, name := range []string{"left", "right"} {
		cp := checkpointOf(t, name)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Save(t.Context(), id, cp, rev)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
				return
			}
			if errors.Is(err, ErrStaleCheckpoint) {
				stale++
			}
		}()
	}
	wg.Wait()
	if wins != 1 || stale != 1 {
		t.Fatalf("wins=%d stale=%d", wins, stale)
	}
	snap, _, err := store.Load(t.Context(), id)
	if err != nil || (holds(snap) != "left" && holds(snap) != "right") {
		t.Fatalf("winner %+v err %v", snap, err)
	}
}
