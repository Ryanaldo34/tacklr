package brain_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestSearch_aSessionsMessagesJoinOnlyAnUnfilteredSearch: an engine search
// in a session finds its messages next to the records; a kind or title
// filter keeps only the records it names.
func TestSearch_aSessionsMessagesJoinOnlyAnUnfilteredSearch(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := tenant(t)
	scope := brain.Scope{Namespace: ns, SessionID: "s1"}
	fact := save(t, eng, brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: "fact", Content: "marker"})
	if _, err := eng.SaveSessionMessages(t.Context(), ns, "s1", "k", []brain.SessionMessage{{Role: "tool", SearchText: "marker"}}); err != nil {
		t.Fatal(err)
	}
	kinds := func(req brain.SearchRequest) []string {
		page, err := eng.Search(t.Context(), scope, req)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for o := range page.Results.Objects() {
			out = append(out, o.Kind)
		}
		slices.Sort(out)
		return out
	}

	merged := kinds(brain.SearchRequest{Query: "marker"})
	byKind := kinds(brain.SearchRequest{Query: "marker", Filters: brain.Filter{Kind: brain.StringMatch{Eq: "Fact"}}})
	byTitle := kinds(brain.SearchRequest{Query: "marker", Filters: brain.Filter{Title: brain.StringMatch{Eq: fact.Title}}})

	if !slices.Equal(merged, []string{"Fact", brain.KindSessionMessage}) {
		t.Errorf("search the session = %v, want the fact and the message", merged)
	}
	if !slices.Equal(byKind, []string{"Fact"}) || !slices.Equal(byTitle, []string{"Fact"}) {
		t.Errorf("by kind = %v, by title = %v, want the fact alone", byKind, byTitle)
	}
}

// TestEngine_sessionWritesNeedASessionAndALiveContext: deleting a blank
// session is invalid, and a cancelled save or delete is Canceled.
func TestEngine_sessionWritesNeedASessionAndALiveContext(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", uuid.NewString())
	stopped, stop := context.WithCancel(t.Context())
	stop()

	blankErr := eng.DeleteSessionMessages(t.Context(), ns, " ")
	deleteErr := eng.DeleteSessionMessages(stopped, ns, "s1")
	_, saveErr := eng.SaveSessionMessages(stopped, ns, "s1", "k", []brain.SessionMessage{{Role: "user"}})

	if !errors.Is(blankErr, brain.ErrInvalid) {
		t.Errorf("delete a blank session = %v, want ErrInvalid", blankErr)
	}
	if !errors.Is(deleteErr, context.Canceled) || !errors.Is(saveErr, context.Canceled) {
		t.Errorf("cancelled delete = %v, save = %v, want Canceled", deleteErr, saveErr)
	}
}

// TestMemoryStore_aZeroStoreSavesSessionMessages: the zero MemoryStore saves
// a session's window.
func TestMemoryStore_aZeroStoreSavesSessionMessages(t *testing.T) {
	t.Parallel()
	var store brain.MemoryStore

	gen, err := store.SaveSessionMessages(t.Context(), tenant(t), "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "hi"}})

	if err != nil || gen != 1 {
		t.Errorf("save to a zero MemoryStore = %d %v, want generation 1", gen, err)
	}
}
