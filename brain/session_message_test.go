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

// TestSessionMessages_saveOnceSearchWithRecordsAndDelete: a session's saved
// window is found next to the records, a retried save keeps its generation,
// another session sees only the records, a kind or title filter keeps only
// the records it names, and a delete starts the generations again. A save
// needs a session, a generation key and messages, and a zero MemoryStore
// saves too.
func TestSessionMessages_saveOnceSearchWithRecordsAndDelete(t *testing.T) {
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", uuid.NewString())
	ctx := t.Context()
	const (
		sessionID = "sess.with.dots"
		marker    = "message-marker"
	)
	scope := brain.Scope{Namespace: ns, SessionID: sessionID}
	stopped, stop := context.WithCancel(ctx)
	stop()
	fact := save(t, eng, brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: "fact", Content: "durable"})
	save(t, eng, brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", ParentID: &fact.ID, Title: "chunk", Content: marker + " durable-fact"})
	msg := []brain.SessionMessage{{Role: "tool", Body: []byte(`{"role":"tool","content":"` + marker + `"}`), SearchText: marker}}
	search := func(req brain.SearchRequest) []brain.RichObject {
		page, err := eng.Search(ctx, scope, req)
		if err != nil {
			t.Fatal(err)
		}
		return slices.Collect(page.Results.Objects())
	}
	kinds := func(objs []brain.RichObject) []string {
		out := make([]string, len(objs))
		for i, o := range objs {
			out[i] = o.Kind
		}
		slices.Sort(out)
		return out
	}

	gen, genErr := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	again, againErr := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	next, nextErr := eng.SaveSessionMessages(ctx, ns, sessionID, "key-2", []brain.SessionMessage{{Role: "user", SearchText: "later-window"}})
	merged := search(brain.SearchRequest{Query: marker})
	otherSession, otherErr := eng.Search(ctx, brain.Scope{Namespace: ns, SessionID: "other"}, brain.SearchRequest{Query: marker})
	byKind := search(brain.SearchRequest{Query: marker, Filters: brain.Filter{Kind: brain.StringMatch{Eq: "Fact"}}})
	byTitle := search(brain.SearchRequest{Query: marker, Filters: brain.Filter{Title: brain.StringMatch{Eq: "fact"}}})
	deleteErr := eng.DeleteSessionMessages(ctx, ns, sessionID)
	afterDelete := search(brain.SearchRequest{Query: marker})
	restart, restartErr := eng.SaveSessionMessages(ctx, ns, sessionID, "key-3", msg)
	_, zeroErr := (&brain.MemoryStore{}).SaveSessionMessages(ctx, ns, sessionID, "k", msg)
	invalid := map[string]error{
		"blank session delete":  eng.DeleteSessionMessages(ctx, ns, " "),
		"cancelled delete":      eng.DeleteSessionMessages(stopped, ns, sessionID),
		"save with no session":  saveErr(store.SaveSessionMessages(ctx, ns, "", "k", msg)),
		"save with no key":      saveErr(store.SaveSessionMessages(ctx, ns, sessionID, " ", msg)),
		"save with no messages": saveErr(store.SaveSessionMessages(ctx, ns, sessionID, "k", nil)),
		"cancelled save":        saveErr(eng.SaveSessionMessages(stopped, ns, sessionID, "k", msg)),
	}

	if genErr != nil || againErr != nil || again != gen || nextErr != nil || next != gen+1 {
		t.Errorf("generations = %d %v, retry %d %v, next %d %v, want the retry to keep its generation", gen, genErr, again, againErr, next, nextErr)
	}
	if got := kinds(merged); !slices.Equal(got, []string{"Fact", brain.KindSessionMessage}) {
		t.Errorf("search the session = %v, want the fact and the message", got)
	}
	if got := slices.Collect(otherSession.Results.Objects()); otherErr != nil || !slices.Equal(idsOf(got), []uuid.UUID{fact.ID}) {
		t.Errorf("search another session = %v %v, want the fact alone", titlesOf(got), otherErr)
	}
	if !slices.Equal(idsOf(byKind), []uuid.UUID{fact.ID}) || !slices.Equal(idsOf(byTitle), []uuid.UUID{fact.ID}) {
		t.Errorf("by kind = %v, by title = %v, want the fact alone", titlesOf(byKind), titlesOf(byTitle))
	}
	if deleteErr != nil || !slices.Equal(idsOf(afterDelete), []uuid.UUID{fact.ID}) || restartErr != nil || restart != 1 {
		t.Errorf("after delete = %v %v, next generation %d %v, want the fact alone and generation 1", titlesOf(afterDelete), deleteErr, restart, restartErr)
	}
	if zeroErr != nil {
		t.Errorf("save to a zero MemoryStore = %v", zeroErr)
	}
	for name, err := range invalid {
		if err == nil {
			t.Errorf("%s succeeded, want an error", name)
		}
	}
	if err := invalid["blank session delete"]; !errors.Is(err, brain.ErrInvalid) {
		t.Errorf("blank session delete = %v, want ErrInvalid", err)
	}
}

// saveErr is the error of a session save.
func saveErr(_ int, err error) error { return err }
