package postgres_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestSessionMessages_aSessionsWindowJoinsItsSearchesUntilDeleted: a saved
// window is found and opened next to the records, by its words, a quoted
// token and a blank body; a retried save keeps its generation, another
// session and a kind filter see only the records, and a delete starts the
// generations again. A window with a malformed body saves nothing, and a save
// needs a session, a generation key and messages.
func TestSessionMessages_aSessionsWindowJoinsItsSearchesUntilDeleted(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, _ := testDBWith(t, postgres.Options{Embedding: brain.Embedding{Embedder: &storetest.Embedder{}}})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", "acme")
	const (
		sessionID = "sess.with/dots"
		marker    = "zebra"
		token     = "onlyinsession"
	)
	scope := brain.Scope{Namespace: ns, SessionID: sessionID}
	msg := []brain.SessionMessage{{Role: "tool", Body: []byte(`{"role":"tool","content":"` + token + `"}`), SearchText: marker + " " + token}}
	malformed := []brain.SessionMessage{
		{Role: "user", Body: []byte(`{"role":"user","content":"ok"}`), SearchText: "ok"},
		{Role: "user", Body: []byte(`{not json`), SearchText: "broken"},
	}
	fact, err := eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: "fact", Content: "durable"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Fact", Title: "chunk", Content: marker + " durablefact", ParentID: &fact.ID, Position: new(1),
	}); err != nil {
		t.Fatal(err)
	}
	search := func(scope brain.Scope, query string, filter brain.Filter) []brain.RichObject {
		page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: query, Filters: filter})
		if err != nil {
			t.Fatal(err)
		}
		return slices.Collect(page.Results.Objects())
	}
	saveErr := func(_ int, err error) error { return err }

	invalid := map[string]error{
		"no session":       saveErr(store.SaveSessionMessages(ctx, ns, "", "k", msg)),
		"no key":           saveErr(store.SaveSessionMessages(ctx, ns, sessionID, "", msg)),
		"no messages":      saveErr(store.SaveSessionMessages(ctx, ns, sessionID, "k", nil)),
		"a malformed body": saveErr(store.SaveSessionMessages(ctx, ns, sessionID, "bad", malformed)),
	}
	blankGen, blankErr := eng.SaveSessionMessages(ctx, ns, sessionID, "blank", []brain.SessionMessage{{Role: "user", SearchText: "blankbody"}})
	gen, genErr := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	again, againErr := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	merged := search(scope, marker, brain.Filter{})
	exact := search(scope, `"`+token+`"`, brain.Filter{})
	blank := search(scope, "blankbody", brain.Filter{})
	byKind := search(scope, marker, storetest.Filter(t, map[string]any{"kind": "Fact"}))
	other := search(brain.Scope{Namespace: ns, SessionID: "other"}, marker, brain.Filter{})
	// The store also ranks by meaning, so a search finds every row; the
	// message is the one quoted with its token.
	var saved brain.RichObject
	for _, o := range merged {
		if o.Kind == brain.KindSessionMessage && slices.ContainsFunc(o.Evidence, func(ev brain.Evidence) bool { return strings.Contains(ev.Snippet, token) }) {
			saved = o
		}
	}
	opened, openErr := eng.Open(ctx, scope, saved.ID)
	_, missingErr := eng.Open(ctx, scope, uuid.New())
	deleteErr := eng.DeleteSessionMessages(ctx, ns, sessionID)
	afterDelete := search(scope, marker, brain.Filter{})
	restart, restartErr := eng.SaveSessionMessages(ctx, ns, sessionID, "key-3", msg)

	for name, err := range invalid {
		if err == nil {
			t.Errorf("save with %s succeeded, want an error", name)
		}
	}
	// The malformed window left no generation behind.
	if blankErr != nil || blankGen != 1 || genErr != nil || gen != 2 || againErr != nil || again != gen {
		t.Errorf("generations = %d %v, then %d %v, retry %d %v, want 1, 2 and 2", blankGen, blankErr, gen, genErr, again, againErr)
	}
	if !slices.ContainsFunc(merged, func(o brain.RichObject) bool { return o.ID == fact.ID }) || saved.ID == uuid.Nil {
		t.Errorf("search the session = %+v, want the fact and the message, quoted", merged)
	}
	if openErr != nil || opened.Object.Kind != brain.KindSessionMessage || !strings.Contains(opened.Object.Content, token) || !errors.Is(missingErr, brain.ErrNotFound) {
		t.Errorf("open the message = %+v %v, an unknown id = %v, want the message's body and ErrNotFound", opened.Object, openErr, missingErr)
	}
	if len(exact) == 0 || exact[0].Kind != brain.KindSessionMessage || len(blank) == 0 || blank[0].Kind != brain.KindSessionMessage || blank[0].Title != "user" {
		t.Errorf("exact token = %+v, blank body = %+v, want the messages", exact, blank)
	}
	for name, got := range map[string][]brain.RichObject{"a kind filter": byKind, "another session": other, "the session after delete": afterDelete} {
		if len(got) != 1 || got[0].ID != fact.ID {
			t.Errorf("search with %s = %+v, want the fact alone", name, got)
		}
	}
	if deleteErr != nil || restartErr != nil || restart != 1 {
		t.Errorf("delete = %v, next generation %d %v, want 1", deleteErr, restart, restartErr)
	}
}
