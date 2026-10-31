package postgres_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

func TestSessionMessages_liveSearchUnion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := testDBWith(t, postgres.Options{Embedding: brain.Embedding{Embedder: &storetest.Embedder{}}})
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", "acme")
	const (
		sessionID = "sess.with/dots"
		marker    = "zebra"
		token     = "onlyinsession"
	)
	scope := brain.Scope{Namespace: ns, SessionID: sessionID}
	msg := []brain.SessionMessage{{
		Role:       "tool",
		Body:       []byte(`{"role":"tool","content":"` + token + `"}`),
		SearchText: marker + " " + token,
	}}
	if _, err := store.SaveSessionMessages(ctx, ns, "", "k", msg); err == nil {
		t.Fatal("session id")
	}
	if _, err := store.SaveSessionMessages(ctx, ns, sessionID, "", msg); err == nil {
		t.Fatal("generation key")
	}
	if _, err := store.SaveSessionMessages(ctx, ns, sessionID, "k", nil); err == nil {
		t.Fatal("empty window")
	}
	if err := store.DeleteSessionMessages(ctx, nil, sessionID); err == nil {
		t.Fatal("unscoped delete")
	}
	if _, err := eng.SaveSessionMessages(ctx, ns, sessionID, "blank", []brain.SessionMessage{{
		Role: "user", SearchText: "blankbody",
	}}); err != nil {
		t.Fatal(err)
	}
	blankPage, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "blankbody"})
	if err != nil || len(slices.Collect(blankPage.Results.Objects())) != 1 || slices.Collect(blankPage.Results.Objects())[0].Kind != brain.KindSessionMessage {
		t.Fatalf("blank body = %+v err=%v", slices.Collect(blankPage.Results.Objects()), err)
	}
	if _, err := store.SaveSessionMessages(ctx, ns, sessionID, "badjson", []brain.SessionMessage{{
		Role: "user", Body: []byte("not-json"), SearchText: "x",
	}}); err == nil {
		t.Fatal("bad json")
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	if _, err := store.SaveSessionMessages(stopped, ns, sessionID, "cancel-key", msg); err == nil {
		t.Fatal("cancelled save")
	}
	if err := store.DeleteSessionMessages(stopped, ns, sessionID); err == nil {
		t.Fatal("cancelled delete")
	}
	if _, err := store.Get(ctx, scope, uuid.Nil); err == nil {
		t.Fatal("nil id")
	}
	fact, err := eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Fact", Title: "fact", Content: "durable",
	})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	if _, err := eng.Save(ctx, brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Fact", Title: "chunk", Content: marker + " durablefact",
		ParentID: &fact.ID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
	gen, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	if err != nil {
		t.Fatal(err)
	}
	again, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	if err != nil || again != gen {
		t.Fatalf("retry gen = %d err = %v", again, err)
	}
	next, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-2", []brain.SessionMessage{{
		Role: "user", Body: []byte(`{"role":"user","content":"later"}`), SearchText: "laterwindow",
	}})
	if err != nil || next != gen+1 {
		t.Fatalf("next gen = %d err = %v", next, err)
	}

	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: marker})
	if err != nil {
		t.Fatal(err)
	}
	var facts int
	var savedID uuid.UUID
	for _, obj := range slices.Collect(page.Results.Objects()) {
		switch obj.Kind {
		case "Fact":
			facts++
		case brain.KindSessionMessage:
			for _, ev := range obj.Evidence {
				if strings.Contains(ev.Snippet, token) {
					savedID = obj.ID
				}
			}
		}
	}
	if facts != 1 || savedID == uuid.Nil {
		t.Fatalf("merged search = %+v", slices.Collect(page.Results.Objects()))
	}
	lex, err := store.Search(ctx, scope, brain.RecordSearch{Query: marker, Exact: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range lex {
		if strings.Contains(hit.Content, "laterwindow") {
			t.Fatalf("lexical non-match: %+v", hit)
		}
	}
	got, err := eng.Open(ctx, scope, savedID)
	if err != nil || got.Object.Kind != brain.KindSessionMessage || !strings.Contains(got.Object.Content, token) {
		t.Fatalf("open = %+v err=%v", got, err)
	}
	if _, err := eng.Open(ctx, scope, uuid.New()); err == nil {
		t.Fatal("missing message")
	}
	exact, err := eng.Search(ctx, scope, brain.SearchRequest{Query: `"` + token + `"`})
	if err != nil || len(slices.Collect(exact.Results.Objects())) == 0 || slices.Collect(exact.Results.Objects())[0].Kind != brain.KindSessionMessage {
		t.Fatalf("find exact = %+v err=%v", slices.Collect(exact.Results.Objects()), err)
	}

	filtered, err := eng.Search(ctx, scope, brain.SearchRequest{
		Query:   marker,
		Filters: mustFilter(t, map[string]any{"kind": "Fact"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(slices.Collect(filtered.Results.Objects())) != 1 || slices.Collect(filtered.Results.Objects())[0].Kind != "Fact" {
		t.Fatalf("kind filter = %+v", slices.Collect(filtered.Results.Objects()))
	}

	other, err := eng.Search(ctx, brain.Scope{Namespace: ns, SessionID: "other"}, brain.SearchRequest{
		Query: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range slices.Collect(other.Results.Objects()) {
		if obj.Kind == brain.KindSessionMessage {
			t.Fatalf("other session = %+v", slices.Collect(other.Results.Objects()))
		}
	}
	if err := eng.DeleteSessionMessages(ctx, ns, sessionID); err != nil {
		t.Fatal(err)
	}
	gone, err := eng.Search(ctx, scope, brain.SearchRequest{Query: token})
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range slices.Collect(gone.Results.Objects()) {
		if obj.Kind == brain.KindSessionMessage {
			t.Fatalf("deleted session messages = %+v", slices.Collect(gone.Results.Objects()))
		}
	}
	factPage, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "durablefact"})
	if err != nil || len(slices.Collect(factPage.Results.Objects())) != 1 || slices.Collect(factPage.Results.Objects())[0].Kind != "Fact" {
		t.Fatalf("fact after delete = %+v err=%v", slices.Collect(factPage.Results.Objects()), err)
	}
	restart, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-3", msg)
	if err != nil || restart != 1 {
		t.Fatalf("generation after delete = %d err = %v", restart, err)
	}
}

func TestSessionMessages_aWindowWithAMalformedBodySavesNothing(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, _ := testDB(t)
	host := mustNS(t, "org", "a")
	window := []brain.SessionMessage{
		{Role: "user", Body: []byte(`{"role":"user","content":"ok"}`), SearchText: "ok"},
		{Role: "user", Body: []byte(`{not json`), SearchText: "broken"},
	}

	_, badErr := store.SaveSessionMessages(ctx, host, "s1", "k1", window)
	gen, err := store.SaveSessionMessages(ctx, host, "s1", "k2", window[:1])

	if badErr == nil {
		t.Error("a window with a malformed body saved")
	}
	// The failed window left no generation behind.
	if err != nil || gen != 1 {
		t.Errorf("next save = generation %d %v, want 1", gen, err)
	}
}
