package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/postgres"
)

func TestSessionMessages_liveSearchUnion(t *testing.T) {
	ctx := context.Background()
	pool := sharedPostgresPool(t)
	mustExec(t, pool, `TRUNCATE objects, object_kinds, session_messages`)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store, brain.WithEmbedder(liveStubEmbedder{v: []float32{1, 0, 0}}))
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
	if err := store.DeleteSessionMessages(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SaveSessionMessages(ctx, ns, sessionID, "blank", []brain.SessionMessage{{
		Role: "user", SearchText: "blankbody",
	}}); err != nil {
		t.Fatal(err)
	}
	blankPage, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "blankbody"}, brain.NewSearchContext())
	if err != nil || len(blankPage.Objects) != 1 || blankPage.Objects[0].Kind != brain.KindSessionMessage {
		t.Fatalf("blank body = %+v err=%v", blankPage.Objects, err)
	}
	if _, err := store.SaveSessionMessages(ctx, ns, sessionID, "badjson", []brain.SessionMessage{{
		Role: "user", Body: []byte("not-json"), SearchText: "x",
	}}); err == nil {
		t.Fatal("bad json")
	}
	if _, err := store.SaveSessionMessages(ctx, ns, sessionID, "badv", []brain.SessionMessage{{
		Role: "user", Body: []byte(`{}`), SearchText: "x", Embedding: []float32{1, 0},
	}}); err == nil {
		t.Fatal("bad vector")
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	if _, err := store.SaveSessionMessages(stopped, ns, sessionID, "cancel-key", msg); err == nil {
		t.Fatal("cancelled save")
	}
	if err := store.DeleteSessionMessages(stopped, sessionID); err == nil {
		t.Fatal("cancelled delete")
	}
	if _, err := store.Get(ctx, scope, uuid.Nil); err == nil {
		t.Fatal("nil id")
	}
	fact, err := eng.Put(ctx, brain.Scope{Namespace: ns}, brain.Object{
		Kind: "Fact", Title: "fact", Content: "durable",
	})
	if err != nil {
		t.Fatal(err)
	}
	pos := 1
	if _, err := eng.Put(ctx, brain.Scope{Namespace: ns}, brain.Object{
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

	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: marker}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	var facts int
	var savedID uuid.UUID
	for _, obj := range page.Objects {
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
		t.Fatalf("merged search = %+v", page.Objects)
	}
	got, err := eng.Read(ctx, scope, savedID)
	if err != nil || got.Kind != brain.KindSessionMessage || !strings.Contains(got.Content, token) {
		t.Fatalf("read = %+v err=%v", got, err)
	}
	if _, err := eng.Read(ctx, scope, uuid.New()); err == nil {
		t.Fatal("missing message")
	}
	exact, err := eng.FindExact(ctx, scope, brain.SearchRequest{Query: token}, brain.NewSearchContext())
	if err != nil || len(exact.Objects) == 0 || exact.Objects[0].Kind != brain.KindSessionMessage {
		t.Fatalf("find exact = %+v err=%v", exact.Objects, err)
	}

	filtered, err := eng.Search(ctx, scope, brain.SearchRequest{
		Query:   marker,
		Filters: mustFilter(t, map[string]any{"kind": "Fact"}),
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Objects) != 1 || filtered.Objects[0].Kind != "Fact" {
		t.Fatalf("kind filter = %+v", filtered.Objects)
	}

	other, err := eng.Search(ctx, brain.Scope{Namespace: ns, SessionID: "other"}, brain.SearchRequest{
		Query: token,
	}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range other.Objects {
		if obj.Kind == brain.KindSessionMessage {
			t.Fatalf("other session = %+v", other.Objects)
		}
	}
	if err := eng.DeleteSessionMessages(ctx, sessionID); err != nil {
		t.Fatal(err)
	}
	gone, err := eng.Search(ctx, scope, brain.SearchRequest{Query: token}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range gone.Objects {
		if obj.Kind == brain.KindSessionMessage {
			t.Fatalf("deleted session messages = %+v", gone.Objects)
		}
	}
	factPage, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "durablefact"}, brain.NewSearchContext())
	if err != nil || len(factPage.Objects) != 1 || factPage.Objects[0].Kind != "Fact" {
		t.Fatalf("fact after delete = %+v err=%v", factPage.Objects, err)
	}
	restart, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-3", msg)
	if err != nil || restart != 1 {
		t.Fatalf("generation after delete = %d err = %v", restart, err)
	}
}
