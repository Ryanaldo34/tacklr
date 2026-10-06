package brain

import (
	"testing"

	"github.com/google/uuid"
)

func TestSessionMessages_searchKindFilterIdempotentDelete(t *testing.T) {
	store := NewMemoryStore()
	eng, err := NewEngine(store, WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	ns, err := ParseNamespace("org", "acme")
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	const (
		sessionID = "sess.with.dots"
		marker    = "message-marker"
		token     = "only-in-session"
	)
	factID := uuid.New()
	if _, err := eng.Put(ctx, Scope{Namespace: ns}, Object{
		ID: factID, Kind: "Fact", Title: "fact", Content: "durable",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Put(ctx, Scope{Namespace: ns}, Object{
		Kind: "Fact", ParentID: &factID, Title: "chunk", Content: marker + " durable-fact",
	}); err != nil {
		t.Fatal(err)
	}
	msg := []SessionMessage{{
		Role:       "tool",
		Body:       []byte(`{"role":"tool","content":"` + marker + " " + token + `"}`),
		SearchText: marker + " " + token,
	}}
	gen, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	if err != nil {
		t.Fatal(err)
	}
	again, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-1", msg)
	if err != nil || again != gen {
		t.Fatalf("retry gen = %d err = %v", again, err)
	}
	next, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-2", []SessionMessage{{
		Role: "user", Body: []byte(`{"role":"user","content":"later"}`), SearchText: "later-window",
	}})
	if err != nil || next != gen+1 {
		t.Fatalf("next gen = %d err = %v", next, err)
	}

	scope := Scope{Namespace: ns, SessionID: sessionID}
	page, err := eng.Search(ctx, scope, SearchRequest{Query: marker}, NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	var facts, messages int
	for _, obj := range page.Objects {
		switch obj.Kind {
		case "Fact":
			facts++
		case KindSessionMessage:
			messages++
		}
	}
	if len(page.Objects) != 2 || facts != 1 || messages != 1 {
		t.Fatalf("merged search = %+v", page.Objects)
	}

	filtered, err := eng.Search(ctx, scope, SearchRequest{
		Query:   marker,
		Filters: Filter{Kind: StringMatch{Eq: "Fact"}},
	}, NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Objects) != 1 || filtered.Objects[0].Kind != "Fact" {
		t.Fatalf("kind filter = %+v", filtered.Objects)
	}

	if err := eng.DeleteSessionMessages(ctx, sessionID); err != nil {
		t.Fatal(err)
	}
	factPage, err := eng.Search(ctx, scope, SearchRequest{Query: "durable-fact"}, NewSearchContext())
	if err != nil || len(factPage.Objects) != 1 || factPage.Objects[0].Kind != "Fact" {
		t.Fatalf("fact after delete = %+v err=%v", factPage.Objects, err)
	}

	restart, err := eng.SaveSessionMessages(ctx, ns, sessionID, "key-3", msg)
	if err != nil || restart != 1 {
		t.Fatalf("generation after delete = %d err = %v", restart, err)
	}
	other, err := eng.Search(ctx, Scope{Namespace: ns, SessionID: "other"}, SearchRequest{Query: token}, NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Objects) != 0 {
		t.Fatalf("other session = %+v", other.Objects)
	}
}
