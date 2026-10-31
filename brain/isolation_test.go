package brain_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

func TestEngine_everyEntryPointRejectsAnEmptyScope(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	var none brain.Scope
	id := uuid.New()
	errs := map[string]error{
		"soft delete":    eng.SoftDelete(ctx, none, id),
		"link":           eng.Link(ctx, none, brain.LinkRequest{From: id, To: id, Relation: "rel"}),
		"erase":          eng.Erase(ctx, none),
		"delete session": eng.DeleteSessionMessages(ctx, nil, "s"),
	}
	_, errs["save"] = eng.Save(ctx, none, brain.Object{Kind: "Note"})
	_, errs["put tree"] = eng.PutTree(ctx, none, brain.Object{Kind: "Note"}, nil)
	_, errs["purge deleted"] = eng.PurgeDeleted(ctx, none, time.Now())
	_, errs["get"] = eng.Get(ctx, none, id)
	_, errs["open"] = eng.Open(ctx, none, id)
	_, errs["resolve"] = eng.Resolve(ctx, none, brain.Ref{Path: "/x"})
	_, errs["search"] = eng.Search(ctx, none, brain.SearchRequest{Query: "q"})
	_, errs["next"] = eng.Next(ctx, none, brain.Rest{}, 1)
	_, errs["related"] = eng.Related(ctx, none, brain.RelatedRequest{ID: id})
	_, errs["save session"] = eng.SaveSessionMessages(ctx, nil, "s", "k", []brain.SessionMessage{{Role: "user"}})
	_, errs["purge session"] = eng.PurgeSessionMessages(ctx, nil, time.Now())

	for name, err := range errs {
		if !errors.Is(err, brain.ErrInvalid) {
			t.Errorf("%s with empty scope = %v, want ErrInvalid", name, err)
		}
	}
}

func TestEngine_putKeepsObjectsInsideTheCallersScope(t *testing.T) {
	ctx := context.Background()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	a := brain.Scope{Namespace: mustNS(t, "org", "a")}
	b := brain.Scope{Namespace: mustNS(t, "org", "b")}
	parentA, err := eng.Save(ctx, a, brain.Object{Kind: "Document", Title: "A's doc"})
	if err != nil {
		t.Fatal(err)
	}

	chunk, err := eng.Save(ctx, a, brain.Object{Kind: "Chunk", ParentID: &parentA.ID, Content: "body"})
	if err != nil {
		t.Fatal(err)
	}

	_, outsideErr := eng.Save(ctx, a, brain.Object{Kind: "Note", Namespace: b.Namespace})
	_, nestedErr := eng.Save(ctx, a, brain.Object{Kind: "Chunk", ParentID: &chunk.ID, Content: "nested"})
	_, partErr := eng.Save(ctx, b, brain.Object{Kind: "Chunk", ParentID: &parentA.ID, Content: "planted"})
	narrowed, narrowErr := eng.Save(ctx, a, brain.Object{Kind: "Note", Namespace: mustNS(t, "org", "a", "team", "x")})

	if !errors.Is(outsideErr, brain.ErrInvalid) {
		t.Errorf("object namespace outside scope = %v, want ErrInvalid", outsideErr)
	}
	if !errors.Is(nestedErr, brain.ErrInvalid) {
		t.Errorf("part under a part = %v, want ErrInvalid", nestedErr)
	}
	if !errors.Is(partErr, brain.ErrNotFound) {
		t.Errorf("part under another tenant's parent = %v, want ErrNotFound", partErr)
	}
	if narrowErr != nil || !narrowed.Namespace.Equal(mustNS(t, "org", "a", "team", "x")) {
		t.Errorf("narrower namespace = %+v %v", narrowed.Namespace, narrowErr)
	}
}

func TestEngine_auditRecordsEveryServedReadAndWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var events []brain.AuditEvent
	record := func(_ context.Context, ev brain.AuditEvent) error {
		events = append(events, ev)
		return nil
	}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(record), brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t), Principals: []string{"user:ann"}, SessionID: "s1"}
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "Roadmap"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "launch plan"}))
	if err != nil {
		t.Fatal(err)
	}
	other := save(t, eng, scope, brain.Object{Kind: "Person", Title: "Ann"})
	var page brain.Page

	// Each call, and the operation its event must record.
	for _, step := range []struct {
		op   brain.AuditOp
		call func()
	}{
		{brain.AuditLink, func() { _ = eng.Link(ctx, scope, brain.LinkRequest{From: doc.ID, To: other.ID, Relation: "about"}) }},
		{brain.AuditUnlink, func() {
			_ = eng.Link(ctx, scope, brain.LinkRequest{From: doc.ID, To: other.ID, Relation: "about", Remove: true})
		}},
		{brain.AuditRead, func() { page, _ = eng.Search(ctx, scope, brain.SearchRequest{Query: "launch"}) }},
		{brain.AuditRead, func() { _, _ = eng.Open(ctx, scope, doc.ID) }},
		{brain.AuditRead, func() { _, _ = eng.Get(ctx, scope, doc.ID) }},
		{brain.AuditRead, func() { _, _ = eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID}) }},
		{brain.AuditSessionSave, func() {
			_, _ = eng.SaveSessionMessages(ctx, scope.Namespace, "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "hi"}})
		}},
		{brain.AuditSessionDelete, func() { _ = eng.DeleteSessionMessages(ctx, scope.Namespace, "s1") }},
		{brain.AuditSessionPurge, func() { _, _ = eng.PurgeSessionMessages(ctx, scope.Namespace, time.Now()) }},
		{brain.AuditDelete, func() { _ = eng.SoftDelete(ctx, scope, other.ID) }},
		{brain.AuditPurge, func() { _, _ = eng.PurgeDeleted(ctx, scope, time.Now().Add(time.Minute)) }},
		{brain.AuditErase, func() { _ = eng.Erase(ctx, scope) }},
	} {
		before := len(events)
		step.call()
		if !slices.ContainsFunc(events[before:], func(ev brain.AuditEvent) bool { return ev.Op == step.op }) {
			t.Errorf("call %d recorded %v, want a %s event", len(events), events[before:], step.op)
		}
	}

	put := events[0]
	if put.Op != brain.AuditPut || !put.Namespace.Equal(scope.Namespace) || !slices.Equal(put.Principals, scope.Principals) ||
		put.SessionID != "s1" || !slices.Equal(put.ObjectIDs, []uuid.UUID{doc.ID}) {
		t.Errorf("put tree event = %+v", put)
	}
	if len(slices.Collect(page.Results.Objects())) != 1 || !slices.ContainsFunc(events, func(ev brain.AuditEvent) bool {
		return ev.Op == brain.AuditRead && slices.Equal(ev.ObjectIDs, []uuid.UUID{slices.Collect(page.Results.Objects())[0].ID})
	}) {
		t.Errorf("no read event lists the search result %v", slices.Collect(page.Results.Objects()))
	}
	if !slices.ContainsFunc(events, func(ev brain.AuditEvent) bool { return ev.Op == brain.AuditPurge && ev.Count == 1 }) {
		t.Error("purge event does not count the purged object")
	}
}

func TestEngine_aFailedAuditRecordFailsEveryServedReadAndWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var down atomic.Bool
	record := func(context.Context, brain.AuditEvent) error {
		if down.Load() {
			return errors.New("audit store down")
		}
		return nil
	}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(record), brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: tenant(t)}
	var docs []brain.Object
	for _, title := range []string{"Roadmap", "Launch review"} {
		doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: title}, brain.Parts(brain.Object{Kind: "Chunk", Content: "launch plan"}))
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, doc)
	}
	owner := save(t, eng, scope, brain.Object{Kind: "Person", Title: "Ann"})
	link(t, eng, scope, docs[0].ID, owner.ID, "about", "launch owner")
	first, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "launch", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	down.Store(true)

	errs := map[string]error{"link": eng.Link(ctx, scope, brain.LinkRequest{From: docs[1].ID, To: owner.ID, Relation: "about"})}
	_, errs["get"] = eng.Get(ctx, scope, docs[0].ID)
	_, errs["open"] = eng.Open(ctx, scope, docs[0].ID)
	_, errs["search"] = eng.Search(ctx, scope, brain.SearchRequest{Query: "launch"})
	_, errs["next"] = eng.Next(ctx, scope, first.Rest, 1)
	_, errs["related"] = eng.Related(ctx, scope, brain.RelatedRequest{ID: docs[0].ID})
	_, errs["save"] = eng.Save(ctx, scope, brain.Object{Kind: "Note"})

	for name, err := range errs {
		if err == nil || !strings.Contains(err.Error(), "audit store down") {
			t.Errorf("%s with a failing audit log = %v", name, err)
		}
	}
}

func TestNamespace_keyIsCollisionFreeWhereStringIsNot(t *testing.T) {
	dotted := brain.Namespace{{Name: "a", Value: "x"}, {Name: "b", Value: "y"}}
	other := brain.Namespace{{Name: "c", Value: "x"}, {Name: "d", Value: "y"}}

	if dotted.String() != other.String() || dotted.Key() == other.Key() {
		t.Fatalf("String %q/%q, Key %q/%q", dotted.String(), other.String(), dotted.Key(), other.Key())
	}
}
