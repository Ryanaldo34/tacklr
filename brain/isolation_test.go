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
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	var none brain.Scope
	id := uuid.New()
	sc := brain.NewSearchContext()
	errs := map[string]error{
		"soft delete":    eng.SoftDelete(ctx, none, id),
		"link":           kg.Link(ctx, none, id, id, "rel"),
		"unlink":         kg.Unlink(ctx, none, id, id, "rel"),
		"erase":          eng.Erase(ctx, none),
		"delete session": eng.DeleteSessionMessages(ctx, nil, "s"),
	}
	_, errs["put"] = eng.Put(ctx, none, brain.Object{Kind: "Note"})
	_, errs["put tree"] = eng.PutTree(ctx, none, brain.Object{Kind: "Note"}, nil)
	_, errs["purge deleted"] = eng.PurgeDeleted(ctx, none, time.Now())
	_, errs["get"] = eng.Get(ctx, none, id)
	_, errs["read"] = eng.Read(ctx, none, id)
	_, errs["list children"] = eng.ListChildren(ctx, none, id)
	_, errs["list by kind"] = eng.ListByKind(ctx, none, "Note", 1)
	_, errs["get by property"] = eng.GetByProperty(ctx, none, "k", "v")
	_, errs["kinds with objects"] = eng.KindsWithObjects(ctx, none)
	_, errs["search"] = eng.Search(ctx, none, brain.SearchRequest{Query: "q"}, sc)
	_, errs["find exact"] = eng.FindExact(ctx, none, brain.SearchRequest{Query: "q"}, sc)
	_, errs["continue"] = eng.Continue(ctx, none, id, 1, sc)
	_, errs["find objects"] = kg.FindObjects(ctx, none, brain.FindObjectsRequest{Query: "q"}, sc)
	_, errs["find links"] = kg.FindLinks(ctx, none, brain.FindLinksRequest{RelationType: "rel", Query: "q"})
	_, errs["expand"] = kg.Expand(ctx, none, brain.ExpandRequest{ObjectID: id}, sc)
	_, errs["expand many"] = kg.ExpandMany(ctx, none, brain.ExpandManyRequest{ObjectIDs: []uuid.UUID{id}})
	_, errs["shortest path"] = kg.ShortestPath(ctx, none, brain.PathRequest{From: id, To: id})
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
	parentA, err := eng.Put(ctx, a, brain.Object{Kind: "Document", Title: "A's doc"})
	if err != nil {
		t.Fatal(err)
	}

	chunk, err := eng.Put(ctx, a, brain.Object{Kind: "Chunk", ParentID: &parentA.ID, Content: "body"})
	if err != nil {
		t.Fatal(err)
	}

	_, outsideErr := eng.Put(ctx, a, brain.Object{Kind: "Note", Namespace: b.Namespace})
	_, nestedErr := eng.Put(ctx, a, brain.Object{Kind: "Chunk", ParentID: &chunk.ID, Content: "nested"})
	_, partErr := eng.Put(ctx, b, brain.Object{Kind: "Chunk", ParentID: &parentA.ID, Content: "planted"})
	narrowed, narrowErr := eng.Put(ctx, a, brain.Object{Kind: "Note", Namespace: mustNS(t, "org", "a", "team", "x")})

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
	tenantA := uuid.NewString()
	ctx := context.Background()
	var events []brain.AuditEvent
	record := func(_ context.Context, ev brain.AuditEvent) error {
		events = append(events, ev)
		return nil
	}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(record))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	scope := brain.Scope{Namespace: mustNS(t, "org", tenantA), Principals: []string{"user:ann"}, SessionID: "s1"}
	sc := brain.NewSearchContext()
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "Roadmap"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "launch plan"}))
	if err != nil {
		t.Fatal(err)
	}
	other, _ := eng.Put(ctx, scope, brain.Object{Kind: "Person", Title: "Ann"})
	var page brain.SearchPage

	// Each call, and the operation its event must record.
	for _, step := range []struct {
		op   brain.AuditOp
		call func()
	}{
		{brain.AuditLink, func() { _ = kg.Link(ctx, scope, doc.ID, other.ID, "about") }},
		{brain.AuditUnlink, func() { _ = kg.Unlink(ctx, scope, doc.ID, other.ID, "about") }},
		{brain.AuditRead, func() { page, _ = eng.Search(ctx, scope, brain.SearchRequest{Query: "launch"}, sc) }},
		{brain.AuditRead, func() { _, _ = eng.Read(ctx, scope, doc.ID) }},
		{brain.AuditRead, func() { _, _ = eng.ListChildren(ctx, scope, doc.ID) }},
		{brain.AuditRead, func() { _, _ = eng.ListByKind(ctx, scope, "Document", 5) }},
		{brain.AuditRead, func() { _, _ = kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: doc.ID}, sc) }},
		{brain.AuditRead, func() {
			_, _ = kg.ExpandMany(ctx, scope, brain.ExpandManyRequest{ObjectIDs: []uuid.UUID{doc.ID}, WantContainment: true})
		}},
		{brain.AuditRead, func() { _, _ = kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "Roadmap"}, sc) }},
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
	if len(page.Objects) != 1 || !slices.ContainsFunc(events, func(ev brain.AuditEvent) bool {
		return ev.Op == brain.AuditRead && slices.Equal(ev.ObjectIDs, []uuid.UUID{page.Objects[0].ID})
	}) {
		t.Errorf("no read event lists the search result %v", page.Objects)
	}
	if !slices.ContainsFunc(events, func(ev brain.AuditEvent) bool { return ev.Op == brain.AuditPurge && ev.Count == 1 }) {
		t.Error("purge event does not count the purged object")
	}
}

func TestEngine_aFailedAuditRecordFailsEveryServedReadAndWrite(t *testing.T) {
	t.Parallel()
	tenantA := uuid.NewString()
	ctx := context.Background()
	var down atomic.Bool
	record := func(context.Context, brain.AuditEvent) error {
		if down.Load() {
			return errors.New("audit store down")
		}
		return nil
	}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(record))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))
	scope := brain.Scope{Namespace: mustNS(t, "org", tenantA)}
	var docs []brain.Object
	for _, title := range []string{"Roadmap", "Launch review"} {
		doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: title, Properties: map[string]any{"path": "/" + title}},
			brain.Parts(brain.Object{Kind: "Chunk", Content: "launch plan"}))
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, doc)
	}
	owner, err := eng.Put(ctx, scope, brain.Object{Kind: "Person", Title: "Ann"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kg.LinkWith(ctx, scope, docs[0].ID, owner.ID, "about", brain.EdgeMeta{Note: "launch owner"}); err != nil {
		t.Fatal(err)
	}
	sc := brain.NewSearchContext()
	first, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "launch", Limit: 1}, sc)
	if err != nil {
		t.Fatal(err)
	}
	down.Store(true)

	errs := map[string]error{}
	_, errs["get"] = eng.Get(ctx, scope, docs[0].ID)
	_, errs["list children"] = eng.ListChildren(ctx, scope, docs[0].ID)
	_, errs["list by kind"] = eng.ListByKind(ctx, scope, "Document", 5)
	_, errs["get by property"] = eng.GetByProperty(ctx, scope, "path", "/Roadmap")
	_, errs["search"] = eng.Search(ctx, scope, brain.SearchRequest{Query: "launch"}, brain.NewSearchContext())
	_, errs["continue"] = eng.Continue(ctx, scope, first.ResultSetID, 1, sc)
	_, errs["expand"] = kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: docs[0].ID}, brain.NewSearchContext())
	_, errs["find objects"] = kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "Roadmap"}, brain.NewSearchContext())
	_, errs["find links"] = kg.FindLinks(ctx, scope, brain.FindLinksRequest{RelationType: "about", Query: "launch"})
	_, errs["put"] = eng.Put(ctx, scope, brain.Object{Kind: "Note"})

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
