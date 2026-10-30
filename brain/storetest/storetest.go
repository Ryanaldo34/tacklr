// Package storetest is the isolation contract every brain store must meet.
// Run it from a store's own tests (MemoryStore and postgres.Store do), so a
// new store method or backend cannot ship without the same tenant checks.
//
// Two tenants share one store. Each case asserts what a caller gets under
// its own scope: its own rows, ErrNotFound for another tenant's ids, and an
// error for an empty scope.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// Store is every capability the contract checks.
type Store interface {
	brain.Store
	brain.ObjectWriter
	brain.SessionWriter
	brain.Purger
	brain.ObjectLister
}

// Model is the embedding model name the contract writes and searches with.
// Vectors have three dimensions.
const Model = "contract-model"

// Run runs the contract. newStore returns an empty store for each case.
func Run(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("another tenant's id is not found for reads and writes", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		takeover := newDoc(f.b, "stolen")
		takeover.ID = f.docA

		putErr := s.Put(ctx, f.scopeB, takeover)
		treeErr := putTree(ctx, s, f.scopeB, takeover, nil)
		state, stateErr := s.TreeState(ctx, f.scopeB, f.docA)
		delErr := s.SoftDelete(ctx, f.scopeB, f.docA)
		_, getErr := s.Get(ctx, f.scopeB, f.docA)
		kids, kidsErr := s.ListChildren(ctx, f.scopeB, f.docA)
		mine, mineErr := s.Get(ctx, f.scopeA, f.docA)

		for name, err := range map[string]error{"put": putErr, "put tree": treeErr, "soft delete": delErr, "get": getErr} {
			if !errors.Is(err, brain.ErrNotFound) {
				t.Errorf("%s as tenant B = %v, want ErrNotFound", name, err)
			}
		}
		if kidsErr != nil || len(kids) != 0 {
			t.Errorf("list children as tenant B = %v %v", kids, kidsErr)
		}
		if stateErr != nil || state.Rev != 0 || len(state.Live) != 0 {
			t.Errorf("tree state as tenant B = %+v %v, want an empty tree", state, stateErr)
		}
		if mineErr != nil || mine.Title != "doc a" || !mine.Namespace.Equal(f.a) {
			t.Errorf("tenant A row after B's writes = %+v %v", mine, mineErr)
		}
	})

	t.Run("every read and search returns only the caller's tenant", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		q := brain.Filter{}

		many, _ := s.GetMany(ctx, f.scopeB, []uuid.UUID{f.docA, f.docB})
		listed, _ := s.ListByKind(ctx, f.scopeB, "Document", 10)
		byProp, propErr := s.GetByProperty(ctx, f.scopeB, "path", "/shared.md")
		kinds, _ := s.KindsWithObjects(ctx, f.scopeB)
		lex, _ := s.SearchLexical(ctx, f.scopeB, "shared", q, 10)
		tri, _ := s.SearchTrigram(ctx, f.scopeB, "shared", q, 10)
		vec, _ := s.SearchVector(ctx, f.scopeB, Model, []float32{1, 0, 0}, q, 10)

		if got := objectIDs(many); !slices.Equal(got, []uuid.UUID{f.docB}) {
			t.Errorf("get many = %v, want [%s]", got, f.docB)
		}
		if got := objectIDs(listed); !slices.Equal(got, []uuid.UUID{f.docB}) {
			t.Errorf("list by kind = %v", got)
		}
		if propErr != nil || byProp.ID != f.docB {
			t.Errorf("get by property = %s %v", byProp.ID, propErr)
		}
		if !slices.Equal(kinds, []string{"Document"}) {
			t.Errorf("kinds with objects = %v", kinds)
		}
		for name, hits := range map[string][]brain.ScoredID{"lexical": lex, "trigram": tri, "vector": vec} {
			if got := scoredIDs(hits); !slices.Equal(got, []uuid.UUID{f.chunkB}) {
				t.Errorf("%s = %v, want [%s]", name, got, f.chunkB)
			}
		}
	})

	t.Run("an empty scope is rejected by every scoped method", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		var none brain.Scope
		q := brain.Filter{}

		errs := map[string]error{"put": s.Put(ctx, none, newDoc(f.a, "x")), "soft delete": s.SoftDelete(ctx, none, f.docA),
			"stage parts": s.StageParts(ctx, none, uuid.New(), nil),
			"commit tree": s.CommitTree(ctx, none, uuid.New(), 0, newDoc(f.a, "x"))}
		_, errs["tree state"] = s.TreeState(ctx, none, f.docA)
		_, errs["get"] = s.Get(ctx, none, f.docA)
		_, errs["get many"] = s.GetMany(ctx, none, []uuid.UUID{f.docA})
		_, errs["list children"] = s.ListChildren(ctx, none, f.docA)
		_, errs["list by kind"] = s.ListByKind(ctx, none, "Document", 10)
		_, errs["get by property"] = s.GetByProperty(ctx, none, "path", "/shared.md")
		_, errs["kinds with objects"] = s.KindsWithObjects(ctx, none)
		_, errs["lexical"] = s.SearchLexical(ctx, none, "shared", q, 10)
		_, errs["trigram"] = s.SearchTrigram(ctx, none, "shared", q, 10)
		_, errs["vector"] = s.SearchVector(ctx, none, Model, []float32{1, 0, 0}, q, 10)
		_, errs["purge deleted"] = s.PurgeDeleted(ctx, none, time.Now())
		_, errs["erase"] = s.Erase(ctx, none)
		errs["delete session"] = s.DeleteSessionMessages(ctx, nil, "s")
		_, errs["purge session"] = s.PurgeSessionMessages(ctx, nil, time.Now())
		_, errs["save session"] = s.SaveSessionMessages(ctx, nil, "s", "k", []brain.SessionMessage{{Role: "user"}})

		for name, err := range errs {
			if !errors.Is(err, brain.ErrInvalid) {
				t.Errorf("%s with empty scope = %v, want ErrInvalid", name, err)
			}
		}
	})

	t.Run("readers limit an object to scopes holding one of its principals", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		restricted := newDoc(f.a, "board minutes")
		restricted.Readers = []string{"group:board"}
		chunk := newChunk(restricted.ID, f.a, "quarterly board minutes", []float32{0, 1, 0})
		chunk.Readers = restricted.Readers
		must(t, putTree(ctx, s, f.scopeA, restricted, []brain.Object{chunk}))
		board := brain.Scope{Namespace: f.a, Principals: []string{"user:ann", "group:board"}}

		got, getErr := s.Get(ctx, board, restricted.ID)
		hits, _ := s.SearchLexical(ctx, board, "quarterly", brain.Filter{}, 10)
		_, hiddenErr := s.Get(ctx, f.scopeA, restricted.ID)
		updateErr := s.Put(ctx, f.scopeA, restricted)

		if getErr != nil || !slices.Equal(got.Readers, []string{"group:board"}) {
			t.Errorf("board member get = %+v %v", got, getErr)
		}
		if got := scoredIDs(hits); !slices.Equal(got, []uuid.UUID{chunk.ID}) {
			t.Errorf("board member search = %v", got)
		}
		if !errors.Is(hiddenErr, brain.ErrNotFound) || !errors.Is(updateErr, brain.ErrNotFound) {
			t.Errorf("non-member get = %v, put = %v, want ErrNotFound", hiddenErr, updateErr)
		}
	})

	t.Run("put tree replaces parts atomically and soft delete takes the parts too", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		next := newChunk(f.docA, f.a, "replacement text", []float32{0, 0, 1})
		outside := newChunk(f.docA, f.b, "other tenant", []float32{0, 0, 1})

		rejected := putTree(ctx, s, f.scopeA, parent, []brain.Object{next, outside})
		kept, _ := s.ListChildren(ctx, f.scopeA, f.docA)
		replaced := putTree(ctx, s, f.scopeA, parent, []brain.Object{next})
		swapped, _ := s.ListChildren(ctx, f.scopeA, f.docA)
		deleted := s.SoftDelete(ctx, f.scopeA, f.docA)
		_, partErr := s.Get(ctx, f.scopeA, next.ID)

		if !errors.Is(rejected, brain.ErrInvalid) {
			t.Errorf("tree with a part outside scope = %v, want ErrInvalid", rejected)
		}
		if got := objectIDs(kept); !slices.Equal(got, []uuid.UUID{f.chunkA}) {
			t.Errorf("parts after a rejected tree = %v, want the original [%s]", got, f.chunkA)
		}
		if replaced != nil || !slices.Equal(objectIDs(swapped), []uuid.UUID{next.ID}) {
			t.Errorf("parts after replace = %v %v", objectIDs(swapped), replaced)
		}
		if deleted != nil || !errors.Is(partErr, brain.ErrNotFound) {
			t.Errorf("part after parent soft delete = %v (delete %v)", partErr, deleted)
		}
	})

	t.Run("a tree commit keeps reused rows in their new place and beats a later commit of the same revision", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		state, stateErr := s.TreeState(ctx, f.scopeA, f.docA)
		added := newChunk(f.docA, f.a, "appendix text", []float32{0, 1, 0})
		moved := brain.Object{ID: f.chunkA, ParentID: &f.docA, Namespace: f.a, Position: new(2), Properties: map[string]any{"start_line": 9.0}}
		run, late := uuid.New(), uuid.New()
		must(t, s.StageParts(ctx, f.scopeA, run, []brain.TreePart{{Object: added}, {Object: moved, Reused: true}}))
		must(t, s.StageParts(ctx, f.scopeA, late, []brain.TreePart{{Object: newChunk(f.docA, f.a, "late text", []float32{0, 0, 1})}}))

		hidden, _ := s.SearchLexical(ctx, f.scopeA, "appendix", brain.Filter{}, 10)
		commitErr := s.CommitTree(ctx, f.scopeA, run, state.Rev, parent)
		lateErr := s.CommitTree(ctx, f.scopeA, late, state.Rev, parent)
		kids, _ := s.ListChildren(ctx, f.scopeA, f.docA)
		vec, _ := s.SearchVector(ctx, f.scopeA, Model, []float32{1, 0, 0}, brain.Filter{}, 10)
		_, purgeErr := s.PurgeDeleted(ctx, f.scopeA, time.Now().Add(time.Minute))
		emptied := s.CommitTree(ctx, f.scopeA, late, state.Rev+1, parent)
		left, _ := s.ListChildren(ctx, f.scopeA, f.docA)

		if _, live := state.Live[f.chunkA]; stateErr != nil || !live {
			t.Errorf("tree state = %+v %v, want chunk a live", state, stateErr)
		}
		if len(hidden) != 0 {
			t.Errorf("search before commit = %v, want staged parts hidden", scoredIDs(hidden))
		}
		if commitErr != nil || !slices.Equal(objectIDs(kids), []uuid.UUID{added.ID, f.chunkA}) || kids[1].Properties["start_line"] != 9.0 {
			t.Errorf("parts after commit = %v %v (%v)", objectIDs(kids), commitErr, kids)
		}
		if got := scoredIDs(vec); len(got) == 0 || got[0] != f.chunkA {
			t.Errorf("vector search after commit = %v, want the reused part's vector kept", got)
		}
		if !errors.Is(lateErr, brain.ErrConflict) {
			t.Errorf("commit of a stale revision = %v, want ErrConflict", lateErr)
		}
		// Purge dropped the late run's staged part, so committing it now leaves no parts.
		if purgeErr != nil || emptied != nil || len(left) != 0 {
			t.Errorf("commit of a purged run = %v (purge %v), parts %v", emptied, purgeErr, objectIDs(left))
		}
	})

	t.Run("a tree write refuses malformed parts, another tenant's run, and reused parts that are gone", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		parent, docB := newDoc(f.a, "doc a"), newDoc(f.b, "doc b")
		parent.ID, docB.ID = f.docA, f.docB
		state, _ := s.TreeState(ctx, f.scopeA, f.docA)
		stateB, _ := s.TreeState(ctx, f.scopeB, f.docB)
		noParent := brain.TreePart{Object: brain.Object{ID: uuid.New(), Kind: "Chunk", Namespace: f.a}}
		noID := brain.TreePart{Reused: true, Object: brain.Object{ParentID: &f.docA, Namespace: f.a}}
		twice := brain.TreePart{Object: newChunk(f.docA, f.a, "twice", nil)}
		foreign, gone := uuid.New(), uuid.New()
		must(t, s.StageParts(ctx, f.scopeA, foreign, []brain.TreePart{{Object: newChunk(f.docA, f.a, "tenant a text", nil)}}))
		must(t, s.StageParts(ctx, f.scopeA, gone, []brain.TreePart{{Reused: true, Object: brain.Object{
			ID: uuid.New(), ParentID: &f.docA, Namespace: f.a, Position: new(1)}}}))

		invalid := map[string]error{
			"stage with no run":              s.StageParts(ctx, f.scopeA, uuid.Nil, nil),
			"stage a part with no parent":    s.StageParts(ctx, f.scopeA, uuid.New(), []brain.TreePart{noParent}),
			"stage a reused part with no id": s.StageParts(ctx, f.scopeA, uuid.New(), []brain.TreePart{noID}),
			"commit with no run":             s.CommitTree(ctx, f.scopeA, uuid.Nil, state.Rev, parent),
			"commit a parent outside scope":  s.CommitTree(ctx, f.scopeA, uuid.New(), 0, newDoc(f.b, "x")),
			"commit another tenant's run":    s.CommitTree(ctx, f.scopeB, foreign, stateB.Rev, docB),
		}
		twiceErr := s.StageParts(ctx, f.scopeA, uuid.New(), []brain.TreePart{twice, twice})
		goneErr := s.CommitTree(ctx, f.scopeA, gone, state.Rev, parent)
		kidsA, _ := s.ListChildren(ctx, f.scopeA, f.docA)
		kidsB, _ := s.ListChildren(ctx, f.scopeB, f.docB)

		for name, err := range invalid {
			if !errors.Is(err, brain.ErrInvalid) {
				t.Errorf("%s = %v, want ErrInvalid", name, err)
			}
		}
		if twiceErr == nil {
			t.Error("staging one id twice in a run succeeded")
		}
		if !errors.Is(goneErr, brain.ErrConflict) {
			t.Errorf("commit of a reused part that is gone = %v, want ErrConflict", goneErr)
		}
		if !slices.Equal(objectIDs(kidsA), []uuid.UUID{f.chunkA}) || !slices.Equal(objectIDs(kidsB), []uuid.UUID{f.chunkB}) {
			t.Errorf("parts after refused writes = %v and %v, want both trees unchanged", objectIDs(kidsA), objectIDs(kidsB))
		}
	})

	t.Run("purge drops only old staged runs and erase only the scope's", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		kept, erased := uuid.New(), uuid.New()
		must(t, s.StageParts(ctx, f.scopeA, kept, []brain.TreePart{{Object: newChunk(f.docA, f.a, "kept text", nil)}}))

		_, purgeErr := s.PurgeDeleted(ctx, f.scopeA, time.Now().Add(-time.Hour))
		_, eraseErr := s.Erase(ctx, f.scopeB)
		state, _ := s.TreeState(ctx, f.scopeA, f.docA)
		keptErr := s.CommitTree(ctx, f.scopeA, kept, state.Rev, parent)
		kept1, _ := s.ListChildren(ctx, f.scopeA, f.docA)
		must(t, s.StageParts(ctx, f.scopeA, erased, []brain.TreePart{{Object: newChunk(f.docA, f.a, "erased text", nil)}}))
		_, eraseAErr := s.Erase(ctx, f.scopeA)
		erasedErr := s.CommitTree(ctx, f.scopeA, erased, 0, parent)
		left, _ := s.ListChildren(ctx, f.scopeA, f.docA)

		if purgeErr != nil || eraseErr != nil || keptErr != nil || len(kept1) != 1 || kept1[0].Content != "kept text" {
			t.Errorf("run after an old-cutoff purge and another tenant's erase = %v (purge %v, erase %v, commit %v)", kept1, purgeErr, eraseErr, keptErr)
		}
		if eraseAErr != nil || erasedErr != nil || len(left) != 0 {
			t.Errorf("run after its tenant's erase = %v (erase %v, commit %v), want no parts", objectIDs(left), eraseAErr, erasedErr)
		}
	})

	t.Run("search filters narrow hits the same way in every store", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		open := newChunk(f.docA, f.a, "roadmap open item", nil)
		open.Properties = map[string]any{"stage": "open", "amount": 10.0, "urgent": true}
		closed := newChunk(f.docA, f.a, "roadmap closed item", nil)
		closed.Kind, closed.Position = "Note", new(2)
		closed.Properties = map[string]any{"stage": "closed", "amount": 20.0, "urgent": false, "owner": nil, "meta": map[string]any{"a": 1.0}}
		must(t, putTree(ctx, s, f.scopeA, parent, []brain.Object{open, closed}))
		both := []uuid.UUID{open.ID, closed.ID}

		for _, tc := range []struct {
			filter map[string]any
			want   []uuid.UUID
		}{
			{map[string]any{"stage": "open"}, []uuid.UUID{open.ID}},
			{map[string]any{"stage": []any{"open", "closed"}}, both},
			{map[string]any{"amount": 10}, []uuid.UUID{open.ID}},
			{map[string]any{"amount": int64(20)}, []uuid.UUID{closed.ID}},
			{map[string]any{"amount": float32(10)}, []uuid.UUID{open.ID}},
			{map[string]any{"amount": json.Number("20")}, []uuid.UUID{closed.ID}},
			{map[string]any{"amount": []any{int32(10), 20.0}}, both},
			{map[string]any{"urgent": false}, []uuid.UUID{closed.ID}},
			{map[string]any{"kind": []any{"Note"}}, []uuid.UUID{closed.ID}},
			{map[string]any{"title": "chunk"}, both},
			{map[string]any{"updated_after": "2000-01-01"}, both},
			{map[string]any{"updated_before": "2000-01-01"}, nil},
			{map[string]any{"missing": "x"}, nil},
			{map[string]any{"owner": "x"}, nil},
			{map[string]any{"meta": "x"}, nil},
			{map[string]any{"created_after": "2000-01-01"}, both},
		} {
			filter, err := brain.DecodeFilter(tc.filter)
			must(t, err)
			hits, err := s.SearchLexical(ctx, f.scopeA, "roadmap", filter, 10)
			got := scoredIDs(hits)
			slices.SortFunc(got, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
			want := slices.SortedFunc(slices.Values(tc.want), func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
			if err != nil || !slices.Equal(got, want) {
				t.Errorf("filter %v = %v %v, want %v", tc.filter, got, err, want)
			}
		}
	})

	t.Run("vector search compares only vectors from the query's model", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)

		same, _ := s.SearchVector(ctx, f.scopeA, Model, []float32{1, 0, 0}, brain.Filter{}, 10)
		other, _ := s.SearchVector(ctx, f.scopeA, "other-model", []float32{1, 0, 0}, brain.Filter{}, 10)

		if got := scoredIDs(same); !slices.Equal(got, []uuid.UUID{f.chunkA}) {
			t.Errorf("same model = %v", got)
		}
		if len(other) != 0 {
			t.Errorf("other model = %v, want no hits", scoredIDs(other))
		}
	})

	t.Run("purge and erase remove only the scope's rows", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		_, err := s.SaveSessionMessages(ctx, f.a, "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "transcript"}})
		must(t, err)
		_, err = s.SaveSessionMessages(ctx, f.b, "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "transcript"}})
		must(t, err)
		must(t, s.SoftDelete(ctx, f.scopeB, f.chunkB))

		purgedB, purgeErr := s.PurgeDeleted(ctx, f.scopeB, time.Now().Add(time.Minute))
		erased, eraseErr := s.Erase(ctx, f.scopeA)
		_, goneErr := s.Get(ctx, f.scopeA, f.docA)
		sessionA, _ := s.SearchLexical(ctx, brain.Scope{Namespace: f.a, SessionID: "s1"}, "transcript", brain.Filter{}, 10)
		keptB, keptErr := s.Get(ctx, f.scopeB, f.docB)
		sessionB, _ := s.SearchLexical(ctx, brain.Scope{Namespace: f.b, SessionID: "s1"}, "transcript", brain.Filter{}, 10)
		purgedSessions, sessErr := s.PurgeSessionMessages(ctx, f.b, time.Now().Add(time.Minute))

		if purgeErr != nil || purgedB != 1 {
			t.Errorf("purge deleted = %d %v, want 1", purgedB, purgeErr)
		}
		if eraseErr != nil || !slices.Equal(erased, []uuid.UUID{f.docA}) || !errors.Is(goneErr, brain.ErrNotFound) || len(sessionA) != 0 {
			t.Errorf("erase = %v %v, then get %v, session hits %v", erased, eraseErr, goneErr, sessionA)
		}
		if keptErr != nil || keptB.ID != f.docB || len(sessionB) != 1 {
			t.Errorf("tenant B after A's erase = %v %v, session hits %v", keptB.ID, keptErr, sessionB)
		}
		if sessErr != nil || purgedSessions != 1 {
			t.Errorf("purge session messages = %d %v, want 1", purgedSessions, sessErr)
		}
	})

	t.Run("deleting a session's messages leaves the same session id under another host", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore)
		msg := []brain.SessionMessage{{Role: "user", SearchText: "transcript"}}
		_, err := s.SaveSessionMessages(ctx, f.a, "s1", "k", msg)
		must(t, err)
		_, err = s.SaveSessionMessages(ctx, f.b, "s1", "k", msg)
		must(t, err)

		must(t, s.DeleteSessionMessages(ctx, f.a, "s1"))
		left, _ := s.SearchLexical(ctx, brain.Scope{Namespace: f.b, SessionID: "s1"}, "transcript", brain.Filter{}, 10)

		if len(left) != 1 {
			t.Errorf("host B session hits after host A delete = %v, want 1", left)
		}
	})
}

type fixture struct {
	a, b           brain.Namespace
	scopeA, scopeB brain.Scope
	docA, docB     uuid.UUID
	chunkA, chunkB uuid.UUID
}

// arrange writes one Document with one Chunk per tenant. Both carry the same
// path property and text, so only the scope tells them apart.
func arrange(t *testing.T, newStore func(t *testing.T) Store) (context.Context, Store, fixture) {
	t.Helper()
	ctx := t.Context()
	s := newStore(t)
	f := fixture{
		a: brain.Namespace{{Name: "org", Value: "a"}},
		b: brain.Namespace{{Name: "org", Value: "b"}},
	}
	f.scopeA, f.scopeB = brain.Scope{Namespace: f.a}, brain.Scope{Namespace: f.b}
	for _, tenant := range []struct {
		ns    brain.Namespace
		doc   *uuid.UUID
		chunk *uuid.UUID
		title string
	}{{f.a, &f.docA, &f.chunkA, "doc a"}, {f.b, &f.docB, &f.chunkB, "doc b"}} {
		doc := newDoc(tenant.ns, tenant.title)
		chunk := newChunk(doc.ID, tenant.ns, "shared roadmap text", []float32{1, 0, 0})
		must(t, putTree(ctx, s, brain.Scope{Namespace: tenant.ns}, doc, []brain.Object{chunk}))
		*tenant.doc, *tenant.chunk = doc.ID, chunk.ID
	}
	return ctx, s, f
}

// putTree writes parent with parts through the three tree steps, staging
// every part as new.
func putTree(ctx context.Context, s Store, scope brain.Scope, parent brain.Object, parts []brain.Object) error {
	state, err := s.TreeState(ctx, scope, parent.ID)
	if err != nil {
		return err
	}
	run := uuid.New()
	staged := make([]brain.TreePart, len(parts))
	for i, p := range parts {
		staged[i] = brain.TreePart{Object: p}
	}
	if err := s.StageParts(ctx, scope, run, staged); err != nil {
		return err
	}
	return s.CommitTree(ctx, scope, run, state.Rev, parent)
}

func newDoc(ns brain.Namespace, title string) brain.Object {
	return brain.Object{
		ID: uuid.New(), Kind: "Document", Title: title, Namespace: ns,
		Properties: map[string]any{"path": "/shared.md"},
	}
}

func newChunk(parent uuid.UUID, ns brain.Namespace, text string, vec []float32) brain.Object {
	return brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "chunk", Content: text, Namespace: ns,
		ParentID: &parent, Position: new(1), Embedding: vec, EmbeddingModel: Model,
	}
}

func objectIDs(objs []brain.Object) []uuid.UUID {
	out := make([]uuid.UUID, len(objs))
	for i, o := range objs {
		out[i] = o.ID
	}
	return out
}

func scoredIDs(hits []brain.ScoredID) []uuid.UUID {
	out := make([]uuid.UUID, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
