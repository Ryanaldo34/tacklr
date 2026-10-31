// Package storetest is the contract every brain store must meet. Run it from
// a store's own tests (MemoryStore and postgres.Store do), so a new store or
// store method cannot ship without the same tenant, tree and search rules.
//
// Two tenants share one store. Each case asserts what a caller gets under
// its own scope: its own rows, ErrNotFound for another tenant's ids, and an
// error for an empty scope. A store owns its embedding, so the contract hands
// newStore the Embedder a case needs (nil: lexical only).
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// Dimension is the length of every vector Embedder returns.
const Dimension = 3

// Embedder embeds a text as the vector of the first word of a synonym group
// in it (car and automobile share one vector, banana and fruit another;
// anything else is a third), counts every text it embeds, and fails on a
// text with the word poison.
type Embedder struct{ texts atomic.Int64 }

var synonyms = map[string][]float32{
	"car": {1, 0, 0}, "automobile": {1, 0, 0},
	"banana": {0, 1, 0}, "fruit": {0, 1, 0},
}

// Embed implements brain.Embedder.
func (e *Embedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		if strings.Contains(text, "poison") {
			return nil, errors.New("embedder down")
		}
		out[i] = []float32{0, 0, 1}
		for _, word := range strings.Fields(strings.ToLower(text)) {
			if v, ok := synonyms[word]; ok {
				out[i] = v
				break
			}
		}
	}
	e.texts.Add(int64(len(texts)))
	return out, nil
}

// Model implements brain.Embedder.
func (*Embedder) Model() string { return "storetest-synonyms" }

// Texts is how many texts e has embedded.
func (e *Embedder) Texts() int { return int(e.texts.Load()) }

// Run runs the contract. newStore returns an empty store for each case,
// which embeds with emb (nil: lexical only).
func Run(t *testing.T, newStore func(t *testing.T, emb brain.Embedder) brain.Store) {
	t.Run("another tenant's id is not found for reads and writes", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		takeover := newDoc(f.b, "stolen")
		takeover.ID = f.docA

		_, putErr := s.Put(ctx, f.scopeB, indexed(takeover))
		_, treeErr := s.PutTree(ctx, f.scopeB, indexed(takeover), parts())
		delErr := s.SoftDelete(ctx, f.scopeB, f.docA)
		_, getErr := s.Get(ctx, f.scopeB, f.docA)
		_, treeReadErr := s.GetTree(ctx, f.scopeB, f.docA, 0)
		mine, mineErr := s.Get(ctx, f.scopeA, f.docA)

		for name, err := range map[string]error{"put": putErr, "put tree": treeErr, "soft delete": delErr, "get": getErr, "get tree": treeReadErr} {
			if !errors.Is(err, brain.ErrNotFound) {
				t.Errorf("%s as tenant B = %v, want ErrNotFound", name, err)
			}
		}
		if mineErr != nil || mine.Title != "doc a" || !mine.Namespace.Equal(f.a) {
			t.Errorf("tenant A row after B's writes = %+v %v", mine, mineErr)
		}
	})

	t.Run("every read and search returns only the caller's tenant", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, &Embedder{})

		many, _ := s.GetMany(ctx, f.scopeB, []uuid.UUID{f.docA, f.docB})
		byProp, propErr := s.GetByProperty(ctx, f.scopeB, "path", "/shared.md")
		semantic, _ := s.Search(ctx, f.scopeB, brain.RecordSearch{Query: "shared", Limit: 10})
		exact, _ := s.Search(ctx, f.scopeB, brain.RecordSearch{Query: "shared", Exact: true, Limit: 10})

		if got := objectIDs(many); !slices.Equal(got, []uuid.UUID{f.docB}) {
			t.Errorf("get many = %v, want [%s]", got, f.docB)
		}
		if propErr != nil || byProp.ID != f.docB {
			t.Errorf("get by property = %s %v", byProp.ID, propErr)
		}
		for name, hits := range map[string][]brain.ScoredID{"search": semantic, "exact search": exact} {
			mine := []uuid.UUID{f.docB, f.chunkB}
			for _, h := range hits {
				if !slices.Contains(mine, h.ID) || (h.ID == f.chunkB && *h.ParentID != f.docB) {
					t.Errorf("%s hit %s, want only tenant B's record and its part", name, h.ID)
				}
			}
			if !slices.Contains(scoredIDs(hits), f.chunkB) {
				t.Errorf("%s = %v, want tenant B's matching part", name, scoredIDs(hits))
			}
		}
	})

	t.Run("an empty scope is rejected by every scoped method", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		var none brain.Scope

		errs := map[string]error{"soft delete": s.SoftDelete(ctx, none, f.docA)}
		_, errs["put"] = s.Put(ctx, none, indexed(newDoc(f.a, "x")))
		_, errs["put tree"] = s.PutTree(ctx, none, indexed(newDoc(f.a, "x")), parts())
		_, errs["get"] = s.Get(ctx, none, f.docA)
		_, errs["get many"] = s.GetMany(ctx, none, []uuid.UUID{f.docA})
		_, errs["get tree"] = s.GetTree(ctx, none, f.docA, 0)
		_, errs["get by property"] = s.GetByProperty(ctx, none, "path", "/shared.md")
		_, errs["search"] = s.Search(ctx, none, brain.RecordSearch{Query: "shared", Limit: 10})
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

	t.Run("a scope on another tenant reaches nothing, even with the same attrs", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		teamNS := append(f.a.Clone(), brain.Attr{Name: "team", Value: "x"})
		doc := newDoc(teamNS, "team plan")
		_, err := s.PutTree(ctx, brain.Scope{Namespace: teamNS}, indexed(doc), parts(newChunk(doc, "shared roadmap text", 0)))
		must(t, err)
		// The same attrs in another order name another tenant (team=x).
		other := brain.Scope{Namespace: brain.Namespace{{Name: "team", Value: "x"}, f.a[0]}}

		_, getErr := s.Get(ctx, other, doc.ID)
		hits, _ := s.Search(ctx, other, brain.RecordSearch{Query: "shared", Limit: 10})
		erased, eraseErr := s.Erase(ctx, other)
		kept, keptErr := s.Get(ctx, brain.Scope{Namespace: teamNS}, doc.ID)

		if !errors.Is(getErr, brain.ErrNotFound) || len(hits) != 0 {
			t.Errorf("another tenant's get = %v, search = %v, want nothing", getErr, scoredIDs(hits))
		}
		if eraseErr != nil || len(erased) != 0 || keptErr != nil || kept.ID != doc.ID {
			t.Errorf("another tenant's erase = %v %v, then the owner's get = %v, want nothing erased", erased, eraseErr, keptErr)
		}
	})

	t.Run("readers limit an object to scopes holding one of its principals", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		restricted := newDoc(f.a, "board minutes")
		restricted.Readers = []string{"group:board"}
		chunk := newChunk(restricted, "quarterly board minutes", 0)
		_, err := s.PutTree(ctx, f.scopeA, indexed(restricted), parts(chunk))
		must(t, err)
		board := brain.Scope{Namespace: f.a, Principals: []string{"user:ann", "group:board"}}

		got, getErr := s.Get(ctx, board, restricted.ID)
		hits, _ := s.Search(ctx, board, brain.RecordSearch{Query: "quarterly", Limit: 10})
		_, hiddenErr := s.Get(ctx, f.scopeA, restricted.ID)
		hidden, _ := s.Search(ctx, f.scopeA, brain.RecordSearch{Query: "quarterly", Limit: 10})
		_, updateErr := s.Put(ctx, f.scopeA, indexed(restricted))

		if getErr != nil || !slices.Equal(got.Readers, []string{"group:board"}) {
			t.Errorf("board member get = %+v %v", got, getErr)
		}
		if got := scoredIDs(hits); !slices.Equal(got, []uuid.UUID{chunk.ID}) {
			t.Errorf("board member search = %v", got)
		}
		if !errors.Is(hiddenErr, brain.ErrNotFound) || len(hidden) != 0 || !errors.Is(updateErr, brain.ErrNotFound) {
			t.Errorf("non-member get = %v, search = %v, put = %v, want nothing", hiddenErr, scoredIDs(hidden), updateErr)
		}
	})

	t.Run("put tree replaces parts atomically and soft delete takes the parts too", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		next := newChunk(parent, "replacement text", 0)
		outside := newChunk(parent, "other tenant", 1)
		outside.Namespace = f.b
		failing := func(yield func(brain.Indexed, error) bool) {
			if yield(next, nil) {
				yield(brain.Indexed{}, errors.New("source read failed"))
			}
		}

		_, outsideErr := s.PutTree(ctx, f.scopeA, indexed(parent), parts(next, outside))
		_, sourceErr := s.PutTree(ctx, f.scopeA, indexed(parent), failing)
		kept := treeParts(ctx, s, f.scopeA, f.docA)
		_, replaced := s.PutTree(ctx, f.scopeA, indexed(parent), parts(next))
		swapped := treeParts(ctx, s, f.scopeA, f.docA)
		deleted := s.SoftDelete(ctx, f.scopeA, f.docA)
		_, partErr := s.Get(ctx, f.scopeA, next.ID)

		if !errors.Is(outsideErr, brain.ErrInvalid) || sourceErr == nil || !strings.Contains(sourceErr.Error(), "source read failed") {
			t.Errorf("tree with a part outside scope = %v, with a failing source = %v, want both to fail", outsideErr, sourceErr)
		}
		if got := objectIDs(kept); !slices.Equal(got, []uuid.UUID{f.chunkA}) {
			t.Errorf("parts after the failed trees = %v, want the original [%s]", got, f.chunkA)
		}
		if replaced != nil || !slices.Equal(objectIDs(swapped), []uuid.UUID{next.ID}) {
			t.Errorf("parts after replace = %v %v", objectIDs(swapped), replaced)
		}
		if deleted != nil || !errors.Is(partErr, brain.ErrNotFound) {
			t.Errorf("part after parent soft delete = %v (delete %v)", partErr, deleted)
		}
	})

	t.Run("put tree indexes only new parts, moves reused ones, and loses to a tree committed first", func(t *testing.T) {
		emb := &Embedder{}
		ctx, s, f := arrange(t, newStore, emb)
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		first, second := newChunk(parent, "car manual", 0), newChunk(parent, "banana bread", 1)
		_, err := s.PutTree(ctx, f.scopeA, indexed(parent), parts(first, second))
		must(t, err)
		before := emb.Texts()
		moved := second
		moved.Position = new(0)
		added := newChunk(parent, "appendix text", 1)

		_, reorderErr := s.PutTree(ctx, f.scopeA, indexed(parent), parts(moved, added))
		reordered := treeParts(ctx, s, f.scopeA, f.docA)
		embedded := emb.Texts() - before
		// A tree of the same parent commits while this one reads its parts.
		racing := func(yield func(brain.Indexed, error) bool) {
			if !yield(newChunk(parent, "loser text", 0), nil) {
				return
			}
			_, err := s.PutTree(ctx, f.scopeA, indexed(parent), parts(newChunk(parent, "winner text", 0)))
			must(t, err)
		}
		_, conflictErr := s.PutTree(ctx, f.scopeA, indexed(parent), racing)
		won := treeParts(ctx, s, f.scopeA, f.docA)
		bySynonym, _ := s.Search(ctx, f.scopeA, brain.RecordSearch{Query: "winner", Limit: 10})
		// A reused part is purged while this tree reads its parts.
		purging := func(yield func(brain.Indexed, error) bool) {
			must(t, s.SoftDelete(ctx, f.scopeA, won[0].ID))
			_, err := s.PurgeDeleted(ctx, f.scopeA, time.Now().Add(time.Minute))
			must(t, err)
			yield(brain.Indexed{Object: won[0], Text: won[0].Content}, nil)
		}
		_, goneErr := s.PutTree(ctx, f.scopeA, indexed(parent), purging)
		twin := newChunk(parent, "twin text", 0)
		_, twiceErr := s.PutTree(ctx, f.scopeA, indexed(parent), parts(twin, twin))

		if reorderErr != nil || !slices.Equal(contents(reordered), []string{"banana bread", "appendix text"}) {
			t.Errorf("parts after reorder = %v %v, want the reused part first, then the new one", contents(reordered), reorderErr)
		}
		if embedded != 2 {
			t.Errorf("reorder embedded %d texts, want the parent and the new part (2)", embedded)
		}
		if !errors.Is(conflictErr, brain.ErrConflict) || !slices.Equal(contents(won), []string{"winner text"}) || !slices.Contains(scoredIDs(bySynonym), won[0].ID) {
			t.Errorf("tree that lost the race = %v, parts %v, want ErrConflict and the winner's tree", conflictErr, contents(won))
		}
		if !errors.Is(goneErr, brain.ErrConflict) || !errors.Is(twiceErr, brain.ErrInvalid) {
			t.Errorf("tree reusing a purged part = %v, with a part twice = %v, want ErrConflict and ErrInvalid", goneErr, twiceErr)
		}
	})

	t.Run("get tree reads a record with its first parts, or a part with its record and its record's parts", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		first, second, third := newChunk(parent, "one", 0), newChunk(parent, "two", 1), newChunk(parent, "three", 2)
		_, err := s.PutTree(ctx, f.scopeA, indexed(parent), parts(third, first, second))
		must(t, err)

		record, recordErr := s.GetTree(ctx, f.scopeA, f.docA, 2)
		part, partErr := s.GetTree(ctx, f.scopeA, second.ID, 0)
		_, missingErr := s.GetTree(ctx, f.scopeA, uuid.New(), 0)

		if recordErr != nil || record.Object.ID != f.docA || record.Parent != nil || !slices.Equal(objectIDs(record.Parts), []uuid.UUID{first.ID, second.ID}) {
			t.Errorf("tree of doc a, two parts = %+v %v, want doc a with its first two parts by position", record, recordErr)
		}
		if partErr != nil || part.Object.ID != second.ID || part.Parent == nil || part.Parent.ID != f.docA || !slices.Equal(objectIDs(part.Parts), []uuid.UUID{first.ID, second.ID, third.ID}) {
			t.Errorf("tree of a part = %+v %v, want the part, doc a, and every part of doc a", part, partErr)
		}
		if !errors.Is(missingErr, brain.ErrNotFound) {
			t.Errorf("tree of an unknown id = %v, want ErrNotFound", missingErr)
		}
	})

	t.Run("search ranks by meaning, and exact search by the words and the title", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, &Embedder{})
		parent := newDoc(f.a, "doc a")
		parent.ID = f.docA
		car, title := newChunk(parent, "the car is red", 0), newChunk(parent, "red notes", 1)
		title.Title = "red"
		_, err := s.PutTree(ctx, f.scopeA, indexed(parent), parts(car, title))
		must(t, err)
		search := func(q brain.RecordSearch) []uuid.UUID {
			q.Limit = 10
			hits, err := s.Search(ctx, f.scopeA, q)
			must(t, err)
			return scoredIDs(hits)
		}

		if got := search(brain.RecordSearch{Query: "automobile"}); len(got) == 0 || got[0] != car.ID {
			t.Errorf("search by a synonym = %v, want the car part first", got)
		}
		if got := search(brain.RecordSearch{Query: "automobile", Exact: true}); len(got) != 0 {
			t.Errorf("exact search by a synonym = %v, want nothing", got)
		}
		if got := search(brain.RecordSearch{Query: "red", Exact: true}); len(got) != 2 || got[0] != title.ID {
			t.Errorf("exact search = %v, want the part titled red first", got)
		}
		if hits, _ := s.Search(ctx, f.scopeA, brain.RecordSearch{Query: "red", Limit: 1}); len(hits) != 1 {
			t.Errorf("search with limit 1 = %v", scoredIDs(hits))
		}
		if got := search(brain.RecordSearch{Query: "!?", Exact: true}); len(got) != 0 {
			t.Errorf("exact search with no words = %v, want nothing", got)
		}
	})

	t.Run("an embedding failure fails a write, but not a session save or a search", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, &Embedder{})
		poisoned := newDoc(f.a, "poison")

		_, putErr := s.Put(ctx, f.scopeA, indexed(poisoned))
		tree := newDoc(f.a, "doc")
		_, treeErr := s.PutTree(ctx, f.scopeA, indexed(tree), parts(newChunk(tree, "poison part", 0)))
		_, getErr := s.Get(ctx, f.scopeA, poisoned.ID)
		_, saveErr := s.SaveSessionMessages(ctx, f.a, "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "poison transcript"}})
		hits, _ := s.Search(ctx, brain.Scope{Namespace: f.a, SessionID: "s1"}, brain.RecordSearch{Query: "transcript", Exact: true, Limit: 10})
		byWords, searchErr := s.Search(ctx, f.scopeA, brain.RecordSearch{Query: "poison roadmap", Limit: 10})

		if searchErr != nil || !slices.Equal(scoredIDs(byWords), []uuid.UUID{f.chunkA}) {
			t.Errorf("search whose query does not embed = %v %v, want the lexical hit", scoredIDs(byWords), searchErr)
		}
		if putErr == nil || treeErr == nil || !errors.Is(getErr, brain.ErrNotFound) {
			t.Errorf("writes with a failing embedder = %v, %v, then get %v, want them to fail and store nothing", putErr, treeErr, getErr)
		}
		if saveErr != nil || len(hits) != 1 {
			t.Errorf("session save with a failing embedder = %v, then %d hits, want it saved and found", saveErr, len(hits))
		}
	})

	t.Run("search filters narrow hits the same way in every store", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		// Filters test the record a row belongs to, so the records carry the
		// fields and the hits are their parts.
		openDoc := newDoc(f.a, "open deal")
		openDoc.ID = f.docA
		openDoc.Properties = map[string]any{"stage": "open", "amount": 10.0, "urgent": true}
		closedDoc := newDoc(f.a, "closed deal")
		closedDoc.Kind = "Note"
		closedDoc.Properties = map[string]any{"stage": "closed", "amount": 20.0, "urgent": false, "owner": nil, "meta": map[string]any{"a": 1.0}}
		open, closed := newChunk(openDoc, "roadmap open item", 0), newChunk(closedDoc, "roadmap closed item", 0)
		for _, tree := range []struct {
			doc  brain.Object
			part brain.Indexed
		}{{openDoc, open}, {closedDoc, closed}} {
			_, err := s.PutTree(ctx, f.scopeA, indexed(tree.doc), parts(tree.part))
			must(t, err)
		}
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
			{map[string]any{"title": "open deal"}, []uuid.UUID{open.ID}},
			{map[string]any{"updated_after": "2000-01-01"}, both},
			{map[string]any{"updated_before": "2000-01-01"}, nil},
			{map[string]any{"missing": "x"}, nil},
			{map[string]any{"owner": "x"}, nil},
			{map[string]any{"meta": "x"}, nil},
			{map[string]any{"created_after": "2000-01-01"}, both},
		} {
			filter, err := brain.DecodeFilter(tc.filter)
			must(t, err)
			hits, err := s.Search(ctx, f.scopeA, brain.RecordSearch{Query: "roadmap", Filters: filter, Limit: 10})
			got := sortedIDs(scoredIDs(hits))
			if err != nil || !slices.Equal(got, sortedIDs(tc.want)) {
				t.Errorf("filter %v = %v %v, want %v", tc.filter, got, err, tc.want)
			}
		}
	})

	t.Run("within keeps one record's parts, and an empty query lists the filtered records newest first", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		hour := time.Hour
		older, newer, closed := newDoc(f.a, "older"), newDoc(f.a, "newer"), newDoc(f.a, "closed")
		older.UpdatedAt, newer.UpdatedAt, closed.UpdatedAt = older.UpdatedAt.Add(-3*hour), newer.UpdatedAt.Add(-2*hour), closed.UpdatedAt.Add(-hour)
		older.Properties = map[string]any{"stage": "open"}
		newer.Properties = map[string]any{"stage": "open"}
		closed.Properties = map[string]any{"stage": "closed"}
		_, err := s.PutTree(ctx, f.scopeA, indexed(older), parts(newChunk(older, "shared roadmap text", 0)))
		must(t, err)
		for _, doc := range []brain.Object{newer, closed} {
			_, err := s.Put(ctx, f.scopeA, indexed(doc))
			must(t, err)
		}
		_, err = s.SaveSessionMessages(ctx, f.a, "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "shared roadmap text"}})
		must(t, err)
		session := brain.Scope{Namespace: f.a, SessionID: "s1"}
		open, err := brain.DecodeFilter(map[string]any{"stage": "open"})
		must(t, err)
		all, err := brain.DecodeFilter(map[string]any{"updated_after": "2000-01-01"})
		must(t, err)

		within, withinErr := s.Search(ctx, session, brain.RecordSearch{Query: "roadmap", Within: f.docA, Limit: 10})
		exactWithin, exactErr := s.Search(ctx, session, brain.RecordSearch{Query: "roadmap", Exact: true, Within: f.docA, Limit: 10})
		byPart, byPartErr := s.Search(ctx, f.scopeA, brain.RecordSearch{Query: "roadmap", Within: f.chunkA, Limit: 10})
		listed, listErr := s.Search(ctx, f.scopeA, brain.RecordSearch{Filters: open, Limit: 10})
		newest, newestErr := s.Search(ctx, f.scopeA, brain.RecordSearch{Filters: all, Limit: 2})

		for name, tc := range map[string]struct {
			got  []brain.ScoredID
			err  error
			want []uuid.UUID
		}{
			"search within doc a":           {within, withinErr, []uuid.UUID{f.chunkA}},
			"exact search within doc a":     {exactWithin, exactErr, []uuid.UUID{f.chunkA}},
			"search within a part of doc a": {byPart, byPartErr, []uuid.UUID{f.chunkA}},
			"list the open records":         {listed, listErr, []uuid.UUID{newer.ID, older.ID}},
			"list the newest two records":   {newest, newestErr, []uuid.UUID{f.docA, closed.ID}},
		} {
			if tc.err != nil || !slices.Equal(scoredIDs(tc.got), tc.want) {
				t.Errorf("%s = %v %v, want %v", name, scoredIDs(tc.got), tc.err, tc.want)
			}
		}
	})

	t.Run("search ranks a record by its own text, with no part", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		policy := newDoc(f.a, "Refund policy")
		policy.Content = "refunds within thirty days"
		_, err := s.Put(ctx, f.scopeA, brain.Indexed{Object: policy, Text: policy.Title + " " + policy.Content})
		must(t, err)

		hits, err := s.Search(ctx, f.scopeA, brain.RecordSearch{Query: "refunds", Limit: 10})

		if err != nil || !slices.Equal(scoredIDs(hits), []uuid.UUID{policy.ID}) || hits[0].ParentID != nil {
			t.Errorf("search a record's own words = %v %v, want the record, with no parent", scoredIDs(hits), err)
		}
	})

	t.Run("purge and erase remove only the scope's rows", func(t *testing.T) {
		ctx, s, f := arrange(t, newStore, nil)
		_, err := s.SaveSessionMessages(ctx, f.a, "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "transcript"}})
		must(t, err)
		_, err = s.SaveSessionMessages(ctx, f.b, "s1", "k", []brain.SessionMessage{{Role: "user", SearchText: "transcript"}})
		must(t, err)
		must(t, s.SoftDelete(ctx, f.scopeB, f.chunkB))

		purgedB, purgeErr := s.PurgeDeleted(ctx, f.scopeB, time.Now().Add(time.Minute))
		erased, eraseErr := s.Erase(ctx, f.scopeA)
		_, goneErr := s.Get(ctx, f.scopeA, f.docA)
		sessionA, _ := s.Search(ctx, brain.Scope{Namespace: f.a, SessionID: "s1"}, brain.RecordSearch{Query: "transcript", Limit: 10})
		keptB, keptErr := s.Get(ctx, f.scopeB, f.docB)
		sessionB, _ := s.Search(ctx, brain.Scope{Namespace: f.b, SessionID: "s1"}, brain.RecordSearch{Query: "transcript", Limit: 10})
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
		ctx, s, f := arrange(t, newStore, &Embedder{})
		msg := []brain.SessionMessage{{Role: "user", SearchText: "transcript about a car"}}
		_, err := s.SaveSessionMessages(ctx, f.a, "s1", "k", msg)
		must(t, err)
		_, err = s.SaveSessionMessages(ctx, f.b, "s1", "k", msg)
		must(t, err)

		must(t, s.DeleteSessionMessages(ctx, f.a, "s1"))
		left, _ := s.Search(ctx, brain.Scope{Namespace: f.b, SessionID: "s1"}, brain.RecordSearch{Query: "automobile", Limit: 10})

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

// arrange writes one Document with one Chunk per tenant into a store that
// embeds with emb. Both carry the same path property and text, so only the
// scope tells them apart.
func arrange(t *testing.T, newStore func(t *testing.T, emb brain.Embedder) brain.Store, emb *Embedder) (context.Context, brain.Store, fixture) {
	t.Helper()
	ctx := t.Context()
	var e brain.Embedder
	if emb != nil {
		e = emb
	}
	s := newStore(t, e)
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
		chunk := newChunk(doc, "shared roadmap text", 0)
		_, err := s.PutTree(ctx, brain.Scope{Namespace: tenant.ns}, indexed(doc), parts(chunk))
		must(t, err)
		*tenant.doc, *tenant.chunk = doc.ID, chunk.ID
	}
	return ctx, s, f
}

func newDoc(ns brain.Namespace, title string) brain.Object {
	now := time.Now().UTC()
	return brain.Object{
		ID: uuid.New(), Kind: "Document", Title: title, Namespace: ns,
		Properties: map[string]any{"path": "/shared.md"}, CreatedAt: now, UpdatedAt: now,
	}
}

// newChunk is a part of parent at position, indexed by its content, as
// Engine hands it to PutTree.
func newChunk(parent brain.Object, content string, position int) brain.Indexed {
	return brain.Indexed{Object: brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "chunk", Content: content, Namespace: parent.Namespace,
		Readers: parent.Readers, ParentID: &parent.ID, Position: new(position), Properties: map[string]any{},
		CreatedAt: parent.CreatedAt, UpdatedAt: parent.UpdatedAt,
	}, Text: content}
}

func indexed(obj brain.Object) brain.Indexed {
	return brain.Indexed{Object: obj, Text: obj.Title}
}

func parts(in ...brain.Indexed) iter.Seq2[brain.Indexed, error] {
	return func(yield func(brain.Indexed, error) bool) {
		for _, p := range in {
			if !yield(p, nil) {
				return
			}
		}
	}
}

func contents(objs []brain.Object) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Content
	}
	return out
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

func sortedIDs(in []uuid.UUID) []uuid.UUID {
	return slices.SortedFunc(slices.Values(in), func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// treeParts is id's record's parts under scope.
func treeParts(ctx context.Context, s brain.Store, scope brain.Scope, id uuid.UUID) []brain.Object {
	t, _ := s.GetTree(ctx, scope, id, 0)
	return t.Parts
}
