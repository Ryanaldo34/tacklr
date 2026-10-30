// Package graphtest is the contract every brain graph backend must meet. Run
// it from a backend's own tests (MemoryGraph and helixgraph do), so the
// in-memory graph and the real one cannot drift apart.
//
// Each case writes fresh object ids, namespaces and search words, so cases
// never see each other's data, even on one long-lived graph.
package graphtest

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// Graph is every capability the contract checks.
type Graph interface {
	brain.GraphWriter
	brain.GraphObjectSearcher
	brain.GraphEdgeSearcher
}

// EdgeSearchLabel is the one relation the contract searches edge notes on.
// A backend that indexes edge text per relation indexes it before Run.
const EdgeSearchLabel = "cites"

// Run runs the contract. newGraph returns the graph each case writes to.
// Vectors have two dimensions.
func Run(t *testing.T, newGraph func(t *testing.T) Graph) {
	t.Run("neighbors follow edges both ways, by relation, once per object", func(t *testing.T) {
		ctx, g := t.Context(), newGraph(t)
		ns := namespace()
		a, b, c := node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, "")
		must(t, g.AddEdge(ctx, a, b, "references", brain.EdgeMeta{}))
		must(t, g.AddEdge(ctx, c, a, "references", brain.EdgeMeta{}))
		must(t, g.AddEdge(ctx, a, b, "depends_on", brain.EdgeMeta{}))

		refs, _ := g.Neighbors(ctx, a, []string{"references"}, 10)
		deps, _ := g.Neighbors(ctx, a, []string{"depends_on"}, 10)
		both, _ := g.Neighbors(ctx, a, []string{"references", "depends_on"}, 10)
		first, _ := g.Neighbors(ctx, a, []string{"references"}, 1)
		none := map[string][]string{"no relations": nil, "blank relations": {" ", ""}}
		for name, rels := range none {
			if got, err := g.Neighbors(ctx, a, rels, 10); err != nil || len(got) != 0 {
				t.Errorf("%s = %v %v, want none", name, got, err)
			}
		}
		for name, id := range map[string]uuid.UUID{"unknown object": uuid.New(), "nil object": uuid.Nil} {
			if got, err := g.Neighbors(ctx, id, []string{"references"}, 10); err != nil || len(got) != 0 {
				t.Errorf("%s = %v %v, want none", name, got, err)
			}
		}
		if got, err := g.Neighbors(ctx, a, []string{"references"}, 0); err != nil || len(got) != 0 {
			t.Errorf("limit 0 = %v %v, want none", got, err)
		}

		want := []brain.GraphNeighbor{{ObjectID: b, RelationType: "references", Direction: "out"}, {ObjectID: c, RelationType: "references", Direction: "in"}}
		if got := shape(refs); !slices.Equal(got, want) {
			t.Errorf("references = %v, want %v (outbound first)", got, want)
		}
		if got := shape(deps); !slices.Equal(got, []brain.GraphNeighbor{{ObjectID: b, RelationType: "depends_on", Direction: "out"}}) {
			t.Errorf("depends_on = %v", got)
		}
		if got := shape(both); !slices.Equal(got, want) {
			t.Errorf("both relations = %v, want %v (each object once, request order)", got, want)
		}
		if got := shape(first); !slices.Equal(got, want[:1]) {
			t.Errorf("limit 1 = %v", got)
		}
	})

	t.Run("an edge carries its metadata, and adding it again replaces it", func(t *testing.T) {
		ctx, g := t.Context(), newGraph(t)
		ns := namespace()
		a, b := node(t, g, ns, ""), node(t, g, ns, "")
		evidence := uuid.New()
		first := brain.EdgeMeta{Note: "first note", Status: "active", Role: "primary", Confidence: 0.75, EvidenceID: &evidence}
		must(t, g.AddEdge(ctx, a, b, "references", first))
		before, _ := g.Neighbors(ctx, a, []string{"references"}, 10)
		must(t, g.AddEdge(ctx, a, b, "references", brain.EdgeMeta{Note: "second note"}))
		after, _ := g.Neighbors(ctx, a, []string{"references"}, 10)

		if len(before) != 1 || !sameMeta(before[0].Meta, first) || before[0].Meta.CreatedAt.IsZero() || before[0].Meta.UpdatedAt.IsZero() {
			t.Errorf("edge meta = %+v, want %+v with times set", before, first)
		}
		if len(after) != 1 || after[0].Meta.Note != "second note" || after[0].Meta.Status != "" {
			t.Errorf("after re-adding the edge = %+v, want one edge with the new meta only", after)
		}
	})

	t.Run("removing an edge or an object takes exactly its edges", func(t *testing.T) {
		ctx, g := t.Context(), newGraph(t)
		ns := namespace()
		word := searchWord()
		a, b, c := node(t, g, ns, word), node(t, g, ns, ""), node(t, g, ns, "")
		must(t, g.AddEdge(ctx, a, b, "references", brain.EdgeMeta{}))
		must(t, g.AddEdge(ctx, a, b, "depends_on", brain.EdgeMeta{}))
		must(t, g.AddEdge(ctx, c, a, "references", brain.EdgeMeta{}))

		must(t, g.RemoveEdge(ctx, a, b, "references"))
		must(t, g.RemoveEdge(ctx, a, b, "references"))
		kept, _ := g.Neighbors(ctx, b, []string{"references", "depends_on"}, 10)
		must(t, g.RemoveObject(ctx, a))
		fromB, _ := g.Neighbors(ctx, b, []string{"references", "depends_on"}, 10)
		fromC, _ := g.Neighbors(ctx, c, []string{"references"}, 10)
		found, _ := g.SearchText(ctx, word, 10, ns)

		if got := shape(kept); !slices.Equal(got, []brain.GraphNeighbor{{ObjectID: a, RelationType: "depends_on", Direction: "in"}}) {
			t.Errorf("after removing one edge (twice) = %v, want only depends_on", got)
		}
		if len(fromB) != 0 || len(fromC) != 0 || len(found) != 0 {
			t.Errorf("after removing the object: neighbors %v and %v, search %v, want none", fromB, fromC, found)
		}
	})

	t.Run("ensuring an object again updates it in place and keeps its edges", func(t *testing.T) {
		ctx, g := t.Context(), newGraph(t)
		ns := namespace()
		oldWord, newWord, titleWord := searchWord(), searchWord(), searchWord()
		a, b := node(t, g, ns, oldWord), node(t, g, ns, "")
		must(t, g.AddEdge(ctx, a, b, "references", brain.EdgeMeta{Note: "kept"}))

		must(t, g.EnsureObject(ctx, brain.Object{ID: a, Kind: "Document", Title: titleWord, Namespace: ns}, newWord))
		edges, _ := g.Neighbors(ctx, a, []string{"references"}, 10)
		byNew, _ := g.SearchText(ctx, newWord, 10, ns)
		byOld, _ := g.SearchText(ctx, oldWord, 10, ns)
		byTitle, _ := g.SearchText(ctx, titleWord, 10, ns)

		if len(edges) != 1 || edges[0].ObjectID != b || edges[0].Meta.Note != "kept" {
			t.Errorf("edges after re-ensure = %+v, want the original edge", edges)
		}
		if !slices.Equal(ids(byNew), []uuid.UUID{a}) || len(byOld) != 0 || len(byTitle) != 0 {
			t.Errorf("search after re-ensure: new text %v, old text %v, title %v, want only the given search text to match", ids(byNew), ids(byOld), ids(byTitle))
		}
	})

	t.Run("object search finds only nodes the namespace covers", func(t *testing.T) {
		ctx, g := t.Context(), newGraph(t)
		word := searchWord()
		tenantA, tenantB := namespace(), namespace()
		teamA := append(tenantA.Clone(), brain.Attr{Name: "team", Value: "x"})
		a := vectorNode(t, g, tenantA, word, []float32{1, 0})
		team := vectorNode(t, g, teamA, word, []float32{0.9, 0.1})
		b := vectorNode(t, g, tenantB, word, []float32{1, 0})

		cases := map[string]struct {
			got  []brain.ScoredID
			err  error
			want []uuid.UUID
		}{}
		add := func(name string, want []uuid.UUID, got []brain.ScoredID, err error) {
			cases[name] = struct {
				got  []brain.ScoredID
				err  error
				want []uuid.UUID
			}{got, err, want}
		}
		got, err := g.SearchText(ctx, word, 10, tenantA)
		add("text, tenant a", []uuid.UUID{a, team}, got, err)
		got, err = g.SearchText(ctx, word, 10, teamA)
		add("text, team in tenant a", []uuid.UUID{team}, got, err)
		got, err = g.SearchText(ctx, word, 10, tenantB)
		add("text, tenant b", []uuid.UUID{b}, got, err)
		got, err = g.SearchText(ctx, word, 10, nil)
		add("text, no namespace", nil, got, err)
		got, err = g.SearchText(ctx, " ", 10, tenantA)
		add("text, blank query", nil, got, err)
		got, err = g.SearchText(ctx, word, 0, tenantA)
		add("text, limit 0", nil, got, err)
		got, err = g.SearchVector(ctx, []float32{1, 0}, 10, tenantA)
		add("vector, tenant a", []uuid.UUID{a, team}, got, err)
		got, err = g.SearchVector(ctx, []float32{1, 0}, 10, tenantB)
		add("vector, tenant b", []uuid.UUID{b}, got, err)
		got, err = g.SearchVector(ctx, []float32{1, 0}, 10, nil)
		add("vector, no namespace", nil, got, err)
		got, err = g.SearchVector(ctx, nil, 10, tenantA)
		add("vector, no vector", nil, got, err)
		got, err = g.SearchVector(ctx, []float32{1, 0}, 0, tenantA)
		add("vector, limit 0", nil, got, err)
		one, oneErr := g.SearchText(ctx, word, 1, tenantA)

		for name, tc := range cases {
			if gotIDs := sortedIDs(tc.got); tc.err != nil || !slices.Equal(gotIDs, sortedIDs(scored(tc.want))) {
				t.Errorf("%s = %v %v, want %v", name, gotIDs, tc.err, tc.want)
			}
		}
		if oneErr != nil || len(one) != 1 || !slices.Contains([]uuid.UUID{a, team}, one[0].ID) {
			t.Errorf("text, limit 1 = %v %v, want one of tenant a's nodes", ids(one), oneErr)
		}
	})

	t.Run("edge search matches notes on the one relation asked for", func(t *testing.T) {
		ctx, g := t.Context(), newGraph(t)
		ns := namespace()
		word := searchWord()
		a, b, c, d := node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, "")
		must(t, g.AddEdge(ctx, a, b, EdgeSearchLabel, brain.EdgeMeta{Note: "the " + word + " clause", Role: "source"}))
		must(t, g.AddEdge(ctx, d, b, EdgeSearchLabel, brain.EdgeMeta{Note: "another " + word + " clause", Role: "source"}))
		must(t, g.AddEdge(ctx, a, c, "references", brain.EdgeMeta{Note: "the " + word + " clause"}))

		hits, err := g.SearchEdgesText(ctx, EdgeSearchLabel, word, 10)
		one, _ := g.SearchEdgesText(ctx, EdgeSearchLabel, word, 1)
		blankLabel, _ := g.SearchEdgesText(ctx, " ", word, 10)
		blankQuery, _ := g.SearchEdgesText(ctx, EdgeSearchLabel, " ", 10)
		noLimit, _ := g.SearchEdgesText(ctx, EdgeSearchLabel, word, 0)

		from := map[uuid.UUID]bool{}
		for _, h := range hits {
			from[h.FromID] = h.ToID == b && h.RelationType == EdgeSearchLabel && h.Meta.Role == "source" && strings.Contains(h.Meta.Note, word)
		}
		if err != nil || len(hits) != 2 || !from[a] || !from[d] {
			t.Errorf("edge search = %+v %v, want a→b and d→b with their meta, not the other relation", hits, err)
		}
		if len(one) != 1 {
			t.Errorf("edge search, limit 1 = %+v", one)
		}
		if len(blankLabel) != 0 || len(blankQuery) != 0 || len(noLimit) != 0 {
			t.Errorf("blank label %v, blank query %v, limit 0 %v, want none", blankLabel, blankQuery, noLimit)
		}
	})

	t.Run("writes without ids or a relation are invalid, and a cancelled call does nothing", func(t *testing.T) {
		ctx, g := t.Context(), newGraph(t)
		ns := namespace()
		a, b := node(t, g, ns, ""), node(t, g, ns, "")
		invalid := map[string]error{
			"ensure without id":       g.EnsureObject(ctx, brain.Object{Namespace: ns}, ""),
			"remove without id":       g.RemoveObject(ctx, uuid.Nil),
			"edge without from":       g.AddEdge(ctx, uuid.Nil, b, "references", brain.EdgeMeta{}),
			"edge without relation":   g.AddEdge(ctx, a, b, " ", brain.EdgeMeta{}),
			"unlink without to":       g.RemoveEdge(ctx, a, uuid.Nil, "references"),
			"unlink without relation": g.RemoveEdge(ctx, a, b, ""),
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		stopped := map[string]error{
			"ensure": g.EnsureObject(cancelled, brain.Object{ID: a, Namespace: ns}, ""),
			"remove": g.RemoveObject(cancelled, a),
			"link":   g.AddEdge(cancelled, a, b, "references", brain.EdgeMeta{}),
			"unlink": g.RemoveEdge(cancelled, a, b, "references"),
		}
		_, stopped["neighbors"] = g.Neighbors(cancelled, a, []string{"references"}, 10)
		_, stopped["text search"] = g.SearchText(cancelled, "q", 10, ns)
		_, stopped["vector search"] = g.SearchVector(cancelled, []float32{1, 0}, 10, ns)
		_, stopped["edge search"] = g.SearchEdgesText(cancelled, EdgeSearchLabel, "q", 10)
		left, _ := g.Neighbors(ctx, a, []string{"references"}, 10)

		for name, err := range invalid {
			if !errors.Is(err, brain.ErrInvalid) {
				t.Errorf("%s = %v, want ErrInvalid", name, err)
			}
		}
		for name, err := range stopped {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("cancelled %s = %v, want context.Canceled", name, err)
			}
		}
		if len(left) != 0 {
			t.Errorf("edges after a cancelled link = %v, want none", left)
		}
	})
}

// node ensures a Document under ns whose search text is word and returns its id.
func node(t *testing.T, g Graph, ns brain.Namespace, word string) uuid.UUID {
	t.Helper()
	return vectorNode(t, g, ns, word, nil)
}

func vectorNode(t *testing.T, g Graph, ns brain.Namespace, word string, vec []float32) uuid.UUID {
	t.Helper()
	id := uuid.New()
	must(t, g.EnsureObject(t.Context(), brain.Object{ID: id, Kind: "Document", Title: "node", Namespace: ns, Embedding: vec}, word))
	return id
}

// namespace is a tenant no other case uses.
func namespace() brain.Namespace {
	return brain.Namespace{{Name: "org", Value: letters()}}
}

// searchWord is a word no other case indexes.
func searchWord() string { return "w" + letters() }

func letters() string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return 'a' + (r - '0')
		}
		return r
	}, strings.ReplaceAll(uuid.NewString(), "-", ""))
}

// shape is neighbors without their metadata.
func shape(ns []brain.GraphNeighbor) []brain.GraphNeighbor {
	out := make([]brain.GraphNeighbor, len(ns))
	for i, n := range ns {
		out[i] = brain.GraphNeighbor{ObjectID: n.ObjectID, RelationType: n.RelationType, Direction: n.Direction}
	}
	return out
}

func sameMeta(got, want brain.EdgeMeta) bool {
	return got.Note == want.Note && got.Status == want.Status && got.Role == want.Role &&
		got.Confidence == want.Confidence && got.EvidenceID != nil && want.EvidenceID != nil && *got.EvidenceID == *want.EvidenceID
}

func ids(hits []brain.ScoredID) []uuid.UUID {
	out := make([]uuid.UUID, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}

func scored(ids []uuid.UUID) []brain.ScoredID {
	out := make([]brain.ScoredID, len(ids))
	for i, id := range ids {
		out[i] = brain.ScoredID{ID: id}
	}
	return out
}

func sortedIDs(hits []brain.ScoredID) []uuid.UUID {
	return slices.SortedFunc(slices.Values(ids(hits)), func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
