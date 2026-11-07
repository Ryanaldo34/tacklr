// Package graphtest is the contract every brain graph backend must meet. Run
// it from a backend's own tests, as helixgraph does, so any graph database a
// host substitutes behaves as Engine expects.
//
// Each case writes fresh object ids, namespaces and search words, so cases
// never see each other's data, even on one long-lived graph. A backend owns
// its embedding, so the contract hands newGraph the Embedder each case needs:
// nil for a text-only graph, or Embedder, whose vectors the contract chooses.
package graphtest

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// EdgeSearchLabel is the one relation the contract searches edges on.
// A backend that indexes edges per relation indexes it (text and vector)
// before Run.
const EdgeSearchLabel = "cites"

// OtherEdgeSearchLabel is the second relation the contract searches edges
// on. A graph under test indexes both for edge search.
const OtherEdgeSearchLabel = "quotes"

// Dimension is the length of every vector Embedder returns.
const Dimension = 2

// Embedder embeds a text as the vector of the first marker word in it:
// veca is (1, 0), vecb (0.6, 0.8), vecc (0.9, 0.1) and anything else
// (0, 1). Markers are ordinary words to text search. A text with the word
// poison fails.
type Embedder struct{}

// Embed implements brain.Embedder.
func (Embedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		if strings.Contains(text, "poison") {
			return nil, errors.New("embedder down")
		}
		out[i] = []float32{0, 1}
		for _, word := range strings.Fields(text) {
			if v, ok := markers[word]; ok {
				out[i] = v
				break
			}
		}
	}
	return out, nil
}

// Model implements brain.Embedder.
func (Embedder) Model() string { return "graphtest-markers" }

var markers = map[string][]float32{"veca": {1, 0}, "vecb": {0.6, 0.8}, "vecc": {0.9, 0.1}}

// Run runs the contract. newGraph returns the graph a case writes to, which
// embeds with emb (nil: text only).
func Run(t *testing.T, newGraph func(t *testing.T, emb brain.Embedder) brain.Graph) {
	t.Run("a walk pages what its relations, direction and filters reach, nearest and newest first, each object once", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, nil)
		ns := namespace()
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		at := func(minute int, kind string) uuid.UUID {
			return put(t, g, brain.Object{Kind: kind, Title: "node", Namespace: ns, CreatedAt: base.Add(time.Duration(minute) * time.Minute)}, "")
		}
		a := at(0, "Document")
		// Seven records a links to, newest last. The Notes' edges are
		// active, primary, confident and recent; three other edges each
		// miss one of those.
		recent := base.Add(24 * time.Hour)
		weighed := brain.EdgeMeta{Status: "active", Role: "primary", Confidence: 0.9, UpdatedAt: recent}
		var linked []uuid.UUID
		for i := range 7 {
			kind, meta := "Document", brain.EdgeMeta{}
			switch i {
			case 0:
				meta = brain.EdgeMeta{Role: "cc", Confidence: 0.9, UpdatedAt: recent}
			case 2:
				meta = brain.EdgeMeta{Role: "primary", Confidence: 0.2, UpdatedAt: recent}
			case 4:
				meta = brain.EdgeMeta{Role: "primary", Confidence: 0.9, UpdatedAt: base}
			case 1, 3, 5:
				kind, meta = "Note", weighed
			}
			b := at(10+i, kind)
			link(t, g, ns, a, b, "references", meta)
			linked = append(linked, b)
		}
		c, d1, d2 := at(5, "Document"), at(1, "Document"), at(2, "Document")
		link(t, g, ns, c, a, "references", brain.EdgeMeta{})
		link(t, g, ns, a, linked[0], "depends_on", brain.EdgeMeta{})
		link(t, g, ns, linked[0], d1, "references", brain.EdgeMeta{})
		link(t, g, ns, linked[0], d2, "references", brain.EdgeMeta{})
		out := func(id uuid.UUID, rel string, depth int, via uuid.UUID) brain.GraphNeighbor {
			return brain.GraphNeighbor{ObjectID: id, SourceID: via, RelationType: rel, Direction: brain.DirectionOut, Depth: depth}
		}
		newestFirst := func(rel string, ids ...uuid.UUID) []brain.GraphNeighbor {
			var want []brain.GraphNeighbor
			for _, id := range slices.Backward(ids) {
				want = append(want, out(id, rel, 1, a))
			}
			return want
		}
		every := newestFirst("references", linked...)
		notes := newestFirst("references", linked[1], linked[3], linked[5])
		cIn := brain.GraphNeighbor{ObjectID: c, SourceID: a, RelationType: "references", Direction: brain.DirectionIn, Depth: 1}
		// Pages of 2 put a cursor inside the second depth.
		deeper := []brain.GraphNeighbor{out(d2, "references", 2, linked[0]), out(d1, "references", 2, linked[0])}
		query := func(rels []string, dir string, hops int) brain.NeighborhoodQuery {
			return brain.NeighborhoodQuery{Walk: brain.Walk{Namespace: ns, ObjectID: a, Relations: rels, Direction: dir, MaxHops: hops, FrontierK: 100}, Limit: 2}
		}
		refs := []string{"references"}
		active, picky, noteKind := query(refs, brain.DirectionOut, 1), query(refs, brain.DirectionOut, 1), query(refs, brain.DirectionOut, 1)
		active.Edges, active.Limit = brain.EdgeFilter{Status: "active"}, 1
		picky.Edges, picky.Limit = brain.EdgeFilter{Role: "primary", MinConfidence: 0.5, UpdatedAfter: base.Add(time.Hour)}, 1
		noteKind.Filters, noteKind.Limit = brain.Filter{Kind: brain.StringMatch{Eq: "Note"}}, 1
		both := query([]string{"depends_on", "references"}, brain.DirectionBoth, 1)
		firstRelation := slices.Clone(every)
		firstRelation[len(firstRelation)-1].RelationType = "depends_on"

		cases := map[string]struct{ got, want []brain.GraphNeighbor }{
			"one hop out, in pages of 2":    {walkAll(t, g, query(refs, brain.DirectionOut, 1)), every},
			"one hop, both ways":            {walkAll(t, g, query(refs, brain.DirectionBoth, 1)), append(slices.Clone(every), cIn)},
			"two hops out":                  {walkAll(t, g, query(refs, brain.DirectionOut, 2)), append(slices.Clone(every), deeper...)},
			"two hops in":                   {walkAll(t, g, query(refs, brain.DirectionIn, 2)), []brain.GraphNeighbor{cIn}},
			"an edge status, pages of 1":    {walkAll(t, g, active), notes},
			"role, confidence and recency":  {walkAll(t, g, picky), notes},
			"a record filter, pages of 1":   {walkAll(t, g, noteKind), notes},
			"two relations, the first wins": {walkAll(t, g, both), append(firstRelation, cIn)},
			"an unknown object":             {walkAll(t, g, brain.NeighborhoodQuery{Walk: brain.Walk{Namespace: ns, ObjectID: uuid.New(), MaxHops: 1, FrontierK: 100}, Limit: 2}), nil},
		}
		for name, tc := range cases {
			if got := shape(tc.got); !slices.Equal(got, tc.want) {
				t.Errorf("%s = %v, want %v", name, got, tc.want)
			}
		}

		// A record linked between two pages shows only when its key falls
		// after the first page: here the oldest does, and the newest does not.
		first, err := g.Neighborhood(ctx, query(refs, brain.DirectionOut, 1))
		must(t, err)
		newest, oldest := at(60, "Document"), at(-60, "Document")
		link(t, g, ns, a, newest, "references", brain.EdgeMeta{})
		link(t, g, ns, a, oldest, "references", brain.EdgeMeta{})
		rest := query(refs, brain.DirectionOut, 1)
		rest.After = first.Next
		got := append(shape(first.Neighbors), shape(walkAll(t, g, rest))...)
		if want := append(slices.Clone(every), out(oldest, "references", 1, a)); !slices.Equal(got, want) {
			t.Errorf("pages around two new links = %v, want %v", got, want)
		}
		for name, q := range map[string]brain.NeighborhoodQuery{
			"after a bad cursor": {Walk: rest.Walk, After: "not a cursor", Limit: 2},
			"with a bad filter":  {Walk: rest.Walk, Filters: brain.Filter{UpdatedAfter: "yesterday"}, Limit: 2},
		} {
			if _, err := g.Neighborhood(ctx, q); !errors.Is(err, brain.ErrInvalid) {
				t.Errorf("a walk %s = %v, want ErrInvalid", name, err)
			}
		}
	})

	t.Run("counts and reach cover what a walk reaches, and a hop past FrontierK is truncated", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, nil)
		ns := namespace()
		word := searchWord()
		a := node(t, g, ns, "")
		b1, b2 := node(t, g, ns, word), put(t, g, brain.Object{Kind: "Note", Title: "node", Namespace: ns}, "")
		d1, d2 := node(t, g, ns, word), node(t, g, ns, word)
		link(t, g, ns, a, b1, "references", brain.EdgeMeta{})
		link(t, g, ns, a, b2, "references", brain.EdgeMeta{})
		link(t, g, ns, a, b1, "depends_on", brain.EdgeMeta{})
		link(t, g, ns, b1, d1, "references", brain.EdgeMeta{})
		link(t, g, ns, b2, d2, "references", brain.EdgeMeta{})
		walk := brain.Walk{Namespace: ns, ObjectID: a, MaxHops: 2, FrontierK: 10}
		counts := func(w brain.Walk, filters brain.Filter) brain.NeighborPage {
			page, err := g.Neighborhood(ctx, brain.NeighborhoodQuery{Walk: w, Filters: filters, Limit: 1, Count: true})
			must(t, err)
			return page
		}
		reach := func(w brain.Walk) brain.Reached {
			got, err := g.Reach(ctx, w)
			must(t, err)
			got.IDs = sortedIDs(got.IDs)
			return got
		}
		narrow := walk
		narrow.FrontierK = 1
		named := walk
		named.Relations = []string{"references", "depends_on"}

		all, notes, byName, cut := counts(walk, brain.Filter{}), counts(walk, brain.Filter{Kind: brain.StringMatch{Eq: "Note"}}), counts(named, brain.Filter{}), counts(narrow, brain.Filter{})
		whole, part := reach(walk), reach(narrow)
		near := ids(findObjects(t, g, brain.ObjectSearch{Namespace: ns, Query: word, IDs: []uuid.UUID{b1, d2}, Limit: 10}))

		wantAll := brain.Counts{Total: 4, ByRelation: map[string]int{"references": 4, "depends_on": 1}}
		for name, page := range map[string]brain.NeighborPage{"every relation": all, "named relations": byName} {
			if page.Counts == nil || page.Counts.Total != wantAll.Total || !maps.Equal(page.Counts.ByRelation, wantAll.ByRelation) || page.Truncated {
				t.Errorf("counts of %s = %+v (truncated %v), want %+v", name, page.Counts, page.Truncated, wantAll)
			}
		}
		if notes.Counts == nil || notes.Counts.Total != 1 || !maps.Equal(notes.Counts.ByRelation, map[string]int{"references": 1}) {
			t.Errorf("counts of the notes = %+v, want the one note", notes.Counts)
		}
		if !cut.Truncated || !part.Truncated || len(part.IDs) != 2 {
			t.Errorf("a frontier of 1: walk truncated %v, reach %+v, want both truncated and one record per hop", cut.Truncated, part)
		}
		if whole.Truncated || !slices.Equal(whole.IDs, sortedIDs([]uuid.UUID{b1, b2, d1, d2})) {
			t.Errorf("reach = %+v, want the four records two hops away", whole)
		}
		if !slices.Equal(sortedIDs(near), sortedIDs([]uuid.UUID{b1, d2})) {
			t.Errorf("object search among ids = %v, want only the matching ones among them", near)
		}
	})

	t.Run("neighborhood neither returns nor walks through objects the namespace does not cover", func(t *testing.T) {
		t.Parallel()
		g := newGraph(t, nil)
		tenant := namespace()
		teamX := append(tenant.Clone(), brain.Attr{Name: "team", Value: "x"})
		teamY := append(tenant.Clone(), brain.Attr{Name: "team", Value: "y"})
		a, bridge, beyond := node(t, g, teamX, ""), node(t, g, teamY, ""), node(t, g, teamX, "")
		mid, leaf := node(t, g, teamX, ""), node(t, g, teamX, "")
		link(t, g, teamX, a, bridge, "references", brain.EdgeMeta{})
		link(t, g, teamY, bridge, beyond, "references", brain.EdgeMeta{})
		link(t, g, teamX, a, mid, "references", brain.EdgeMeta{})
		link(t, g, teamX, mid, leaf, "references", brain.EdgeMeta{})
		walk := func(ns brain.Namespace) []uuid.UUID {
			return sortedIDs(neighborIDs(walkAll(t, g, brain.NeighborhoodQuery{Walk: brain.Walk{Namespace: ns, ObjectID: a, Relations: []string{"references"}, Direction: brain.DirectionBoth, MaxHops: 2, FrontierK: 10}, Limit: 10})))
		}

		if got, want := walk(teamX), sortedIDs([]uuid.UUID{mid, leaf}); !slices.Equal(got, want) {
			t.Errorf("team x walk = %v, want %v (not through team y)", got, want)
		}
		if got, want := walk(tenant), sortedIDs([]uuid.UUID{bridge, beyond, mid, leaf}); !slices.Equal(got, want) {
			t.Errorf("tenant walk = %v, want %v", got, want)
		}
	})

	t.Run("an edge carries its metadata, and linking again replaces it", func(t *testing.T) {
		t.Parallel()
		g := newGraph(t, nil)
		ns := namespace()
		a, b := node(t, g, ns, ""), node(t, g, ns, "")
		evidence := uuid.New()
		at := time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC) // times keep milliseconds
		first := brain.EdgeMeta{Note: "first note", Status: "active", Role: "primary", Confidence: 0.75, EvidenceID: &evidence, CreatedAt: at, UpdatedAt: at.Add(time.Hour)}
		link(t, g, ns, a, b, "references", first)
		before := neighbors(t, g, ns, a, "references")
		link(t, g, ns, a, b, "references", brain.EdgeMeta{Note: "second note"})
		after := neighbors(t, g, ns, a, "references")

		if len(before) != 1 || !sameMeta(before[0].Meta, first) {
			t.Errorf("edge meta = %+v, want %+v", before, first)
		}
		if len(after) != 1 || after[0].Meta.Note != "second note" || after[0].Meta.Status != "" {
			t.Errorf("after linking again = %+v, want one edge with the new meta only", after)
		}
	})

	t.Run("unlinking an edge or removing an object takes exactly its edges", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, nil)
		ns := namespace()
		word := searchWord()
		a, b, c := node(t, g, ns, word), node(t, g, ns, ""), node(t, g, ns, "")
		link(t, g, ns, a, b, "references", brain.EdgeMeta{})
		link(t, g, ns, a, b, "depends_on", brain.EdgeMeta{})
		link(t, g, ns, c, a, "references", brain.EdgeMeta{})

		must(t, g.Unlink(ctx, a, b, "references"))
		must(t, g.Unlink(ctx, a, b, "references"))
		kept := neighbors(t, g, ns, b, "references", "depends_on")
		must(t, g.Remove(ctx, a))
		fromB := neighbors(t, g, ns, b, "references", "depends_on")
		fromC := neighbors(t, g, ns, c, "references")
		found := findObjects(t, g, brain.ObjectSearch{Namespace: ns, Query: word, Limit: 10})

		if got := shape(kept); len(got) != 1 || got[0].ObjectID != a || got[0].RelationType != "depends_on" || got[0].Direction != brain.DirectionIn {
			t.Errorf("after unlinking one edge (twice) = %v, want only depends_on", got)
		}
		if len(fromB) != 0 || len(fromC) != 0 || len(found) != 0 {
			t.Errorf("after removing the object: neighbors %v and %v, search %v, want none", fromB, fromC, found)
		}
	})

	t.Run("putting an object again updates it in place and keeps its edges", func(t *testing.T) {
		t.Parallel()
		g := newGraph(t, nil)
		ns := namespace()
		oldWord, newWord := searchWord(), searchWord()
		a, b := node(t, g, ns, oldWord), node(t, g, ns, "")
		link(t, g, ns, a, b, "references", brain.EdgeMeta{Note: "kept"})

		put(t, g, brain.Object{ID: a, Kind: "Document", Title: "node", Namespace: ns}, newWord)
		edges := neighbors(t, g, ns, a, "references")
		text := func(word string) []uuid.UUID {
			return ids(findObjects(t, g, brain.ObjectSearch{Namespace: ns, Query: word, Limit: 10}))
		}

		if len(edges) != 1 || edges[0].ObjectID != b || edges[0].Meta.Note != "kept" {
			t.Errorf("edges after putting again = %+v, want the original edge", edges)
		}
		if byNew, byOld := text(newWord), text(oldWord); !slices.Equal(byNew, []uuid.UUID{a}) || len(byOld) != 0 {
			t.Errorf("search after putting again: new text %v, old text %v, want only the new text to match", byNew, byOld)
		}
	})

	t.Run("readers limit search and walks to the principals they name", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, Embedder{})
		word := searchWord()
		ns := namespace()
		legal := []string{"user:ann", "group:legal"}
		open := put(t, g, brain.Object{Kind: "Document", Title: "node", Namespace: ns}, word+" vecb")
		secret := put(t, g, brain.Object{Kind: "Document", Title: "node", Namespace: ns, Readers: []string{"group:legal"}}, word+" veca")
		beyond, mid, mid2 := node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, "")
		link(t, g, ns, open, secret, "references", brain.EdgeMeta{Note: "about the secret"})
		link(t, g, ns, secret, beyond, "references", brain.EdgeMeta{})
		link(t, g, ns, open, mid, "references", brain.EdgeMeta{})
		link(t, g, ns, mid, mid2, "references", brain.EdgeMeta{})
		link(t, g, ns, mid2, beyond, "references", brain.EdgeMeta{})
		link(t, g, ns, open, secret, EdgeSearchLabel, brain.EdgeMeta{Note: "the " + word + " clause veca"})
		link(t, g, ns, open, mid, EdgeSearchLabel, brain.EdgeMeta{Note: "a " + word + " note vecb"})
		objects := func(principals []string, filters brain.Filter, limit int) []uuid.UUID {
			return ids(findObjects(t, g, brain.ObjectSearch{Namespace: ns, Principals: principals, Query: word + " veca", Filters: filters, Limit: limit}))
		}
		edges := func(principals []string, limit int) [][2]uuid.UUID {
			hits, err := g.FindLinks(ctx, brain.LinkSearch{Namespace: ns, Principals: principals, RelationType: EdgeSearchLabel, Query: word + " veca", Limit: limit})
			must(t, err)
			return edgeEnds(hits)
		}
		readerWalk := func(principals []string) brain.Walk {
			return brain.Walk{Namespace: ns, Principals: principals, ObjectID: open, Relations: []string{"references"}, Direction: brain.DirectionOut, MaxHops: 2, FrontierK: 10}
		}
		walk := func(principals []string) []uuid.UUID {
			return sortedIDs(neighborIDs(walkAll(t, g, brain.NeighborhoodQuery{Walk: readerWalk(principals), Limit: 10})))
		}
		reached := func(principals []string) []uuid.UUID {
			got, err := g.Reach(ctx, readerWalk(principals))
			must(t, err)
			return sortedIDs(got.IDs)
		}
		counted := func(principals []string) int {
			page, err := g.Neighborhood(ctx, brain.NeighborhoodQuery{Walk: readerWalk(principals), Limit: 1, Count: true})
			must(t, err)
			return page.Counts.Total
		}
		toMid, toSecret := [2]uuid.UUID{open, mid}, [2]uuid.UUID{open, secret}

		cases := map[string]struct{ got, want []uuid.UUID }{
			"search, an outsider's one slot": {objects(nil, brain.Filter{}, 1), []uuid.UUID{open}},
			"search, a reader":               {sortedIDs(objects(legal, brain.Filter{}, 10)), sortedIDs([]uuid.UUID{open, secret})},
			"search by kind, an outsider":    {objects(nil, brain.Filter{Kind: brain.StringMatch{Eq: "Document"}}, 1), []uuid.UUID{open}},
			"walk, an outsider":              {walk(nil), sortedIDs([]uuid.UUID{mid, mid2})},
			"walk, a reader":                 {walk(legal), sortedIDs([]uuid.UUID{secret, beyond, mid, mid2})},
			"reach, an outsider":             {reached(nil), sortedIDs([]uuid.UUID{mid, mid2})},
		}
		if outsider, reader := counted(nil), counted(legal); outsider != 2 || reader != 4 {
			t.Errorf("counted: an outsider %d, a reader %d, want 2 and 4", outsider, reader)
		}
		for name, tc := range cases {
			if !slices.Equal(tc.got, tc.want) {
				t.Errorf("%s = %v, want %v", name, tc.got, tc.want)
			}
		}
		if got := edges(nil, 1); !slices.Equal(got, [][2]uuid.UUID{toMid}) {
			t.Errorf("edge search, an outsider's one slot = %v, want the edge to mid", got)
		}
		if got := edges(legal, 10); !slices.Contains(got, toSecret) || !slices.Contains(got, toMid) {
			t.Errorf("edge search, a reader = %v, want both edges", got)
		}
	})

	t.Run("putting an object again replaces its readers and namespace", func(t *testing.T) {
		t.Parallel()
		g := newGraph(t, nil)
		word := searchWord()
		tenant := namespace()
		team := append(tenant.Clone(), brain.Attr{Name: "team", Value: "x"})
		id := put(t, g, brain.Object{Kind: "Document", Namespace: team, Readers: []string{"group:legal"}}, word)
		found := func(ns brain.Namespace, principals ...string) bool {
			return slices.Equal(ids(findObjects(t, g, brain.ObjectSearch{Namespace: ns, Principals: principals, Query: word, Limit: 10})), []uuid.UUID{id})
		}
		again := func(ns brain.Namespace, readers ...string) {
			put(t, g, brain.Object{ID: id, Kind: "Document", Namespace: ns, Readers: readers}, word)
		}

		again(team, "group:hr")
		hr, legalAfterHR := found(team, "group:hr"), found(team, "group:legal")
		again(tenant)
		anyone, oldTeam := found(tenant), found(team)

		if !hr || legalAfterHR {
			t.Errorf("readers moved to hr: hr reads %v, legal reads %v, want only hr", hr, legalAfterHR)
		}
		if !anyone || oldTeam {
			t.Errorf("moved out of the team with no readers: tenant reads %v, team reads %v, want only the tenant", anyone, oldTeam)
		}
	})

	t.Run("object search ranks text and vectors over only the nodes the namespace covers", func(t *testing.T) {
		t.Parallel()
		g := newGraph(t, Embedder{})
		word := searchWord()
		tenantA, tenantB := namespace(), namespace()
		teamA := append(tenantA.Clone(), brain.Attr{Name: "team", Value: "x"})
		a := put(t, g, brain.Object{Kind: "Document", Title: "node", Namespace: tenantA}, word+" veca")
		team := put(t, g, brain.Object{Kind: "Note", Title: "node", Namespace: teamA}, word+" vecc")
		b := put(t, g, brain.Object{Kind: "Document", Title: "node", Namespace: tenantB}, word+" veca")
		// The first attr is the tenant: matching the other attrs is not enough.
		otherTenant := brain.Namespace{{Name: "team", Value: "x"}, tenantA[0]}
		search := func(ns brain.Namespace, query string, kind string, limit int) []uuid.UUID {
			var f brain.Filter
			if kind != "" {
				f.Kind = brain.StringMatch{Eq: kind}
			}
			return ids(findObjects(t, g, brain.ObjectSearch{Namespace: ns, Query: query, Filters: f, Limit: limit}))
		}

		cases := map[string]struct{ got, want []uuid.UUID }{
			"tenant a":                        {sortedIDs(search(tenantA, word+" veca", "", 10)), sortedIDs([]uuid.UUID{a, team})},
			"the team in tenant a":            {search(teamA, word, "", 10), []uuid.UUID{team}},
			"tenant b":                        {search(tenantB, word, "", 10), []uuid.UUID{b}},
			"kind Note":                       {search(tenantA, word, "Note", 10), []uuid.UUID{team}},
			"another tenant":                  {search(otherTenant, word, "", 10), nil},
			"the marker's nearest node first": {search(tenantA, "nomatch veca", "", 1), []uuid.UUID{a}},
			"a query that does not embed":     {search(tenantB, "poison "+word, "", 10), []uuid.UUID{b}},
		}
		for name, tc := range cases {
			if !slices.Equal(tc.got, tc.want) {
				t.Errorf("%s = %v, want %v", name, tc.got, tc.want)
			}
		}
	})

	t.Run("filters apply before ranking, and putting again replaces the properties", func(t *testing.T) {
		t.Parallel()
		g := newGraph(t, nil)
		word := searchWord()
		ns := namespace()
		old := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		won := put(t, g, brain.Object{Kind: "Deal", Title: "acme", Namespace: ns, CreatedAt: old, UpdatedAt: old, Properties: map[string]any{
			"stage": "won", "amount": 5, "hot": true, "seats": int32(3), "users": int64(40), "ratio": float32(0.5), "score": json.Number("9"), "meta": map[string]any{"a": 1},
		}}, word)
		lost := put(t, g, brain.Object{Kind: "Deal", Title: "globex", Namespace: ns, Properties: map[string]any{"stage": "lost", "amount": 7.0}}, word+" "+word)
		filtered := func(f brain.Filter, limit int) []uuid.UUID {
			return sortedIDs(ids(findObjects(t, g, brain.ObjectSearch{Namespace: ns, Query: word, Filters: f, Limit: limit})))
		}
		prop := func(key string, pf brain.PropFilter) brain.Filter {
			return brain.Filter{Props: map[string]brain.PropFilter{key: pf}}
		}
		since := old.Add(24 * time.Hour).Format(time.RFC3339)
		cases := map[string]struct{ got, want []uuid.UUID }{
			"one value":                         {filtered(prop("stage", brain.PropFilter{Eq: "won"}), 10), []uuid.UUID{won}},
			"any of values":                     {filtered(prop("stage", brain.PropFilter{In: []any{"won", "lost"}}), 10), sortedIDs([]uuid.UUID{won, lost})},
			"a number":                          {filtered(prop("amount", brain.PropFilter{Eq: 5.0}), 10), []uuid.UUID{won}},
			"a number given as text":            {filtered(prop("amount", brain.PropFilter{Eq: "5"}), 10), nil},
			"a bool":                            {filtered(prop("hot", brain.PropFilter{Eq: true}), 10), []uuid.UUID{won}},
			"a title":                           {filtered(brain.Filter{Title: brain.StringMatch{Eq: "globex"}}, 10), []uuid.UUID{lost}},
			"created after":                     {filtered(brain.Filter{CreatedAfter: since}, 10), []uuid.UUID{lost}},
			"updated before":                    {filtered(brain.Filter{UpdatedBefore: since}, 10), []uuid.UUID{won}},
			"the better match fails the filter": {filtered(prop("stage", brain.PropFilter{Eq: "won"}), 1), []uuid.UUID{won}},
			"an int32":                          {filtered(prop("seats", brain.PropFilter{Eq: 3}), 10), []uuid.UUID{won}},
			"an int64":                          {filtered(prop("users", brain.PropFilter{Eq: int64(40)}), 10), []uuid.UUID{won}},
			"a float32":                         {filtered(prop("ratio", brain.PropFilter{Eq: 0.5}), 10), []uuid.UUID{won}},
			"a JSON number":                     {filtered(prop("score", brain.PropFilter{Eq: 9}), 10), []uuid.UUID{won}},
			"a nested value":                    {filtered(prop("meta", brain.PropFilter{Eq: "a"}), 10), nil},
		}
		for name, tc := range cases {
			if !slices.Equal(tc.got, tc.want) {
				t.Errorf("%s = %v, want %v", name, tc.got, tc.want)
			}
		}
		put(t, g, brain.Object{ID: won, Kind: "Deal", Title: "acme", Namespace: ns, Properties: map[string]any{"amount": 5}}, word)
		if got := filtered(prop("stage", brain.PropFilter{Eq: "won"}), 10); len(got) != 0 {
			t.Errorf("a property removed by putting again still matches: %v", got)
		}
		if _, err := g.FindObjects(t.Context(), brain.ObjectSearch{Namespace: ns, Query: word, Filters: brain.Filter{UpdatedAfter: "yesterday"}, Limit: 10}); !errors.Is(err, brain.ErrInvalid) {
			t.Errorf("search with an invalid filter = %v, want ErrInvalid", err)
		}
	})

	t.Run("edge search without a relation ranks every indexed relation", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, nil)
		ns := namespace()
		word := searchWord()
		a, b, c, d := node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, "")
		link(t, g, ns, a, b, EdgeSearchLabel, brain.EdgeMeta{Note: "the " + word + " clause"})
		link(t, g, ns, a, c, OtherEdgeSearchLabel, brain.EdgeMeta{Note: "a " + word + " quote"})
		link(t, g, ns, a, d, "references", brain.EdgeMeta{Note: "an unindexed " + word})

		hits, err := g.FindLinks(ctx, brain.LinkSearch{Namespace: ns, Query: word, Limit: 10})

		got := map[[2]uuid.UUID]string{}
		for _, h := range hits {
			got[[2]uuid.UUID{h.FromID, h.ToID}] = h.RelationType
		}
		if err != nil || len(got) != 2 || got[[2]uuid.UUID{a, b}] != EdgeSearchLabel || got[[2]uuid.UUID{a, c}] != OtherEdgeSearchLabel {
			t.Errorf("edge search without a relation = %v %v, want the %s and %s edges", got, err, EdgeSearchLabel, OtherEdgeSearchLabel)
		}
	})

	t.Run("edge search ranks notes and vectors on the one relation and namespace asked for", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, Embedder{})
		ns := namespace()
		word := searchWord()
		a, b, c, d := node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, ""), node(t, g, ns, "")
		link(t, g, ns, a, b, EdgeSearchLabel, brain.EdgeMeta{Note: "the " + word + " clause veca", Role: "source"})
		link(t, g, ns, d, b, EdgeSearchLabel, brain.EdgeMeta{Note: "another " + word + " clause", Role: "source"})
		link(t, g, ns, a, c, "references", brain.EdgeMeta{Note: "the " + word + " clause veca"})
		other := namespace()
		e, f := node(t, g, other, ""), node(t, g, other, "")
		link(t, g, other, e, f, EdgeSearchLabel, brain.EdgeMeta{Note: "the " + word + " clause veca"})
		team := append(ns.Clone(), brain.Attr{Name: "team", Value: "x"})
		x, y := node(t, g, team, ""), node(t, g, team, "")
		link(t, g, team, x, y, EdgeSearchLabel, brain.EdgeMeta{Note: "the team " + word + " clause veca"})
		search := func(ns brain.Namespace, query string, limit int) []brain.EdgeSearchHit {
			hits, err := g.FindLinks(ctx, brain.LinkSearch{Namespace: ns, RelationType: EdgeSearchLabel, Query: query, Limit: limit})
			must(t, err)
			return hits
		}
		hits := search(ns, word+" veca", 10)
		tenantEdges := [][2]uuid.UUID{{a, b}, {d, b}, {x, y}}

		for _, h := range hits {
			if !slices.Contains(tenantEdges, [2]uuid.UUID{h.FromID, h.ToID}) || h.RelationType != EdgeSearchLabel {
				t.Errorf("edge search hit %s→%s %s, want only this tenant's %s edges", h.FromID, h.ToID, h.RelationType, EdgeSearchLabel)
			}
		}
		if len(hits) != 3 || hits[0].FromID != a || hits[0].Meta.Role != "source" || !strings.Contains(hits[0].Meta.Note, word) {
			t.Errorf("edge search = %+v, want a→b first with its meta, then the other two", hits)
		}
		if got := edgeEnds(search(ns, "nomatch veca", 1)); len(got) != 1 || got[0] == [2]uuid.UUID{d, b} {
			t.Errorf("edge search for the marker = %v, want a note marked veca", got)
		}
		if got := edgeEnds(search(team, "poison "+word, 10)); !slices.Equal(got, [][2]uuid.UUID{{x, y}}) {
			t.Errorf("team edge search whose query does not embed = %v, want only the team's edge by its words", got)
		}
	})

	t.Run("erase drops every node the namespace covers, readers or not, and keeps the rest", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, nil)
		word := searchWord()
		tenant, elsewhere := namespace(), namespace()
		team := append(tenant.Clone(), brain.Attr{Name: "team", Value: "x"})
		other := append(tenant.Clone(), brain.Attr{Name: "team", Value: "y"})
		open := node(t, g, team, word)
		put(t, g, brain.Object{Kind: "Document", Title: "node", Namespace: team, Readers: []string{"legal"}}, word)
		kept, far := node(t, g, other, word), node(t, g, elsewhere, word)
		link(t, g, team, open, kept, "references", brain.EdgeMeta{})

		first := g.Erase(ctx, brain.Scope{Namespace: team})
		again := g.Erase(ctx, brain.Scope{Namespace: team})
		left := ids(findObjects(t, g, brain.ObjectSearch{Namespace: tenant, Principals: []string{"legal"}, Query: word, Limit: 10}))
		farLeft := ids(findObjects(t, g, brain.ObjectSearch{Namespace: elsewhere, Query: word, Limit: 10}))
		edges := neighbors(t, g, tenant, kept, "references")

		if first != nil || again != nil {
			t.Errorf("erase, then erase again = %v, %v, want both to succeed", first, again)
		}
		if !slices.Equal(left, []uuid.UUID{kept}) || !slices.Equal(farLeft, []uuid.UUID{far}) || len(edges) != 0 {
			t.Errorf("after erasing the team: tenant %v, other tenant %v, edges %v, want only the other team's and tenant's nodes and no edge", left, farLeft, edges)
		}
	})

	t.Run("an edge needs both nodes, a write that does not embed fails, and a cancelled call does nothing", func(t *testing.T) {
		t.Parallel()
		ctx, g := t.Context(), newGraph(t, nil)
		ns := namespace()
		a, b := node(t, g, ns, ""), node(t, g, ns, "")
		edge := brain.GraphEdge{Namespace: ns, From: a, To: b, RelationType: "references"}
		missing := g.Link(ctx, brain.GraphEdge{Namespace: ns, From: a, To: uuid.New(), RelationType: "references"})
		embedding := newGraph(t, Embedder{})
		poisoned := map[string]error{
			"put":  embedding.Put(ctx, brain.Indexed{Object: brain.Object{ID: uuid.New(), Kind: "Document", Namespace: ns}, Text: "poison"}),
			"link": embedding.Link(ctx, brain.GraphEdge{Namespace: ns, From: a, To: b, RelationType: EdgeSearchLabel, Meta: brain.EdgeMeta{Note: "poison"}}),
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		stopped := map[string]error{
			"put":    g.Put(cancelled, brain.Indexed{Object: brain.Object{ID: a, Kind: "Document", Namespace: ns}}),
			"remove": g.Remove(cancelled, a),
			"link":   g.Link(cancelled, edge),
			"unlink": g.Unlink(cancelled, a, b, "references"),
		}
		_, stopped["neighborhood"] = g.Neighborhood(cancelled, brain.NeighborhoodQuery{Walk: brain.Walk{Namespace: ns, ObjectID: a, MaxHops: 1, FrontierK: 10}, Limit: 10})
		_, stopped["reach"] = g.Reach(cancelled, brain.Walk{Namespace: ns, ObjectID: a, MaxHops: 1, FrontierK: 10})
		_, stopped["object search"] = g.FindObjects(cancelled, brain.ObjectSearch{Namespace: ns, Query: "q", Limit: 10})
		_, stopped["edge search"] = g.FindLinks(cancelled, brain.LinkSearch{Namespace: ns, RelationType: EdgeSearchLabel, Query: "q", Limit: 10})
		left := neighbors(t, g, ns, a, "references")

		if !errors.Is(missing, brain.ErrNotFound) {
			t.Errorf("edge to a missing node = %v, want ErrNotFound", missing)
		}
		for name, err := range poisoned {
			if err == nil {
				t.Errorf("%s whose text does not embed succeeded, want an error", name)
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

// node puts a Document under ns whose text is word and returns its id.
func node(t *testing.T, g brain.Graph, ns brain.Namespace, word string) uuid.UUID {
	t.Helper()
	return put(t, g, brain.Object{Kind: "Document", Title: "node", Namespace: ns}, word)
}

// put puts obj (a new id when it has none) with text and returns its id.
func put(t *testing.T, g brain.Graph, obj brain.Object, text string) uuid.UUID {
	t.Helper()
	if obj.ID == uuid.Nil {
		obj.ID = uuid.New()
	}
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	if obj.UpdatedAt.IsZero() {
		obj.UpdatedAt = now
	}
	must(t, g.Put(t.Context(), brain.Indexed{Object: obj, Text: text}))
	return obj.ID
}

func link(t *testing.T, g brain.Graph, ns brain.Namespace, from, to uuid.UUID, rel string, meta brain.EdgeMeta) {
	t.Helper()
	must(t, g.Link(t.Context(), brain.GraphEdge{Namespace: ns, From: from, To: to, RelationType: rel, Meta: meta}))
}

func findObjects(t *testing.T, g brain.Graph, q brain.ObjectSearch) []brain.ScoredID {
	t.Helper()
	hits, err := g.FindObjects(t.Context(), q)
	must(t, err)
	return hits
}

// neighbors is the one-hop neighborhood of id on rels.
func neighbors(t *testing.T, g brain.Graph, ns brain.Namespace, id uuid.UUID, rels ...string) []brain.GraphNeighbor {
	t.Helper()
	return walkAll(t, g, brain.NeighborhoodQuery{Walk: brain.Walk{Namespace: ns, ObjectID: id, Relations: rels, Direction: brain.DirectionBoth, MaxHops: 1, FrontierK: 10}, Limit: 10})
}

// walkAll is every page of q, in order. A page longer than q.Limit fails.
func walkAll(t *testing.T, g brain.Graph, q brain.NeighborhoodQuery) []brain.GraphNeighbor {
	t.Helper()
	var all []brain.GraphNeighbor
	for {
		page, err := g.Neighborhood(t.Context(), q)
		must(t, err)
		if len(page.Neighbors) > q.Limit {
			t.Fatalf("a page of %d, want at most %d", len(page.Neighbors), q.Limit)
		}
		all = append(all, page.Neighbors...)
		if page.Next == "" {
			return all
		}
		q.After = page.Next
	}
}

func edgeEnds(hits []brain.EdgeSearchHit) [][2]uuid.UUID {
	var out [][2]uuid.UUID
	for _, h := range hits {
		out = append(out, [2]uuid.UUID{h.FromID, h.ToID})
	}
	return out
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
	var out []brain.GraphNeighbor
	for _, n := range ns {
		n.Meta = brain.EdgeMeta{}
		out = append(out, n)
	}
	return out
}

func sameMeta(got, want brain.EdgeMeta) bool {
	return got.Note == want.Note && got.Status == want.Status && got.Role == want.Role &&
		got.Confidence == want.Confidence && got.EvidenceID != nil && want.EvidenceID != nil && *got.EvidenceID == *want.EvidenceID &&
		got.CreatedAt.Equal(want.CreatedAt) && got.UpdatedAt.Equal(want.UpdatedAt)
}

func ids(hits []brain.ScoredID) []uuid.UUID {
	var out []uuid.UUID
	for _, h := range hits {
		out = append(out, h.ID)
	}
	return out
}

func neighborIDs(ns []brain.GraphNeighbor) []uuid.UUID {
	var out []uuid.UUID
	for _, n := range ns {
		out = append(out, n.ObjectID)
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
