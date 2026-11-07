package helixgraph

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	helix "github.com/helixdb/helix-db/sdks/go"

	"github.com/ryanaldo34/tacklr/brain"
)

// A walk is built hop by hop in one request. Hop d starts from the frontier
// of hop d-1 (the start for hop 1), follows the edges that pass the edge
// predicates, and keeps the readable nodes no earlier hop reached: that set
// is reached(d). frontier(d) is its first FrontierK nodes by object id, and
// over(d) is 1 when the hop reached more. Filters, the order and the limit
// apply only to the rows a call returns, so every condition runs before a
// row takes a slot.

// newestFirst is the order of a walk's rows within one depth: the newest
// node first, then the object id. Neither ever changes, so a cursor holds.
var newestFirst = []helix.Ordering{{Property: propCreatedAt, Order: helix.OrderDesc}, {Property: propObjectID, Order: helix.OrderAsc}}

type walkBuilder struct {
	q     *helix.ReadQueryBuilder
	w     brain.Walk
	scope scopeFilter
	edges []helix.Predicate
	k     helix.ParamRef
	overs []string
}

func newWalk(q *helix.ReadQueryBuilder, w brain.Walk) *walkBuilder {
	b := &walkBuilder{w: w, scope: newScopeFilter(q.ParamString, w.Namespace, w.Principals)}
	b.k = q.ParamI64("frontier_k", int64(w.FrontierK))
	b.edges = edgePredicates(q, w.Relations, w.Edges)
	b.q = q.VarAs(reachedName(0), helix.G().NWhere(helix.SourceEq(propObjectID, q.ParamString("object_id", w.ObjectID.String()))))
	return b
}

func reachedName(d int) string { return fmt.Sprintf("r%d", d) }

func overName(d int) string { return fmt.Sprintf("over%d", d) }

// frontierName is the node set hop d+1 starts from: the start for d = 0.
func frontierName(d int) string {
	if d == 0 {
		return reachedName(0)
	}
	return fmt.Sprintf("f%d", d)
}

// edgePredicates keep the edges of rels (every relation when empty) that
// pass f.
func edgePredicates(q *helix.ReadQueryBuilder, rels []string, f brain.EdgeFilter) []helix.Predicate {
	var preds []helix.Predicate
	if len(rels) > 0 {
		preds = append(preds, helix.PredIsInExpr("$label", q.ParamArray("relations", rels, helix.ParamTypeString()).Expr()))
	}
	if f.Status != "" {
		preds = append(preds, helix.PredEq(propEdgeStatus, q.ParamString("edge_status", f.Status)))
	}
	if f.Role != "" {
		preds = append(preds, helix.PredEq(propEdgeRole, q.ParamString("edge_role", f.Role)))
	}
	if f.MinConfidence > 0 {
		preds = append(preds, helix.PredGte(propEdgeConfidence, q.ParamF64("edge_confidence", f.MinConfidence)))
	}
	if !f.UpdatedAfter.IsZero() {
		preds = append(preds, helix.PredGte(propEdgeUpdatedAt, q.ParamI64("edge_updated_after", f.UpdatedAfter.UnixMilli())))
	}
	return preds
}

// edgesFrom is the edges of hop d that pass the walk's edge predicates and
// extra, from the frontier of hop d-1 through t.
func (b *walkBuilder) edgesFrom(t *helix.Traversal, extra ...helix.Predicate) *helix.Traversal {
	switch b.w.Direction {
	case brain.DirectionOut:
		t = t.OutE()
	case brain.DirectionIn:
		t = t.InE()
	default:
		t = t.BothE()
	}
	if preds := append(slices.Clone(b.edges), extra...); len(preds) > 0 {
		t = t.Where(helix.PredAnd(preds...))
	}
	return t
}

// hop adds reached(d) and, when the walk continues from it, frontier(d)
// and over(d).
func (b *walkBuilder) hop(d int, frontier bool) {
	r := b.edgesFrom(helix.G().N(helix.NodeVar(frontierName(d - 1)))).OtherN().Where(b.scope.nodes())
	for e := range d {
		r = r.Without(reachedName(e))
	}
	b.q = b.q.VarAs(reachedName(d), r.Dedup())
	if frontier {
		reached := func() *helix.Traversal { return helix.G().N(helix.NodeVar(reachedName(d))) }
		b.q = b.q.
			VarAs(frontierName(d), reached().OrderBy(propObjectID, helix.OrderAsc).Limit(b.k)).
			VarAs(overName(d), reached().Skip(b.k).Limit(1).Count())
		b.overs = append(b.overs, overName(d))
	}
}

// truncated reports a hop whose frontier left nodes out.
func (b *walkBuilder) truncated(out map[string]json.RawMessage) bool {
	return slices.ContainsFunc(b.overs, func(name string) bool { return count(out[name]) > 0 })
}

// count reads a Count variable.
func count(raw json.RawMessage) int {
	var n int
	_ = json.Unmarshal(raw, &n) // a Count is always a number
	return n
}

// cursor is the key of the last row of a page: its depth, the node's
// creation time and its object id.
type cursor struct {
	Depth   int       `json:"d"`
	Created int64     `json:"c"`
	ID      uuid.UUID `json:"id"`
}

func decodeCursor(after string) (cursor, error) {
	if after == "" {
		return cursor{Depth: 1}, nil
	}
	var c cursor
	if err := json.Unmarshal([]byte(after), &c); err != nil || c.Depth < 1 {
		return cursor{}, fmt.Errorf("%w: walk cursor %q", brain.ErrInvalid, after)
	}
	return c, nil
}

func (c cursor) String() string {
	b, _ := json.Marshal(c) // a cursor always encodes
	return string(b)
}

// rowsAfter keeps the rows whose key comes after c in newestFirst order.
func (c cursor) rowsAfter(q *helix.ReadQueryBuilder) helix.Predicate {
	created := q.ParamI64("after_created", c.Created)
	return helix.PredOr(
		helix.PredLt(propCreatedAt, created),
		helix.PredAnd(helix.PredEq(propCreatedAt, created), helix.PredGt(propObjectID, q.ParamString("after_id", c.ID.String()))),
	)
}

// Neighborhood implements brain.Graph in one request, or two when it
// counts every relation: the first finds the relations the walk follows.
// For each depth from the cursor's, the request returns up to Limit+1 rows
// in order, and the edges from the depth's frontier into those rows.
func (g *Graph) Neighborhood(ctx context.Context, nq brain.NeighborhoodQuery) (brain.NeighborPage, error) {
	after, err := decodeCursor(nq.After)
	if err != nil {
		return brain.NeighborPage{}, err
	}
	filters, err := filterPredicates(nq.Filters)
	if err != nil {
		return brain.NeighborPage{}, err
	}
	counted := nq.Relations
	if nq.Count && len(counted) == 0 {
		if counted, err = g.walkLabels(ctx, nq.Walk); err != nil {
			return brain.NeighborPage{}, err
		}
	}
	b := newWalk(helix.ReadQuery("brain_neighborhood"), nq.Walk)
	limit := b.q.ParamI64("limit", int64(nq.Limit+1))
	labels := make([]helix.Predicate, len(counted))
	for i, rel := range counted {
		labels[i] = helix.PredEq("$label", b.q.ParamString(fmt.Sprintf("count_label%d", i), rel))
	}
	var names []string
	for d := 1; d <= nq.MaxHops; d++ {
		b.hop(d, d < nq.MaxHops)
		reached := func() *helix.Traversal { return helix.G().N(helix.NodeVar(reachedName(d))) }
		if nq.Count {
			names = append(names, b.countHop(d, labels, filters)...)
		}
		if d < after.Depth {
			continue
		}
		rows := reached()
		preds := slices.Clone(filters)
		if d == after.Depth && nq.After != "" {
			preds = append(preds, after.rowsAfter(b.q))
		}
		if len(preds) > 0 {
			rows = rows.Where(helix.PredAnd(preds...))
		}
		page := fmt.Sprintf("page%d", d)
		b.q = b.q.
			VarAs(page, rows.OrderByMultiple(newestFirst...).Limit(limit)).
			VarAs(page+"_rows", helix.G().N(helix.NodeVar(page)).Project(helix.ProjectPropAs(propObjectID, "id"), helix.ProjectPropAs(propCreatedAt, "created"))).
			VarAs(page+"_edges", b.edgesFrom(helix.G().N(helix.NodeVar(frontierName(d-1))).Bind("src")).Bind("e").OtherN().Within(page).ProjectBindings(neighborProjections()...))
		names = append(names, page+"_rows", page+"_edges")
	}
	var out map[string]json.RawMessage
	if err := g.exec(ctx, b.q.Returning(append(names, b.overs...)...), &out); err != nil {
		return brain.NeighborPage{}, err
	}
	page := brain.NeighborPage{Truncated: b.truncated(out)}
	if nq.Count {
		page.Counts = hopCounts(out, nq.MaxHops, counted)
	}
	var keys []cursor
	dec := decoder{out: out}
	for d := after.Depth; d <= nq.MaxHops; d++ {
		name := fmt.Sprintf("page%d", d)
		rows := decode[[]struct {
			ID      uuid.UUID `json:"id"`
			Created int64     `json:"created"`
		}](&dec, name+"_rows")
		first := firstEdges(decode[[]neighborRow](&dec, name+"_edges"), nq.Relations)
		for _, row := range rows {
			e := first[row.ID]
			page.Neighbors = append(page.Neighbors, brain.GraphNeighbor{ObjectID: row.ID, SourceID: e.Source, RelationType: e.Label, Direction: e.direction(), Depth: d, Meta: e.meta()})
			keys = append(keys, cursor{Depth: d, Created: row.Created, ID: row.ID})
		}
	}
	if dec.err != nil {
		return brain.NeighborPage{}, fmt.Errorf("helixgraph: walk: %w", dec.err)
	}
	if len(page.Neighbors) > nq.Limit {
		page.Neighbors = page.Neighbors[:nq.Limit]
		page.Next = keys[nq.Limit-1].String()
	}
	return page, nil
}

// decoder reads the variables of one answer and keeps the first error.
type decoder struct {
	out map[string]json.RawMessage
	err error
}

// decode reads the variable name as a T; after an error it reads nothing.
func decode[T any](d *decoder, name string) T {
	var v T
	if d.err == nil {
		d.err = json.Unmarshal(d.out[name], &v)
	}
	return v
}

// countHop adds the counts of hop d: the rows that pass filters, and those
// the edges of each label reached.
func (b *walkBuilder) countHop(d int, labels []helix.Predicate, filters []helix.Predicate) []string {
	total := helix.G().N(helix.NodeVar(reachedName(d)))
	if len(filters) > 0 {
		total = total.Where(helix.PredAnd(filters...))
	}
	names := []string{fmt.Sprintf("total%d", d)}
	b.q = b.q.VarAs(names[0], total.Count())
	for i, label := range labels {
		by := b.edgesFrom(helix.G().N(helix.NodeVar(frontierName(d-1))), label).OtherN().Within(reachedName(d))
		if len(filters) > 0 {
			by = by.Where(helix.PredAnd(filters...))
		}
		name := fmt.Sprintf("count%d_%d", d, i)
		b.q = b.q.VarAs(name, by.Dedup().Count())
		names = append(names, name)
	}
	return names
}

// hopCounts sums the counts of every hop. A relation that reached nothing
// is left out.
func hopCounts(out map[string]json.RawMessage, hops int, rels []string) *brain.Counts {
	c := &brain.Counts{ByRelation: map[string]int{}}
	for d := 1; d <= hops; d++ {
		c.Total += count(out[fmt.Sprintf("total%d", d)])
		for i, rel := range rels {
			if n := count(out[fmt.Sprintf("count%d_%d", d, i)]); n > 0 {
				c.ByRelation[rel] += n
			}
		}
	}
	return c
}

// walkLabels lists the relations of the edges the walk follows, so a count
// of every relation can name them.
func (g *Graph) walkLabels(ctx context.Context, w brain.Walk) ([]string, error) {
	b := newWalk(helix.ReadQuery("brain_walk_labels"), w)
	var names []string
	for d := 1; d <= w.MaxHops; d++ {
		b.hop(d, d < w.MaxHops)
		name := fmt.Sprintf("labels%d", d)
		b.q = b.q.VarAs(name, b.edgesFrom(helix.G().N(helix.NodeVar(frontierName(d-1)))).GroupCount("$label"))
		names = append(names, name)
	}
	var out map[string][]struct {
		Label string `json:"$label"`
	}
	if err := g.exec(ctx, b.q.Returning(names...), &out); err != nil {
		return nil, err
	}
	var labels []string
	for _, rows := range out {
		for _, row := range rows {
			labels = append(labels, row.Label)
		}
	}
	slices.Sort(labels)
	return slices.Compact(labels), nil
}

// Reach implements brain.Graph in one request: every hop keeps its
// frontier, and the frontiers are the result.
func (g *Graph) Reach(ctx context.Context, w brain.Walk) (brain.Reached, error) {
	b := newWalk(helix.ReadQuery("brain_reach"), w)
	var names []string
	for d := 1; d <= w.MaxHops; d++ {
		b.hop(d, true)
		name := fmt.Sprintf("ids%d", d)
		b.q = b.q.VarAs(name, helix.G().N(helix.NodeVar(frontierName(d))).Project(helix.ProjectPropAs(propObjectID, "id")))
		names = append(names, name)
	}
	var out map[string]json.RawMessage
	if err := g.exec(ctx, b.q.Returning(append(names, b.overs...)...), &out); err != nil {
		return brain.Reached{}, err
	}
	reached := brain.Reached{Truncated: b.truncated(out)}
	dec := decoder{out: out}
	for _, name := range names {
		for _, row := range decode[[]struct {
			ID uuid.UUID `json:"id"`
		}](&dec, name) {
			reached.IDs = append(reached.IDs, row.ID)
		}
	}
	if dec.err != nil {
		return brain.Reached{}, fmt.Errorf("helixgraph: reach: %w", dec.err)
	}
	return reached, nil
}

// neighborRow is one edge from a hop's frontier into a page row.
type neighborRow struct {
	edgeRow
	Source     uuid.UUID `json:"source"`
	SourceNode uint64    `json:"source_node"`
	FromNode   uint64    `json:"from_node"`
	ID         uuid.UUID `json:"id"`
	Label      string    `json:"label"`
}

// direction is the edge's direction from its source.
func (r neighborRow) direction() string {
	if r.FromNode == r.SourceNode {
		return brain.DirectionOut
	}
	return brain.DirectionIn
}

func neighborProjections() []helix.BindingProjection {
	projections := []helix.BindingProjection{
		helix.ProjectNamedBinding("src", propObjectID, "source"),
		helix.ProjectNamedBinding("src", "$id", "source_node"),
		helix.ProjectNamedBinding("e", "$from", "from_node"),
		helix.ProjectNamedBinding("e", "$label", "label"),
		helix.ProjectCurrentBinding(propObjectID, "id"),
	}
	for _, p := range edgeMetaProps {
		projections = append(projections, helix.ProjectNamedBinding("e", p, p))
	}
	return projections
}

// firstEdges is the edge each row is reported with: the first relation in
// request order (by name when the walk follows every relation), outbound
// first, then the source with the smallest id.
func firstEdges(rows []neighborRow, rels []string) map[uuid.UUID]neighborRow {
	rank := func(r neighborRow) int {
		if i := slices.Index(rels, r.Label); i >= 0 {
			return i
		}
		return len(rels)
	}
	compare := func(a, b neighborRow) int {
		return cmp.Or(
			cmp.Compare(rank(a), rank(b)),
			strings.Compare(a.Label, b.Label),
			strings.Compare(a.direction(), b.direction())*-1, // "out" before "in"
			strings.Compare(a.Source.String(), b.Source.String()),
		)
	}
	first := map[uuid.UUID]neighborRow{}
	for _, r := range rows {
		if have, ok := first[r.ID]; !ok || compare(r, have) < 0 {
			first[r.ID] = r
		}
	}
	return first
}
