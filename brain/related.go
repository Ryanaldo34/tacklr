package brain

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// RelatedRequest asks for the objects related to one object.
type RelatedRequest struct {
	ID uuid.UUID
	// Relations narrows the result: empty returns the children and every
	// graph relation, "contains" names the children, and any other name a
	// graph relation.
	Relations []string
	// Direction, Depth and Edges shape the graph walk: the direction of the
	// edges from ID (DirectionBoth by default), the hops (default 1, capped
	// by MaxExpandHops), and the edges it follows.
	Direction string
	Depth     int
	Edges     EdgeFilter
	// Filters keep the records returned, with the same fields as a search,
	// before the limit. The walk still goes through records that fail them.
	Filters Filter
	Limit   int
	// Count asks for Page.Counts: everything related that passes Filters.
	Count bool
}

// Related returns the objects related to req.ID in one list, each tagged
// with how it is related: a record's children ("child"), or a part's parent
// ("parent") and nearby parts ("sibling"), then, with a graph, the objects
// its relations reach (the relation, direction, depth and note), in the
// graph's order. Next pages through all of them: the graph walk continues
// from a cursor, so no neighbor is out of reach. Without a graph Related
// returns the children only. When the graph fails, Related returns the
// children unless FailOnGraphError is set or only graph relations were
// asked for.
func (e *Engine) Related(ctx context.Context, scope Scope, req RelatedRequest) (Page, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Page{}, err
	}
	walk, children, err := e.walkFrom(req.ID, req.Relations, req.Direction, req.Depth, req.Edges)
	if err != nil {
		return Page{}, err
	}
	if err := req.Filters.ValidateAgainst(e.catalog); err != nil {
		return Page{}, err
	}
	plan, _ := req.Filters.Compile() // ValidateAgainst checked it
	tree, err := e.store.GetTree(ctx, scope, req.ID, 0)
	if err != nil {
		return Page{}, err
	}
	limit := e.normalizeLimit(req.Limit)
	rest := Rest{filter: req.Filters}
	var counts *Counts
	if children {
		rest.refs = e.containment(tree, plan)
		counts = containmentCounts(rest.refs)
	}
	if e.graph != nil && (len(req.Relations) == 0 || len(walk.Relations) > 0) {
		first, err := e.graph.Neighborhood(ctx, e.walkQuery(scope, walk, req.Filters, limit, req.Count))
		switch {
		case err == nil:
			rest.refs = append(rest.refs, neighborRefs(first.Neighbors)...)
			rest.walk, rest.truncated = walk.after(first), first.Truncated
			counts = counts.add(first.Counts)
		case children && !e.cfg.FailOnGraphError:
			slog.WarnContext(ctx, "graph walk failed; returning children only", "error", err)
		default:
			return Page{}, fmt.Errorf("brain: graph walk: %w", err)
		}
	}
	page, err := e.page(ctx, scope, rest, limit)
	if err != nil {
		return Page{}, err
	}
	if req.Count {
		page.Counts = counts
	}
	return page, nil
}

// walkFrom checks and normalizes a walk from id: the graph relations among
// rels (trimmed and unique), the direction (DirectionBoth when empty), and
// the depth (at least 1, at most MaxExpandHops). children reports that rels
// are empty or name containment.
func (e *Engine) walkFrom(id uuid.UUID, rels []string, dir string, depth int, edges EdgeFilter) (w *walkRest, children bool, err error) {
	if id == uuid.Nil {
		return nil, false, fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	w = &walkRest{ID: id, Direction: cmp.Or(strings.TrimSpace(dir), DirectionBoth), Depth: min(max(depth, 1), e.cfg.MaxExpandHops), Edges: edges}
	if !slices.Contains([]string{DirectionOut, DirectionIn, DirectionBoth}, w.Direction) {
		return nil, false, fmt.Errorf("%w: direction %q: want %s, %s or %s", ErrInvalid, dir, DirectionOut, DirectionIn, DirectionBoth)
	}
	children = len(rels) == 0
	for _, rel := range rels {
		rel = strings.TrimSpace(rel)
		switch {
		case rel == "":
			return nil, false, fmt.Errorf("%w: a relation is empty", ErrInvalid)
		case isContainment(rel):
			children = true
		case !slices.Contains(w.Relations, rel):
			w.Relations = append(w.Relations, rel)
		}
	}
	return w, children, nil
}

// containment is a record's children, or a part's parent and the other
// parts within SiblingRadius of it, that pass plan.
func (e *Engine) containment(t Tree, plan FilterPlan) []ref {
	var refs []ref
	add := func(o Object, rel string) {
		if plan.Match(o) {
			refs = append(refs, ref{ID: o.ID, Relation: &Relation{Type: rel}})
		}
	}
	if !t.Object.IsPart() {
		for _, part := range t.Parts {
			add(part, RelationChild)
		}
		return refs
	}
	if t.Parent != nil {
		add(*t.Parent, RelationParent)
	}
	i := slices.IndexFunc(t.Parts, func(o Object) bool { return o.ID == t.Object.ID })
	for _, sib := range t.Parts[max(0, i-e.cfg.SiblingRadius):min(len(t.Parts), i+e.cfg.SiblingRadius+1)] {
		if sib.ID != t.Object.ID {
			add(sib, RelationSibling)
		}
	}
	return refs
}

// containmentCounts counts refs by their relation.
func containmentCounts(refs []ref) *Counts {
	c := &Counts{Total: len(refs), ByRelation: map[string]int{}}
	for _, r := range refs {
		c.ByRelation[r.Relation.Type]++
	}
	return c
}

// add is c with more's counts added; either may be nil.
func (c *Counts) add(more *Counts) *Counts {
	if c == nil || more == nil {
		return cmp.Or(c, more)
	}
	out := &Counts{Total: c.Total + more.Total, ByRelation: maps.Clone(c.ByRelation)}
	for rel, n := range more.ByRelation {
		out.ByRelation[rel] += n
	}
	return out
}

// walkRest is a graph walk that Next continues: the walk and the cursor
// after its last page.
type walkRest struct {
	ID        uuid.UUID  `json:"id"`
	Relations []string   `json:"relations,omitempty"`
	Direction string     `json:"direction"`
	Depth     int        `json:"depth"`
	Edges     EdgeFilter `json:"edges,omitzero"`
	After     string     `json:"after"`
}

// after is w continued after page, or nil when the walk is done.
func (w *walkRest) after(page NeighborPage) *walkRest {
	if page.Next == "" || len(page.Neighbors) == 0 {
		return nil
	}
	next := *w
	next.After = page.Next
	return &next
}

// walk is w under scope.
func (e *Engine) walk(scope Scope, w *walkRest) Walk {
	return Walk{
		Namespace: scope.Namespace, Principals: scope.Principals,
		ObjectID: w.ID, Relations: w.Relations, Direction: w.Direction, MaxHops: w.Depth, Edges: w.Edges,
		FrontierK: e.cfg.FrontierK,
	}
}

// walkQuery is the page of w under scope after w.After.
func (e *Engine) walkQuery(scope Scope, w *walkRest, filters Filter, limit int, count bool) NeighborhoodQuery {
	return NeighborhoodQuery{Walk: e.walk(scope, w), Filters: filters, After: w.After, Limit: limit, Count: count}
}

// neighborRefs are the refs of a walk's page. The stored row of each must
// still pass the filters: the graph's copy can be behind the store.
func neighborRefs(hits []GraphNeighbor) []ref {
	refs := make([]ref, len(hits))
	for i, n := range hits {
		refs[i] = ref{ID: n.ObjectID, Check: true, Relation: &Relation{Type: n.RelationType, Direction: n.Direction, Depth: n.Depth, Via: new(n.SourceID), EdgeMeta: n.Meta}}
	}
	return refs
}

// Relation types of the containment results of Related.
const (
	RelationChild   = "child"
	RelationParent  = "parent"
	RelationSibling = "sibling"
)
