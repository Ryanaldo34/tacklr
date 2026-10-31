package brain

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// RelatedRequest asks for the objects related to one object.
type RelatedRequest struct {
	ID uuid.UUID
	// Relation narrows the result: empty returns children and every graph
	// relation, "contains" only the children, any other name only that
	// graph relation.
	Relation string
	Depth    int // graph hops; default 1, capped by MaxExpandHops
	Limit    int
}

// Related returns the objects related to req.ID in one list, each tagged
// with how it is related: a record's children ("child"), or a part's parent
// ("parent") and nearby parts ("sibling"), then, with a graph, the objects
// its relations reach (the relation, direction, depth and note). Without a
// graph Related returns the children only. When the graph fails, Related
// returns the children unless FailOnGraphError is set or only a graph
// relation was asked for.
func (e *Engine) Related(ctx context.Context, scope Scope, req RelatedRequest) (Page, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Page{}, err
	}
	if req.ID == uuid.Nil {
		return Page{}, fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	tree, err := e.store.GetTree(ctx, scope, req.ID, 0)
	if err != nil {
		return Page{}, err
	}
	obj := tree.Object
	rel := strings.TrimSpace(req.Relation)
	children := rel == "" || isContainment(rel)
	var refs []ref
	if children {
		refs = e.containment(tree)
	}
	if e.graph != nil && !isContainment(rel) {
		walked, err := e.neighbors(ctx, scope, obj.ID, rel, req.Depth)
		switch {
		case err == nil:
			refs = append(refs, walked...)
		case children && !e.cfg.FailOnGraphError:
			slog.WarnContext(ctx, "graph walk failed; returning children only", "error", err)
		default:
			return Page{}, err
		}
	}
	return e.page(ctx, scope, Rest{refs: dedupeRefs(refs, obj.ID)}, e.normalizeLimit(req.Limit))
}

// containment is a record's children, or a part's parent and the parts
// within SiblingRadius of it.
func (e *Engine) containment(t Tree) []ref {
	if !t.Object.IsPart() {
		refs := make([]ref, len(t.Parts))
		for i, k := range t.Parts {
			refs[i] = ref{ID: k.ID, Relation: &Relation{Type: RelationChild}}
		}
		return refs
	}
	refs := []ref{{ID: *t.Object.ParentID, Relation: &Relation{Type: RelationParent}}}
	i := slices.IndexFunc(t.Parts, func(o Object) bool { return o.ID == t.Object.ID })
	for _, sib := range t.Parts[max(0, i-e.cfg.SiblingRadius):min(len(t.Parts), i+e.cfg.SiblingRadius+1)] {
		refs = append(refs, ref{ID: sib.ID, Relation: &Relation{Type: RelationSibling}})
	}
	return refs
}

// neighbors walks the graph from id over rel (every relation when empty)
// up to depth hops, in one backend call.
func (e *Engine) neighbors(ctx context.Context, scope Scope, id uuid.UUID, rel string, depth int) ([]ref, error) {
	var rels []string
	if rel != "" {
		rels = []string{rel}
	}
	hits, err := e.graph.Neighborhood(ctx, NeighborhoodQuery{
		Namespace:  scope.Namespace,
		Principals: scope.Principals,
		ObjectID:   id,
		Relations:  rels,
		Direction:  DirectionBoth,
		MaxHops:    min(max(depth, 1), e.cfg.MaxExpandHops),
		Limit:      e.cfg.GraphNeighborK,
	})
	if err != nil {
		return nil, fmt.Errorf("brain: graph walk: %w", err)
	}
	refs := make([]ref, len(hits))
	for i, n := range hits {
		refs[i] = ref{ID: n.ObjectID, Relation: &Relation{Type: n.RelationType, Direction: n.Direction, Depth: n.Depth, Via: new(n.SourceID), EdgeMeta: n.Meta}}
	}
	return refs, nil
}

// dedupeRefs keeps each object's first ref and drops self.
func dedupeRefs(refs []ref, self uuid.UUID) []ref {
	seen := map[uuid.UUID]struct{}{self: {}}
	out := refs[:0]
	for _, r := range refs {
		if _, ok := seen[r.ID]; ok {
			continue
		}
		seen[r.ID] = struct{}{}
		out = append(out, r)
	}
	return out
}

// Relation types of the containment results of Related.
const (
	RelationChild   = "child"
	RelationParent  = "parent"
	RelationSibling = "sibling"
)
