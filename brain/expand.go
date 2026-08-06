package brain

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ExpandRequest is the engine input for expand.
type ExpandRequest struct {
	ObjectID      uuid.UUID
	RelationTypes []string // graph labels; contains/part_of also request containment
	MaxHops       int      // graph depth; default 1; capped by MaxExpandHops
	Direction     string   // out | in | both (default both)
	Limit         int
	// WantContainment forces Postgres containment (children / parent+siblings)
	// alongside any graph labels. When RelationTypes is empty, containment is
	// always applied (default expand). Prefer this flag over smuggling "contains"
	// into RelationTypes when registering recipes or ExpandMany.
	WantContainment bool
}

// ExpandResult is the agent-facing expand payload.
type ExpandResult struct {
	Objects     []RichObject `json:"objects"`
	ResultSetID uuid.UUID    `json:"result_set_id,omitempty"`
	HasMore     bool         `json:"has_more"`
	Mode        string       `json:"mode"` // children | neighborhood | graph | mixed
}

// ExpandManyRequest expands several landing objects with shared hop parameters.
type ExpandManyRequest struct {
	ObjectIDs       []uuid.UUID
	RelationTypes   []string
	MaxHops         int
	Direction       string
	NeighborBudget  int  // max unique neighbors total; default MaxResultSetSize
	WantContainment bool // same semantics as ExpandRequest.WantContainment
}

// ExpandManyResult is a flat neighbor list; Relation.SourceID is the landing id.
type ExpandManyResult struct {
	Objects []RichObject `json:"objects"`
}

// ExpandRecipe is a host-registered named ExpandRequest template.
// ObjectID (and optional Limit / ResultSetStore) are supplied at call time.
// Register at construct via WithExpandRecipes, or later via RegisterExpandRecipe.
type ExpandRecipe struct {
	Name            string
	RelationTypes   []string
	MaxHops         int
	Direction       string
	WantContainment bool
}

// Expand returns the structural neighborhood of object_id under scope.
func (e *Engine) Expand(ctx context.Context, scope Scope, req ExpandRequest, results ResultSetStore) (res ExpandResult, err error) {
	ctx, span := e.observer.StartOp(ctx, OpExpand)
	degrade := DegradeNone
	defer func() { span.End(len(res.Objects), degrade, err) }()

	if e := ctx.Err(); e != nil {
		err = e
		return res, err
	}
	if req.ObjectID == uuid.Nil {
		return res, ErrObjectIDRequired
	}
	obj, err := e.store.Get(ctx, scope, req.ObjectID)
	if err != nil {
		return res, err
	}

	wantContainment, graphLabels := resolveExpandRelations(req.RelationTypes, req.WantContainment)
	if len(graphLabels) > 0 && e.graph == nil {
		return res, fmt.Errorf("%w for relation types %v", ErrGraphRequired, graphLabels)
	}

	var (
		ids             []uuid.UUID
		relByID         map[uuid.UUID]Relation
		usedContainment bool
		usedGraph       bool
	)

	if wantContainment {
		cIDs, cErr := e.containmentIDs(ctx, scope, obj)
		if cErr != nil {
			return res, cErr
		}
		ids = cIDs
		usedContainment = true
	}
	if len(graphLabels) > 0 {
		hits, gErr := e.graphNeighborsMulti(ctx, scope, obj.ID, graphLabels, req.MaxHops, req.Direction)
		if gErr != nil {
			if e.cfg.allowGraphDegrade() && usedContainment {
				degrade = DegradeContainmentOnly
			} else {
				return res, gErr
			}
		} else {
			seen := make(map[uuid.UUID]struct{}, len(ids)+len(hits))
			for _, id := range ids {
				seen[id] = struct{}{}
			}
			relByID = make(map[uuid.UUID]Relation, len(hits))
			for _, h := range hits {
				if h.n.ObjectID == uuid.Nil {
					continue
				}
				if _, ok := seen[h.n.ObjectID]; ok {
					continue
				}
				seen[h.n.ObjectID] = struct{}{}
				ids = append(ids, h.n.ObjectID)
				rel := RelationFromNeighbor(h.n)
				rel.Depth = h.depth
				relByID[h.n.ObjectID] = rel
			}
			usedGraph = true
		}
	}

	mode := expandMode(usedContainment, usedGraph, !obj.IsPart())

	if len(ids) <= e.cfg.ExpandInlineMax {
		objs, hErr := e.hydrateIDs(ctx, scope, ids)
		if hErr != nil {
			return res, hErr
		}
		attachRelations(objs, relByID)
		return ExpandResult{Objects: objs, Mode: mode}, nil
	}
	page, pErr := e.pageIDs(ctx, scope, ids, req.Limit, results, relByID)
	if pErr != nil {
		return res, pErr
	}
	return ExpandResult{
		Objects:     page.Objects,
		ResultSetID: page.ResultSetID,
		HasMore:     page.HasMore,
		Mode:        mode,
	}, nil
}

// ExpandMany walks the graph from many landing ids without paging / SearchContext.
// First seed to claim a neighbor wins Relation.SourceID. Out-of-scope seeds are skipped.
func (e *Engine) ExpandMany(ctx context.Context, scope Scope, req ExpandManyRequest) (res ExpandManyResult, err error) {
	ctx, span := e.observer.StartOp(ctx, OpExpandMany)
	defer func() { span.End(len(res.Objects), DegradeNone, err) }()

	if e := ctx.Err(); e != nil {
		err = e
		return res, err
	}
	if len(req.ObjectIDs) == 0 {
		return res, nil
	}
	budget := req.NeighborBudget
	if budget <= 0 {
		budget = e.cfg.MaxResultSetSize
		if budget <= 0 {
			budget = 1000
		}
	}

	wantContainment, graphLabels := resolveExpandRelations(req.RelationTypes, req.WantContainment)
	if len(graphLabels) > 0 && e.graph == nil {
		return res, fmt.Errorf("%w for relation types %v", ErrGraphRequired, graphLabels)
	}

	seen := make(map[uuid.UUID]struct{}, budget)
	out := make([]RichObject, 0, min(budget, 32))

	for _, seed := range req.ObjectIDs {
		if seed == uuid.Nil {
			continue
		}
		if e := ctx.Err(); e != nil {
			err = e
			return res, err
		}
		obj, gErr := e.store.Get(ctx, scope, seed)
		if gErr != nil {
			if errors.Is(gErr, ErrNotFound) {
				continue
			}
			return res, gErr
		}

		var ids []uuid.UUID
		relByID := make(map[uuid.UUID]Relation)
		if wantContainment {
			cIDs, cErr := e.containmentIDs(ctx, scope, obj)
			if cErr != nil {
				return res, cErr
			}
			ids = append(ids, cIDs...)
		}
		if len(graphLabels) > 0 {
			hits, nErr := e.graphNeighborsMulti(ctx, scope, seed, graphLabels, req.MaxHops, req.Direction)
			if nErr != nil {
				return res, nErr
			}
			for _, h := range hits {
				ids = append(ids, h.n.ObjectID)
				rel := RelationFromNeighbor(h.n)
				rel.Depth = h.depth
				relByID[h.n.ObjectID] = rel
			}
		}
		if len(ids) == 0 {
			continue
		}
		ids = uniqueUUIDs(ids)
		objs, hErr := e.hydrateIDs(ctx, scope, ids)
		if hErr != nil {
			return res, hErr
		}
		attachRelations(objs, relByID)
		sid := seed
		for i := range objs {
			o := objs[i]
			if _, ok := seen[o.ID]; ok {
				continue
			}
			seen[o.ID] = struct{}{}
			if o.Relation == nil {
				rel := Relation{}
				o.Relation = &rel
			}
			o.Relation.SourceID = &sid
			out = append(out, o)
			if len(out) >= budget {
				return ExpandManyResult{Objects: out}, nil
			}
		}
	}
	return ExpandManyResult{Objects: out}, nil
}

// ExpandByRecipe looks up a host-registered ExpandRecipe and runs Expand with it.
func (e *Engine) ExpandByRecipe(ctx context.Context, scope Scope, objectID uuid.UUID, recipeName string, results ResultSetStore) (ExpandResult, error) {
	r, ok := e.recipe(recipeName)
	if !ok {
		return ExpandResult{}, fmt.Errorf("%w: %q", ErrExpandRecipeNotFound, strings.TrimSpace(recipeName))
	}
	return e.Expand(ctx, scope, ExpandRequest{
		ObjectID:        objectID,
		RelationTypes:   r.RelationTypes,
		MaxHops:         r.MaxHops,
		Direction:       r.Direction,
		WantContainment: r.WantContainment,
	}, results)
}

// RegisterExpandRecipe adds or replaces a named expand view.
// Safe for concurrent use with ExpandByRecipe.
func (e *Engine) RegisterExpandRecipe(r ExpandRecipe) error {
	name := strings.TrimSpace(r.Name)
	if name == "" {
		return ErrExpandRecipeNameRequired
	}
	r.Name = name
	e.recipeMu.Lock()
	defer e.recipeMu.Unlock()
	if e.recipes == nil {
		e.recipes = make(map[string]ExpandRecipe)
	}
	e.recipes[name] = r
	return nil
}

func (e *Engine) recipe(name string) (ExpandRecipe, bool) {
	e.recipeMu.RLock()
	defer e.recipeMu.RUnlock()
	if e.recipes == nil {
		return ExpandRecipe{}, false
	}
	r, ok := e.recipes[strings.TrimSpace(name)]
	return r, ok
}

// resolveExpandRelations combines RelationTypes with the explicit WantContainment flag.
// Empty RelationTypes → containment only (default expand). Non-empty → graph labels,
// plus containment when WantContainment is true or a containment label is present.
func resolveExpandRelations(rels []string, wantContainment bool) (bool, []string) {
	fromRels, labels := SplitRelationTypes(rels)
	if wantContainment {
		return true, labels
	}
	return fromRels, labels
}

func (e *Engine) containmentIDs(ctx context.Context, scope Scope, obj Object) ([]uuid.UUID, error) {
	if !obj.IsPart() {
		kids, err := e.store.ListChildren(ctx, scope, obj.ID)
		if err != nil {
			return nil, err
		}
		ids := make([]uuid.UUID, len(kids))
		for i, k := range kids {
			ids[i] = k.ID
		}
		return ids, nil
	}

	parentID := *obj.ParentID
	if _, err := e.store.Get(ctx, scope, parentID); err != nil {
		return nil, err
	}
	sibs, err := e.siblingWindowIDs(ctx, scope, parentID, obj.ID)
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, 1+len(sibs))
	ids = append(ids, parentID)
	seen := map[uuid.UUID]struct{}{parentID: {}}
	for _, id := range sibs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func expandMode(containment, graph, isParent bool) string {
	switch {
	case containment && graph:
		return "mixed"
	case graph:
		return "graph"
	case isParent:
		return "children"
	default:
		return "neighborhood"
	}
}

func (e *Engine) siblingWindowIDs(ctx context.Context, scope Scope, parentID, partID uuid.UUID) ([]uuid.UUID, error) {
	kids, err := e.store.ListChildren(ctx, scope, parentID)
	if err != nil {
		return nil, err
	}
	idx := -1
	for i, k := range kids {
		if k.ID == partID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, nil
	}
	start := max(0, idx-e.cfg.SiblingRadius)
	end := min(len(kids), idx+e.cfg.SiblingRadius+1)
	ids := make([]uuid.UUID, 0, end-start)
	for _, k := range kids[start:end] {
		ids = append(ids, k.ID)
	}
	return ids, nil
}

type graphNeighborHit struct {
	n     GraphNeighbor
	depth int
}

// graphNeighborsMulti BFS-walks relation labels up to maxHops.
// Scope is applied each hop: out-of-scope nodes are dropped and never used as
// frontier seeds for deeper hops. Neighbors RPC count is capped by MaxGraphExpandRPCs.
func (e *Engine) graphNeighborsMulti(ctx context.Context, scope Scope, id uuid.UUID, labels []string, maxHops int, direction string) ([]graphNeighborHit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxHops <= 0 {
		maxHops = 1
	}
	if maxHops > e.cfg.MaxExpandHops {
		maxHops = e.cfg.MaxExpandHops
	}
	rpcBudget := e.cfg.MaxGraphExpandRPCs
	if rpcBudget <= 0 {
		rpcBudget = 64
	}
	dir := normalizeExpandDirection(direction)

	frontier := []uuid.UUID{id}
	visited := map[uuid.UUID]struct{}{id: {}}
	var hits []graphNeighborHit
	rpcCount := 0

	for hop := 1; hop <= maxHops && len(frontier) > 0; hop++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Collect candidates for this hop (may include out-of-scope ids).
		var hopHits []graphNeighborHit
		candSeen := make(map[uuid.UUID]struct{})
		stopRPCs := false
		for _, fid := range frontier {
			if rpcCount >= rpcBudget {
				stopRPCs = true
				break
			}
			rpcCount++
			ns, err := e.graph.Neighbors(ctx, fid, labels, e.cfg.GraphNeighborK)
			if err != nil {
				return nil, err
			}
			for _, n := range ns {
				if n.ObjectID == uuid.Nil {
					continue
				}
				if !directionAllows(dir, n.Direction) {
					continue
				}
				if _, ok := visited[n.ObjectID]; ok {
					continue
				}
				if _, ok := candSeen[n.ObjectID]; ok {
					continue
				}
				candSeen[n.ObjectID] = struct{}{}
				hopHits = append(hopHits, graphNeighborHit{n: n, depth: hop})
			}
		}

		if len(hopHits) == 0 {
			if stopRPCs {
				break
			}
			frontier = nil
			continue
		}

		// Scope filter before promoting to result / next frontier.
		ids := make([]uuid.UUID, len(hopHits))
		for i, h := range hopHits {
			ids[i] = h.n.ObjectID
		}
		objs, err := e.store.GetMany(ctx, scope, ids)
		if err != nil {
			return nil, fmt.Errorf("brain: graph neighbors hydrate: %w", err)
		}
		visible := make(map[uuid.UUID]struct{}, len(objs))
		for _, o := range objs {
			visible[o.ID] = struct{}{}
		}

		var next []uuid.UUID
		for _, h := range hopHits {
			if _, ok := visible[h.n.ObjectID]; !ok {
				// Mark visited so we do not re-query this out-of-scope id.
				visited[h.n.ObjectID] = struct{}{}
				continue
			}
			if _, ok := visited[h.n.ObjectID]; ok {
				continue
			}
			visited[h.n.ObjectID] = struct{}{}
			hits = append(hits, h)
			// Only in-scope nodes seed the next hop.
			if hop < maxHops {
				next = append(next, h.n.ObjectID)
			}
		}
		frontier = next
		if stopRPCs {
			break
		}
	}
	return hits, nil
}

func normalizeExpandDirection(d string) string {
	switch d = strings.ToLower(strings.TrimSpace(d)); d {
	case "out", "in":
		return d
	default:
		return "both"
	}
}

func directionAllows(want, got string) bool {
	if want == "both" || want == "" {
		return true
	}
	return want == got
}

func uniqueUUIDs(ids []uuid.UUID) []uuid.UUID {
	if len(ids) <= 1 {
		return ids
	}
	seen := make(map[uuid.UUID]struct{}, len(ids))
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id == uuid.Nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
