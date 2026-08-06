package brain

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// ExpandRequest is the engine input for expand.
type ExpandRequest struct {
	ObjectID      uuid.UUID
	RelationTypes []string
	Limit         int
}

// ExpandResult is the agent-facing expand payload.
type ExpandResult struct {
	Objects     []RichObject `json:"objects"`
	ResultSetID uuid.UUID    `json:"result_set_id,omitempty"`
	HasMore     bool         `json:"has_more"`
	Mode        string       `json:"mode"` // children | neighborhood | graph | mixed
}

// Expand returns the structural neighborhood of object_id under scope.
func (e *Engine) Expand(ctx context.Context, scope Scope, req ExpandRequest, results ResultSetStore) (ExpandResult, error) {
	ctx, span := e.observer.StartOp(ctx, OpExpand)
	res, degrade, err := e.expandInner(ctx, scope, req, results)
	span.End(len(res.Objects), degrade, err)
	return res, err
}

func (e *Engine) expandInner(ctx context.Context, scope Scope, req ExpandRequest, results ResultSetStore) (ExpandResult, DegradeMode, error) {
	if err := ctx.Err(); err != nil {
		return ExpandResult{}, DegradeNone, err
	}
	if req.ObjectID == uuid.Nil {
		return ExpandResult{}, DegradeNone, ErrObjectIDRequired
	}
	obj, err := e.store.Get(ctx, scope, req.ObjectID)
	if err != nil {
		return ExpandResult{}, DegradeNone, err
	}

	wantContainment, graphLabels := SplitRelationTypes(req.RelationTypes)
	if len(graphLabels) > 0 && e.graph == nil {
		return ExpandResult{}, DegradeNone, fmt.Errorf("%w for relation types %v", ErrGraphRequired, graphLabels)
	}

	var (
		ids             []uuid.UUID
		relByID         map[uuid.UUID]Relation
		usedContainment bool
		usedGraph       bool
		degrade         = DegradeNone
	)

	if wantContainment {
		cIDs, err := e.containmentIDs(ctx, scope, obj)
		if err != nil {
			return ExpandResult{}, degrade, err
		}
		ids = cIDs
		usedContainment = true
	}
	if len(graphLabels) > 0 {
		neighbors, err := e.graphNeighbors(ctx, scope, obj.ID, graphLabels)
		if err != nil {
			if e.cfg.allowGraphDegrade() && usedContainment {
				degrade = DegradeContainmentOnly
			} else {
				return ExpandResult{}, degrade, err
			}
		} else {
			// One seen set for the whole expand — avoid rebuild-per-appendUnique.
			seen := make(map[uuid.UUID]struct{}, len(ids)+len(neighbors))
			for _, id := range ids {
				seen[id] = struct{}{}
			}
			relByID = make(map[uuid.UUID]Relation, len(neighbors))
			for _, n := range neighbors {
				if n.ObjectID == uuid.Nil {
					continue
				}
				if _, ok := seen[n.ObjectID]; ok {
					continue
				}
				seen[n.ObjectID] = struct{}{}
				ids = append(ids, n.ObjectID)
				relByID[n.ObjectID] = RelationFromNeighbor(n)
			}
			usedGraph = true
		}
	}

	mode := expandMode(usedContainment, usedGraph, !obj.IsPart())

	if len(ids) <= e.cfg.ExpandInlineMax {
		objs, err := e.hydrateIDs(ctx, scope, ids)
		if err != nil {
			return ExpandResult{}, degrade, err
		}
		attachRelations(objs, relByID)
		return ExpandResult{Objects: objs, Mode: mode}, degrade, nil
	}
	page, err := e.pageIDs(ctx, scope, ids, req.Limit, results, relByID)
	if err != nil {
		return ExpandResult{}, degrade, err
	}
	return ExpandResult{
		Objects:     page.Objects,
		ResultSetID: page.ResultSetID,
		HasMore:     page.HasMore,
		Mode:        mode,
	}, degrade, nil
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
	// parent first, then siblings (window may include self).
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

// graphNeighbors returns scope-visible graph hops in graph order (with edge meta).
func (e *Engine) graphNeighbors(ctx context.Context, scope Scope, id uuid.UUID, labels []string) ([]GraphNeighbor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ns, err := e.graph.Neighbors(ctx, id, labels, e.cfg.GraphNeighborK)
	if err != nil {
		return nil, err
	}
	if len(ns) == 0 {
		return nil, nil
	}
	// Dedupe hops, preserve order; single candidate list for one GetMany.
	candidates := make([]uuid.UUID, 0, len(ns))
	byID := make(map[uuid.UUID]GraphNeighbor, len(ns))
	seen := map[uuid.UUID]struct{}{id: {}}
	for _, n := range ns {
		if n.ObjectID == uuid.Nil {
			continue
		}
		if _, ok := seen[n.ObjectID]; ok {
			continue
		}
		seen[n.ObjectID] = struct{}{}
		candidates = append(candidates, n.ObjectID)
		byID[n.ObjectID] = n
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	objs, err := e.store.GetMany(ctx, scope, candidates)
	if err != nil {
		return nil, fmt.Errorf("brain: graph neighbors hydrate: %w", err)
	}
	visible := make(map[uuid.UUID]struct{}, len(objs))
	for _, o := range objs {
		visible[o.ID] = struct{}{}
	}
	out := make([]GraphNeighbor, 0, len(objs))
	for _, cid := range candidates {
		if _, ok := visible[cid]; ok {
			out = append(out, byID[cid])
		}
	}
	return out, nil
}
