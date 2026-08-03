package brain

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/telemetry"
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
	ctx, span := telemetry.StartBrainSpan(ctx, telemetry.BrainOpExpand)
	res, degrade, err := e.expandInner(ctx, scope, req, results)
	span.End(len(res.Objects), degrade, err)
	return res, err
}

func (e *Engine) expandInner(ctx context.Context, scope Scope, req ExpandRequest, results ResultSetStore) (ExpandResult, string, error) {
	if req.ObjectID == uuid.Nil {
		return ExpandResult{}, telemetry.BrainDegradeNone, fmt.Errorf("brain: object id is required")
	}
	obj, err := e.store.Get(ctx, scope, req.ObjectID)
	if err != nil {
		return ExpandResult{}, telemetry.BrainDegradeNone, err
	}

	wantContainment, graphLabels := SplitRelationTypes(req.RelationTypes)
	if len(graphLabels) > 0 && e.graph == nil {
		return ExpandResult{}, telemetry.BrainDegradeNone, fmt.Errorf("brain: graph backend is required for relation types %v", graphLabels)
	}

	var (
		ids             []uuid.UUID
		usedContainment bool
		usedGraph       bool
		degrade         = telemetry.BrainDegradeNone
	)

	if wantContainment {
		cIDs, err := e.containmentIDs(ctx, scope, obj)
		if err != nil {
			return ExpandResult{}, degrade, err
		}
		ids = append(ids, cIDs...)
		usedContainment = true
	}
	if len(graphLabels) > 0 {
		gIDs, err := e.graphNeighborIDs(ctx, scope, obj.ID, graphLabels)
		if err != nil {
			if e.cfg.degradeGraph() && usedContainment {
				degrade = telemetry.BrainDegradeContainmentOnly
			} else {
				return ExpandResult{}, degrade, err
			}
		} else {
			ids = appendUnique(ids, gIDs...)
			usedGraph = true
		}
	}

	res, err := e.expandMaybePage(ctx, scope, ids, expandMode(usedContainment, usedGraph, !obj.IsPart()), req.Limit, results)
	return res, degrade, err
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
	ids := []uuid.UUID{parentID}
	sibs, err := e.siblingWindowIDs(ctx, scope, parentID, obj.ID)
	if err != nil {
		return nil, err
	}
	return appendUnique(ids, sibs...), nil
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

func (e *Engine) expandMaybePage(ctx context.Context, scope Scope, ids []uuid.UUID, mode string, limit int, results ResultSetStore) (ExpandResult, error) {
	if len(ids) <= e.cfg.ExpandInlineMax {
		objs, err := e.hydrateParents(ctx, scope, ids)
		if err != nil {
			return ExpandResult{}, err
		}
		return ExpandResult{Objects: objs, Mode: mode}, nil
	}
	if results == nil {
		return ExpandResult{}, fmt.Errorf("brain: result set store is required for large expand")
	}
	page, err := e.pageIDs(ctx, scope, ids, limit, results)
	if err != nil {
		return ExpandResult{}, err
	}
	return ExpandResult{
		Objects:     page.Objects,
		ResultSetID: page.ResultSetID,
		HasMore:     page.HasMore,
		Mode:        mode,
	}, nil
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

func (e *Engine) graphNeighborIDs(ctx context.Context, scope Scope, id uuid.UUID, labels []string) ([]uuid.UUID, error) {
	ns, err := e.graph.Neighbors(ctx, id, labels, e.cfg.GraphNeighborK)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	seen := map[uuid.UUID]struct{}{id: {}}
	for _, n := range ns {
		if n.ObjectID == uuid.Nil {
			continue
		}
		if _, ok := seen[n.ObjectID]; ok {
			continue
		}
		visible, err := e.visible(ctx, scope, n.ObjectID)
		if err != nil {
			return nil, err
		}
		if !visible {
			continue
		}
		seen[n.ObjectID] = struct{}{}
		ids = append(ids, n.ObjectID)
	}
	return ids, nil
}

func (e *Engine) visible(ctx context.Context, scope Scope, id uuid.UUID) (bool, error) {
	_, err := e.store.Get(ctx, scope, id)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return false, err
}

func appendUnique(dst []uuid.UUID, extra ...uuid.UUID) []uuid.UUID {
	seen := map[uuid.UUID]struct{}{}
	for _, id := range dst {
		seen[id] = struct{}{}
	}
	for _, id := range extra {
		if id == uuid.Nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		dst = append(dst, id)
	}
	return dst
}
