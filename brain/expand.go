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

// walker walks the graph from id over labels. KnowledgeGraph passes its
// graph's walk; Engine allows containment only, so it passes none.
type walker func(ctx context.Context, scope Scope, id uuid.UUID, labels []string, maxHops int, direction string) ([]GraphNeighbor, error)

// Expand returns the children of object_id, or its parent and siblings when
// it is a part. Relation types other than containment need a
// KnowledgeGraph (ErrUnsupported).
func (e *Engine) Expand(ctx context.Context, scope Scope, req ExpandRequest, results ResultSetStore) (ExpandResult, error) {
	if err := containmentOnly(req.RelationTypes); err != nil {
		return ExpandResult{}, err
	}
	return e.expand(ctx, scope, req, results, nil)
}

// Expand returns the structural neighborhood of object_id under scope:
// children (or parent and siblings) and the objects its relations reach.
func (k *KnowledgeGraph) Expand(ctx context.Context, scope Scope, req ExpandRequest, results ResultSetStore) (ExpandResult, error) {
	return k.eng.expand(ctx, scope, req, results, k.neighbors)
}

// containmentOnly rejects relation types other than containment.
func containmentOnly(rels []string) error {
	if _, labels := SplitRelationTypes(rels); len(labels) > 0 {
		return fmt.Errorf("%w: relation types %v need a KnowledgeGraph", ErrUnsupported, labels)
	}
	return nil
}

// expand walks containment, and relations with walk when the request names
// any.
func (e *Engine) expand(ctx context.Context, scope Scope, req ExpandRequest, results ResultSetStore, walk walker) (res ExpandResult, err error) {
	if e := ctx.Err(); e != nil {
		err = e
		return res, err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return res, err
	}
	if req.ObjectID == uuid.Nil {
		return res, fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	obj, err := e.store.Get(ctx, scope, req.ObjectID)
	if err != nil {
		return res, err
	}

	wantContainment, graphLabels := resolveExpandRelations(req.RelationTypes, req.WantContainment)

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
		hits, gErr := walk(ctx, scope, obj.ID, graphLabels, req.MaxHops, req.Direction)
		if gErr != nil {
			if e.cfg.FailOnGraphError || !usedContainment {
				return res, gErr
			}
		} else {
			seen := make(map[uuid.UUID]struct{}, len(ids)+len(hits))
			for _, id := range ids {
				seen[id] = struct{}{}
			}
			relByID = make(map[uuid.UUID]Relation, len(hits))
			for _, n := range hits {
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
		objs, hErr := e.hydrateIDs(ctx, scope, ids)
		if hErr != nil {
			return res, hErr
		}
		if err := e.recordRead(ctx, scope, richIDs(objs)); err != nil {
			return res, err
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

// ExpandMany expands many landing ids without paging / SearchContext, over
// containment only (relations need a KnowledgeGraph). First seed to claim a
// neighbor wins Relation.SourceID. Out-of-scope seeds are skipped.
func (e *Engine) ExpandMany(ctx context.Context, scope Scope, req ExpandManyRequest) (ExpandManyResult, error) {
	if err := containmentOnly(req.RelationTypes); err != nil {
		return ExpandManyResult{}, err
	}
	return e.expandMany(ctx, scope, req, nil)
}

// ExpandMany is Engine.ExpandMany over children and relations.
func (k *KnowledgeGraph) ExpandMany(ctx context.Context, scope Scope, req ExpandManyRequest) (ExpandManyResult, error) {
	return k.eng.expandMany(ctx, scope, req, k.neighbors)
}

func (e *Engine) expandMany(ctx context.Context, scope Scope, req ExpandManyRequest, walk walker) (res ExpandManyResult, err error) {
	if e := ctx.Err(); e != nil {
		err = e
		return res, err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return res, err
	}
	if len(req.ObjectIDs) == 0 {
		return res, nil
	}
	budget := req.NeighborBudget
	if budget <= 0 {
		budget = e.cfg.MaxResultSetSize
	}

	wantContainment, graphLabels := resolveExpandRelations(req.RelationTypes, req.WantContainment)

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
			hits, nErr := walk(ctx, scope, seed, graphLabels, req.MaxHops, req.Direction)
			if nErr != nil {
				return res, nErr
			}
			for _, n := range hits {
				ids = append(ids, n.ObjectID)
				relByID[n.ObjectID] = RelationFromNeighbor(n)
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
				return ExpandManyResult{Objects: out}, e.recordRead(ctx, scope, richIDs(out))
			}
		}
	}
	return ExpandManyResult{Objects: out}, e.recordRead(ctx, scope, richIDs(out))
}

// ExpandByRecipe looks up a host-registered ExpandRecipe and runs Expand with it.
func (e *Engine) ExpandByRecipe(ctx context.Context, scope Scope, objectID uuid.UUID, recipeName string, results ResultSetStore) (ExpandResult, error) {
	req, err := e.recipeRequest(objectID, recipeName)
	if err != nil {
		return ExpandResult{}, err
	}
	return e.Expand(ctx, scope, req, results)
}

// ExpandByRecipe is Engine.ExpandByRecipe over children and relations.
func (k *KnowledgeGraph) ExpandByRecipe(ctx context.Context, scope Scope, objectID uuid.UUID, recipeName string, results ResultSetStore) (ExpandResult, error) {
	req, err := k.eng.recipeRequest(objectID, recipeName)
	if err != nil {
		return ExpandResult{}, err
	}
	return k.Expand(ctx, scope, req, results)
}

// recipeRequest is the ExpandRequest of the recipe named recipeName.
func (e *Engine) recipeRequest(objectID uuid.UUID, recipeName string) (ExpandRequest, error) {
	r, ok := e.recipe(recipeName)
	if !ok {
		return ExpandRequest{}, fmt.Errorf("%w: expand recipe %q", ErrNotFound, strings.TrimSpace(recipeName))
	}
	return ExpandRequest{
		ObjectID:        objectID,
		RelationTypes:   r.RelationTypes,
		MaxHops:         r.MaxHops,
		Direction:       r.Direction,
		WantContainment: r.WantContainment,
	}, nil
}

// RegisterExpandRecipe adds or replaces a named expand view.
// Safe for concurrent use with ExpandByRecipe.
func (e *Engine) RegisterExpandRecipe(r ExpandRecipe) error {
	name := strings.TrimSpace(r.Name)
	if name == "" {
		return fmt.Errorf("%w: expand recipe name is required", ErrInvalid)
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

// neighbors walks labels up to maxHops (capped by MaxExpandHops) in one
// backend call. The backend walks only nodes scope covers; Engine still
// hydrates every id under scope.
func (k *KnowledgeGraph) neighbors(ctx context.Context, scope Scope, id uuid.UUID, labels []string, maxHops int, direction string) ([]GraphNeighbor, error) {
	e := k.eng
	hits, err := k.graph.Neighborhood(ctx, NeighborhoodQuery{
		Namespace:  scope.Namespace,
		Principals: scope.Principals,
		ObjectID:   id,
		Relations:  labels,
		Direction:  normalizeDirection(direction),
		MaxHops:    min(max(maxHops, 1), e.cfg.MaxExpandHops),
		Limit:      e.cfg.GraphNeighborK,
	})
	if err != nil {
		return nil, fmt.Errorf("brain: graph neighborhood: %w", err)
	}
	return hits, nil
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
