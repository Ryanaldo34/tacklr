package brain

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// PathRequest asks for the fewest-hop path between two objects.
type PathRequest struct {
	From, To     uuid.UUID
	RelationType string // optional; empty follows every relation
	Direction    string // out | in | both (default both)
	MaxDepth     int    // default and cap MaxPathDepth
}

// PathResult is the path, From first and To last.
type PathResult struct {
	Objects []RichObject `json:"objects"`
}

// ShortestPath finds the fewest-hop path From→To through objects the scope can
// read. No such path within MaxDepth is ErrNotFound.
func (k *KnowledgeGraph) ShortestPath(ctx context.Context, scope Scope, req PathRequest) (PathResult, error) {
	e := k.eng
	if err := ctx.Err(); err != nil {
		return PathResult{}, err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return PathResult{}, err
	}
	if req.From == uuid.Nil || req.To == uuid.Nil {
		return PathResult{}, fmt.Errorf("%w: from and to are required", ErrInvalid)
	}
	depth := e.cfg.MaxPathDepth
	if req.MaxDepth > 0 {
		depth = min(req.MaxDepth, depth)
	}
	ids, err := k.graph.ShortestPath(ctx, PathQuery{
		Namespace:    scope.Namespace,
		Principals:   scope.Principals,
		From:         req.From,
		To:           req.To,
		RelationType: strings.TrimSpace(req.RelationType),
		Direction:    normalizeDirection(req.Direction),
		MaxDepth:     depth,
	})
	if err != nil {
		return PathResult{}, fmt.Errorf("brain: shortest path: %w", err)
	}
	objs, err := e.hydrateIDs(ctx, scope, ids)
	if err != nil {
		return PathResult{}, err
	}
	if len(ids) == 0 || len(objs) != len(ids) {
		return PathResult{}, fmt.Errorf("%w: no visible path within %d hops", ErrNotFound, depth)
	}
	if err := e.recordRead(ctx, scope, ids); err != nil {
		return PathResult{}, err
	}
	return PathResult{Objects: objs}, nil
}
