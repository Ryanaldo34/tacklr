package brain

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// FindLinksRequest searches graph edges by text (Helix edge text index or MemoryGraph).
type FindLinksRequest struct {
	RelationType string // required edge label (e.g. about, references)
	Query        string
	Limit        int
}

// LinkHit is one edge land result with hydrated endpoints when visible under scope.
type LinkHit struct {
	From         RichObject `json:"from"`
	To           RichObject `json:"to"`
	RelationType string     `json:"relation_type"`
	Meta         EdgeMeta   `json:"meta,omitempty"`
	Score        float64    `json:"score,omitempty"`
}

// FindLinksResult is the agent-facing edge search payload.
type FindLinksResult struct {
	Links []LinkHit `json:"links"`
}

// FindLinks lands on relationships via GraphEdgeSearcher, then hydrates endpoints under Scope.
func (e *Engine) FindLinks(ctx context.Context, scope Scope, req FindLinksRequest) (res FindLinksResult, err error) {
	ctx, span := e.observer.StartOp(ctx, OpFindLinks)
	defer func() { span.End(len(res.Links), DegradeNone, err) }()

	if e := ctx.Err(); e != nil {
		err = e
		return res, err
	}
	if e.graphE == nil {
		return res, fmt.Errorf("%w: graph edge search is not available", ErrUnsupported)
	}
	rel := strings.TrimSpace(req.RelationType)
	query := strings.TrimSpace(req.Query)
	if rel == "" || query == "" {
		return res, fmt.Errorf("%w: relation type and query are required", ErrInvalid)
	}
	limit := e.normalizeLimit(req.Limit)
	hits, err := e.graphE.SearchEdgesText(ctx, rel, query, max(limit*2, e.cfg.CandidateK))
	if err != nil {
		return res, err
	}
	if len(hits) == 0 {
		return res, nil
	}
	ids := make([]uuid.UUID, 0, len(hits)*2)
	for _, h := range hits {
		ids = append(ids, h.FromID, h.ToID)
	}
	objs, err := e.store.GetMany(ctx, scope, ids)
	if err != nil {
		return res, fmt.Errorf("brain: hydrate link endpoints: %w", err)
	}
	byID := make(map[uuid.UUID]Object, len(objs))
	for _, o := range objs {
		byID[o.ID] = o
	}
	out := make([]LinkHit, 0, min(limit, len(hits)))
	for _, h := range hits {
		from, okF := byID[h.FromID]
		to, okT := byID[h.ToID]
		if !okF || !okT {
			continue
		}
		out = append(out, LinkHit{
			From:         RichFromObject(from, false),
			To:           RichFromObject(to, false),
			RelationType: h.RelationType,
			Meta:         h.Meta,
			Score:        h.Score,
		})
		if len(out) >= limit {
			break
		}
	}
	res.Links = out
	return res, nil
}

// HasEdgeSearch reports whether FindLinks is available.
func (e *Engine) HasEdgeSearch() bool {
	return e.graphE != nil
}
