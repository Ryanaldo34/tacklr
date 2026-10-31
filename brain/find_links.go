package brain

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// FindLinksRequest searches graph edges by their notes; the graph ranks them
// its own way (text, and vectors when it embeds).
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

// FindLinks lands on relationships (Graph.FindLinks), then hydrates both
// endpoints under Scope and drops an edge with an end it cannot read.
func (k *KnowledgeGraph) FindLinks(ctx context.Context, scope Scope, req FindLinksRequest) (res FindLinksResult, err error) {
	e := k.eng
	if e := ctx.Err(); e != nil {
		err = e
		return res, err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return res, err
	}
	rel := strings.TrimSpace(req.RelationType)
	query := strings.TrimSpace(req.Query)
	if rel == "" || query == "" {
		return res, fmt.Errorf("%w: relation type and query are required", ErrInvalid)
	}
	limit := e.normalizeLimit(req.Limit)
	hits, err := k.graph.FindLinks(ctx, LinkSearch{
		Namespace:    scope.Namespace,
		Principals:   scope.Principals,
		RelationType: rel,
		Query:        query,
		Limit:        max(limit*2, e.cfg.CandidateK),
	})
	if err != nil {
		return res, fmt.Errorf("brain: edge search: %w", err)
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
	ids = ids[:0]
	for _, l := range out {
		ids = append(ids, l.From.ID, l.To.ID)
	}
	if err := e.recordRead(ctx, scope, uniqueUUIDs(ids)); err != nil {
		return res, err
	}
	res.Links = out
	return res, nil
}
