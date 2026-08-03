// Package helixgraph adapts the HelixDB Go SDK to brain.GraphReader.
// Nodes use property object_id (UUID) matching objects.id. Neighbors use Both.
package helixgraph

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	helix "github.com/helixdb/helix-db/sdks/go"
	"github.com/ryanaldo34/tacklr/brain"
)

// Graph implements brain.GraphReader via HelixDB.
type Graph struct {
	client *helix.Client
}

// New builds a client. Empty baseURL defaults to http://localhost:6969.
func New(baseURL string, opts ...helix.ClientOption) (*Graph, error) {
	c, err := helix.NewClient(baseURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("helixgraph: client: %w", err)
	}
	return &Graph{client: c}, nil
}

// NewFromClient uses an existing SDK client.
func NewFromClient(client *helix.Client) (*Graph, error) {
	if client == nil {
		return nil, fmt.Errorf("helixgraph: client is required")
	}
	return &Graph{client: client}, nil
}

// Client returns the underlying SDK client (e.g. for host-side seeding).
func (g *Graph) Client() *helix.Client {
	if g == nil {
		return nil
	}
	return g.client
}

type neighborRow struct {
	ObjectID string `json:"object_id"`
}

type neighborsResponse struct {
	Neighbors []neighborRow `json:"neighbors"`
}

// Neighbors implements brain.GraphReader via Helix Both(label) traversal.
func (g *Graph) Neighbors(ctx context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]brain.GraphNeighbor, error) {
	if g == nil || g.client == nil {
		return nil, fmt.Errorf("helixgraph: not configured")
	}
	if objectID == uuid.Nil {
		return nil, nil
	}
	labels := normalizeLabels(relationTypes)
	if len(labels) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	var out []brain.GraphNeighbor
	seen := map[uuid.UUID]struct{}{objectID: {}}
	for _, label := range labels {
		rows, err := g.neighborsForLabel(ctx, objectID, label, limit)
		if err != nil {
			return nil, err
		}
		for _, id := range rows {
			if _, ok := seen[id]; ok || len(out) >= limit {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, brain.GraphNeighbor{
				ObjectID:     id,
				RelationType: label,
				Direction:    "both",
			})
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (g *Graph) neighborsForLabel(ctx context.Context, objectID uuid.UUID, label string, limit int) ([]uuid.UUID, error) {
	q := helix.ReadQuery("brain_expand_neighbors")
	oid := q.ParamString("object_id", objectID.String())
	lim := q.ParamI64("limit", int64(limit))

	req := q.
		VarAs("neighbors",
			helix.G().
				NWhere(helix.SourceEq("object_id", oid)).
				Both(label).
				Limit(lim).
				ValueMap("object_id"),
		).
		Returning("neighbors")

	var resp neighborsResponse
	if err := g.client.Exec(ctx, req, &resp); err != nil {
		return nil, fmt.Errorf("helixgraph: neighbors %q: %w", label, err)
	}

	var ids []uuid.UUID
	for _, row := range resp.Neighbors {
		s := strings.TrimSpace(row.ObjectID)
		if s == "" {
			continue
		}
		id, err := uuid.Parse(s)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func normalizeLabels(rels []string) []string {
	var out []string
	seen := map[string]struct{}{}
	for _, r := range rels {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		key := strings.ToLower(r)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, r)
	}
	return out
}

var _ brain.GraphReader = (*Graph)(nil)
