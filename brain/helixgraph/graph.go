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
	return g.client
}

// NodeLabel is the Helix node label used for knowledge objects.
const NodeLabel = "Object"

// EnsureObjectIndex creates an equality index on Object.object_id when missing.
// Safe to call once at host startup or in tests before seeding.
func (g *Graph) EnsureObjectIndex(ctx context.Context) error {
	if g.client == nil {
		return fmt.Errorf("helixgraph: not configured")
	}
	req := helix.WriteQuery("brain_ensure_object_id_index").
		VarAs("idx", helix.G().CreateIndexIfNotExists(
			helix.NodeEqualityIndex(NodeLabel, "object_id"),
		)).
		Returning()
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: ensure object_id index: %w", err)
	}
	return nil
}

// PutObject upserts a graph node with property object_id (objects.id).
// Hosts call this when dual-writing knowledge objects into Helix.
func (g *Graph) PutObject(ctx context.Context, objectID uuid.UUID) error {
	if g.client == nil {
		return fmt.Errorf("helixgraph: not configured")
	}
	if objectID == uuid.Nil {
		return fmt.Errorf("helixgraph: object id is required")
	}
	// Drop existing node(s) with this object_id, then insert one (idempotent enough for tests/hosts).
	q := helix.WriteQuery("brain_put_object")
	oid := q.ParamString("object_id", objectID.String())
	req := q.
		VarAs("dropped", helix.G().
			NWhere(helix.SourceEq("object_id", oid)).
			Drop().
			Count()).
		VarAs("n", helix.G().AddN(NodeLabel, helix.Props{
			helix.Prop("object_id", oid),
		})).
		Returning("n")
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: put object: %w", err)
	}
	return nil
}

// AddEdge creates a labeled edge from→to between nodes identified by object_id.
// Both endpoints must already exist (see PutObject).
func (g *Graph) AddEdge(ctx context.Context, from, to uuid.UUID, relationType string) error {
	if g.client == nil {
		return fmt.Errorf("helixgraph: not configured")
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("helixgraph: from, to, and relation type are required")
	}
	q := helix.WriteQuery("brain_add_edge")
	fromOID := q.ParamString("from_oid", from.String())
	toOID := q.ParamString("to_oid", to.String())
	req := q.
		VarAs("from", helix.G().NWhere(helix.SourceEq("object_id", fromOID))).
		VarAs("to", helix.G().NWhere(helix.SourceEq("object_id", toOID))).
		VarAs("e", helix.G().N(helix.NodeVar("from")).AddE(rel, helix.NodeVar("to"), helix.Props{})).
		Returning("e")
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: add edge %q: %w", rel, err)
	}
	return nil
}

type neighborRow struct {
	ObjectID string `json:"object_id"`
}

// Neighbors implements brain.GraphReader via Helix Both(label) traversal.
func (g *Graph) Neighbors(ctx context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]brain.GraphNeighbor, error) {
	if g.client == nil {
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

	// Helix ValueMap returns {"neighbors":{"properties":[{"object_id":"..."},...]}}.
	var raw struct {
		Neighbors struct {
			Properties []neighborRow `json:"properties"`
		} `json:"neighbors"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: neighbors %q: %w", label, err)
	}

	var ids []uuid.UUID
	for _, row := range raw.Neighbors.Properties {
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
