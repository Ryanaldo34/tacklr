// Package helixgraph adapts the HelixDB Go SDK to brain.GraphReader / GraphWriter.
// Nodes use property object_id (UUID) matching objects.id. Neighbors use Both.
package helixgraph

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	helix "github.com/helixdb/helix-db/sdks/go"

	"github.com/ryanaldo34/tacklr/brain"
)

// Graph implements brain.GraphWriter via HelixDB.
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
func (g *Graph) EnsureObjectIndex(ctx context.Context) error {
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

// PutObject upserts a graph node with only object_id (legacy host helper).
func (g *Graph) PutObject(ctx context.Context, objectID uuid.UUID) error {
	return g.EnsureObject(ctx, brain.Object{ID: objectID})
}

// EnsureObject implements brain.GraphWriter: drop+insert node with searchable props.
func (g *Graph) EnsureObject(ctx context.Context, obj brain.Object) error {
	if obj.ID == uuid.Nil {
		return fmt.Errorf("helixgraph: object id is required")
	}
	q := helix.WriteQuery("brain_ensure_object")
	oid := q.ParamString("object_id", obj.ID.String())
	props := objectProps(oid, obj)
	req := q.
		VarAs("dropped", helix.G().
			NWhere(helix.SourceEq("object_id", oid)).
			Drop().
			Count()).
		VarAs("n", helix.G().AddN(NodeLabel, props)).
		Returning("n")
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: ensure object: %w", err)
	}
	return nil
}

func objectProps(oid helix.ParamRef, obj brain.Object) helix.Props {
	props := helix.Props{helix.Prop("object_id", oid)}
	if obj.Kind != "" {
		props = append(props, helix.Prop("kind", obj.Kind))
	}
	if obj.Title != "" {
		props = append(props, helix.Prop("title", obj.Title))
	}
	if obj.Summary != "" {
		props = append(props, helix.Prop("summary", obj.Summary))
	}
	if st := brain.IndexText(obj); st != "" {
		props = append(props, helix.Prop("search_text", st))
	}
	if obj.NamespaceID != uuid.Nil {
		props = append(props, helix.Prop("namespace_id", obj.NamespaceID.String()))
	}
	if !obj.CreatedAt.IsZero() {
		props = append(props, helix.Prop("created_at", obj.CreatedAt.UTC().Format(time.RFC3339Nano)))
	}
	if !obj.UpdatedAt.IsZero() {
		props = append(props, helix.Prop("updated_at", obj.UpdatedAt.UTC().Format(time.RFC3339Nano)))
	}
	if obj.ParentID != nil {
		props = append(props, helix.Prop("parent_id", obj.ParentID.String()))
	}
	if len(obj.Embedding) > 0 {
		props = append(props, helix.Prop("embedding", obj.Embedding))
	}
	return props
}

// AddEdge implements brain.GraphWriter.
func (g *Graph) AddEdge(ctx context.Context, from, to uuid.UUID, relationType string) error {
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

	var raw struct {
		Neighbors struct {
			Properties []neighborRow `json:"properties"`
		} `json:"neighbors"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: neighbors %q: %w", label, err)
	}

	ids := make([]uuid.UUID, 0, len(raw.Neighbors.Properties))
	for _, row := range raw.Neighbors.Properties {
		id, err := uuid.Parse(strings.TrimSpace(row.ObjectID))
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

var (
	_ brain.GraphReader = (*Graph)(nil)
	_ brain.GraphWriter = (*Graph)(nil)
)
