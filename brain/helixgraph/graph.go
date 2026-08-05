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

// PropSearchText / PropEmbedding are dual-written node properties used for native search.
const (
	PropObjectID    = "object_id"
	PropSearchText  = "search_text"
	PropEmbedding   = "embedding"
	PropNamespaceID = "namespace_id"
)

// EnsureObjectIndex creates an equality index on Object.object_id when missing.
func (g *Graph) EnsureObjectIndex(ctx context.Context) error {
	req := helix.WriteQuery("brain_ensure_object_id_index").
		VarAs("idx", helix.G().CreateIndexIfNotExists(
			helix.NodeEqualityIndex(NodeLabel, PropObjectID),
		)).
		Returning()
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: ensure object_id index: %w", err)
	}
	return nil
}

// EnsureSearchIndexes creates equality + text + vector indexes for find_objects.
// When withNamespaceTenant is true, text/vector indexes use namespace_id as tenant.
func (g *Graph) EnsureSearchIndexes(ctx context.Context, withNamespaceTenant bool) error {
	if err := g.EnsureObjectIndex(ctx); err != nil {
		return err
	}
	var textIdx, vecIdx helix.IndexSpec
	if withNamespaceTenant {
		textIdx = helix.NodeTextIndex(NodeLabel, PropSearchText, PropNamespaceID)
		vecIdx = helix.NodeVectorIndex(NodeLabel, PropEmbedding, PropNamespaceID)
	} else {
		textIdx = helix.NodeTextIndex(NodeLabel, PropSearchText)
		vecIdx = helix.NodeVectorIndex(NodeLabel, PropEmbedding)
	}
	req := helix.WriteQuery("brain_ensure_search_indexes").
		VarAs("text", helix.G().CreateIndexIfNotExists(textIdx)).
		VarAs("vec", helix.G().CreateIndexIfNotExists(vecIdx)).
		Returning()
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: ensure search indexes: %w", err)
	}
	return nil
}

// SearchText implements brain.GraphObjectSearcher via TextSearchNodes on search_text.
// Namespace is enforced on Engine hydrate (GetMany under Scope), not as a Helix tenant
// parameter: tenant-scoped text indexes are optional and image-dependent.
func (g *Graph) SearchText(ctx context.Context, query string, limit int, _ *uuid.UUID) ([]brain.ScoredID, error) {
	query = strings.TrimSpace(query)
	if query == "" || limit <= 0 {
		return nil, nil
	}
	q := helix.ReadQuery("brain_text_search_nodes")
	qt := q.ParamString("query", query)
	lim := q.ParamI64("limit", int64(limit))
	trav := helix.G().TextSearchNodes(NodeLabel, PropSearchText, qt, lim)
	req := q.VarAs("hits", trav.ValueMap(PropObjectID, "$distance")).Returning("hits")
	return g.execSearchHits(ctx, req, "text search")
}

// SearchVector implements brain.GraphObjectSearcher via VectorSearchNodes on embedding.
// Namespace isolation is applied by Engine hydrate under Scope (same as SearchText).
func (g *Graph) SearchVector(ctx context.Context, embedding []float32, limit int, _ *uuid.UUID) ([]brain.ScoredID, error) {
	if len(embedding) == 0 || limit <= 0 {
		return nil, nil
	}
	q := helix.ReadQuery("brain_vector_search_nodes")
	lim := q.ParamI64("limit", int64(limit))
	trav := helix.G().VectorSearchNodes(NodeLabel, PropEmbedding, embedding, lim)
	req := q.VarAs("hits", trav.ValueMap(PropObjectID, "$distance")).Returning("hits")
	return g.execSearchHits(ctx, req, "vector search")
}

type searchHitRow struct {
	ObjectID string   `json:"object_id"`
	Distance *float64 `json:"$distance"`
}

func (g *Graph) execSearchHits(ctx context.Context, req helix.Request, label string) ([]brain.ScoredID, error) {
	var raw struct {
		Hits struct {
			Properties []searchHitRow `json:"properties"`
		} `json:"hits"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: %s: %w", label, err)
	}
	out := make([]brain.ScoredID, 0, len(raw.Hits.Properties))
	for i, row := range raw.Hits.Properties {
		id, err := uuid.Parse(strings.TrimSpace(row.ObjectID))
		if err != nil {
			continue
		}
		score := float64(len(raw.Hits.Properties) - i) // preserve Helix order for RRF
		if row.Distance != nil {
			// Smaller distance is better; convert to a descending score.
			score = 1.0 / (1.0 + *row.Distance)
		}
		out = append(out, brain.ScoredID{ID: id, Score: score})
	}
	return out, nil
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
	_ brain.GraphReader         = (*Graph)(nil)
	_ brain.GraphWriter         = (*Graph)(nil)
	_ brain.GraphObjectSearcher = (*Graph)(nil)
)
