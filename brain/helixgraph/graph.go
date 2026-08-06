// Package helixgraph adapts the HelixDB Go SDK to brain.GraphReader / GraphWriter.
// Nodes use property object_id (UUID) matching objects.id.
// Neighbors use OutE/InE projections so edge metadata is returned with each hop.
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
	client      *helix.Client
	searchReady bool // true after successful Bootstrap / EnsureSearchIndexes
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

// Bootstrap prepares Helix for entity search (indexes) and marks the graph ready for find_objects.
// Call once at process start when using Helix. withNamespaceTenant is usually false
// (namespace is enforced on Engine hydrate; some Helix images reject tenant text indexes).
func (g *Graph) Bootstrap(ctx context.Context, withNamespaceTenant bool) error {
	if err := ctx.Err(); err != nil {
		g.searchReady = false
		return err
	}
	if err := g.EnsureSearchIndexes(ctx, withNamespaceTenant); err != nil {
		g.searchReady = false
		return err
	}
	g.searchReady = true
	return nil
}

// ObjectSearchReady reports whether Bootstrap (or EnsureSearchIndexes) succeeded.
func (g *Graph) ObjectSearchReady() bool { return g.searchReady }

// EnsureSearchIndexes creates equality + text + vector indexes for find_objects.
// Prefer Bootstrap, which also sets ObjectSearchReady.
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
	g.searchReady = true
	return nil
}

// RemoveObject implements brain.GraphWriter: drop graph nodes for this object_id.
func (g *Graph) RemoveObject(ctx context.Context, id uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return fmt.Errorf("helixgraph: object id is required")
	}
	q := helix.WriteQuery("brain_remove_object")
	oid := q.ParamString("object_id", id.String())
	req := q.
		VarAs("dropped", helix.G().
			NWhere(helix.SourceEq(PropObjectID, oid)).
			Drop().
			Count()).
		Returning()
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: remove object: %w", err)
	}
	return nil
}

// SearchText implements brain.GraphObjectSearcher via TextSearchNodes on search_text.
// Namespace is enforced on Engine hydrate (GetMany under Scope), not as a Helix tenant
// parameter: tenant-scoped text indexes are optional and image-dependent.
func (g *Graph) SearchText(ctx context.Context, query string, limit int, _ *uuid.UUID) ([]brain.ScoredID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

// EnsureObject implements brain.GraphWriter: insert if missing, else update props in place.
// Incident edges are preserved (never drop+recreate the node identity).
func (g *Graph) EnsureObject(ctx context.Context, obj brain.Object) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if obj.ID == uuid.Nil {
		return fmt.Errorf("helixgraph: object id is required")
	}
	exists, err := g.objectExists(ctx, obj.ID)
	if err != nil {
		return err
	}
	if exists {
		return g.updateObjectProps(ctx, obj)
	}
	return g.insertObject(ctx, obj)
}

func (g *Graph) objectExists(ctx context.Context, id uuid.UUID) (bool, error) {
	q := helix.ReadQuery("brain_object_exists")
	oid := q.ParamString("object_id", id.String())
	req := q.
		VarAs("n", helix.G().NWhere(helix.SourceEq(PropObjectID, oid)).Count()).
		Returning("n")
	var raw struct {
		N int64 `json:"n"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return false, fmt.Errorf("helixgraph: object exists: %w", err)
	}
	return raw.N > 0, nil
}

func (g *Graph) insertObject(ctx context.Context, obj brain.Object) error {
	q := helix.WriteQuery("brain_insert_object")
	oid := q.ParamString("object_id", obj.ID.String())
	props := objectProps(oid, obj)
	req := q.
		VarAs("n", helix.G().AddN(NodeLabel, props)).
		Returning("n")
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: insert object: %w", err)
	}
	return nil
}

func (g *Graph) updateObjectProps(ctx context.Context, obj brain.Object) error {
	q := helix.WriteQuery("brain_update_object")
	oid := q.ParamString("object_id", obj.ID.String())
	pairs := objectPropPairs(obj)
	if len(pairs) == 0 {
		return nil
	}
	trav := helix.G().NWhere(helix.SourceEq(PropObjectID, oid))
	for _, p := range pairs {
		trav = trav.SetProperty(p.name, p.value)
	}
	req := q.VarAs("n", trav.Count()).Returning("n")
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: update object: %w", err)
	}
	return nil
}

type namedProp struct {
	name  string
	value any
}

// objectPropPairs returns updatable node properties (excludes object_id identity).
func objectPropPairs(obj brain.Object) []namedProp {
	var props []namedProp
	if obj.Kind != "" {
		props = append(props, namedProp{"kind", obj.Kind})
	}
	if obj.Title != "" {
		props = append(props, namedProp{"title", obj.Title})
	}
	if obj.Summary != "" {
		props = append(props, namedProp{"summary", obj.Summary})
	}
	if st := brain.EntityIndexText(obj); st != "" {
		props = append(props, namedProp{PropSearchText, st})
	}
	if obj.NamespaceID != uuid.Nil {
		props = append(props, namedProp{PropNamespaceID, obj.NamespaceID.String()})
	}
	if !obj.CreatedAt.IsZero() {
		props = append(props, namedProp{"created_at", obj.CreatedAt.UTC().Format(time.RFC3339Nano)})
	}
	if !obj.UpdatedAt.IsZero() {
		props = append(props, namedProp{"updated_at", obj.UpdatedAt.UTC().Format(time.RFC3339Nano)})
	}
	if obj.ParentID != nil {
		props = append(props, namedProp{"parent_id", obj.ParentID.String()})
	}
	if len(obj.Embedding) > 0 {
		props = append(props, namedProp{PropEmbedding, obj.Embedding})
	}
	return props
}

func objectProps(oid helix.ParamRef, obj brain.Object) helix.Props {
	pairs := objectPropPairs(obj)
	props := make(helix.Props, 0, len(pairs)+1)
	props = append(props, helix.Prop(PropObjectID, oid))
	for _, p := range pairs {
		props = append(props, helix.Prop(p.name, p.value))
	}
	return props
}

// Edge property keys written on AddE (Phase A relationship metadata).
const (
	PropEdgeNote       = "note"
	PropEdgeStatus     = "status"
	PropEdgeRole       = "role"
	PropEdgeConfidence = "confidence"
	PropEdgeEvidenceID = "evidence_id"
	PropEdgeCreatedAt  = "created_at"
	PropEdgeUpdatedAt  = "updated_at"
)

// AddEdge implements brain.GraphWriter. Drops any existing labeled edge from→to, then adds with meta props.
func (g *Graph) AddEdge(ctx context.Context, from, to uuid.UUID, relationType string, meta brain.EdgeMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("helixgraph: from, to, and relation type are required")
	}
	now := time.Now().UTC()
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = now
	}
	if meta.UpdatedAt.IsZero() {
		meta.UpdatedAt = now
	}
	q := helix.WriteQuery("brain_add_edge")
	fromOID := q.ParamString("from_oid", from.String())
	toOID := q.ParamString("to_oid", to.String())
	props := edgeMetaProps(meta)
	req := q.
		VarAs("from", helix.G().NWhere(helix.SourceEq("object_id", fromOID))).
		VarAs("to", helix.G().NWhere(helix.SourceEq("object_id", toOID))).
		// Upsert: remove prior edge of this label between the endpoints, then insert.
		VarAs("dropped", helix.G().N(helix.NodeVar("from")).DropEdgeLabeled(helix.NodeVar("to"), rel).Count()).
		VarAs("e", helix.G().N(helix.NodeVar("from")).AddE(rel, helix.NodeVar("to"), props)).
		Returning("e")
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: add edge %q: %w", rel, err)
	}
	return nil
}

func edgeMetaProps(meta brain.EdgeMeta) helix.Props {
	props := helix.Props{}
	if meta.Note != "" {
		props = append(props, helix.Prop(PropEdgeNote, meta.Note))
	}
	if meta.Status != "" {
		props = append(props, helix.Prop(PropEdgeStatus, meta.Status))
	}
	if meta.Role != "" {
		props = append(props, helix.Prop(PropEdgeRole, meta.Role))
	}
	if meta.Confidence != 0 {
		props = append(props, helix.Prop(PropEdgeConfidence, meta.Confidence))
	}
	if meta.EvidenceID != nil && *meta.EvidenceID != uuid.Nil {
		props = append(props, helix.Prop(PropEdgeEvidenceID, meta.EvidenceID.String()))
	}
	if !meta.CreatedAt.IsZero() {
		props = append(props, helix.Prop(PropEdgeCreatedAt, meta.CreatedAt.UTC().Format(time.RFC3339Nano)))
	}
	if !meta.UpdatedAt.IsZero() {
		props = append(props, helix.Prop(PropEdgeUpdatedAt, meta.UpdatedAt.UTC().Format(time.RFC3339Nano)))
	}
	return props
}

type neighborRow struct {
	ObjectID   string   `json:"object_id"`
	Note       string   `json:"note,omitempty"`
	Status     string   `json:"status,omitempty"`
	Role       string   `json:"role,omitempty"`
	Confidence *float64 `json:"confidence,omitempty"`
	EvidenceID string   `json:"evidence_id,omitempty"`
}

// Neighbors implements brain.GraphReader via OutE/InE projections (neighbor id + edge meta).
// Each direction RPC is gated on ctx so cancellation stops the multi-query walk early.
func (g *Graph) Neighbors(ctx context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]brain.GraphNeighbor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if objectID == uuid.Nil {
		return nil, nil
	}
	labels := brain.NormalizeRelationTypes(relationTypes)
	if len(labels) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	var out []brain.GraphNeighbor
	seen := map[uuid.UUID]struct{}{objectID: {}}
	for _, label := range labels {
		for _, dir := range []string{"out", "in"} {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rows, err := g.neighborsForLabelDir(ctx, objectID, label, dir, limit)
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				id, meta, ok := parseNeighborRow(row)
				if !ok {
					continue
				}
				if _, exists := seen[id]; exists || len(out) >= limit {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, brain.GraphNeighbor{
					ObjectID:     id,
					RelationType: label,
					Direction:    dir,
					Meta:         meta,
				})
			}
			if len(out) >= limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func parseNeighborRow(row neighborRow) (uuid.UUID, brain.EdgeMeta, bool) {
	id, err := uuid.Parse(strings.TrimSpace(row.ObjectID))
	if err != nil {
		return uuid.Nil, brain.EdgeMeta{}, false
	}
	meta := brain.EdgeMeta{
		Note:   strings.TrimSpace(row.Note),
		Status: strings.TrimSpace(row.Status),
		Role:   strings.TrimSpace(row.Role),
	}
	if row.Confidence != nil {
		meta.Confidence = *row.Confidence
	}
	if eid := strings.TrimSpace(row.EvidenceID); eid != "" {
		if u, err := uuid.Parse(eid); err == nil {
			meta.EvidenceID = &u
		}
	}
	return id, meta, true
}

func (g *Graph) neighborsForLabelDir(ctx context.Context, objectID uuid.UUID, label, direction string, limit int) ([]neighborRow, error) {
	q := helix.ReadQuery("brain_expand_neighbors_" + direction)
	oid := q.ParamString("object_id", objectID.String())
	lim := q.ParamI64("limit", int64(limit))

	// Project edge props + endpoint object_id ($to for out, $from for in).
	// Fixed-size stack array avoids the double-slice append used previously.
	var endpoint helix.Projection
	if direction == "out" {
		endpoint = helix.ProjectToEndpoint(PropObjectID, PropObjectID)
	} else {
		endpoint = helix.ProjectFromEndpoint(PropObjectID, PropObjectID)
	}
	projections := []helix.Projection{
		endpoint,
		helix.ProjectProp(PropEdgeNote),
		helix.ProjectProp(PropEdgeStatus),
		helix.ProjectProp(PropEdgeRole),
		helix.ProjectProp(PropEdgeConfidence),
		helix.ProjectProp(PropEdgeEvidenceID),
	}
	var trav *helix.Traversal
	if direction == "out" {
		trav = helix.G().
			NWhere(helix.SourceEq(PropObjectID, oid)).
			OutE(label).
			Limit(lim).
			Project(projections...)
	} else {
		trav = helix.G().
			NWhere(helix.SourceEq(PropObjectID, oid)).
			InE(label).
			Limit(lim).
			Project(projections...)
	}

	req := q.VarAs("neighbors", trav).Returning("neighbors")

	var raw struct {
		Neighbors struct {
			Properties []neighborRow `json:"properties"`
		} `json:"neighbors"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: neighbors %s %q: %w", direction, label, err)
	}
	return raw.Neighbors.Properties, nil
}

var (
	_ brain.GraphReader         = (*Graph)(nil)
	_ brain.GraphWriter         = (*Graph)(nil)
	_ brain.GraphObjectSearcher = (*Graph)(nil)
)
