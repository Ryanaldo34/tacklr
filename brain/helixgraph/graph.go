// Package helixgraph adapts HelixDB to brain.GraphReader / GraphWriter / searchers.
// Helix owns topology, text/vector indexes, edge props, and $distance ranking;
// this package dual-writes props and runs native Helix queries for Engine hydrate.
package helixgraph

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	helix "github.com/helixdb/helix-db/sdks/go"

	"github.com/ryanaldo34/tacklr/brain"
)

// Graph implements brain.GraphWriter via HelixDB.
type Graph struct {
	client        *helix.Client
	searchReady   bool // true after successful Bootstrap / EnsureSearchIndexes
	tenantEnabled bool // true when indexes were created with namespace_id tenant
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
	PropKind        = "kind"
	PropTitle       = "title"
	PropSummary     = "summary"
)

// Edge property keys written on AddE (relationship metadata Helix stores natively).
const (
	PropEdgeNote       = "note"
	PropEdgeStatus     = "status"
	PropEdgeRole       = "role"
	PropEdgeConfidence = "confidence"
	PropEdgeEvidenceID = "evidence_id"
	PropEdgeCreatedAt  = "created_at"
	PropEdgeUpdatedAt  = "updated_at"
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

// Bootstrap prepares Helix native indexes for entity search and marks the graph ready.
// withNamespaceTenant enables Helix tenant-scoped text/vector indexes on namespace_id
// (prefer this when the Helix image supports it so Search* filters in-engine).
// Call once at process start when using Helix.
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

// TenantEnabled reports whether search indexes were created with a namespace tenant property.
func (g *Graph) TenantEnabled() bool { return g.tenantEnabled }

// EnsureSearchIndexes creates Helix native equality + text + vector indexes for find_objects.
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
	// Equality on kind supports Has/Where filters after search without app-side maps.
	kindIdx := helix.NodeEqualityIndex(NodeLabel, PropKind)
	req := helix.WriteQuery("brain_ensure_search_indexes").
		VarAs("text", helix.G().CreateIndexIfNotExists(textIdx)).
		VarAs("vec", helix.G().CreateIndexIfNotExists(vecIdx)).
		VarAs("kind", helix.G().CreateIndexIfNotExists(kindIdx)).
		Returning()
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: ensure search indexes: %w", err)
	}
	g.tenantEnabled = withNamespaceTenant
	g.searchReady = true
	return nil
}

// EnsureEdgeTextIndex creates a Helix EdgeText index on note for a relation label.
// Relation labels are dynamic (about, has_buyer, …); hosts call this for labels they search.
func (g *Graph) EnsureEdgeTextIndex(ctx context.Context, relationLabel string) error {
	rel := strings.TrimSpace(relationLabel)
	if rel == "" {
		return fmt.Errorf("helixgraph: relation label is required")
	}
	req := helix.WriteQuery("brain_ensure_edge_text_"+rel).
		VarAs("idx", helix.G().CreateIndexIfNotExists(
			helix.EdgeTextIndex(rel, PropEdgeNote),
		)).
		Returning()
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: ensure edge text index %q: %w", rel, err)
	}
	return nil
}

// RemoveObject implements brain.GraphWriter: drop graph nodes for this object_id.
// Helix drops incident edges with the node.
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

// SearchText implements brain.GraphObjectSearcher via Helix TextSearchNodes.
// When tenant indexes are enabled and namespace is set, Helix filters by tenant natively.
func (g *Graph) SearchText(ctx context.Context, query string, limit int, namespace *uuid.UUID) ([]brain.ScoredID, error) {
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
	trav := g.textSearchTrav(qt, lim, namespace)
	req := q.VarAs("hits", trav.ValueMap(PropObjectID, "$distance")).Returning("hits")
	return g.execSearchHits(ctx, req, "text search")
}

// SearchVector implements brain.GraphObjectSearcher via Helix VectorSearchNodes.
func (g *Graph) SearchVector(ctx context.Context, embedding []float32, limit int, namespace *uuid.UUID) ([]brain.ScoredID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(embedding) == 0 || limit <= 0 {
		return nil, nil
	}
	q := helix.ReadQuery("brain_vector_search_nodes")
	lim := q.ParamI64("limit", int64(limit))
	trav := g.vectorSearchTrav(embedding, lim, namespace)
	req := q.VarAs("hits", trav.ValueMap(PropObjectID, "$distance")).Returning("hits")
	return g.execSearchHits(ctx, req, "vector search")
}

// SearchEdgesText implements brain.GraphEdgeSearcher via Helix TextSearchEdges on note.
// Requires EnsureEdgeTextIndex(rel) for that label.
func (g *Graph) SearchEdgesText(ctx context.Context, relationLabel, query string, limit int) ([]brain.EdgeSearchHit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rel := strings.TrimSpace(relationLabel)
	query = strings.TrimSpace(query)
	if rel == "" || query == "" || limit <= 0 {
		return nil, nil
	}
	q := helix.ReadQuery("brain_text_search_edges")
	qt := q.ParamString("query", query)
	lim := q.ParamI64("limit", int64(limit))
	trav := helix.G().TextSearchEdges(rel, PropEdgeNote, qt, lim).Project(
		helix.ProjectFromEndpoint(PropObjectID, "from_id"),
		helix.ProjectToEndpoint(PropObjectID, "to_id"),
		helix.ProjectProp(PropEdgeNote),
		helix.ProjectProp(PropEdgeStatus),
		helix.ProjectProp(PropEdgeRole),
		helix.ProjectProp(PropEdgeConfidence),
		helix.ProjectProp(PropEdgeEvidenceID),
		helix.ProjectProp("$distance"),
	)
	req := q.VarAs("hits", trav).Returning("hits")
	var raw struct {
		Hits struct {
			Properties []edgeHitRow `json:"properties"`
		} `json:"hits"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: edge text search %q: %w", rel, err)
	}
	out := make([]brain.EdgeSearchHit, 0, len(raw.Hits.Properties))
	for i, row := range raw.Hits.Properties {
		h, ok := parseEdgeHitRow(row, rel, i, len(raw.Hits.Properties))
		if ok {
			out = append(out, h)
		}
	}
	return out, nil
}

func (g *Graph) textSearchTrav(query helix.ParamRef, lim helix.ParamRef, namespace *uuid.UUID) *helix.Traversal {
	if g.tenantEnabled && namespace != nil && *namespace != uuid.Nil {
		return helix.G().TextSearchNodes(NodeLabel, PropSearchText, query, lim, namespace.String())
	}
	return helix.G().TextSearchNodes(NodeLabel, PropSearchText, query, lim)
}

func (g *Graph) vectorSearchTrav(embedding []float32, lim helix.ParamRef, namespace *uuid.UUID) *helix.Traversal {
	if g.tenantEnabled && namespace != nil && *namespace != uuid.Nil {
		return helix.G().VectorSearchNodes(NodeLabel, PropEmbedding, embedding, lim, namespace.String())
	}
	return helix.G().VectorSearchNodes(NodeLabel, PropEmbedding, embedding, lim)
}

type searchHitRow struct {
	ObjectID string   `json:"object_id"`
	Distance *float64 `json:"$distance"`
}

type edgeHitRow struct {
	FromID     string   `json:"from_id"`
	ToID       string   `json:"to_id"`
	Note       string   `json:"note,omitempty"`
	Status     string   `json:"status,omitempty"`
	Role       string   `json:"role,omitempty"`
	Confidence *float64 `json:"confidence,omitempty"`
	EvidenceID string   `json:"evidence_id,omitempty"`
	Distance   *float64 `json:"$distance,omitempty"`
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
		// Prefer Helix $distance; fall back to order-preserving rank score for fusion.
		score := float64(len(raw.Hits.Properties) - i)
		if row.Distance != nil {
			score = 1.0 / (1.0 + *row.Distance)
		}
		out = append(out, brain.ScoredID{ID: id, Score: score})
	}
	return out, nil
}

func parseEdgeHitRow(row edgeHitRow, rel string, i, n int) (brain.EdgeSearchHit, bool) {
	from, err1 := uuid.Parse(strings.TrimSpace(row.FromID))
	to, err2 := uuid.Parse(strings.TrimSpace(row.ToID))
	if err1 != nil || err2 != nil {
		return brain.EdgeSearchHit{}, false
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
	score := float64(n - i)
	if row.Distance != nil {
		score = 1.0 / (1.0 + *row.Distance)
	}
	return brain.EdgeSearchHit{FromID: from, ToID: to, RelationType: rel, Meta: meta, Score: score}, true
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
	// Helix Count over the equality index is the portable existence probe.
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
		props = append(props, namedProp{PropKind, obj.Kind})
	}
	if obj.Title != "" {
		props = append(props, namedProp{PropTitle, obj.Title})
	}
	if obj.Summary != "" {
		props = append(props, namedProp{PropSummary, obj.Summary})
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
	if len(obj.Embedding) > 0 {
		props = append(props, namedProp{PropEmbedding, obj.Embedding})
	}
	for _, k := range sortedPropKeys(obj.Properties) {
		if v, ok := scalarPropValue(obj.Properties[k]); ok {
			props = append(props, namedProp{k, v})
		}
	}
	return props
}

func sortedPropKeys(props map[string]any) []string {
	if len(props) == 0 {
		return nil
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		if strings.TrimSpace(k) != "" {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

func scalarPropValue(v any) (any, bool) {
	switch x := v.(type) {
	case string:
		x = strings.TrimSpace(x)
		if x == "" {
			return nil, false
		}
		return x, true
	case bool:
		return x, true
	case float64, float32, int, int32, int64:
		return x, true
	default:
		return nil, false
	}
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

// AddEdge implements brain.GraphWriter via Helix DropEdgeLabeled + AddE (native upsert).
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
		VarAs("from", helix.G().NWhere(helix.SourceEq(PropObjectID, fromOID))).
		VarAs("to", helix.G().NWhere(helix.SourceEq(PropObjectID, toOID))).
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

// Neighbors uses Helix BothE (one RPC per relation label) for undirected hop discovery
// with edge property projection — not separate OutE/InE walks managed in-process.
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := g.neighborsBothE(ctx, objectID, label, limit)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			peer, dir, meta, ok := parseBothERow(row, objectID)
			if !ok {
				continue
			}
			if _, exists := seen[peer]; exists || len(out) >= limit {
				continue
			}
			seen[peer] = struct{}{}
			out = append(out, brain.GraphNeighbor{
				ObjectID:     peer,
				RelationType: label,
				Direction:    dir,
				Meta:         meta,
			})
		}
		if len(out) >= limit {
			return out, nil
		}
	}
	return out, nil
}

type bothERow struct {
	FromID     string   `json:"from_id"`
	ToID       string   `json:"to_id"`
	Note       string   `json:"note,omitempty"`
	Status     string   `json:"status,omitempty"`
	Role       string   `json:"role,omitempty"`
	Confidence *float64 `json:"confidence,omitempty"`
	EvidenceID string   `json:"evidence_id,omitempty"`
}

func (g *Graph) neighborsBothE(ctx context.Context, objectID uuid.UUID, label string, limit int) ([]bothERow, error) {
	q := helix.ReadQuery("brain_expand_neighbors_bothe")
	oid := q.ParamString("object_id", objectID.String())
	lim := q.ParamI64("limit", int64(limit))
	trav := helix.G().
		NWhere(helix.SourceEq(PropObjectID, oid)).
		BothE(label).
		Limit(lim).
		Project(
			helix.ProjectFromEndpoint(PropObjectID, "from_id"),
			helix.ProjectToEndpoint(PropObjectID, "to_id"),
			helix.ProjectProp(PropEdgeNote),
			helix.ProjectProp(PropEdgeStatus),
			helix.ProjectProp(PropEdgeRole),
			helix.ProjectProp(PropEdgeConfidence),
			helix.ProjectProp(PropEdgeEvidenceID),
		)
	req := q.VarAs("neighbors", trav).Returning("neighbors")
	var raw struct {
		Neighbors struct {
			Properties []bothERow `json:"properties"`
		} `json:"neighbors"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: neighbors BothE %q: %w", label, err)
	}
	return raw.Neighbors.Properties, nil
}

func parseBothERow(row bothERow, self uuid.UUID) (peer uuid.UUID, dir string, meta brain.EdgeMeta, ok bool) {
	from, err1 := uuid.Parse(strings.TrimSpace(row.FromID))
	to, err2 := uuid.Parse(strings.TrimSpace(row.ToID))
	if err1 != nil || err2 != nil {
		return uuid.Nil, "", brain.EdgeMeta{}, false
	}
	switch {
	case from == self:
		peer, dir = to, "out"
	case to == self:
		peer, dir = from, "in"
	default:
		return uuid.Nil, "", brain.EdgeMeta{}, false
	}
	if peer == uuid.Nil || peer == self {
		return uuid.Nil, "", brain.EdgeMeta{}, false
	}
	meta = brain.EdgeMeta{
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
	return peer, dir, meta, true
}

var (
	_ brain.GraphReader         = (*Graph)(nil)
	_ brain.GraphWriter         = (*Graph)(nil)
	_ brain.GraphObjectSearcher = (*Graph)(nil)
	_ brain.GraphEdgeSearcher   = (*Graph)(nil)
)
