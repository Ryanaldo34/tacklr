// Package helixgraph adapts HelixDB to brain.GraphReader / GraphWriter / searchers.
//
// Host surface: New / NewFromClient, Bootstrap (preferred boot), EnsureSearchIndexes,
// EnsureEdgeTextIndex (required per relation label before find_links), Client
// (escape hatch for host seeding), ObjectSearchReady / TenantEnabled, and the
// GraphWriter / searcher methods used via brain.WithGraph.
//
// Helix owns topology, text/vector indexes, edge props, and $distance ranking;
// this package dual-writes props and runs native Helix queries for Engine hydrate.
// Node labels and property key strings are package-private.
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

// nodeLabel is the Helix node label for knowledge objects (not part of the public API).
const nodeLabel = "Object"

// Dual-written Helix property keys (package-private schema; hosts use Engine + Bootstrap).
const (
	propObjectID    = "object_id"
	propSearchText  = "search_text"
	propEmbedding   = "embedding"
	propNamespaceID = "namespace_id"
	propKind        = "kind"
	propTitle       = "title"
	propSummary     = "summary"
)

// Edge property keys written on AddE (relationship metadata Helix stores natively).
const (
	propEdgeNote       = "note"
	propEdgeStatus     = "status"
	propEdgeRole       = "role"
	propEdgeConfidence = "confidence"
	propEdgeEvidenceID = "evidence_id"
	propEdgeCreatedAt  = "created_at"
	propEdgeUpdatedAt  = "updated_at"
)

// ensureObjectIndex creates an equality index on Object.object_id when missing.
func (g *Graph) ensureObjectIndex(ctx context.Context) error {
	req := helix.WriteQuery("brain_ensure_object_id_index").
		VarAs("idx", helix.G().CreateIndexIfNotExists(
			helix.NodeEqualityIndex(nodeLabel, propObjectID),
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
	if err := g.ensureObjectIndex(ctx); err != nil {
		return err
	}
	var textIdx, vecIdx helix.IndexSpec
	if withNamespaceTenant {
		textIdx = helix.NodeTextIndex(nodeLabel, propSearchText, propNamespaceID)
		vecIdx = helix.NodeVectorIndex(nodeLabel, propEmbedding, propNamespaceID)
	} else {
		textIdx = helix.NodeTextIndex(nodeLabel, propSearchText)
		vecIdx = helix.NodeVectorIndex(nodeLabel, propEmbedding)
	}
	// Equality on kind supports Has/Where filters after search without app-side maps.
	kindIdx := helix.NodeEqualityIndex(nodeLabel, propKind)
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
			helix.EdgeTextIndex(rel, propEdgeNote),
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
		return brain.ErrObjectIDRequired
	}
	q := helix.WriteQuery("brain_remove_object")
	oid := q.ParamString("object_id", id.String())
	req := q.
		VarAs("dropped", helix.G().
			NWhere(helix.SourceEq(propObjectID, oid)).
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
	req := q.VarAs("hits", trav.ValueMap(propObjectID, "$distance")).Returning("hits")
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
	req := q.VarAs("hits", trav.ValueMap(propObjectID, "$distance")).Returning("hits")
	return g.execSearchHits(ctx, req, "vector search")
}

// SearchEdgesText implements brain.GraphEdgeSearcher via Helix TextSearchEdges on note.
// Requires EnsureEdgeTextIndex(rel) for that label.
//
// Live Helix returns edge props with internal $from/$to node ids (ProjectFromEndpoint
// on edges is not supported); we resolve those ids to object_id UUIDs.
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
	// EdgeProperties: note/status/role + $from/$to internal ids + $score.
	req := q.VarAs("hits", helix.G().TextSearchEdges(rel, propEdgeNote, qt, lim).EdgeProperties()).Returning("hits")
	var raw struct {
		Hits struct {
			Properties []edgePropRow `json:"properties"`
		} `json:"hits"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: edge text search %q: %w", rel, err)
	}
	rows := raw.Hits.Properties
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]uint64, 0, len(rows)*2)
	for _, row := range rows {
		if row.From != nil {
			ids = append(ids, *row.From)
		}
		if row.To != nil {
			ids = append(ids, *row.To)
		}
	}
	oidByInternal, err := g.resolveNodeObjectIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]brain.EdgeSearchHit, 0, len(rows))
	for i, row := range rows {
		if row.From == nil || row.To == nil {
			continue
		}
		from, okF := oidByInternal[*row.From]
		to, okT := oidByInternal[*row.To]
		if !okF || !okT {
			continue
		}
		score := float64(len(rows) - i)
		if row.Score != nil {
			score = *row.Score
		} else if row.Distance != nil {
			score = 1.0 / (1.0 + *row.Distance)
		}
		out = append(out, brain.EdgeSearchHit{
			FromID: from, ToID: to, RelationType: rel,
			Meta: edgeMetaFromPropRow(row), Score: score,
		})
	}
	return out, nil
}

func (g *Graph) textSearchTrav(query helix.ParamRef, lim helix.ParamRef, namespace *uuid.UUID) *helix.Traversal {
	if g.tenantEnabled && namespace != nil && *namespace != uuid.Nil {
		return helix.G().TextSearchNodes(nodeLabel, propSearchText, query, lim, namespace.String())
	}
	return helix.G().TextSearchNodes(nodeLabel, propSearchText, query, lim)
}

func (g *Graph) vectorSearchTrav(embedding []float32, lim helix.ParamRef, namespace *uuid.UUID) *helix.Traversal {
	if g.tenantEnabled && namespace != nil && *namespace != uuid.Nil {
		return helix.G().VectorSearchNodes(nodeLabel, propEmbedding, embedding, lim, namespace.String())
	}
	return helix.G().VectorSearchNodes(nodeLabel, propEmbedding, embedding, lim)
}

type searchHitRow struct {
	ObjectID string   `json:"object_id"`
	Distance *float64 `json:"$distance"`
}

// edgePropRow is Helix EdgeProperties / TextSearchEdges payload.
// $from/$to are internal node ids; resolve via resolveNodeObjectIDs.
type edgePropRow struct {
	From       *uint64  `json:"$from"`
	To         *uint64  `json:"$to"`
	Note       string   `json:"note,omitempty"`
	Status     string   `json:"status,omitempty"`
	Role       string   `json:"role,omitempty"`
	Confidence *float64 `json:"confidence,omitempty"`
	EvidenceID string   `json:"evidence_id,omitempty"`
	Score      *float64 `json:"$score,omitempty"`
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

func edgeMetaFromPropRow(row edgePropRow) brain.EdgeMeta {
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
	return meta
}

// resolveNodeObjectIDs maps Helix internal node ids → object_id UUIDs.
// ValueMap order matches NodeIDs input order on current Helix.
func (g *Graph) resolveNodeObjectIDs(ctx context.Context, internal []uint64) (map[uint64]uuid.UUID, error) {
	if len(internal) == 0 {
		return nil, nil
	}
	seen := make(map[uint64]struct{}, len(internal))
	ids := make([]uint64, 0, len(internal))
	for _, id := range internal {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	q := helix.ReadQuery("brain_resolve_node_object_ids")
	req := q.VarAs("nodes", helix.G().N(helix.NodeIDs(ids...)).ValueMap(propObjectID)).Returning("nodes")
	var raw struct {
		Nodes struct {
			Properties []struct {
				ObjectID string `json:"object_id"`
			} `json:"properties"`
		} `json:"nodes"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: resolve node object ids: %w", err)
	}
	out := make(map[uint64]uuid.UUID, len(ids))
	for i, row := range raw.Nodes.Properties {
		if i >= len(ids) {
			break
		}
		oid, err := uuid.Parse(strings.TrimSpace(row.ObjectID))
		if err != nil {
			continue
		}
		out[ids[i]] = oid
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
		return brain.ErrObjectIDRequired
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
	// Live Helix returns {"n":{"count":N}} (not a bare int).
	req := q.
		VarAs("n", helix.G().NWhere(helix.SourceEq(propObjectID, oid)).Count()).
		Returning("n")
	var raw struct {
		N struct {
			Count int64 `json:"count"`
		} `json:"n"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return false, fmt.Errorf("helixgraph: object exists: %w", err)
	}
	return raw.N.Count > 0, nil
}

func (g *Graph) insertObject(ctx context.Context, obj brain.Object) error {
	q := helix.WriteQuery("brain_insert_object")
	oid := q.ParamString("object_id", obj.ID.String())
	props := objectProps(oid, obj)
	req := q.
		VarAs("n", helix.G().AddN(nodeLabel, props)).
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
	trav := helix.G().NWhere(helix.SourceEq(propObjectID, oid))
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
		props = append(props, namedProp{propKind, obj.Kind})
	}
	if obj.Title != "" {
		props = append(props, namedProp{propTitle, obj.Title})
	}
	if obj.Summary != "" {
		props = append(props, namedProp{propSummary, obj.Summary})
	}
	if st := brain.EntityIndexText(obj); st != "" {
		props = append(props, namedProp{propSearchText, st})
	}
	if obj.NamespaceID != uuid.Nil {
		props = append(props, namedProp{propNamespaceID, obj.NamespaceID.String()})
	}
	if !obj.CreatedAt.IsZero() {
		props = append(props, namedProp{"created_at", obj.CreatedAt.UTC().Format(time.RFC3339Nano)})
	}
	if !obj.UpdatedAt.IsZero() {
		props = append(props, namedProp{"updated_at", obj.UpdatedAt.UTC().Format(time.RFC3339Nano)})
	}
	if len(obj.Embedding) > 0 {
		props = append(props, namedProp{propEmbedding, obj.Embedding})
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
	props = append(props, helix.Prop(propObjectID, oid))
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
		VarAs("from", helix.G().NWhere(helix.SourceEq(propObjectID, fromOID))).
		VarAs("to", helix.G().NWhere(helix.SourceEq(propObjectID, toOID))).
		VarAs("dropped", helix.G().N(helix.NodeVar("from")).DropEdgeLabeled(helix.NodeVar("to"), rel).Count()).
		VarAs("e", helix.G().N(helix.NodeVar("from")).AddE(rel, helix.NodeVar("to"), props)).
		Returning("e")
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: add edge %q: %w", rel, err)
	}
	return nil
}

// RemoveEdge implements brain.GraphWriter via Helix DropEdgeLabeled.
func (g *Graph) RemoveEdge(ctx context.Context, from, to uuid.UUID, relationType string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("helixgraph: from, to, and relation type are required")
	}
	q := helix.WriteQuery("brain_remove_edge")
	fromOID := q.ParamString("from_oid", from.String())
	toOID := q.ParamString("to_oid", to.String())
	req := q.
		VarAs("from", helix.G().NWhere(helix.SourceEq(propObjectID, fromOID))).
		VarAs("to", helix.G().NWhere(helix.SourceEq(propObjectID, toOID))).
		VarAs("dropped", helix.G().N(helix.NodeVar("from")).DropEdgeLabeled(helix.NodeVar("to"), rel).Count()).
		Returning()
	if err := g.client.Exec(ctx, req, nil, helix.WriterOnly()); err != nil {
		return fmt.Errorf("helixgraph: remove edge %q: %w", rel, err)
	}
	return nil
}

func edgeMetaProps(meta brain.EdgeMeta) helix.Props {
	props := helix.Props{}
	if meta.Note != "" {
		props = append(props, helix.Prop(propEdgeNote, meta.Note))
	}
	if meta.Status != "" {
		props = append(props, helix.Prop(propEdgeStatus, meta.Status))
	}
	if meta.Role != "" {
		props = append(props, helix.Prop(propEdgeRole, meta.Role))
	}
	if meta.Confidence != 0 {
		props = append(props, helix.Prop(propEdgeConfidence, meta.Confidence))
	}
	if meta.EvidenceID != nil && *meta.EvidenceID != uuid.Nil {
		props = append(props, helix.Prop(propEdgeEvidenceID, meta.EvidenceID.String()))
	}
	if !meta.CreatedAt.IsZero() {
		props = append(props, helix.Prop(propEdgeCreatedAt, meta.CreatedAt.UTC().Format(time.RFC3339Nano)))
	}
	if !meta.UpdatedAt.IsZero() {
		props = append(props, helix.Prop(propEdgeUpdatedAt, meta.UpdatedAt.UTC().Format(time.RFC3339Nano)))
	}
	return props
}

// Neighbors walks OutE then InE per label via EdgeProperties (internal $from/$to),
// then resolves peers to object_id. ProjectFromEndpoint on edges is unsupported on
// current Helix images.
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
		for _, dir := range []string{"out", "in"} {
			if len(out) >= limit {
				return out, nil
			}
			rows, err := g.neighborEdgeProps(ctx, objectID, label, dir, limit)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 {
				continue
			}
			peerInternals := make([]uint64, 0, len(rows))
			for _, row := range rows {
				var peer *uint64
				if dir == "out" {
					peer = row.To
				} else {
					peer = row.From
				}
				if peer != nil {
					peerInternals = append(peerInternals, *peer)
				}
			}
			oidByInternal, err := g.resolveNodeObjectIDs(ctx, peerInternals)
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				var peerInternal *uint64
				if dir == "out" {
					peerInternal = row.To
				} else {
					peerInternal = row.From
				}
				if peerInternal == nil {
					continue
				}
				peer, ok := oidByInternal[*peerInternal]
				if !ok || peer == uuid.Nil || peer == objectID {
					continue
				}
				if _, exists := seen[peer]; exists || len(out) >= limit {
					continue
				}
				seen[peer] = struct{}{}
				out = append(out, brain.GraphNeighbor{
					ObjectID: peer, RelationType: label, Direction: dir,
					Meta: edgeMetaFromPropRow(row),
				})
			}
		}
	}
	return out, nil
}

func (g *Graph) neighborEdgeProps(ctx context.Context, objectID uuid.UUID, label, dir string, limit int) ([]edgePropRow, error) {
	name := "brain_expand_neighbors_oute"
	if dir == "in" {
		name = "brain_expand_neighbors_ine"
	}
	q := helix.ReadQuery(name)
	oid := q.ParamString("object_id", objectID.String())
	lim := q.ParamI64("limit", int64(limit))
	base := helix.G().NWhere(helix.SourceEq(propObjectID, oid))
	var trav *helix.Traversal
	if dir == "in" {
		trav = base.InE(label).Limit(lim).EdgeProperties()
	} else {
		trav = base.OutE(label).Limit(lim).EdgeProperties()
	}
	req := q.VarAs("neighbors", trav).Returning("neighbors")
	var raw struct {
		Neighbors struct {
			Properties []edgePropRow `json:"properties"`
		} `json:"neighbors"`
	}
	if err := g.client.Exec(ctx, req, &raw); err != nil {
		return nil, fmt.Errorf("helixgraph: neighbors %s %q: %w", dir, label, err)
	}
	return raw.Neighbors.Properties, nil
}

var (
	_ brain.GraphReader         = (*Graph)(nil)
	_ brain.GraphWriter         = (*Graph)(nil)
	_ brain.GraphObjectSearcher = (*Graph)(nil)
	_ brain.GraphEdgeSearcher   = (*Graph)(nil)
)
