package brain

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// EdgeMeta is optional metadata on a non-containment relationship (why/how/when linked).
// Kept short and relational — full bodies stay on objects in Postgres.
type EdgeMeta struct {
	Note       string     `json:"note,omitempty"`
	Status     string     `json:"status,omitempty"`     // e.g. active, resolved
	Role       string     `json:"role,omitempty"`       // e.g. primary buyer vs cc
	Confidence float64    `json:"confidence,omitempty"` // 0 means unset; otherwise typically (0,1]
	EvidenceID *uuid.UUID `json:"evidence_id,omitempty"`
	CreatedAt  time.Time  `json:"created_at,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at,omitempty"`
}

// IsZero reports whether meta carries no meaningful fields.
func (m EdgeMeta) IsZero() bool {
	return m.Note == "" && m.Status == "" && m.Role == "" && m.Confidence == 0 &&
		m.EvidenceID == nil && m.CreatedAt.IsZero() && m.UpdatedAt.IsZero()
}

// GraphNeighbor is one edge-adjacent object from the knowledge graph.
type GraphNeighbor struct {
	ObjectID     uuid.UUID
	RelationType string
	Direction    string // "out" | "in"
	Meta         EdgeMeta
}

// GraphReader traverses non-containment relations. Engine hydrates ids under Scope.
type GraphReader interface {
	Neighbors(ctx context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]GraphNeighbor, error)
}

// GraphWriter persists graph nodes and non-containment edges (Helix dual-write / MemoryGraph).
// Embeds GraphReader so a single WithGraph value can satisfy both read and write.
type GraphWriter interface {
	GraphReader
	// EnsureObject upserts a graph node for obj.ID (searchable props when available).
	// Must preserve incident edges (update in place, not drop+recreate).
	EnsureObject(ctx context.Context, obj Object) error
	// RemoveObject drops the node (and incident edges) after/with Postgres soft-delete.
	RemoveObject(ctx context.Context, id uuid.UUID) error
	// AddEdge creates a directed edge from→to with optional relationship metadata.
	AddEdge(ctx context.Context, from, to uuid.UUID, relationType string, meta EdgeMeta) error
	// RemoveEdge drops the directed labeled edge from→to (idempotent if missing).
	RemoveEdge(ctx context.Context, from, to uuid.UUID, relationType string) error
}

// GraphObjectSearcher finds entity nodes by text and/or vector (Helix native indexes
// or MemoryGraph in-process). Results are ranked best-first; Engine hydrates under Scope.
// Namespace is applied by MemoryGraph via Covers; Helix returns unscoped candidates
// and Engine hydrates under Scope.
type GraphObjectSearcher interface {
	SearchText(ctx context.Context, query string, limit int, namespace Namespace) ([]ScoredID, error)
	SearchVector(ctx context.Context, embedding []float32, limit int, namespace Namespace) ([]ScoredID, error)
}

// EdgeSearchHit is one graph edge search result (endpoints + meta + score).
type EdgeSearchHit struct {
	FromID       uuid.UUID
	ToID         uuid.UUID
	RelationType string
	Meta         EdgeMeta
	Score        float64
}

// GraphEdgeSearcher finds edges by text (e.g. Helix TextSearchEdges on note).
type GraphEdgeSearcher interface {
	SearchEdgesText(ctx context.Context, relationType, query string, limit int) ([]EdgeSearchHit, error)
}

// edgeKey uniquely identifies a directed labeled edge.
type edgeKey struct {
	from, to uuid.UUID
	rel      string
}

var (
	_ GraphReader         = (*MemoryGraph)(nil)
	_ GraphWriter         = (*MemoryGraph)(nil)
	_ GraphObjectSearcher = (*MemoryGraph)(nil)
	_ GraphEdgeSearcher   = (*MemoryGraph)(nil)
)

// MemoryGraph is an in-process GraphReader/GraphWriter/GraphObjectSearcher (tests / offline).
// Edges are a single map; directions are derived on Neighbors.
type MemoryGraph struct {
	mu    sync.RWMutex
	edges map[edgeKey]EdgeMeta
	nodes map[uuid.UUID]memGraphNode
}

type memGraphNode struct {
	kind       string
	title      string
	summary    string
	searchText string
	namespace  Namespace
	embedding  []float32
	updatedAt  time.Time
}

// NewMemoryGraph returns an empty graph.
func NewMemoryGraph() *MemoryGraph {
	return &MemoryGraph{
		edges: make(map[edgeKey]EdgeMeta),
		nodes: make(map[uuid.UUID]memGraphNode),
	}
}

// EnsureObject implements GraphWriter and stores searchable props for FindObjects.
// Replaces any prior node for the same id (live update; edges are independent).
func (g *MemoryGraph) EnsureObject(ctx context.Context, obj Object) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if obj.ID == uuid.Nil {
		return fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n := memGraphNode{
		kind:       obj.Kind,
		title:      obj.Title,
		summary:    obj.Summary,
		searchText: EntityIndexText(obj),
		namespace:  obj.Namespace.Clone(),
		updatedAt:  obj.UpdatedAt,
	}
	if len(obj.Embedding) > 0 {
		n.embedding = slices.Clone(obj.Embedding)
	}
	g.nodes[obj.ID] = n
	return nil
}

// RemoveObject implements GraphWriter.
func (g *MemoryGraph) RemoveObject(ctx context.Context, id uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.nodes, id)
	for k := range g.edges {
		if k.from == id || k.to == id {
			delete(g.edges, k)
		}
	}
	return nil
}

// SearchText implements GraphObjectSearcher (case-fold substring on entity index text).
func (g *MemoryGraph) SearchText(ctx context.Context, query string, limit int, namespace Namespace) ([]ScoredID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" || limit <= 0 {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	var scored []ScoredID
	for id, n := range g.nodes {
		if !namespace.Covers(n.namespace) {
			continue
		}
		text := strings.ToLower(n.searchText)
		if text == "" || !strings.Contains(text, q) {
			continue
		}
		score := 1.0
		if strings.Contains(strings.ToLower(n.title), q) {
			score = 2.0
		}
		scored = append(scored, ScoredID{ID: id, Score: score, UpdatedAt: n.updatedAt, Title: n.title})
	}
	sortScored(scored)
	if len(scored) > limit {
		scored = scored[:limit]
	}
	return scored, nil
}

// SearchEdgesText implements GraphEdgeSearcher (substring match on edge note).
func (g *MemoryGraph) SearchEdgesText(ctx context.Context, relationType, query string, limit int) ([]EdgeSearchHit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rel := strings.TrimSpace(relationType)
	q := strings.ToLower(strings.TrimSpace(query))
	if rel == "" || q == "" || limit <= 0 {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]EdgeSearchHit, 0, limit)
	for k, meta := range g.edges {
		if k.rel != rel {
			continue
		}
		if !strings.Contains(strings.ToLower(meta.Note), q) {
			continue
		}
		out = append(out, EdgeSearchHit{
			FromID: k.from, ToID: k.to, RelationType: rel, Meta: meta, Score: 1,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// SearchVector implements GraphObjectSearcher via cosine similarity on stored embeddings.
func (g *MemoryGraph) SearchVector(ctx context.Context, embedding []float32, limit int, namespace Namespace) ([]ScoredID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(embedding) == 0 || limit <= 0 {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	var scored []ScoredID
	for id, n := range g.nodes {
		if !namespace.Covers(n.namespace) {
			continue
		}
		if len(n.embedding) != len(embedding) {
			continue
		}
		sim := cosine(embedding, n.embedding)
		if sim <= 0 {
			continue
		}
		scored = append(scored, ScoredID{ID: id, Score: float64(sim), UpdatedAt: n.updatedAt, Title: n.title})
	}
	sortScored(scored)
	if len(scored) > limit {
		scored = scored[:limit]
	}
	return scored, nil
}

// AddEdge implements GraphWriter. Upserts the edge for (from, to, relationType).
func (g *MemoryGraph) AddEdge(ctx context.Context, from, to uuid.UUID, relationType string, meta EdgeMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("brain: from, to, and relation type are required")
	}
	now := time.Now().UTC()
	key := edgeKey{from: from, to: to, rel: rel}
	g.mu.Lock()
	defer g.mu.Unlock()
	if prev, ok := g.edges[key]; ok {
		if meta.CreatedAt.IsZero() {
			meta.CreatedAt = prev.CreatedAt
		}
		if meta.CreatedAt.IsZero() {
			meta.CreatedAt = now
		}
		meta.UpdatedAt = now
		g.edges[key] = meta
		return nil
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = now
	}
	if meta.UpdatedAt.IsZero() {
		meta.UpdatedAt = meta.CreatedAt
	}
	g.edges[key] = meta
	return nil
}

// RemoveEdge implements GraphWriter. Missing edges succeed (idempotent).
func (g *MemoryGraph) RemoveEdge(ctx context.Context, from, to uuid.UUID, relationType string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("brain: from, to, and relation type are required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.edges, edgeKey{from: from, to: to, rel: rel})
	return nil
}

// Neighbors implements GraphReader (both directions, deduped by object id).
// Single scan of the edge map, then ordered by request relation list / out-before-in.
func (g *MemoryGraph) Neighbors(ctx context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]GraphNeighbor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if objectID == uuid.Nil || limit <= 0 {
		return nil, nil
	}
	types := NormalizeRelationTypes(relationTypes)
	if len(types) == 0 {
		return nil, nil
	}
	wantOrd := make(map[string]int, len(types))
	for i, t := range types {
		wantOrd[t] = i
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	type hit struct {
		peer uuid.UUID
		dir  byte // 0 out, 1 in
		rel  string
		ord  int
		meta EdgeMeta
	}
	hits := make([]hit, 0, min(limit*2, len(g.edges)))
	for k, meta := range g.edges {
		ord, ok := wantOrd[k.rel]
		if !ok {
			continue
		}
		switch {
		case k.from == objectID:
			hits = append(hits, hit{peer: k.to, dir: 0, rel: k.rel, ord: ord, meta: meta})
		case k.to == objectID:
			hits = append(hits, hit{peer: k.from, dir: 1, rel: k.rel, ord: ord, meta: meta})
		}
	}
	slices.SortFunc(hits, func(a, b hit) int {
		if c := cmp.Compare(a.ord, b.ord); c != 0 {
			return c
		}
		return cmp.Compare(a.dir, b.dir)
	})

	seen := map[uuid.UUID]struct{}{objectID: {}}
	out := make([]GraphNeighbor, 0, min(limit, len(hits)))
	for _, h := range hits {
		if len(out) >= limit {
			break
		}
		if _, ok := seen[h.peer]; ok {
			continue
		}
		seen[h.peer] = struct{}{}
		dir := "out"
		if h.dir == 1 {
			dir = "in"
		}
		out = append(out, GraphNeighbor{
			ObjectID: h.peer, RelationType: h.rel, Direction: dir, Meta: h.meta,
		})
	}
	return out, nil
}

// NormalizeRelationTypes trims, drops empties, and dedupes labels (case-insensitive).
// Exported so backends (e.g. helixgraph) share one normalizer.
func NormalizeRelationTypes(rels []string) []string {
	out := make([]string, 0, len(rels))
	seen := make(map[string]struct{}, len(rels))
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

// IsContainmentRelation is true for contains / part_of (and partof).
func IsContainmentRelation(rel string) bool {
	switch strings.ToLower(strings.TrimSpace(rel)) {
	case "contains", "part_of", "partof":
		return true
	default:
		return false
	}
}

// SplitRelationTypes returns whether containment apply and remaining graph labels.
// Empty input means containment-only.
func SplitRelationTypes(rels []string) (wantContainment bool, graphLabels []string) {
	if len(rels) == 0 {
		return true, nil
	}
	for _, r := range rels {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if IsContainmentRelation(r) {
			wantContainment = true
			continue
		}
		graphLabels = append(graphLabels, r)
	}
	if !wantContainment && len(graphLabels) == 0 {
		wantContainment = true
	}
	return wantContainment, NormalizeRelationTypes(graphLabels)
}

var (
	_ GraphWriter         = (*MemoryGraph)(nil)
	_ GraphObjectSearcher = (*MemoryGraph)(nil)
	_ GraphEdgeSearcher   = (*MemoryGraph)(nil)
)
