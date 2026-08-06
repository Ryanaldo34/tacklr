package brain

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// GraphNeighbor is one edge-adjacent object from the knowledge graph.
type GraphNeighbor struct {
	ObjectID     uuid.UUID
	RelationType string
	Direction    string // "out" | "in" | "both"
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
	// Call on every parent Put so nodes stay current (not a one-shot artifact).
	EnsureObject(ctx context.Context, obj Object) error
	// RemoveObject drops the node (and MemoryGraph edges) after Postgres soft-delete.
	RemoveObject(ctx context.Context, id uuid.UUID) error
	// AddEdge creates a directed edge from→to with the given relation type.
	AddEdge(ctx context.Context, from, to uuid.UUID, relationType string) error
}

// GraphObjectSearcher finds entity nodes by text and/or vector (Helix native indexes
// or MemoryGraph in-process). Results are ranked best-first; Engine hydrates under Scope.
// Optional namespace isolates multi-tenant graphs when the backend supports it.
type GraphObjectSearcher interface {
	SearchText(ctx context.Context, query string, limit int, namespace *uuid.UUID) ([]ScoredID, error)
	SearchVector(ctx context.Context, embedding []float32, limit int, namespace *uuid.UUID) ([]ScoredID, error)
}

// MemoryGraph is an in-process GraphReader/GraphWriter/GraphObjectSearcher (tests / offline).
type MemoryGraph struct {
	mu    sync.RWMutex
	out   map[uuid.UUID]map[string][]uuid.UUID // from → type → tos
	in    map[uuid.UUID]map[string][]uuid.UUID // to → type → froms
	nodes map[uuid.UUID]memGraphNode
}

type memGraphNode struct {
	kind       string
	title      string
	summary    string
	searchText string
	namespace  uuid.UUID
	embedding  []float32
	updatedAt  time.Time
}

// NewMemoryGraph returns an empty graph.
func NewMemoryGraph() *MemoryGraph {
	return &MemoryGraph{
		out:   make(map[uuid.UUID]map[string][]uuid.UUID),
		in:    make(map[uuid.UUID]map[string][]uuid.UUID),
		nodes: make(map[uuid.UUID]memGraphNode),
	}
}

// EnsureObject implements GraphWriter and stores searchable props for FindObjects.
// Replaces any prior node for the same id (live update, not a static snapshot).
func (g *MemoryGraph) EnsureObject(_ context.Context, obj Object) error {
	if obj.ID == uuid.Nil {
		return fmt.Errorf("brain: object id is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n := memGraphNode{
		kind:       obj.Kind,
		title:      obj.Title,
		summary:    obj.Summary,
		searchText: EntityIndexText(obj),
		namespace:  obj.NamespaceID,
		updatedAt:  obj.UpdatedAt,
	}
	if len(obj.Embedding) > 0 {
		n.embedding = slices.Clone(obj.Embedding)
	}
	g.nodes[obj.ID] = n
	return nil
}

// RemoveObject implements GraphWriter.
func (g *MemoryGraph) RemoveObject(_ context.Context, id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("brain: object id is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.nodes, id)
	// Drop edges involving id.
	for from, byRel := range g.out {
		for rel, tos := range byRel {
			g.out[from][rel] = slices.DeleteFunc(tos, func(to uuid.UUID) bool { return to == id })
		}
		if from == id {
			delete(g.out, from)
		}
	}
	for to, byRel := range g.in {
		for rel, froms := range byRel {
			g.in[to][rel] = slices.DeleteFunc(froms, func(from uuid.UUID) bool { return from == id })
		}
		if to == id {
			delete(g.in, to)
		}
	}
	delete(g.out, id)
	delete(g.in, id)
	return nil
}

// SearchText implements GraphObjectSearcher (case-fold substring on entity index text).
func (g *MemoryGraph) SearchText(_ context.Context, query string, limit int, namespace *uuid.UUID) ([]ScoredID, error) {
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" || limit <= 0 {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	var scored []ScoredID
	for id, n := range g.nodes {
		if namespace != nil && n.namespace != *namespace {
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

// SearchVector implements GraphObjectSearcher via cosine similarity on stored embeddings.
func (g *MemoryGraph) SearchVector(_ context.Context, embedding []float32, limit int, namespace *uuid.UUID) ([]ScoredID, error) {
	if len(embedding) == 0 || limit <= 0 {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	var scored []ScoredID
	for id, n := range g.nodes {
		if namespace != nil && n.namespace != *namespace {
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

// AddEdge implements GraphWriter.
func (g *MemoryGraph) AddEdge(_ context.Context, from, to uuid.UUID, relationType string) error {
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("brain: from, to, and relation type are required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.out[from] == nil {
		g.out[from] = make(map[string][]uuid.UUID)
	}
	g.out[from][rel] = append(g.out[from][rel], to)
	if g.in[to] == nil {
		g.in[to] = make(map[string][]uuid.UUID)
	}
	g.in[to][rel] = append(g.in[to][rel], from)
	return nil
}

// Neighbors implements GraphReader (both directions, deduped by object id).
func (g *MemoryGraph) Neighbors(_ context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]GraphNeighbor, error) {
	if objectID == uuid.Nil || limit <= 0 {
		return nil, nil
	}
	types := normalizeRelationList(relationTypes)
	g.mu.RLock()
	defer g.mu.RUnlock()

	seen := map[uuid.UUID]struct{}{objectID: {}}
	out := make([]GraphNeighbor, 0, limit)
	for _, rel := range types {
		for _, id := range g.out[objectID][rel] {
			if _, ok := seen[id]; ok || len(out) >= limit {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, GraphNeighbor{ObjectID: id, RelationType: rel, Direction: "out"})
		}
		for _, id := range g.in[objectID][rel] {
			if _, ok := seen[id]; ok || len(out) >= limit {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, GraphNeighbor{ObjectID: id, RelationType: rel, Direction: "in"})
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func normalizeRelationList(rels []string) []string {
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
	return wantContainment, normalizeRelationList(graphLabels)
}

var (
	_ GraphWriter         = (*MemoryGraph)(nil)
	_ GraphObjectSearcher = (*MemoryGraph)(nil)
)
