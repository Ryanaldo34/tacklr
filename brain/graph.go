package brain

import (
	"context"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// GraphNeighbor is one edge-adjacent object from the knowledge graph.
type GraphNeighbor struct {
	ObjectID     uuid.UUID
	RelationType string
	Direction    string // "out" | "in"
}

// GraphReader traverses non-containment relations (Helix or MemoryGraph).
// Namespace isolation is enforced by the Engine when hydrating via Store.Get.
type GraphReader interface {
	Neighbors(ctx context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]GraphNeighbor, error)
}

// MemoryGraph is an in-process GraphReader for tests and offline hosts.
// AddEdge seeds undirected adjacency (both directions stored).
type MemoryGraph struct {
	mu  sync.RWMutex
	out map[uuid.UUID]map[string][]uuid.UUID // from → type → tos
	in  map[uuid.UUID]map[string][]uuid.UUID // to → type → froms
}

// NewMemoryGraph returns an empty graph.
func NewMemoryGraph() *MemoryGraph {
	return &MemoryGraph{
		out: make(map[uuid.UUID]map[string][]uuid.UUID),
		in:  make(map[uuid.UUID]map[string][]uuid.UUID),
	}
}

// AddEdge records a directed edge from→to with relationType and mirrors reverse
// lookup so Neighbors can walk Both (out+in).
func (g *MemoryGraph) AddEdge(from, to uuid.UUID, relationType string) {
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return
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
}

// Neighbors implements GraphReader (Both directions, deduped by object id).
func (g *MemoryGraph) Neighbors(_ context.Context, objectID uuid.UUID, relationTypes []string, limit int) ([]GraphNeighbor, error) {
	if objectID == uuid.Nil || limit <= 0 {
		return nil, nil
	}
	types := normalizeRelationList(relationTypes)
	g.mu.RLock()
	defer g.mu.RUnlock()

	seen := map[uuid.UUID]struct{}{objectID: {}}
	var out []GraphNeighbor
	add := func(id uuid.UUID, rel, dir string) {
		if _, ok := seen[id]; ok || len(out) >= limit {
			return
		}
		seen[id] = struct{}{}
		out = append(out, GraphNeighbor{ObjectID: id, RelationType: rel, Direction: dir})
	}
	for _, rel := range types {
		for _, id := range g.out[objectID][rel] {
			add(id, rel, "out")
		}
		for _, id := range g.in[objectID][rel] {
			add(id, rel, "in")
		}
	}
	return out, nil
}

func normalizeRelationList(rels []string) []string {
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

// IsContainmentRelation reports whether rel is a Postgres containment alias.
// Only the documented aliases: contains, part_of (and partof). Empty string is not a label.
func IsContainmentRelation(rel string) bool {
	switch strings.ToLower(strings.TrimSpace(rel)) {
	case "contains", "part_of", "partof":
		return true
	default:
		return false
	}
}

// SplitRelationTypes separates containment aliases from graph edge labels.
// Empty/omitted input means containment-only expand.
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
	// Explicit empty/whitespace-only list → containment default.
	if !wantContainment && len(graphLabels) == 0 {
		wantContainment = true
	}
	return wantContainment, normalizeRelationList(graphLabels)
}
