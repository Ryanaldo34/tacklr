package brain

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MemoryGraph is an in-process Graph for tests, fixtures and offline hosts.
// It embeds node text and the notes of EdgeRelations with its Embedding
// (none means text search only) and fuses its lists by reciprocal rank.
type MemoryGraph struct {
	opts  MemoryGraphOptions
	mu    sync.RWMutex
	nodes map[uuid.UUID]memoryNode
	edges map[memoryEdgeKey]memoryEdge
}

// MemoryGraphOptions configures NewMemoryGraph.
type MemoryGraphOptions struct {
	// Embedding embeds node text, the notes of EdgeRelations and queries.
	Embedding Embedding
	// EdgeRelations are the relations whose edges FindLinks ranks; a
	// FindLinks without a relation ranks them all.
	EdgeRelations []string
}

type memoryNode struct {
	obj  Object
	text string
	vec  []float32
}

type memoryEdgeKey struct {
	from, to uuid.UUID
	rel      string
}

type memoryEdge struct {
	id  uuid.UUID
	ns  Namespace
	key memoryEdgeKey
	vec []float32
	GraphNeighbor
}

// NewMemoryGraph returns an empty memory-backed graph.
func NewMemoryGraph(opts MemoryGraphOptions) *MemoryGraph {
	return &MemoryGraph{opts: opts, nodes: map[uuid.UUID]memoryNode{}, edges: map[memoryEdgeKey]memoryEdge{}}
}

// Put implements Graph. It fails when obj.Text does not embed.
func (g *MemoryGraph) Put(ctx context.Context, in Indexed) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	vec, err := g.opts.Embedding.EmbedOne(ctx, in.Text)
	if err != nil {
		return fmt.Errorf("brain: embed node: %w", err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nodes[in.ID] = memoryNode{obj: in.clone(), text: in.Text, vec: vec}
	return nil
}

// Remove implements Graph.
func (g *MemoryGraph) Remove(ctx context.Context, id uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeLocked(id)
	return nil
}

func (g *MemoryGraph) removeLocked(id uuid.UUID) {
	delete(g.nodes, id)
	for k := range g.edges {
		if k.from == id || k.to == id {
			delete(g.edges, k)
		}
	}
}

// Erase implements Graph.
func (g *MemoryGraph) Erase(_ context.Context, scope Scope) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, n := range g.nodes {
		if scope.Namespace.Covers(n.obj.Namespace) {
			g.removeLocked(id)
		}
	}
	return nil
}

// Link implements Graph. It embeds the note of an EdgeRelations edge.
func (g *MemoryGraph) Link(ctx context.Context, e GraphEdge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var vec []float32
	if slices.Contains(g.opts.EdgeRelations, e.RelationType) {
		var err error
		if vec, err = g.opts.Embedding.EmbedOne(ctx, e.Meta.Note); err != nil {
			return fmt.Errorf("brain: embed edge note: %w", err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, id := range []uuid.UUID{e.From, e.To} {
		if _, ok := g.nodes[id]; !ok {
			return fmt.Errorf("%w: node %s", ErrNotFound, id)
		}
	}
	key := memoryEdgeKey{e.From, e.To, e.RelationType}
	g.edges[key] = memoryEdge{id: uuid.New(), ns: e.Namespace.Clone(), key: key, vec: vec, GraphNeighbor: GraphNeighbor{RelationType: e.RelationType, Meta: e.Meta}}
	return nil
}

// Unlink implements Graph.
func (g *MemoryGraph) Unlink(ctx context.Context, from, to uuid.UUID, rel string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.edges, memoryEdgeKey{from, to, rel})
	return nil
}

// rank fuses the text and vector lists of docs, whose Content is the text
// a query matches. A query that does not embed ranks the text alone.
func (g *MemoryGraph) rank(ctx context.Context, docs []Object, query string) [][]ScoredID {
	lists := [][]ScoredID{lexicalScores(docs, query)}
	if vec, err := g.opts.Embedding.EmbedOne(ctx, query); err == nil && vec != nil {
		lists = append(lists, vectorScores(docs, g.opts.Embedding.Model(), vec))
	}
	return lists
}

// FindObjects implements Graph.
func (g *MemoryGraph) FindObjects(ctx context.Context, q ObjectSearch) ([]ScoredID, error) {
	plan, err := q.Filters.Compile()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scope := Scope{Namespace: q.Namespace, Principals: q.Principals}
	g.mu.RLock()
	var docs []Object
	for _, n := range g.nodes {
		if scope.CanRead(n.obj) && plan.Match(n.obj) && amongIDs(q.IDs, n.obj.ID) {
			docs = append(docs, Object{ID: n.obj.ID, UpdatedAt: n.obj.UpdatedAt, Content: n.text, Embedding: n.vec, EmbeddingModel: g.opts.Embedding.Model()})
		}
	}
	g.mu.RUnlock()
	hits := Ranking{}.Fuse(g.rank(ctx, docs, q.Query)...)
	for i := range hits {
		hits[i].Content = ""
	}
	return hits[:min(len(hits), max(q.Limit, 0))], nil
}

// FindLinks implements Graph.
func (g *MemoryGraph) FindLinks(ctx context.Context, q LinkSearch) ([]EdgeSearchHit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rels := g.opts.EdgeRelations
	if q.RelationType != "" {
		rels = []string{q.RelationType}
	}
	scope := Scope{Namespace: q.Namespace, Principals: q.Principals}
	g.mu.RLock()
	byID := map[uuid.UUID]memoryEdge{}
	var docs []Object
	for _, e := range g.edges {
		if slices.Contains(g.opts.EdgeRelations, e.RelationType) && slices.Contains(rels, e.RelationType) && q.Namespace.Covers(e.ns) &&
			scope.CanRead(g.nodes[e.key.from].obj) && scope.CanRead(g.nodes[e.key.to].obj) {
			byID[e.id] = e
			docs = append(docs, Object{ID: e.id, Content: e.Meta.Note, Embedding: e.vec, EmbeddingModel: g.opts.Embedding.Model()})
		}
	}
	g.mu.RUnlock()
	var lists [][]EdgeSearchHit
	for _, list := range g.rank(ctx, docs, q.Query) {
		hits := make([]EdgeSearchHit, len(list))
		for i, s := range list {
			e := byID[s.ID]
			hits[i] = EdgeSearchHit{FromID: e.key.from, ToID: e.key.to, RelationType: e.RelationType, Meta: e.Meta, Score: s.Score}
		}
		lists = append(lists, hits)
	}
	hits := Ranking{}.FuseEdges(lists...)
	return hits[:min(len(hits), max(q.Limit, 0))], nil
}

// memoryHop is the nodes one hop of a walk reached, each with the edges
// from the previous hop's frontier that reached it, and the frontier the
// next hop starts from.
type memoryHop struct {
	reached  map[uuid.UUID][]GraphNeighbor
	frontier []uuid.UUID
	over     bool
}

// walk runs w hop by hop under the read lock. Each hop keeps the readable
// nodes no earlier hop reached; its frontier is the first FrontierK of
// them by object id.
func (g *MemoryGraph) walk(w Walk) []memoryHop {
	scope := Scope{Namespace: w.Namespace, Principals: w.Principals}
	seen := map[uuid.UUID]bool{w.ObjectID: true}
	frontier := []uuid.UUID{w.ObjectID}
	var hops []memoryHop
	for range w.MaxHops {
		hop := memoryHop{reached: map[uuid.UUID][]GraphNeighbor{}}
		for _, e := range g.edges {
			if (len(w.Relations) > 0 && !slices.Contains(w.Relations, e.RelationType)) || !edgePasses(e.Meta, w.Edges) {
				continue
			}
			for _, side := range []struct {
				dir      string
				src, dst uuid.UUID
			}{{DirectionOut, e.key.from, e.key.to}, {DirectionIn, e.key.to, e.key.from}} {
				if (w.Direction == DirectionOut || w.Direction == DirectionIn) && w.Direction != side.dir {
					continue
				}
				dst, ok := g.nodes[side.dst]
				if !slices.Contains(frontier, side.src) || seen[side.dst] || !ok || !scope.CanRead(dst.obj) {
					continue
				}
				n := e.GraphNeighbor
				n.ObjectID, n.SourceID, n.Direction, n.Depth = side.dst, side.src, side.dir, len(hops)+1
				hop.reached[side.dst] = append(hop.reached[side.dst], n)
			}
		}
		for id := range hop.reached {
			seen[id] = true
			hop.frontier = append(hop.frontier, id)
		}
		slices.SortFunc(hop.frontier, cmpUUID)
		hop.over = len(hop.frontier) > w.FrontierK
		hop.frontier = hop.frontier[:min(len(hop.frontier), w.FrontierK)]
		frontier = hop.frontier
		hops = append(hops, hop)
	}
	return hops
}

func edgePasses(m EdgeMeta, f EdgeFilter) bool {
	return (f.Status == "" || m.Status == f.Status) && (f.Role == "" || m.Role == f.Role) &&
		m.Confidence >= f.MinConfidence && (f.UpdatedAfter.IsZero() || !m.UpdatedAt.Before(f.UpdatedAfter))
}

// firstEdge is the edge a node is reported with: the first relation in
// rels (by name past them), outbound first, then the smallest source id.
func firstEdge(edges []GraphNeighbor, rels []string) GraphNeighbor {
	rank := func(n GraphNeighbor) int {
		if i := slices.Index(rels, n.RelationType); i >= 0 {
			return i
		}
		return len(rels)
	}
	return slices.MinFunc(edges, func(a, b GraphNeighbor) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), strings.Compare(a.RelationType, b.RelationType),
			-strings.Compare(a.Direction, b.Direction), cmpUUID(a.SourceID, b.SourceID))
	})
}

// memoryCursor is the key of the last row of a page.
type memoryCursor struct {
	Depth   int       `json:"d"`
	Created time.Time `json:"c"`
	ID      uuid.UUID `json:"id"`
}

// Neighborhood implements Graph.
func (g *MemoryGraph) Neighborhood(ctx context.Context, q NeighborhoodQuery) (NeighborPage, error) {
	after := memoryCursor{Depth: 1}
	if q.After != "" {
		if err := json.Unmarshal([]byte(q.After), &after); err != nil || after.Depth < 1 {
			return NeighborPage{}, fmt.Errorf("%w: walk cursor %q", ErrInvalid, q.After)
		}
	}
	plan, err := q.Filters.Compile()
	if err != nil {
		return NeighborPage{}, err
	}
	if err := ctx.Err(); err != nil {
		return NeighborPage{}, err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	hops := g.walk(q.Walk)
	var page NeighborPage
	if q.Count {
		page.Counts = &Counts{ByRelation: map[string]int{}}
	}
	type row struct {
		key memoryCursor
		n   GraphNeighbor
	}
	var rows []row
	for i, hop := range hops {
		page.Truncated = page.Truncated || (hop.over && i < len(hops)-1)
		var depth []row
		for id, edges := range hop.reached {
			obj := g.nodes[id].obj
			if !plan.Match(obj) {
				continue
			}
			if q.Count {
				page.Counts.Total++
				labels := map[string]bool{}
				for _, e := range edges {
					labels[e.RelationType] = true
				}
				for rel := range labels {
					page.Counts.ByRelation[rel]++
				}
			}
			key := memoryCursor{Depth: i + 1, Created: obj.CreatedAt, ID: id}
			if key.Depth < after.Depth || (key.Depth == after.Depth && q.After != "" && compareCursor(key, after) <= 0) {
				continue
			}
			depth = append(depth, row{key, firstEdge(edges, q.Relations)})
		}
		slices.SortFunc(depth, func(a, b row) int { return compareCursor(a.key, b.key) })
		rows = append(rows, depth...)
	}
	for _, r := range rows[:min(len(rows), q.Limit)] {
		page.Neighbors = append(page.Neighbors, r.n)
	}
	if len(rows) > q.Limit {
		next, _ := json.Marshal(rows[q.Limit-1].key)
		page.Next = string(next)
	}
	return page, nil
}

// compareCursor orders keys by depth, then the newest first, then id.
func compareCursor(a, b memoryCursor) int {
	return cmp.Or(cmp.Compare(a.Depth, b.Depth), b.Created.Compare(a.Created), cmpUUID(a.ID, b.ID))
}

// Reach implements Graph.
func (g *MemoryGraph) Reach(ctx context.Context, w Walk) (Reached, error) {
	if err := ctx.Err(); err != nil {
		return Reached{}, err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	var out Reached
	for _, hop := range g.walk(w) {
		out.IDs = append(out.IDs, hop.frontier...)
		out.Truncated = out.Truncated || hop.over
	}
	return out, nil
}
