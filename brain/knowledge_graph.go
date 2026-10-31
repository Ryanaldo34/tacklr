package brain

// KnowledgeGraph is an Engine with a Graph. It owns every graph feature:
// find_objects, find_links, links, paths, and expand over children and
// relations. Each method brokers between the graph, which ranks and walks,
// and the engine's store, the source of truth. Engine alone has no graph
// features, so none of them can be reached without a graph.
type KnowledgeGraph struct {
	eng   *Engine
	graph Graph
}

// NewKnowledgeGraph adds g to eng's indexes, so every committed write of a
// first-class object reaches the graph, and returns the graph features over
// both. Call it while building the engine, before it serves requests.
func NewKnowledgeGraph(eng *Engine, g Graph) *KnowledgeGraph {
	eng.indexes = append(eng.indexes, g)
	return &KnowledgeGraph{eng: eng, graph: g}
}

// Engine returns the engine the graph features run on.
func (k *KnowledgeGraph) Engine() *Engine { return k.eng }
