package brain

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
)

// EdgeMeta is optional metadata on a non-containment relationship (why/how/when linked).
// Kept short and relational — full bodies stay on objects in the Store.
type EdgeMeta struct {
	Note       string     `json:"note,omitempty"`
	Status     string     `json:"status,omitempty"`     // e.g. active, resolved
	Role       string     `json:"role,omitempty"`       // e.g. primary buyer vs cc
	Confidence float64    `json:"confidence,omitempty"` // 0 means unset; otherwise typically (0,1]
	EvidenceID *uuid.UUID `json:"evidence_id,omitempty"`
	CreatedAt  time.Time  `json:"created_at,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at,omitempty"`
}

// Edge directions, as seen from the object a walk leaves.
const (
	DirectionOut  = "out"
	DirectionIn   = "in"
	DirectionBoth = "both"
)

// NeighborhoodQuery asks for the objects within MaxHops edges of ObjectID.
type NeighborhoodQuery struct {
	// Namespace and Principals limit the walk as Scope.CanRead does: a node
	// they cannot read is neither returned nor walked through.
	Namespace  Namespace
	Principals []string
	ObjectID   uuid.UUID
	Relations  []string // edge labels to follow; empty follows every relation
	Direction  string   // DirectionOut, DirectionIn or DirectionBoth (the default)
	MaxHops    int
	Limit      int
}

// GraphNeighbor is one object a neighborhood walk reached.
type GraphNeighbor struct {
	ObjectID     uuid.UUID
	SourceID     uuid.UUID // the object the edge was followed from
	RelationType string
	Direction    string // DirectionOut or DirectionIn, from SourceID
	Depth        int    // hops from the walk's start
	Meta         EdgeMeta
}

// GraphEdge is one directed, labeled non-containment edge.
type GraphEdge struct {
	// Namespace is the edge's own (its source object's), which edge search
	// filters on.
	Namespace    Namespace
	From, To     uuid.UUID
	RelationType string
	Meta         EdgeMeta
}

// ObjectSearch is one entity search over a graph's nodes.
type ObjectSearch struct {
	// Namespace and Principals limit the ranking as Scope.CanRead does: only
	// nodes they can read rank.
	Namespace  Namespace
	Principals []string
	Query      string
	// Filters (with Kind holding the requested kinds) apply before ranking,
	// so a node that fails them takes no slot.
	Filters Filter
	Limit   int
}

// LinkSearch is one edge search. Query matches the edge note.
type LinkSearch struct {
	// Namespace and Principals limit the ranking: only an edge Namespace
	// covers whose two ends they can read (as Scope.CanRead decides) ranks.
	Namespace  Namespace
	Principals []string
	// RelationType names one relation; empty searches every relation the
	// graph indexes.
	RelationType string
	Query        string
	Limit        int
}

// EdgeSearchHit is one graph edge search result (endpoints + meta + score).
type EdgeSearchHit struct {
	FromID       uuid.UUID
	ToID         uuid.UUID
	RelationType string
	Meta         EdgeMeta
	Score        float64
}

// Index receives every committed write of a first-class object: Engine
// writes the store, then each index, then the audit record. A Graph is an
// Index; WithIndex adds others (a second search system, a change feed).
type Index interface {
	// Put upserts obj and indexes its Text.
	Put(ctx context.Context, obj Indexed) error
	// Remove drops the object.
	Remove(ctx context.Context, id uuid.UUID) error
	// Erase drops every object scope's namespace covers, whatever its
	// readers, in one step. It is idempotent, so Engine runs it before the
	// store's erase and a retry finishes an erase that failed part way.
	Erase(ctx context.Context, scope Scope) error
}

// Graph is the knowledge graph Engine links and walks objects in (WithGraph):
// a node per first-class object and labeled edges between them. A backend owns its
// retrieval: its Embedder (node text and edge notes), channels, ranking and
// fusion, filters, and the scope and readers rules. helixgraph implements
// it, and brain/graphtest is the contract a graph must pass. A backend
// without a feature returns ErrUnsupported from that method.
//
// Engine validates and normalizes every call first: ids are set, the
// namespace is valid, relations are trimmed and unique, the direction is a
// Direction constant, and limits and depths are positive. Engine also
// hydrates every result from the Store, the source of truth.
type Graph interface {
	// Put (Index) upserts the node of a first-class object, indexes its
	// Text, and keeps the node's edges. The node holds what the backend
	// needs to rank and filter: namespace, readers, kind, title, times,
	// properties. Remove (Index) drops the node and its edges.
	Index
	// Link creates or replaces the edge e.From→e.To with e.RelationType and
	// indexes its note. Both nodes must exist (ErrNotFound otherwise).
	Link(ctx context.Context, e GraphEdge) error
	// Unlink drops the edge from→to with rel (a missing edge succeeds).
	Unlink(ctx context.Context, from, to uuid.UUID, rel string) error

	// FindObjects returns the readable nodes that pass q.Filters, best
	// first, at most q.Limit.
	FindObjects(ctx context.Context, q ObjectSearch) ([]ScoredID, error)
	// FindLinks returns the readable edges of q.RelationType (every indexed
	// relation when it is empty), best first, at most q.Limit.
	FindLinks(ctx context.Context, q LinkSearch) ([]EdgeSearchHit, error)
	// Neighborhood walks breadth-first from q.ObjectID. Each object appears
	// once, at its smallest depth; the start never appears. Results are
	// ordered by depth, then relation (request order), then outbound first.
	// Empty Relations follows every relation.
	Neighborhood(ctx context.Context, q NeighborhoodQuery) ([]GraphNeighbor, error)
}

// isContainment reports whether rel names containment (contains / part_of).
func isContainment(rel string) bool {
	switch strings.ToLower(strings.TrimSpace(rel)) {
	case "contains", "part_of", "partof":
		return true
	default:
		return false
	}
}
