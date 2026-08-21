package brain

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Coarse categories for errors.Is. Wrap a specific message at the call site
// (fmt.Errorf("…: %w", ErrInvalid)) instead of adding a sentinel per situation.
var (
	// ErrNotFound is returned when an object is missing, soft-deleted, or outside scope.
	ErrNotFound = errors.New("brain: object not found")
	// ErrInvalid groups validation failures (empty query, missing args, frozen catalog).
	ErrInvalid = errors.New("brain: invalid")
	// ErrUnsupported groups missing backend capabilities (no writer, no graph, no listing).
	ErrUnsupported = errors.New("brain: unsupported")
	// ErrGraphEnsure and ErrGraphRemove wrap dual-write failures (cause via errors.Unwrap).
	ErrGraphEnsure = errors.New("brain: graph ensure object")
	ErrGraphRemove = errors.New("brain: graph remove object")
)

// Scope is optional retrieval isolation for Engine methods.
// When Namespace is non-nil, results are limited to that namespace.
type Scope struct {
	Namespace *uuid.UUID
}

// Object is one row from the generic objects store (parent or part).
type Object struct {
	ID          uuid.UUID
	Kind        string
	Title       string
	Summary     string
	Properties  map[string]any
	Content     string
	ContentType string
	ParentID    *uuid.UUID
	Position    *int
	// Embedding is optional dense vector for hybrid search fixtures / stores.
	Embedding   []float32
	NamespaceID uuid.UUID
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   *time.Time
}

// IsPart reports whether the object has a parent containment link.
func (o Object) IsPart() bool {
	return o.ParentID != nil
}

// ObjectKind documents a free-form kind for schema() discovery.
type ObjectKind struct {
	Kind             string
	Description      string
	IsPart           bool
	IsParent         bool
	FilterableFields json.RawMessage // JSON array from object_kinds.filterable_fields
}

// Evidence is a part that justified a parent hit during search.
type Evidence struct {
	PartID     uuid.UUID      `json:"part_id"`
	Title      string         `json:"title,omitempty"`
	Snippet    string         `json:"snippet,omitempty"`
	Score      float64        `json:"score,omitempty"`
	Position   *int           `json:"position,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

// RichObject is the agent-facing object reference (never a bare id).
type RichObject struct {
	ID          uuid.UUID      `json:"id"`
	Kind        string         `json:"kind"`
	Title       string         `json:"title,omitempty"`
	Summary     string         `json:"summary,omitempty"`
	Score       *float64       `json:"score,omitempty"`
	Properties  map[string]any `json:"properties,omitempty"`
	Content     string         `json:"content,omitempty"` // set by read; omitted on search hits
	ContentType string         `json:"content_type,omitempty"`
	ParentID    *uuid.UUID     `json:"parent_id,omitempty"`
	Position    *int           `json:"position,omitempty"`
	Evidence    []Evidence     `json:"evidence,omitempty"`
	// Relation is set on expand graph neighbors (how this object was reached).
	Relation  *Relation `json:"relation,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// Relation describes a non-containment hop used to reach a neighbor on expand.
// EdgeMeta fields are embedded so agent JSON stays flat (note, status, role, …).
type Relation struct {
	Type      string     `json:"type"`
	Direction string     `json:"direction,omitempty"` // out | in
	Depth     int        `json:"depth,omitempty"`     // hops from expand seed
	SourceID  *uuid.UUID `json:"source_id,omitempty"` // ExpandMany landing id
	EdgeMeta
}

// RelationFromNeighbor maps a graph hop to the agent-facing Relation payload.
func RelationFromNeighbor(n GraphNeighbor) Relation {
	return Relation{
		Type:      n.RelationType,
		Direction: n.Direction,
		Depth:     1,
		EdgeMeta:  n.Meta,
	}
}

// RichFromObject maps a stored object to a rich reference.
func RichFromObject(o Object, includeContent bool) RichObject {
	r := RichObject{
		ID:          o.ID,
		Kind:        o.Kind,
		Title:       o.Title,
		Summary:     o.Summary,
		Properties:  o.Properties,
		ContentType: o.ContentType,
		ParentID:    o.ParentID,
		Position:    o.Position,
		CreatedAt:   o.CreatedAt,
		UpdatedAt:   o.UpdatedAt,
	}
	if includeContent {
		r.Content = o.Content
	}
	return r
}

// SchemaResult is the payload for the schema tool.
type SchemaResult struct {
	Kinds []ObjectKindInfo `json:"kinds"`
	// FilterUsage tells agents which tools accept filterable_fields and how.
	FilterUsage FilterUsage `json:"filter_usage"`
}

// FilterUsage is agent-facing guidance for structured filters (shared by corpus and entity find).
type FilterUsage struct {
	// Tools list knowledge tools that accept the same property filter keys as filterable_fields.
	Tools []string `json:"tools"`
	// Note is short instruction text for the agent.
	Note string `json:"note"`
}

// DefaultFilterUsage is embedded in every SchemaResult.
func DefaultFilterUsage() FilterUsage {
	return FilterUsage{
		Tools: []string{"search", "find_exact", "find_objects"},
		Note: "Each kind's filterable_fields lists property keys, types, and operators allowed in filters. " +
			"Use those keys on search, find_exact, and find_objects (not invented names). " +
			"When kinds are registered, property filters require a kind filter (or find_objects.kinds). " +
			"Core keys: kind, title, created_after, created_before, updated_after, updated_before. " +
			"Call schema with a kind before filtering.",
	}
}

// ObjectKindInfo is the JSON form of ObjectKind for agents.
type ObjectKindInfo struct {
	Kind             string          `json:"kind"`
	Description      string          `json:"description,omitempty"`
	IsPart           bool            `json:"is_part"`
	IsParent         bool            `json:"is_parent"`
	FilterableFields json.RawMessage `json:"filterable_fields,omitempty"`
}

// KindInfoFrom maps a registry row to the agent-facing shape.
func KindInfoFrom(k ObjectKind) ObjectKindInfo {
	return ObjectKindInfo(k)
}

// Filters narrows retrieval. Keys are field names; values are equality targets
// (or a list for match-any). Special keys: kind, title, created_after,
// created_before, updated_after, updated_before. Any other key matches properties.
type Filters map[string]any

// SearchRequest is the engine input for search and find_exact.
type SearchRequest struct {
	Query   string
	Filters Filters
	Limit   int
	// ScopeIDs, when non-empty, keeps only candidates whose id or parent_id is in the set.
	// Use after expand/find_objects to restrict corpus search to a deal-local neighborhood.
	ScopeIDs []uuid.UUID
}

// SearchPage is one page of ranked rich objects plus ResultSet identity.
type SearchPage struct {
	ResultSetID uuid.UUID    `json:"result_set_id"`
	HasMore     bool         `json:"has_more"`
	Objects     []RichObject `json:"objects"`
}

// ScoredID is a candidate from a retrieval channel before fusion.
type ScoredID struct {
	ID         uuid.UUID
	Score      float64
	UpdatedAt  time.Time
	ParentID   *uuid.UUID
	Title      string
	Content    string
	Position   *int
	Properties map[string]any // part props (e.g. start_line) when the channel provides them
}
