package brain

import (
	"encoding/json"
	"errors"
	"slices"
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
	// ErrConflict is returned when another write changed the object first.
	// Nothing was written; retry from a fresh read.
	ErrConflict = errors.New("brain: conflict")
	// ErrIndexPut and ErrIndexRemove are index write failures (a graph is an
	// index). The store row is the source of truth: re-Put after fixing the
	// index.
	ErrIndexPut    = errors.New("brain: index put")
	ErrIndexRemove = errors.New("brain: index remove")
)

// Scope is the isolation every Engine and store call runs under. Namespace is
// required: a call with an empty namespace fails, so no code path reads or
// writes across tenants by default. A scope sees an object when every scope
// attr is on the object (Namespace.Covers), so a broader scope sees objects
// with extra attrs.
//
// Principals are the caller's identities (user and group ids from the host's
// identity provider). An object with Readers is visible only to a scope that
// shares at least one principal with it. An object without Readers is visible
// to every scope that covers its namespace.
//
// SessionID is harness-owned. When set, search and get also read this
// session's rows in session_messages (Namespace plus the session attribute).
// The model cannot set it.
type Scope struct {
	Namespace  Namespace
	Principals []string
	SessionID  string
}

// CanRead reports whether obj is visible under s: s covers its namespace,
// and obj has no Readers or shares a principal with s.
func (s Scope) CanRead(obj Object) bool {
	if !s.Namespace.Covers(obj.Namespace) {
		return false
	}
	if len(obj.Readers) == 0 {
		return true
	}
	return slices.ContainsFunc(obj.Readers, func(r string) bool { return slices.Contains(s.Principals, r) })
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
	// Embedding is the dense vector stored on the row. Store search reads it
	// on parts. Parent embeddings are copied to the graph for FindObjects.
	Embedding []float32
	// EmbeddingModel names the model that made Embedding. Vector search only
	// compares rows from the query's model.
	EmbeddingModel string
	// Namespace is the ordered named isolation attrs stored on the row.
	Namespace Namespace
	// Readers, when set, limits visibility to scopes holding one of these
	// principals (source ACLs, such as Drive or SharePoint sharing).
	Readers   []string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
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

// Relation is how a related object was reached: a child or parent, or an
// edge. An edge hop says the object it was followed from (Via), so the hops
// of one Related call with depth trace a path back to the start. EdgeMeta
// fields are embedded so agent JSON stays flat (note, status, role, …).
type Relation struct {
	Type      string     `json:"type"`
	Direction string     `json:"direction,omitempty"` // out | in, from Via
	Depth     int        `json:"depth,omitempty"`     // hops from the start
	Via       *uuid.UUID `json:"via,omitempty"`       // the object the edge was followed from
	EdgeMeta
}

// Rich maps a stored object to a rich reference.
func (o Object) Rich(includeContent bool) RichObject {
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

// ObjectKindInfo is the JSON form of ObjectKind for agents.
type ObjectKindInfo struct {
	Kind             string          `json:"kind"`
	Description      string          `json:"description,omitempty"`
	IsPart           bool            `json:"is_part"`
	IsParent         bool            `json:"is_parent"`
	FilterableFields json.RawMessage `json:"filterable_fields,omitempty"`
}

// StringMatch is equality or match-any for a well-known string field.
type StringMatch struct {
	Eq string
	In []string
}

func (s StringMatch) set() bool { return s.Eq != "" || len(s.In) > 0 }

// PropFilter is equality or match-any for one property.
type PropFilter struct {
	Eq any
	In []any
}

// Filter narrows retrieval. Well-known fields plus Props. Eq or list (In).
type Filter struct {
	Kind          StringMatch
	Title         StringMatch
	CreatedAfter  string
	CreatedBefore string
	UpdatedAfter  string
	UpdatedBefore string
	Props         map[string]PropFilter
}

func (f Filter) empty() bool {
	return !f.Kind.set() && !f.Title.set() &&
		f.CreatedAfter == "" && f.CreatedBefore == "" &&
		f.UpdatedAfter == "" && f.UpdatedBefore == "" &&
		len(f.Props) == 0
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
