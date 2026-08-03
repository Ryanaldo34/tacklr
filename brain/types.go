package brain

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrNotFound is returned when an object is missing, soft-deleted, or outside scope.
var ErrNotFound = errors.New("brain: object not found")

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
	CreatedAt   time.Time      `json:"created_at,omitempty"`
	UpdatedAt   time.Time      `json:"updated_at,omitempty"`
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
	return ObjectKindInfo{
		Kind:             k.Kind,
		Description:      k.Description,
		IsPart:           k.IsPart,
		IsParent:         k.IsParent,
		FilterableFields: k.FilterableFields,
	}
}
