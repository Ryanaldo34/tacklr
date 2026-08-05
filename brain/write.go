package brain

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Put upserts a knowledge object under scope.
// Catalog non-empty → ValidateObject. Namespace filled from scope when missing.
// ID generated when nil. Put refuses objects that already have DeletedAt set.
// When WithEmbedder is set and title/summary/content is non-empty, embeds and
// stores the vector; embed errors fail the Put (fail closed).
func (e *Engine) Put(ctx context.Context, scope Scope, obj Object) (Object, error) {
	w, err := e.objectWriter()
	if err != nil {
		return Object{}, err
	}
	if obj.DeletedAt != nil {
		return Object{}, fmt.Errorf("brain: put refuses soft-deleted objects; use SoftDelete")
	}
	obj = preparePut(scope, obj, e.cfg.Now())
	if err := ValidateObject(obj, e.catalog); err != nil {
		return Object{}, err
	}
	indexText := e.indexTextForEmbed(ctx, scope, obj)
	if e.embedder != nil && indexText != "" {
		vec, err := e.embedder.Embed(ctx, indexText)
		if err != nil {
			return Object{}, fmt.Errorf("brain: embed object: %w", err)
		}
		// Own the slice so embedder buffer reuse cannot corrupt stored vectors.
		obj.Embedding = slices.Clone(vec)
	}
	if err := w.Put(ctx, obj); err != nil {
		return Object{}, err
	}
	// Dual-write graph nodes for non-parts only. Containment stays in Postgres;
	// entity find (FindObjects) should not rank document chunks as first-class objects.
	// Part embeds still use parent-prefixed IndexText for dense corpus search.
	if obj.ParentID == nil {
		if gw, ok := e.graphWriter(); ok {
			if err := gw.EnsureObject(ctx, obj); err != nil {
				return Object{}, fmt.Errorf("brain: graph ensure object: %w", err)
			}
		}
	}
	return obj, nil
}

// SoftDelete marks an object deleted under scope. Missing / out-of-scope → ErrNotFound.
// Graph nodes/edges are left in place (v1); cleanup is a later concern.
func (e *Engine) SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error {
	w, err := e.objectWriter()
	if err != nil {
		return err
	}
	return w.SoftDelete(ctx, scope, id)
}

// Link creates a non-containment edge from→to. Requires a GraphWriter (WithGraph).
// Put endpoints first so Helix nodes exist with searchable props; MemoryGraph
// accepts edges without a prior EnsureObject.
func (e *Engine) Link(ctx context.Context, from, to uuid.UUID, relationType string) error {
	gw, ok := e.graphWriter()
	if !ok {
		return fmt.Errorf("brain: graph writer is required for Link")
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("brain: from, to, and relation type are required")
	}
	return gw.AddEdge(ctx, from, to, rel)
}

// HasGraphWriter reports whether Put dual-write and Link are available.
func (e *Engine) HasGraphWriter() bool {
	_, ok := e.graphWriter()
	return ok
}

func (e *Engine) objectWriter() (ObjectWriter, error) {
	w, ok := e.store.(ObjectWriter)
	if !ok {
		return nil, fmt.Errorf("brain: store does not support object writes")
	}
	return w, nil
}

func (e *Engine) graphWriter() (GraphWriter, bool) {
	gw, ok := e.graph.(GraphWriter)
	return gw, ok
}

func preparePut(scope Scope, obj Object, now time.Time) Object {
	if obj.ID == uuid.Nil {
		obj.ID = uuid.New()
	}
	if obj.NamespaceID == uuid.Nil && scope.Namespace != nil {
		obj.NamespaceID = *scope.Namespace
	}
	if obj.Properties == nil {
		obj.Properties = map[string]any{}
	}
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now
	obj.Kind = strings.TrimSpace(obj.Kind)
	return obj
}

// IndexText joins non-empty title, summary, and content for embeddings and graph props.
func IndexText(obj Object) string {
	return joinNonEmpty("\n", obj.Title, obj.Summary, obj.Content)
}

// IndexTextWithParent prefixes parent context (bursting-style) when parentTitle is set.
func IndexTextWithParent(obj Object, parentTitle string) string {
	body := IndexText(obj)
	parentTitle = strings.TrimSpace(parentTitle)
	if parentTitle == "" {
		return body
	}
	if body == "" {
		return parentTitle
	}
	return parentTitle + "\n" + body
}

func joinNonEmpty(sep string, parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, sep)
}

// indexTextForEmbed builds embed text; parts get parent title prefix when readable under scope.
func (e *Engine) indexTextForEmbed(ctx context.Context, scope Scope, obj Object) string {
	if obj.ParentID == nil {
		return IndexText(obj)
	}
	parent, err := e.store.Get(ctx, scope, *obj.ParentID)
	if err != nil {
		return IndexText(obj)
	}
	return IndexTextWithParent(obj, parent.Title)
}

// ValidateObject checks an object against the kind catalog when non-empty.
func ValidateObject(obj Object, cat *KindCatalog) error {
	kind := strings.TrimSpace(obj.Kind)
	if kind == "" {
		return fmt.Errorf("brain: object kind is required")
	}
	if obj.NamespaceID == uuid.Nil {
		return fmt.Errorf("brain: object namespace_id is required")
	}
	if obj.ParentID != nil && *obj.ParentID == uuid.Nil {
		return fmt.Errorf("brain: parent_id must not be the nil UUID")
	}
	if cat == nil || cat.Empty() {
		return nil
	}
	spec, ok := cat.Get(kind)
	if !ok {
		return fmt.Errorf("brain: kind %q is not registered", kind)
	}
	hasParent := obj.ParentID != nil
	switch {
	case spec.IsParent && !spec.IsPart && hasParent:
		return fmt.Errorf("brain: kind %q is a parent kind and must not have parent_id", spec.Kind)
	case spec.IsPart && !spec.IsParent && !hasParent:
		return fmt.Errorf("brain: kind %q is a part kind and requires parent_id", spec.Kind)
	}
	props := obj.Properties
	if props == nil {
		props = map[string]any{}
	}
	byName := make(map[string]FieldSpec, len(spec.Fields))
	for _, f := range spec.Fields {
		byName[f.Name] = f
		if f.Required && props[f.Name] == nil {
			return fmt.Errorf("brain: required property %q is missing", f.Name)
		}
	}
	for name, v := range props {
		if v == nil {
			continue
		}
		f, ok := byName[name]
		if !ok {
			return fmt.Errorf("brain: property %q is not defined on kind %q", name, spec.Kind)
		}
		if err := checkFieldValue(v, f.Type); err != nil {
			return fmt.Errorf("brain: property %q: %w", name, err)
		}
	}
	return nil
}

func requireObjectIdentity(obj Object) error {
	if obj.ID == uuid.Nil {
		return fmt.Errorf("brain: object id is required")
	}
	if strings.TrimSpace(obj.Kind) == "" {
		return fmt.Errorf("brain: object kind is required")
	}
	if obj.NamespaceID == uuid.Nil {
		return fmt.Errorf("brain: object namespace_id is required")
	}
	return nil
}
