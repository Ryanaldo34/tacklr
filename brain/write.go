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
	w, ok := e.store.(ObjectWriter)
	if !ok {
		return Object{}, fmt.Errorf("brain: store does not support object writes")
	}
	if obj.DeletedAt != nil {
		return Object{}, fmt.Errorf("brain: put refuses soft-deleted objects; use SoftDelete")
	}
	obj = preparePut(scope, obj, e.cfg.Now())
	if err := ValidateObject(obj, e.catalog); err != nil {
		return Object{}, err
	}
	if e.embedder != nil {
		if text := embedText(obj); text != "" {
			vec, err := e.embedder.Embed(ctx, text)
			if err != nil {
				return Object{}, fmt.Errorf("brain: embed object: %w", err)
			}
			// Own the slice so embedder buffer reuse cannot corrupt stored vectors.
			obj.Embedding = slices.Clone(vec)
		}
	}
	if err := w.Put(ctx, obj); err != nil {
		return Object{}, err
	}
	return obj, nil
}

// SoftDelete marks an object deleted under scope. Missing / out-of-scope → ErrNotFound.
func (e *Engine) SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error {
	w, ok := e.store.(ObjectWriter)
	if !ok {
		return fmt.Errorf("brain: store does not support object writes")
	}
	return w.SoftDelete(ctx, scope, id)
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

// embedText joins non-empty title/summary/content for dense indexing.
func embedText(obj Object) string {
	t := strings.TrimSpace(obj.Title)
	s := strings.TrimSpace(obj.Summary)
	c := strings.TrimSpace(obj.Content)
	switch {
	case t == "" && s == "" && c == "":
		return ""
	case s == "" && c == "":
		return t
	case t == "" && c == "":
		return s
	case t == "" && s == "":
		return c
	case c == "":
		return t + "\n" + s
	case s == "":
		return t + "\n" + c
	case t == "":
		return s + "\n" + c
	default:
		return t + "\n" + s + "\n" + c
	}
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
	if spec.IsParent && !spec.IsPart && hasParent {
		return fmt.Errorf("brain: kind %q is a parent kind and must not have parent_id", spec.Kind)
	}
	if spec.IsPart && !spec.IsParent && !hasParent {
		return fmt.Errorf("brain: kind %q is a part kind and requires parent_id", spec.Kind)
	}
	props := obj.Properties
	if props == nil {
		props = map[string]any{}
	}
	// Index fields once, then walk props (typical field count is small).
	byName := make(map[string]FieldSpec, len(spec.Fields))
	for _, f := range spec.Fields {
		byName[f.Name] = f
		if !f.Required {
			continue
		}
		if props[f.Name] == nil {
			return fmt.Errorf("brain: required property %q is missing", f.Name)
		}
	}
	for name, v := range props {
		f, ok := byName[name]
		if !ok {
			return fmt.Errorf("brain: property %q is not defined on kind %q", name, spec.Kind)
		}
		if v == nil {
			continue
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
