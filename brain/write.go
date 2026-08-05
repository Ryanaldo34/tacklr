package brain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Put upserts a knowledge object under scope.
// Catalog non-empty → ValidateObject. Namespace filled from scope when missing.
// ID generated when nil. Put refuses objects that already have DeletedAt set.
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
	now = now.UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now
	obj.Kind = strings.TrimSpace(obj.Kind)
	return obj
}

// ValidateObject checks an object against the kind catalog when non-empty.
// Empty/nil catalog: kind and namespace required; parent_id must not be the zero UUID.
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
	return validateObjectProperties(obj.Properties, spec)
}

func validateObjectProperties(props map[string]any, spec KindSpec) error {
	if props == nil {
		props = map[string]any{}
	}
	fields := make(map[string]FieldSpec, len(spec.Fields))
	for _, f := range spec.Fields {
		fields[f.Name] = f
	}
	for name, f := range fields {
		if !f.Required {
			continue
		}
		v, ok := props[name]
		if !ok || v == nil {
			return fmt.Errorf("brain: required property %q is missing", name)
		}
		if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
			return fmt.Errorf("brain: required property %q is empty", name)
		}
	}
	for name, v := range props {
		f, ok := fields[name]
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
