package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// maxEntityContentRunes caps full content included in entity index text for graph nodes.
const maxEntityContentRunes = 2000

// Put upserts a knowledge object under scope.
// Catalog non-empty → ValidateObject. Namespace filled from scope when missing.
// ID generated when nil. Put refuses objects that already have DeletedAt set.
// When WithEmbedder is set and index text is non-empty, embeds and stores the vector;
// embed errors fail the Put (fail closed).
// Parent Puts dual-write the graph node again so entity search stays current.
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
		obj.Embedding = slices.Clone(vec)
	}
	if err := w.Put(ctx, obj); err != nil {
		return Object{}, err
	}
	// Dual-write first-class objects only (no parent_id). Each Put refreshes the node.
	if obj.ParentID == nil {
		if gw, ok := e.graphWriter(); ok {
			if err := gw.EnsureObject(ctx, obj); err != nil {
				return Object{}, fmt.Errorf("brain: graph ensure object: %w", err)
			}
		}
	}
	return obj, nil
}

// SoftDelete marks an object deleted under scope, then removes its graph node when present.
func (e *Engine) SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error {
	w, err := e.objectWriter()
	if err != nil {
		return err
	}
	if err := w.SoftDelete(ctx, scope, id); err != nil {
		return err
	}
	if gw, ok := e.graphWriter(); ok {
		if err := gw.RemoveObject(ctx, id); err != nil {
			return fmt.Errorf("brain: graph remove object: %w", err)
		}
	}
	return nil
}

// Link creates a non-containment edge from→to between first-class, visible objects.
// Both endpoints must exist under scope, must not be soft-deleted, and must not be parts.
func (e *Engine) Link(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string) error {
	gw, ok := e.graphWriter()
	if !ok {
		return fmt.Errorf("brain: graph writer is required for Link")
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("brain: from, to, and relation type are required")
	}
	if err := e.requireLinkEndpoint(ctx, scope, from, "from"); err != nil {
		return err
	}
	if err := e.requireLinkEndpoint(ctx, scope, to, "to"); err != nil {
		return err
	}
	return gw.AddEdge(ctx, from, to, rel)
}

func (e *Engine) requireLinkEndpoint(ctx context.Context, scope Scope, id uuid.UUID, label string) error {
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return fmt.Errorf("brain: link %s: %w", label, err)
	}
	if obj.ParentID != nil {
		return fmt.Errorf("brain: link %s must be a first-class object (not a part)", label)
	}
	return nil
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

// IndexText joins non-empty title, summary, and content for corpus part embeds.
func IndexText(obj Object) string {
	return joinNonEmpty("\n", obj.Title, obj.Summary, obj.Content)
}

// EntityIndexText builds text for first-class graph nodes and parent embeddings:
// title, summary, scalar properties (sorted keys), and capped content.
// Keeps entity find sensitive to attributes (stage, amount, …) without dumping huge bodies.
func EntityIndexText(obj Object) string {
	parts := make([]string, 0, 8)
	if t := strings.TrimSpace(obj.Title); t != "" {
		parts = append(parts, t)
	}
	if s := strings.TrimSpace(obj.Summary); s != "" {
		parts = append(parts, s)
	}
	if len(obj.Properties) > 0 {
		keys := slices.Sorted(maps.Keys(obj.Properties))
		for _, k := range keys {
			line := formatPropertyLine(k, obj.Properties[k])
			if line != "" {
				parts = append(parts, line)
			}
		}
	}
	if c := truncateRunes(strings.TrimSpace(obj.Content), maxEntityContentRunes); c != "" {
		parts = append(parts, c)
	}
	return strings.Join(parts, "\n")
}

func formatPropertyLine(key string, v any) string {
	key = strings.TrimSpace(key)
	if key == "" || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		x = strings.TrimSpace(x)
		if x == "" {
			return ""
		}
		return key + ": " + x
	case bool:
		return key + ": " + strconv.FormatBool(x)
	case float64:
		return key + ": " + strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return key + ": " + strconv.FormatFloat(float64(x), 'g', -1, 32)
	case int:
		return key + ": " + strconv.Itoa(x)
	case int32:
		return key + ": " + strconv.FormatInt(int64(x), 10)
	case int64:
		return key + ": " + strconv.FormatInt(x, 10)
	case json.Number:
		return key + ": " + string(x)
	default:
		// Skip nested maps/slices/blobs to keep entity nodes small and stable.
		return ""
	}
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

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 || s == "" {
		return s
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	n := 0
	for i := range s {
		if n == maxRunes {
			return s[:i]
		}
		n++
	}
	return s
}

// indexTextForEmbed: parents use EntityIndexText; parts use parent-prefixed corpus IndexText.
func (e *Engine) indexTextForEmbed(ctx context.Context, scope Scope, obj Object) string {
	if obj.ParentID == nil {
		return EntityIndexText(obj)
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
