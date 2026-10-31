package brain

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Put upserts a knowledge object under scope.
// Catalog non-empty → ValidateObject. Namespace filled from scope when missing;
// a namespace outside scope is invalid. A part's parent must be visible under
// scope. ID generated when nil. An id already held by a row outside scope
// returns ErrNotFound and changes nothing. Put refuses objects that already
// have DeletedAt set.
// The store indexes the object's text (and embeds it when it has an
// Embedder); an embedding failure fails the Put. A first-class object then
// goes to every index (a graph upserts its node and keeps its edges).
// If an index write fails after the store write, the store row remains
// (source of truth) and the write is still audited; re-Put after fixing the
// index.
func (e *Engine) Put(ctx context.Context, scope Scope, obj Object) (Object, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Object{}, err
	}
	in, err := e.preparePut(ctx, scope, obj)
	if err != nil {
		return Object{}, err
	}
	stored, err := e.store.Put(ctx, scope, in)
	if err != nil {
		return Object{}, err
	}
	if err := e.committed(ctx, scope, AuditEvent{Op: AuditPut, ObjectIDs: []uuid.UUID{stored.ID}}, e.putIndexes(ctx, stored, in.Text)); err != nil {
		return Object{}, err
	}
	return stored, nil
}

// committed audits a write the store has committed, whatever indexErr, the
// result of its index writes, is, and returns both errors.
func (e *Engine) committed(ctx context.Context, scope Scope, ev AuditEvent, indexErr error) error {
	return errors.Join(indexErr, e.record(ctx, scope, ev))
}

// SoftDelete marks an object and its parts deleted. A first-class object
// leaves every index first, so an index failure keeps the store row. If the
// store SoftDelete then fails, re-Put puts the object back in the indexes.
func (e *Engine) SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return err
	}
	// Read under scope first, so a scope that cannot see the object cannot
	// remove it from an index.
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return err
	}
	if obj.ParentID == nil {
		if err := e.removeIndexes(ctx, id); err != nil {
			return err
		}
	}
	if err := e.store.SoftDelete(ctx, scope, id); err != nil {
		return err
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditDelete, ObjectIDs: []uuid.UUID{id}})
}

// PurgeDeleted hard-deletes objects under scope that were soft-deleted
// before cutoff (retention). It returns how many rows it removed.
func (e *Engine) PurgeDeleted(ctx context.Context, scope Scope, before time.Time) (int, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return 0, err
	}
	n, err := e.store.PurgeDeleted(ctx, scope, before)
	if err != nil {
		return 0, err
	}
	return n, e.record(ctx, scope, AuditEvent{Op: AuditPurge, Count: n})
}

// Erase hard-deletes everything under scope (erasure requests and tenant
// offboarding): it erases every index by scope, then the store's objects and
// saved session messages. Readers do not limit Erase. A failure leaves the
// store intact, and calling Erase again finishes the erase everywhere.
func (e *Engine) Erase(ctx context.Context, scope Scope) error {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return err
	}
	// Indexes first: their erase is idempotent, so a failure leaves the
	// store intact and a retry erases what remains everywhere.
	for _, ix := range e.indexes {
		if err := ix.Erase(ctx, scope); err != nil {
			return fmt.Errorf("%w: %w", ErrIndexRemove, err)
		}
	}
	ids, err := e.store.Erase(ctx, scope)
	if err != nil {
		return err
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditErase, ObjectIDs: ids, Count: len(ids)})
}

// putIndexes puts a first-class object in every index (parts are not
// indexed). Every index is tried; their errors are joined.
func (e *Engine) putIndexes(ctx context.Context, obj Object, text string) error {
	if obj.ParentID != nil {
		return nil
	}
	var errs []error
	for _, ix := range e.indexes {
		if err := ix.Put(ctx, Indexed{Object: obj, Text: text}); err != nil {
			errs = append(errs, fmt.Errorf("%w: %w", ErrIndexPut, err))
		}
	}
	return errors.Join(errs...)
}

// removeIndexes removes id from every index, stopping at the first failure.
func (e *Engine) removeIndexes(ctx context.Context, id uuid.UUID) error {
	for _, ix := range e.indexes {
		if err := ix.Remove(ctx, id); err != nil {
			return fmt.Errorf("%w: %w", ErrIndexRemove, err)
		}
	}
	return nil
}

// Link creates a non-containment edge from→to between first-class, visible objects.
// Both endpoints must exist under scope, must not be soft-deleted, and must not be parts.
// Equivalent to LinkWith with zero EdgeMeta.
func (k *KnowledgeGraph) Link(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string) error {
	return k.LinkWith(ctx, scope, from, to, relationType, EdgeMeta{})
}

// LinkWith is Link plus optional relationship metadata (note, status, role, …).
// The edge takes the source object's namespace. The graph indexes the note
// for find_links.
func (k *KnowledgeGraph) LinkWith(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string, meta EdgeMeta) error {
	e := k.eng
	rel, src, err := e.linkEnds(ctx, scope, from, to, relationType)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	meta.CreatedAt, meta.UpdatedAt = cmp.Or(meta.CreatedAt, now), cmp.Or(meta.UpdatedAt, now)
	meta.Note = strings.TrimSpace(meta.Note)
	edge := GraphEdge{Namespace: src.Namespace, From: from, To: to, RelationType: rel, Meta: meta}
	if err := k.graph.Link(ctx, edge); err != nil {
		return fmt.Errorf("brain: link: %w", err)
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditLink, ObjectIDs: []uuid.UUID{from, to}})
}

// Unlink removes a non-containment edge from→to. Endpoints must be visible first-class objects.
func (k *KnowledgeGraph) Unlink(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string) error {
	e := k.eng
	rel, _, err := e.linkEnds(ctx, scope, from, to, relationType)
	if err != nil {
		return err
	}
	if err := k.graph.Unlink(ctx, from, to, rel); err != nil {
		return fmt.Errorf("brain: unlink: %w", err)
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditUnlink, ObjectIDs: []uuid.UUID{from, to}})
}

// linkEnds checks a link request: a graph writer, ids and a relation, and
// two visible first-class endpoints. It returns the trimmed relation and the
// source object.
func (e *Engine) linkEnds(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string) (string, Object, error) {
	if err := ctx.Err(); err != nil {
		return "", Object{}, err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return "", Object{}, err
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return "", Object{}, fmt.Errorf("%w: from, to, and relation type are required", ErrInvalid)
	}
	var ends [2]Object
	for i, end := range []struct {
		id    uuid.UUID
		label string
	}{{from, "from"}, {to, "to"}} {
		obj, err := e.store.Get(ctx, scope, end.id)
		if err != nil {
			return "", Object{}, fmt.Errorf("brain: link %s: %w", end.label, err)
		}
		if obj.ParentID != nil {
			return "", Object{}, fmt.Errorf("brain: link %s: %w: endpoint must be a first-class object (not a part)", end.label, ErrInvalid)
		}
		ends[i] = obj
	}
	return rel, ends[0], nil
}

// preparePut fills defaults, validates obj against scope and the catalog,
// and shapes its index text. A part's parent must be visible under scope.
func (e *Engine) preparePut(ctx context.Context, scope Scope, obj Object) (Indexed, error) {
	if obj.DeletedAt != nil {
		return Indexed{}, fmt.Errorf("%w: put refuses soft-deleted objects; use SoftDelete", ErrInvalid)
	}
	now := e.cfg.Now()
	if obj.ID == uuid.Nil {
		obj.ID = uuid.New()
	}
	if obj.Namespace.Empty() {
		obj.Namespace = scope.Namespace.Clone()
	}
	if obj.Properties == nil {
		obj.Properties = map[string]any{}
	}
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now
	obj.Kind = strings.TrimSpace(obj.Kind)
	obj.Embedding, obj.EmbeddingModel = nil, ""
	if err := ValidateObject(obj, e.catalog); err != nil {
		return Indexed{}, err
	}
	if !scope.Namespace.Covers(obj.Namespace) {
		return Indexed{}, fmt.Errorf("%w: object namespace %s is outside scope %s", ErrInvalid, obj.Namespace, scope.Namespace)
	}
	var parent *Object
	if obj.ParentID != nil {
		p, err := e.store.Get(ctx, scope, *obj.ParentID)
		if err != nil {
			return Indexed{}, fmt.Errorf("brain: parent %s: %w", *obj.ParentID, err)
		}
		if p.ParentID != nil {
			return Indexed{}, fmt.Errorf("%w: parent must be a first-class object", ErrInvalid)
		}
		parent = &p
	}
	return Indexed{Object: obj, Text: e.indexText(obj, parent)}, nil
}

// IndexText joins non-empty title, summary, and content for corpus part embeds.
func IndexText(obj Object) string {
	return joinNonEmpty("\n", obj.Title, obj.Summary, obj.Content)
}

// EntityIndexText builds text for first-class graph nodes and parent embeddings:
// title, summary, scalar properties (sorted keys), and full content.
// Reserved store props (slug, vfs_path) are omitted. Callers with a kind catalog
// should prefer Engine.Put, which also honors FieldSpec.SkipIndex.
func EntityIndexText(obj Object) string {
	return entityIndexText(obj, KindSpec{}, false)
}

func entityIndexText(obj Object, spec KindSpec, haveSpec bool) string {
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
			if skipIndexProperty(k, spec, haveSpec) {
				continue
			}
			line := formatPropertyLine(k, obj.Properties[k])
			if line != "" {
				parts = append(parts, line)
			}
		}
	}
	if c := strings.TrimSpace(obj.Content); c != "" {
		parts = append(parts, c)
	}
	return strings.Join(parts, "\n")
}

func skipIndexProperty(name string, spec KindSpec, haveSpec bool) bool {
	if isReservedStoreProp(name) {
		return true
	}
	if !haveSpec {
		return false
	}
	f, ok := spec.Field(name)
	return ok && f.SkipIndex
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

// capRunes returns s truncated to at most maxRunes (no ellipsis).
func capRunes(s string, maxRunes int) string {
	if maxRunes <= 0 || s == "" {
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

// indexText: parents use entity index text; parts use parent-prefixed corpus IndexText.
func (e *Engine) indexText(obj Object, parent *Object) string {
	var text string
	if parent == nil {
		spec, ok := e.catalog.Get(obj.Kind)
		text = entityIndexText(obj, spec, ok)
	} else {
		text = IndexTextWithParent(obj, parent.Title)
	}
	if e.indexTextFn != nil {
		text = strings.TrimSpace(e.indexTextFn(obj, text))
	}
	return text
}

// ValidateObject checks an object against the kind catalog when non-empty.
func ValidateObject(obj Object, cat *KindCatalog) error {
	kind := strings.TrimSpace(obj.Kind)
	if kind == "" {
		return fmt.Errorf("brain: object kind is required")
	}
	if err := obj.Namespace.Validate(); err != nil {
		return err
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
			// slug / vfs_path are store-owned (Engram filename + Provider lookup).
			// They are not required on every KindSpec and are not front-matter-only.
			if isReservedStoreProp(name) {
				continue
			}
			return fmt.Errorf("brain: property %q is not defined on kind %q", name, spec.Kind)
		}
		if err := checkFieldValue(v, f.Type); err != nil {
			return fmt.Errorf("brain: property %q: %w", name, err)
		}
		if err := checkFieldEnum(v, f); err != nil {
			return fmt.Errorf("brain: property %q: %w", name, err)
		}
	}
	return nil
}

func checkFieldEnum(v any, f FieldSpec) error {
	if len(f.Enum) == 0 {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return nil
	}
	s = strings.TrimSpace(s)
	if slices.Contains(f.Enum, s) {
		return nil
	}
	return fmt.Errorf("value %q is not in enum", s)
}

// Reserved store properties persisted on objects without a KindSpec field.
// slug is the Engram filename stem. vfs_path is Provider-internal (full virtual
// path); it is not a Markdown front-matter key.
const (
	PropSlug    = "slug"
	PropVFSPath = "vfs_path"
)

func isReservedStoreProp(name string) bool {
	switch name {
	case PropSlug, PropVFSPath:
		return true
	default:
		return false
	}
}

// ValidateObjectIdentity checks id, kind, and namespace before a store Put.
func ValidateObjectIdentity(obj Object) error {
	if obj.ID == uuid.Nil {
		return fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	if strings.TrimSpace(obj.Kind) == "" {
		return fmt.Errorf("%w: object kind is required", ErrInvalid)
	}
	return obj.Namespace.Validate()
}

// WriteKinds maps agent save_* tools to host kind names.
// Empty fields omit that tool. When the process catalog is non-empty, named
// kinds must already be registered (ApplyKinds / WithKinds).
type WriteKinds struct {
	Discovery string // save_discovery
	Fact      string // save_fact
	Memory    string // save_memory
}
