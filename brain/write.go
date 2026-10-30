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

	"github.com/google/uuid"
)

// Put upserts a knowledge object under scope.
// Catalog non-empty → ValidateObject. Namespace filled from scope when missing;
// a namespace outside scope is invalid. A part's parent must be visible under
// scope. ID generated when nil. An id already held by a row outside scope
// returns ErrNotFound and changes nothing. Put refuses objects that already
// have DeletedAt set.
// When WithEmbedder is set and index text is non-empty, embeds and stores the vector;
// embed errors fail the Put (fail closed).
// Parent Puts dual-write the graph node (in-place upsert; edges preserved).
// If the graph Ensure fails after a successful store write, the store row remains
// (source of truth); callers should re-Put after fixing the graph.
func (e *Engine) Put(ctx context.Context, scope Scope, obj Object) (Object, error) {
	w, err := capability[ObjectWriter](ctx, e, scope.Namespace, "object writes")
	if err != nil {
		return Object{}, err
	}
	obj, text, err := e.preparePut(ctx, scope, obj, nil)
	if err != nil {
		return Object{}, err
	}
	if err := w.Put(ctx, scope, obj); err != nil {
		return Object{}, err
	}
	if err := e.ensureGraph(ctx, obj, text); err != nil {
		return Object{}, err
	}
	return obj, e.record(ctx, scope, AuditEvent{Op: AuditPut, ObjectIDs: []uuid.UUID{obj.ID}})
}

// SoftDelete marks an object and its parts deleted. It removes the graph node
// first (when present): graph-first keeps the store intact if graph removal
// fails. If the store SoftDelete fails after a successful graph remove,
// re-Put re-creates the graph node.
func (e *Engine) SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error {
	w, err := capability[ObjectWriter](ctx, e, scope.Namespace, "object writes")
	if err != nil {
		return err
	}
	// Validate visibility under scope before mutating graph (so wrong-ns cannot drop graph).
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return err
	}
	if obj.ParentID == nil && e.graphW != nil {
		if err := e.graphW.RemoveObject(ctx, id); err != nil {
			return fmt.Errorf("%w: %w", ErrGraphRemove, err)
		}
	}
	if err := w.SoftDelete(ctx, scope, id); err != nil {
		return err
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditDelete, ObjectIDs: []uuid.UUID{id}})
}

// PurgeDeleted hard-deletes objects under scope that were soft-deleted
// before cutoff (retention). It returns how many rows it removed.
func (e *Engine) PurgeDeleted(ctx context.Context, scope Scope, before time.Time) (int, error) {
	p, err := capability[Purger](ctx, e, scope.Namespace, "purge")
	if err != nil {
		return 0, err
	}
	n, err := p.PurgeDeleted(ctx, scope, before)
	if err != nil {
		return 0, err
	}
	return n, e.record(ctx, scope, AuditEvent{Op: AuditPurge, Count: n})
}

// Erase hard-deletes every object and saved session message under scope,
// then removes the graph nodes of the erased first-class objects (erasure
// requests and tenant offboarding). Readers do not limit Erase.
func (e *Engine) Erase(ctx context.Context, scope Scope) error {
	p, err := capability[Purger](ctx, e, scope.Namespace, "purge")
	if err != nil {
		return err
	}
	ids, err := p.Erase(ctx, scope)
	if err != nil {
		return err
	}
	if e.graphW != nil {
		for _, id := range ids {
			if err := e.graphW.RemoveObject(ctx, id); err != nil {
				return fmt.Errorf("%w: %w", ErrGraphRemove, err)
			}
		}
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditErase, ObjectIDs: ids, Count: len(ids)})
}

func (e *Engine) ensureGraph(ctx context.Context, obj Object, indexText string) error {
	// Dual-write first-class objects only (no parent_id). Each Put refreshes node props.
	if obj.ParentID != nil || e.graphW == nil {
		return nil
	}
	if err := e.graphW.EnsureObject(ctx, obj, indexText); err != nil {
		return fmt.Errorf("%w: %w", ErrGraphEnsure, err)
	}
	return nil
}

// Link creates a non-containment edge from→to between first-class, visible objects.
// Both endpoints must exist under scope, must not be soft-deleted, and must not be parts.
// Equivalent to LinkWith with zero EdgeMeta.
func (e *Engine) Link(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string) error {
	return e.LinkWith(ctx, scope, from, to, relationType, EdgeMeta{})
}

// LinkWith is Link plus optional relationship metadata (note, status, role, …).
func (e *Engine) LinkWith(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string, meta EdgeMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return err
	}
	if e.graphW == nil {
		return fmt.Errorf("%w: graph writer is required for Link", ErrUnsupported)
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("%w: from, to, and relation type are required", ErrInvalid)
	}
	if err := e.requireLinkEndpoint(ctx, scope, from, "from"); err != nil {
		return err
	}
	if err := e.requireLinkEndpoint(ctx, scope, to, "to"); err != nil {
		return err
	}
	if err := e.graphW.AddEdge(ctx, from, to, rel, meta); err != nil {
		return err
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditLink, ObjectIDs: []uuid.UUID{from, to}})
}

// Unlink removes a non-containment edge from→to. Endpoints must be visible first-class objects.
func (e *Engine) Unlink(ctx context.Context, scope Scope, from, to uuid.UUID, relationType string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := scope.Namespace.Validate(); err != nil {
		return err
	}
	if e.graphW == nil {
		return fmt.Errorf("%w: graph writer is required for Link", ErrUnsupported)
	}
	rel := strings.TrimSpace(relationType)
	if from == uuid.Nil || to == uuid.Nil || rel == "" {
		return fmt.Errorf("%w: from, to, and relation type are required", ErrInvalid)
	}
	if err := e.requireLinkEndpoint(ctx, scope, from, "from"); err != nil {
		return err
	}
	if err := e.requireLinkEndpoint(ctx, scope, to, "to"); err != nil {
		return err
	}
	if err := e.graphW.RemoveEdge(ctx, from, to, rel); err != nil {
		return err
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditUnlink, ObjectIDs: []uuid.UUID{from, to}})
}

func (e *Engine) requireLinkEndpoint(ctx context.Context, scope Scope, id uuid.UUID, label string) error {
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return fmt.Errorf("brain: link %s: %w", label, err)
	}
	if obj.ParentID != nil {
		return fmt.Errorf("brain: link %s: %w: endpoint must be a first-class object (not a part)", label, ErrInvalid)
	}
	return nil
}

// HasGraphWriter reports whether Put dual-write and Link are available.
func (e *Engine) HasGraphWriter() bool {
	return e.graphW != nil
}

// preparePut fills defaults, validates obj against scope and the catalog, and
// embeds its index text. parent is the in-flight parent for PutTree parts;
// nil means a part's parent is read from the store and must be visible.
func (e *Engine) preparePut(ctx context.Context, scope Scope, obj Object, parent *Object) (Object, string, error) {
	if obj.DeletedAt != nil {
		return Object{}, "", fmt.Errorf("%w: put refuses soft-deleted objects; use SoftDelete", ErrInvalid)
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
	if err := ValidateObject(obj, e.catalog); err != nil {
		return Object{}, "", err
	}
	if !scope.Namespace.Covers(obj.Namespace) {
		return Object{}, "", fmt.Errorf("%w: object namespace %s is outside scope %s", ErrInvalid, obj.Namespace, scope.Namespace)
	}
	if obj.ParentID != nil && parent == nil {
		p, err := e.store.Get(ctx, scope, *obj.ParentID)
		if err != nil {
			return Object{}, "", fmt.Errorf("brain: parent %s: %w", *obj.ParentID, err)
		}
		if p.ParentID != nil {
			return Object{}, "", fmt.Errorf("%w: parent must be a first-class object", ErrInvalid)
		}
		parent = &p
	}
	text := e.indexText(obj, parent)
	if e.embedder != nil && text != "" {
		vec, err := e.embedOne(ctx, text)
		if err != nil {
			return Object{}, "", fmt.Errorf("brain: embed object: %w", err)
		}
		obj.Embedding = vec
		obj.EmbeddingModel = e.embedder.Model()
	}
	return obj, text, nil
}

// embed returns one vector per text, cloned so the embedder cannot change
// stored vectors later.
func (e *Engine) embed(ctx context.Context, texts []string) ([][]float32, error) {
	vecs, err := e.embedder.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(vecs) != len(texts) {
		return nil, fmt.Errorf("embedder returned %d vectors for %d texts", len(vecs), len(texts))
	}
	for i, v := range vecs {
		vecs[i] = slices.Clone(v)
	}
	return vecs, nil
}

func (e *Engine) embedOne(ctx context.Context, text string) ([]float32, error) {
	vecs, err := e.embed(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
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
