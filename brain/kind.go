package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
)

// FieldType is the closed set of property types for kind schemas.
type FieldType string

const (
	FieldTypeString   FieldType = "string"
	FieldTypeNumber   FieldType = "number"
	FieldTypeBoolean  FieldType = "boolean"
	FieldTypeDateTime FieldType = "datetime"
)

// FieldSpec describes one filterable (and later writable) property on a kind.
type FieldSpec struct {
	Name        string    `json:"name"`
	Type        FieldType `json:"type"`
	Description string    `json:"description,omitempty"`
	Required    bool      `json:"required,omitempty"`
	Operators   []string  `json:"operators,omitempty"` // always eq, or eq+in (see NormalizeKindSpec)
	Examples    []string  `json:"examples,omitempty"`
}

// KindSpec is the host-facing definition of a knowledge object kind.
type KindSpec struct {
	Kind        string
	Description string
	IsParent    bool
	IsPart      bool
	Fields      []FieldSpec
}

// Field returns the named field when present.
func (s KindSpec) Field(name string) (FieldSpec, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return FieldSpec{}, false
}

// KindCatalog is the process-local enforcement view of registered kinds.
// Empty means open mode. Specs are treated as immutable after registration.
type KindCatalog struct {
	mu     sync.RWMutex
	kinds  map[string]KindSpec
	frozen bool
}

func newKindCatalog() *KindCatalog {
	return &KindCatalog{kinds: make(map[string]KindSpec)}
}

func (c *KindCatalog) Empty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.kinds) == 0
}

func (c *KindCatalog) Freeze() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frozen = true
}

func (c *KindCatalog) Get(kind string) (KindSpec, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.kinds[kind]
	return s, ok
}

func (c *KindCatalog) Names() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Sorted(maps.Keys(c.kinds))
}

func (c *KindCatalog) All() []KindSpec {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := slices.Sorted(maps.Keys(c.kinds))
	out := make([]KindSpec, len(names))
	for i, n := range names {
		out[i] = c.kinds[n]
	}
	return out
}

func (c *KindCatalog) register(specs ...KindSpec) error {
	batch, err := normalizeKindBatch(specs)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return fmt.Errorf("%w: kind catalog is frozen", ErrInvalid)
	}
	maps.Copy(c.kinds, batch)
	return nil
}

func (c *KindCatalog) replace(specs []KindSpec) error {
	batch, err := normalizeKindBatch(specs)
	if err != nil {
		return err
	}
	return c.replaceNormalized(batch)
}

func (c *KindCatalog) replaceNormalized(batch map[string]KindSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return fmt.Errorf("%w: kind catalog is frozen", ErrInvalid)
	}
	if batch == nil {
		batch = map[string]KindSpec{}
	}
	c.kinds = batch
	return nil
}

func (c *KindCatalog) ensureNotFrozen() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.frozen {
		return fmt.Errorf("%w: kind catalog is frozen", ErrInvalid)
	}
	return nil
}

func normalizeKindBatch(specs []KindSpec) (map[string]KindSpec, error) {
	out := make(map[string]KindSpec, len(specs))
	for _, s := range specs {
		ns, err := NormalizeKindSpec(s)
		if err != nil {
			return nil, err
		}
		if _, dup := out[ns.Kind]; dup {
			return nil, fmt.Errorf("brain: duplicate kind %q", ns.Kind)
		}
		out[ns.Kind] = ns
	}
	return out, nil
}

// NormalizeKindSpec validates a kind and fills default operators (eq / eq+in).
func NormalizeKindSpec(spec KindSpec) (KindSpec, error) {
	spec.Kind = strings.TrimSpace(spec.Kind)
	if spec.Kind == "" {
		return KindSpec{}, fmt.Errorf("brain: kind is required")
	}
	if strings.Contains(spec.Kind, "/") || strings.Contains(spec.Kind, "..") {
		return KindSpec{}, fmt.Errorf("brain: kind %q must not contain '/' or '..'", spec.Kind)
	}
	spec.Description = strings.TrimSpace(spec.Description)

	seen := make(map[string]struct{}, len(spec.Fields))
	fields := make([]FieldSpec, 0, len(spec.Fields))
	for _, f := range spec.Fields {
		f.Name = strings.TrimSpace(f.Name)
		if f.Name == "" {
			return KindSpec{}, fmt.Errorf("brain: kind %q field: name is required", spec.Kind)
		}
		if isObjectColumn(f.Name) {
			return KindSpec{}, fmt.Errorf("brain: kind %q field: %q collides with an object column", spec.Kind, f.Name)
		}
		if isCoreFilterKey(f.Name) {
			return KindSpec{}, fmt.Errorf("brain: kind %q field: %q collides with a core filter key", spec.Kind, f.Name)
		}
		if sanitizeJSONKey(f.Name) != f.Name {
			return KindSpec{}, fmt.Errorf("brain: kind %q field: %q is not a valid property key", spec.Kind, f.Name)
		}
		switch f.Type {
		case FieldTypeString, FieldTypeNumber, FieldTypeBoolean, FieldTypeDateTime:
		case "":
			return KindSpec{}, fmt.Errorf("brain: kind %q field %q: type is required", spec.Kind, f.Name)
		default:
			return KindSpec{}, fmt.Errorf("brain: kind %q field %q: unknown type %q", spec.Kind, f.Name, f.Type)
		}
		if _, dup := seen[f.Name]; dup {
			return KindSpec{}, fmt.Errorf("brain: kind %q: duplicate field %q", spec.Kind, f.Name)
		}
		seen[f.Name] = struct{}{}
		// Operators are documentation for schema(); always the closed set we implement.
		if f.Type == FieldTypeBoolean {
			f.Operators = []string{"eq"}
		} else {
			f.Operators = []string{"eq", "in"}
		}
		fields = append(fields, f)
	}
	spec.Fields = fields
	return spec, nil
}

func isObjectColumn(k string) bool {
	switch k {
	case "title", "summary", "content":
		return true
	default:
		return false
	}
}

func isCoreFilterKey(k string) bool {
	switch k {
	case filterKind, filterTitle, filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore:
		return true
	default:
		return false
	}
}

// ObjectKindFromSpec maps a typed kind into the durable ObjectKind row shape.
func ObjectKindFromSpec(spec KindSpec) (ObjectKind, error) {
	spec, err := NormalizeKindSpec(spec)
	if err != nil {
		return ObjectKind{}, err
	}
	return objectKindFromNormalized(spec), nil
}

func objectKindFromNormalized(spec KindSpec) ObjectKind {
	raw, _ := json.Marshal(spec.Fields) // FieldSpec is plain JSON; marshal cannot fail
	return ObjectKind{
		Kind:             spec.Kind,
		Description:      spec.Description,
		IsPart:           spec.IsPart,
		IsParent:         spec.IsParent,
		FilterableFields: raw,
	}
}

// KindSpecFromObjectKind parses a registry row into a KindSpec.
func KindSpecFromObjectKind(k ObjectKind) (KindSpec, error) {
	var fields []FieldSpec
	if len(k.FilterableFields) > 0 {
		if err := json.Unmarshal(k.FilterableFields, &fields); err != nil {
			return KindSpec{}, fmt.Errorf("brain: kind %q filterable_fields: %w", k.Kind, err)
		}
	}
	return NormalizeKindSpec(KindSpec{
		Kind: k.Kind, Description: k.Description,
		IsParent: k.IsParent, IsPart: k.IsPart, Fields: fields,
	})
}

// KindInfoFromSpec builds the agent-facing schema payload for one kind.
func KindInfoFromSpec(spec KindSpec) ObjectKindInfo {
	return kindInfoFromObjectKind(objectKindFromNormalized(spec))
}

func kindInfoFromObjectKind(k ObjectKind) ObjectKindInfo {
	return ObjectKindInfo{
		Kind:             k.Kind,
		Description:      k.Description,
		IsPart:           k.IsPart,
		IsParent:         k.IsParent,
		Columns:          CoreColumns(),
		FilterableFields: k.FilterableFields,
	}
}

// KindRegistry is KindReader + KindWriter for durable kind schemas.
type KindRegistry interface {
	KindReader
	KindWriter
}

// PersistKinds upserts validated kind specs into any KindWriter (additive).
func PersistKinds(ctx context.Context, w KindWriter, specs ...KindSpec) error {
	if w == nil {
		return fmt.Errorf("brain: kind writer is required")
	}
	batch, err := normalizeKindBatch(specs)
	if err != nil {
		return err
	}
	return putKindBatch(ctx, w, batch)
}

// ApplyKinds is the host migration entry point: desired process catalog + optional durable upsert.
func (e *Engine) ApplyKinds(ctx context.Context, specs ...KindSpec) error {
	batch, err := normalizeKindBatch(specs)
	if err != nil {
		return err
	}
	if err := e.catalog.ensureNotFrozen(); err != nil {
		return err
	}
	if w, ok := e.store.(KindWriter); ok {
		if err := putKindBatch(ctx, w, batch); err != nil {
			return err
		}
	}
	return e.catalog.replaceNormalized(batch)
}

// SyncKindsToStore pushes the process catalog to the store.
func (e *Engine) SyncKindsToStore(ctx context.Context) error {
	w, ok := e.store.(KindWriter)
	if !ok {
		return fmt.Errorf("brain: store does not support kind writes")
	}
	return PersistKinds(ctx, w, e.catalog.All()...)
}

// LoadKindsFromStore replaces the process catalog from the store.
func (e *Engine) LoadKindsFromStore(ctx context.Context) error {
	rows, err := e.store.ListKinds(ctx)
	if err != nil {
		return err
	}
	specs := make([]KindSpec, 0, len(rows))
	for _, row := range rows {
		spec, err := KindSpecFromObjectKind(row)
		if err != nil {
			return err
		}
		specs = append(specs, spec)
	}
	return e.catalog.replace(specs)
}

func putKindBatch(ctx context.Context, w KindWriter, batch map[string]KindSpec) error {
	for _, name := range slices.Sorted(maps.Keys(batch)) {
		if err := w.PutKind(ctx, objectKindFromNormalized(batch[name])); err != nil {
			return fmt.Errorf("brain: persist kind %q: %w", name, err)
		}
	}
	return nil
}
