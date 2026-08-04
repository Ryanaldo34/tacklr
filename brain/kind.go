package brain

import (
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
	Operators   []string  `json:"operators,omitempty"`
	Examples    []string  `json:"examples,omitempty"`
}

// KindSpec is the host-facing definition of a knowledge object kind.
// Registration is host/user-owned for determinism; agent-defined kinds are out of scope for now.
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
// Empty means open mode (legacy free-form filters and kinds).
type KindCatalog struct {
	mu     sync.RWMutex
	kinds  map[string]KindSpec
	frozen bool
}

func newKindCatalog() *KindCatalog {
	return &KindCatalog{kinds: make(map[string]KindSpec)}
}

// Empty reports whether no kinds are registered (open mode).
func (c *KindCatalog) Empty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.kinds) == 0
}

// Freeze prevents further Register / Replace.
func (c *KindCatalog) Freeze() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frozen = true
}

// Get returns a copy of the kind spec when present.
func (c *KindCatalog) Get(kind string) (KindSpec, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.kinds[kind]
	if !ok {
		return KindSpec{}, false
	}
	return s.clone(), true
}

// Names returns registered kind names sorted ascending.
func (c *KindCatalog) Names() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Sorted(maps.Keys(c.kinds))
}

// All returns copies of all kind specs sorted by kind name.
func (c *KindCatalog) All() []KindSpec {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := slices.Sorted(maps.Keys(c.kinds))
	out := make([]KindSpec, len(names))
	for i, n := range names {
		out[i] = c.kinds[n].clone()
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
		return fmt.Errorf("brain: kind catalog is frozen")
	}
	maps.Copy(c.kinds, batch)
	return nil
}

func (c *KindCatalog) replace(specs []KindSpec) error {
	batch, err := normalizeKindBatch(specs)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return fmt.Errorf("brain: kind catalog is frozen")
	}
	c.kinds = batch
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

// NormalizeKindSpec validates and fills defaults on a kind definition.
func NormalizeKindSpec(spec KindSpec) (KindSpec, error) {
	spec.Kind = strings.TrimSpace(spec.Kind)
	if spec.Kind == "" {
		return KindSpec{}, fmt.Errorf("brain: kind is required")
	}
	spec.Description = strings.TrimSpace(spec.Description)

	seen := make(map[string]struct{}, len(spec.Fields))
	fields := make([]FieldSpec, 0, len(spec.Fields))
	for _, f := range spec.Fields {
		nf, err := normalizeFieldSpec(f)
		if err != nil {
			return KindSpec{}, fmt.Errorf("brain: kind %q field: %w", spec.Kind, err)
		}
		if _, dup := seen[nf.Name]; dup {
			return KindSpec{}, fmt.Errorf("brain: kind %q: duplicate field %q", spec.Kind, nf.Name)
		}
		seen[nf.Name] = struct{}{}
		fields = append(fields, nf)
	}
	spec.Fields = fields
	return spec, nil
}

func normalizeFieldSpec(f FieldSpec) (FieldSpec, error) {
	f.Name = strings.TrimSpace(f.Name)
	if f.Name == "" {
		return FieldSpec{}, fmt.Errorf("name is required")
	}
	if isCoreFilterKey(f.Name) {
		return FieldSpec{}, fmt.Errorf("%q collides with a core filter key", f.Name)
	}
	if sanitizeJSONKey(f.Name) != f.Name {
		return FieldSpec{}, fmt.Errorf("%q is not a valid property key", f.Name)
	}
	switch f.Type {
	case FieldTypeString, FieldTypeNumber, FieldTypeBoolean, FieldTypeDateTime:
	case "":
		return FieldSpec{}, fmt.Errorf("field %q: type is required", f.Name)
	default:
		return FieldSpec{}, fmt.Errorf("field %q: unknown type %q", f.Name, f.Type)
	}

	if len(f.Operators) == 0 {
		if f.Type == FieldTypeBoolean {
			f.Operators = []string{"eq"}
		} else {
			f.Operators = []string{"eq", "in"}
		}
		return f, nil
	}

	ops := make([]string, 0, len(f.Operators))
	seen := map[string]struct{}{}
	for _, op := range f.Operators {
		op = strings.TrimSpace(strings.ToLower(op))
		switch {
		case op == "":
			continue
		case op == "eq":
		case op == "in" && f.Type != FieldTypeBoolean:
		default:
			return FieldSpec{}, fmt.Errorf("field %q: operator %q not allowed for type %s", f.Name, op, f.Type)
		}
		if _, ok := seen[op]; ok {
			continue
		}
		seen[op] = struct{}{}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return FieldSpec{}, fmt.Errorf("field %q: operators is empty", f.Name)
	}
	f.Operators = ops
	return f, nil
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
	raw, err := json.Marshal(spec.Fields)
	if err != nil {
		// FieldSpec is a plain struct graph; marshal fails only on programmer error.
		raw = json.RawMessage("[]")
	}
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
	fields, err := parseFilterableFields(k.FilterableFields)
	if err != nil {
		return KindSpec{}, fmt.Errorf("brain: kind %q: %w", k.Kind, err)
	}
	return NormalizeKindSpec(KindSpec{
		Kind:        k.Kind,
		Description: k.Description,
		IsParent:    k.IsParent,
		IsPart:      k.IsPart,
		Fields:      fields,
	})
}

func parseFilterableFields(raw json.RawMessage) ([]FieldSpec, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var fields []FieldSpec
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("filterable_fields: %w", err)
	}
	return fields, nil
}

// KindInfoFromSpec builds the agent-facing schema payload for one kind.
// Spec must already be normalized (catalog entries always are).
func KindInfoFromSpec(spec KindSpec) ObjectKindInfo {
	return KindInfoFrom(objectKindFromNormalized(spec))
}

func (s KindSpec) clone() KindSpec {
	if len(s.Fields) == 0 {
		return s
	}
	fields := make([]FieldSpec, len(s.Fields))
	for i, f := range s.Fields {
		fields[i] = f
		fields[i].Operators = slices.Clone(f.Operators)
		fields[i].Examples = slices.Clone(f.Examples)
	}
	s.Fields = fields
	return s
}
