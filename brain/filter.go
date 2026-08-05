package brain

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"
)

const (
	filterKind          = "kind"
	filterTitle         = "title"
	filterCreatedAfter  = "created_after"
	filterCreatedBefore = "created_before"
	filterUpdatedAfter  = "updated_after"
	filterUpdatedBefore = "updated_before"
)

// ValidateFilters checks filter keys and value shapes. Empty/nil is valid.
func ValidateFilters(f Filters) error {
	for key, val := range f {
		k := strings.TrimSpace(key)
		if k == "" {
			return fmt.Errorf("brain: filter key is required")
		}
		switch k {
		case filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore:
			if _, err := parseFilterTime(val); err != nil {
				return fmt.Errorf("brain: filter %q: %w", k, err)
			}
		default:
			if err := validateEqValue(k, val); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateFiltersAgainst runs structural validation, then catalog rules when non-empty.
func ValidateFiltersAgainst(f Filters, cat *KindCatalog) error {
	if err := ValidateFilters(f); err != nil {
		return err
	}
	if cat == nil || cat.Empty() {
		return nil
	}

	kinds, err := kindFilterValues(f)
	if err != nil {
		return err
	}
	// Resolve kinds once (avoid repeated catalog Get under property loops).
	var specs []KindSpec
	if len(kinds) > 0 {
		specs = make([]KindSpec, len(kinds))
		for i, name := range kinds {
			spec, ok := cat.Get(name)
			if !ok {
				return fmt.Errorf("brain: kind %q is not registered", name)
			}
			specs[i] = spec
		}
	}

	for key, val := range f {
		if isCoreFilterKey(key) {
			continue
		}
		if len(specs) == 0 {
			return fmt.Errorf("brain: property filters require a kind filter when kinds are registered")
		}
		for _, spec := range specs {
			fs, ok := spec.Field(key)
			if !ok {
				return fmt.Errorf("brain: property %q is not filterable on kind %q", key, spec.Kind)
			}
			if err := checkFilterField(key, val, fs); err != nil {
				return err
			}
		}
	}
	return nil
}

func kindFilterValues(f Filters) ([]string, error) {
	raw, ok := f[filterKind]
	if !ok {
		return nil, nil
	}
	switch v := raw.(type) {
	case string:
		if v == "" {
			return nil, fmt.Errorf("brain: filter %q value is required", filterKind)
		}
		return []string{v}, nil
	case []any:
		if len(v) == 0 {
			return nil, fmt.Errorf("brain: filter %q list is empty", filterKind)
		}
		out := make([]string, len(v))
		for i, item := range v {
			s, ok := item.(string)
			if !ok || s == "" {
				return nil, fmt.Errorf("brain: filter %q list[%d] must be a non-empty string", filterKind, i)
			}
			out[i] = s
		}
		return out, nil
	default:
		return nil, fmt.Errorf("brain: filter %q must be a string or list of strings", filterKind)
	}
}

func checkFilterField(key string, val any, fs FieldSpec) error {
	if items, ok := val.([]any); ok {
		if fs.Type == FieldTypeBoolean {
			return fmt.Errorf("brain: filter %q does not support list values for boolean fields", key)
		}
		for i, item := range items {
			if err := checkFieldValue(item, fs.Type); err != nil {
				return fmt.Errorf("brain: filter %q: %w (list[%d])", key, err, i)
			}
		}
		return nil
	}
	if err := checkFieldValue(val, fs.Type); err != nil {
		return fmt.Errorf("brain: filter %q: %w", key, err)
	}
	return nil
}

// injectKindAllowList returns filters restricted to registered kinds when kind is absent.
// If kind is already set, f is returned as-is (no clone).
func injectKindAllowList(f Filters, cat *KindCatalog) Filters {
	if f != nil {
		if _, ok := f[filterKind]; ok {
			return f
		}
	}
	out := maps.Clone(f)
	if out == nil {
		out = Filters{}
	}
	names := cat.Names()
	list := make([]any, len(names))
	for i, n := range names {
		list[i] = n
	}
	out[filterKind] = list
	return out
}

func validateEqValue(key string, val any) error {
	if val == nil {
		return fmt.Errorf("brain: filter %q value is required", key)
	}
	if isFilterScalar(val) {
		return nil
	}
	items, ok := val.([]any)
	if !ok {
		return fmt.Errorf("brain: filter %q has unsupported type %T", key, val)
	}
	if len(items) == 0 {
		return fmt.Errorf("brain: filter %q list is empty", key)
	}
	for i, item := range items {
		if !isFilterScalar(item) {
			return fmt.Errorf("brain: filter %q list[%d] has unsupported type %T", key, i, item)
		}
	}
	return nil
}

func isFilterScalar(v any) bool {
	switch v.(type) {
	case string, bool, float64, float32, int, int32, int64, json.Number:
		return true
	default:
		return false
	}
}

func parseFilterTime(val any) (time.Time, error) {
	s, ok := val.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("want RFC3339 string, got %T", val)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.DateOnly, s)
		if err != nil {
			return time.Time{}, fmt.Errorf("want RFC3339 or YYYY-MM-DD: %w", err)
		}
	}
	return t.UTC(), nil
}

// matchFilterValue reports whether got equals want, or is any element when want is a list.
func matchFilterValue(got, want any) bool {
	if items, ok := want.([]any); ok {
		for _, item := range items {
			if valuesEqual(got, item) {
				return true
			}
		}
		return false
	}
	return valuesEqual(got, want)
}

func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}
	if sa, ok := a.(string); ok {
		sb, ok := b.(string)
		return ok && sa == sb
	}
	if ba, ok := a.(bool); ok {
		bb, ok := b.(bool)
		return ok && ba == bb
	}
	fa, ok := asFloat64(a)
	if !ok {
		return false
	}
	fb, ok := asFloat64(b)
	return ok && fa == fb
}

// asFloat64 normalizes JSON/Go numeric literals for equality (int vs float64).
func asFloat64(v any) (float64, bool) {
	if n, ok := v.(float64); ok {
		return n, true
	}
	if n, ok := v.(float32); ok {
		return float64(n), true
	}
	if n, ok := v.(int); ok {
		return float64(n), true
	}
	if n, ok := v.(int32); ok {
		return float64(n), true
	}
	if n, ok := v.(int64); ok {
		return float64(n), true
	}
	if n, ok := v.(json.Number); ok {
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

func checkFieldValue(val any, t FieldType) error {
	switch t {
	case FieldTypeString:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("want string, got %T", val)
		}
	case FieldTypeNumber:
		if _, ok := asFloat64(val); !ok {
			return fmt.Errorf("want number, got %T", val)
		}
	case FieldTypeBoolean:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("want boolean, got %T", val)
		}
	case FieldTypeDateTime:
		if s, ok := val.(string); ok {
			_, err := parseFilterTime(s)
			return err
		}
		if tm, ok := val.(time.Time); ok {
			if tm.IsZero() {
				return fmt.Errorf("zero time")
			}
			return nil
		}
		return fmt.Errorf("want datetime string or time.Time, got %T", val)
	default:
		return fmt.Errorf("unknown type %q", t)
	}
	return nil
}
