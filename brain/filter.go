package brain

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
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

// DecodeFilter maps a JSON object (tool boundary) onto Filter.
func DecodeFilter(m map[string]any) (Filter, error) {
	if len(m) == 0 {
		return Filter{}, nil
	}
	var f Filter
	for key, val := range m {
		k := strings.TrimSpace(key)
		if k == "" {
			return Filter{}, fmt.Errorf("brain: filter key is required")
		}
		switch k {
		case filterKind:
			sm, err := decodeStringMatch(k, val)
			if err != nil {
				return Filter{}, err
			}
			f.Kind = sm
		case filterTitle:
			sm, err := decodeStringMatch(k, val)
			if err != nil {
				return Filter{}, err
			}
			f.Title = sm
		case filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore:
			s, ok := val.(string)
			if !ok {
				return Filter{}, fmt.Errorf("brain: filter %q: want RFC3339 string, got %T", k, val)
			}
			if _, err := parseFilterTime(s); err != nil {
				return Filter{}, fmt.Errorf("brain: filter %q: %w", k, err)
			}
			switch k {
			case filterCreatedAfter:
				f.CreatedAfter = s
			case filterCreatedBefore:
				f.CreatedBefore = s
			case filterUpdatedAfter:
				f.UpdatedAfter = s
			case filterUpdatedBefore:
				f.UpdatedBefore = s
			}
		default:
			pf, err := decodePropFilter(k, val)
			if err != nil {
				return Filter{}, err
			}
			if f.Props == nil {
				f.Props = map[string]PropFilter{}
			}
			f.Props[k] = pf
		}
	}
	return f, nil
}

func decodeStringMatch(key string, val any) (StringMatch, error) {
	if err := validateEqValue(key, val); err != nil {
		return StringMatch{}, err
	}
	switch v := val.(type) {
	case string:
		return StringMatch{Eq: v}, nil
	case []any:
		out := make([]string, len(v))
		for i, item := range v {
			s, ok := item.(string)
			if !ok || s == "" {
				return StringMatch{}, fmt.Errorf("brain: filter %q list[%d] must be a non-empty string", key, i)
			}
			out[i] = s
		}
		return StringMatch{In: out}, nil
	default:
		return StringMatch{}, fmt.Errorf("brain: filter %q must be a string or list of strings", key)
	}
}

func decodePropFilter(key string, val any) (PropFilter, error) {
	if err := validateEqValue(key, val); err != nil {
		return PropFilter{}, err
	}
	if items, ok := val.([]any); ok {
		return PropFilter{In: items}, nil
	}
	return PropFilter{Eq: val}, nil
}

// ValidateFilters checks well-known fields and property value shapes. Empty is valid.
func ValidateFilters(f Filter) error {
	if err := validateStringMatch(filterKind, f.Kind); err != nil {
		return err
	}
	if err := validateStringMatch(filterTitle, f.Title); err != nil {
		return err
	}
	for _, pair := range []struct {
		key string
		val string
	}{
		{filterCreatedAfter, f.CreatedAfter},
		{filterCreatedBefore, f.CreatedBefore},
		{filterUpdatedAfter, f.UpdatedAfter},
		{filterUpdatedBefore, f.UpdatedBefore},
	} {
		if strings.TrimSpace(pair.val) == "" {
			continue
		}
		if _, err := parseFilterTime(pair.val); err != nil {
			return fmt.Errorf("brain: filter %q: %w", pair.key, err)
		}
	}
	for key, pf := range f.Props {
		k := strings.TrimSpace(key)
		if k == "" {
			return fmt.Errorf("brain: filter key is required")
		}
		if len(pf.In) > 0 {
			if err := validateEqValue(k, any(pf.In)); err != nil {
				return err
			}
			continue
		}
		if err := validateEqValue(k, pf.Eq); err != nil {
			return err
		}
	}
	return nil
}

func validateStringMatch(key string, s StringMatch) error {
	if !s.set() {
		return nil
	}
	if len(s.In) > 0 {
		list := make([]any, len(s.In))
		for i, v := range s.In {
			list[i] = v
		}
		return validateEqValue(key, list)
	}
	return validateEqValue(key, s.Eq)
}

// ValidateFiltersAgainst runs structural validation, then catalog rules when non-empty.
func ValidateFiltersAgainst(f Filter, cat *KindCatalog) error {
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

	for key, pf := range f.Props {
		if len(specs) == 0 {
			return fmt.Errorf("brain: property filters require a kind filter when kinds are registered")
		}
		val := propFilterValue(pf)
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

func propFilterValue(pf PropFilter) any {
	if len(pf.In) > 0 {
		return pf.In
	}
	return pf.Eq
}

func kindFilterValues(f Filter) ([]string, error) {
	if !f.Kind.set() {
		return nil, nil
	}
	if len(f.Kind.In) > 0 {
		return f.Kind.In, nil
	}
	if f.Kind.Eq == "" {
		return nil, fmt.Errorf("brain: filter %q value is required", filterKind)
	}
	return []string{f.Kind.Eq}, nil
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

func injectKindAllowList(f Filter, cat *KindCatalog) Filter {
	if f.Kind.set() {
		return f
	}
	out := f
	out.Kind = StringMatch{In: cat.Names()}
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

func parseFilterTime(s string) (time.Time, error) {
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

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
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

func cloneFilter(f Filter) Filter {
	out := f
	out.Kind.In = slices.Clone(f.Kind.In)
	out.Title.In = slices.Clone(f.Title.In)
	if f.Props != nil {
		out.Props = maps.Clone(f.Props)
	}
	return out
}
