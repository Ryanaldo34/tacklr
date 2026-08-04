package brain

import (
	"fmt"
	"maps"
)

// ValidateFiltersAgainst runs structural validation, then catalog rules when the
// catalog is non-empty. When cat is nil or empty, behavior matches ValidateFilters.
func ValidateFiltersAgainst(f Filters, cat *KindCatalog) error {
	if err := ValidateFilters(f); err != nil {
		return err
	}
	if cat == nil || cat.Empty() {
		return nil
	}
	return validateFiltersStrict(f, cat)
}

func validateFiltersStrict(f Filters, cat *KindCatalog) error {
	kindNames, hasKind, err := kindFilterValues(f)
	if err != nil {
		return err
	}
	if hasKind {
		for _, name := range kindNames {
			if _, ok := cat.Get(name); !ok {
				return fmt.Errorf("brain: kind %q is not registered", name)
			}
		}
	}

	for key, val := range f {
		if isCoreFilterKey(key) {
			continue
		}
		if !hasKind {
			return fmt.Errorf("brain: property filters require a kind filter when kinds are registered")
		}
		for _, name := range kindNames {
			spec, _ := cat.Get(name)
			fs, ok := spec.Field(key)
			if !ok {
				return fmt.Errorf("brain: property %q is not filterable on kind %q", key, name)
			}
			if err := validateFilterValueType(key, val, fs); err != nil {
				return err
			}
		}
	}
	return nil
}

// kindFilterValues returns the kind names from f["kind"] when present.
func kindFilterValues(f Filters) (names []string, present bool, err error) {
	raw, ok := f[filterKind]
	if !ok {
		return nil, false, nil
	}
	switch v := raw.(type) {
	case string:
		if v == "" {
			return nil, true, fmt.Errorf("brain: filter %q value is required", filterKind)
		}
		return []string{v}, true, nil
	case []any:
		if len(v) == 0 {
			return nil, true, fmt.Errorf("brain: filter %q list is empty", filterKind)
		}
		out := make([]string, 0, len(v))
		for i, item := range v {
			s, ok := item.(string)
			if !ok || s == "" {
				return nil, true, fmt.Errorf("brain: filter %q list[%d] must be a non-empty string", filterKind, i)
			}
			out = append(out, s)
		}
		return out, true, nil
	default:
		return nil, true, fmt.Errorf("brain: filter %q must be a string or list of strings", filterKind)
	}
}

func validateFilterValueType(key string, val any, fs FieldSpec) error {
	check := func(v any) error {
		switch fs.Type {
		case FieldTypeString:
			if _, ok := v.(string); !ok {
				return fmt.Errorf("brain: filter %q wants string, got %T", key, v)
			}
		case FieldTypeNumber:
			if _, ok := asFloat(v); !ok {
				return fmt.Errorf("brain: filter %q wants number, got %T", key, v)
			}
		case FieldTypeBoolean:
			if _, ok := v.(bool); !ok {
				return fmt.Errorf("brain: filter %q wants boolean, got %T", key, v)
			}
		case FieldTypeDateTime:
			if _, err := parseFilterTime(v); err != nil {
				return fmt.Errorf("brain: filter %q: %w", key, err)
			}
		default:
			return fmt.Errorf("brain: filter %q has unknown field type %q", key, fs.Type)
		}
		return nil
	}

	items, isList := val.([]any)
	if !isList {
		return check(val)
	}
	if fs.Type == FieldTypeBoolean {
		return fmt.Errorf("brain: filter %q does not support list values for boolean fields", key)
	}
	for i, item := range items {
		if err := check(item); err != nil {
			return fmt.Errorf("%w (list[%d])", err, i)
		}
	}
	return nil
}

// injectKindAllowList copies f and sets kind to all catalog kinds when absent.
// cat must be non-empty.
func injectKindAllowList(f Filters, cat *KindCatalog) Filters {
	out := Filters{}
	if f != nil {
		out = maps.Clone(f)
	}
	if _, ok := out[filterKind]; ok {
		return out
	}
	names := cat.Names()
	list := make([]any, len(names))
	for i, n := range names {
		list[i] = n
	}
	out[filterKind] = list
	return out
}
