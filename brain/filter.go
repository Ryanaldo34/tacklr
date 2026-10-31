package brain

import (
	"cmp"
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
			return Filter{}, fmt.Errorf("%w: filter key is required", ErrInvalid)
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
				return Filter{}, fmt.Errorf("%w: filter %q: want RFC3339 string, got %T", ErrInvalid, k, val)
			}
			if _, err := parseFilterTime(s); err != nil {
				return Filter{}, fmt.Errorf("%w: filter %q: %w", ErrInvalid, k, err)
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
			if !validPropertyKey(k) {
				return Filter{}, fmt.Errorf("%w: filter %q is not a valid property key", ErrInvalid, k)
			}
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
				return StringMatch{}, fmt.Errorf("%w: filter %q list[%d] must be a non-empty string", ErrInvalid, key, i)
			}
			out[i] = s
		}
		return StringMatch{In: out}, nil
	default:
		return StringMatch{}, fmt.Errorf("%w: filter %q must be a string or list of strings", ErrInvalid, key)
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

// Validate checks well-known fields and property value shapes. Empty is valid.
func (f Filter) Validate() error {
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
			return fmt.Errorf("%w: filter %q: %w", ErrInvalid, pair.key, err)
		}
	}
	for k, pf := range f.Props {
		if !validPropertyKey(k) {
			return fmt.Errorf("%w: filter %q is not a valid property key", ErrInvalid, k)
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

// ValidateAgainst runs structural validation, then catalog rules when non-empty.
func (f Filter) ValidateAgainst(cat *KindCatalog) error {
	if err := f.Validate(); err != nil {
		return err
	}
	if cat == nil || cat.Empty() {
		return nil
	}

	kinds := f.kindValues()
	var specs []KindSpec
	if len(kinds) > 0 {
		specs = make([]KindSpec, len(kinds))
		for i, name := range kinds {
			spec, ok := cat.Get(name)
			if !ok {
				return fmt.Errorf("%w: unknown kind %q; kinds: %s", ErrInvalid, name, strings.Join(cat.RecordKinds(), ", "))
			}
			specs[i] = spec
		}
	}

	for key, pf := range f.Props {
		if len(specs) == 0 {
			return fmt.Errorf("%w: filter %q needs a kind; kinds: %s", ErrInvalid, key, strings.Join(cat.RecordKinds(), ", "))
		}
		val := propFilterValue(pf)
		for _, spec := range specs {
			fs, ok := spec.Field(key)
			if !ok {
				return fmt.Errorf("%w: unknown filter %q for %s; filterable: %s", ErrInvalid, key, spec.Kind, cmp.Or(strings.Join(spec.fieldNames(), ", "), "none"))
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

func (f Filter) kindValues() []string {
	if len(f.Kind.In) > 0 {
		return f.Kind.In
	}
	if f.Kind.Eq != "" {
		return []string{f.Kind.Eq}
	}
	return nil
}

func checkFilterField(key string, val any, fs FieldSpec) error {
	if items, ok := val.([]any); ok {
		if fs.Type == FieldTypeBoolean {
			return fmt.Errorf("%w: filter %q does not support list values for boolean fields", ErrInvalid, key)
		}
		for i, item := range items {
			if err := checkFieldValue(item, fs.Type); err != nil {
				return fmt.Errorf("%w: filter %q: %w (list[%d])", ErrInvalid, key, err, i)
			}
		}
		return nil
	}
	if err := checkFieldValue(val, fs.Type); err != nil {
		return fmt.Errorf("%w: filter %q: %w", ErrInvalid, key, err)
	}
	return nil
}

func (f Filter) withKindAllowList(cat *KindCatalog) Filter {
	if f.Kind.set() {
		return f
	}
	out := f
	out.Kind = StringMatch{In: cat.Names()}
	return out
}

func validateEqValue(key string, val any) error {
	if val == nil {
		return fmt.Errorf("%w: filter %q value is required", ErrInvalid, key)
	}
	if isFilterScalar(val) {
		return nil
	}
	items, ok := val.([]any)
	if !ok {
		return fmt.Errorf("%w: filter %q has unsupported type %T", ErrInvalid, key, val)
	}
	if len(items) == 0 {
		return fmt.Errorf("%w: filter %q list is empty", ErrInvalid, key)
	}
	for i, item := range items {
		if !isFilterScalar(item) {
			return fmt.Errorf("%w: filter %q list[%d] has unsupported type %T", ErrInvalid, key, i, item)
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

// checkFieldValue checks a filter or property value against a registered field type.
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
		switch v := val.(type) {
		case string:
			_, err := parseFilterTime(v)
			return err
		case time.Time:
			if v.IsZero() {
				return fmt.Errorf("zero time")
			}
		default:
			return fmt.Errorf("want datetime string or time.Time, got %T", val)
		}
	}
	return nil
}

func (f Filter) clone() Filter {
	out := f
	out.Kind.In = slices.Clone(f.Kind.In)
	out.Title.In = slices.Clone(f.Title.In)
	if f.Props != nil {
		out.Props = maps.Clone(f.Props)
	}
	return out
}

// fieldNames are spec's field names, sorted; "none" when it has none.
