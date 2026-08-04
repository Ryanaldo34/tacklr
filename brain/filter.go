package brain

import (
	"encoding/json"
	"fmt"
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
	if len(f) == 0 {
		return nil
	}
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

func matchFilterValue(got, want any) bool {
	if items, ok := want.([]any); ok {
		for _, item := range items {
			if scalarEqual(got, item) {
				return true
			}
		}
		return false
	}
	return scalarEqual(got, want)
}

func scalarEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}
	if af, aok := asFloat(a); aok {
		if bf, bok := asFloat(b); bok {
			return af == bf
		}
		return false
	}
	switch av := a.(type) {
	case string:
		bs, ok := b.(string)
		return ok && av == bs
	case bool:
		bb, ok := b.(bool)
		return ok && av == bb
	default:
		return false
	}
}

func asFloat(v any) (float64, bool) {
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
