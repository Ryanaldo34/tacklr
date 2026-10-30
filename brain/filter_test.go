package brain_test

import (
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestFilters_malformedInputIsInvalid(t *testing.T) {
	decode := func(m map[string]any) error {
		_, err := brain.DecodeFilter(m)
		return err
	}
	for name, err := range map[string]error{
		"an empty key":                  decode(map[string]any{" ": "x"}),
		"a nested object":               decode(map[string]any{"stage": map[string]any{"nested": 1}}),
		"a missing value":               decode(map[string]any{"kind": nil}),
		"an empty list":                 decode(map[string]any{"stage": []any{}}),
		"a list of objects":             decode(map[string]any{"stage": []any{map[string]any{}}}),
		"a kind list with a number":     decode(map[string]any{"kind": []any{"Document", 1}}),
		"a title that is no string":     decode(map[string]any{"title": true}),
		"a date that is no string":      decode(map[string]any{"updated_after": 2024}),
		"a date in no known format":     decode(map[string]any{"created_before": "June 1st"}),
		"a hand-built bad date":         brain.ValidateFilters(brain.Filter{UpdatedAfter: "nope"}),
		"a hand-built empty key":        brain.ValidateFilters(brain.Filter{Props: map[string]brain.PropFilter{"": {Eq: "x"}}}),
		"a hand-built empty list":       brain.ValidateFilters(brain.Filter{Props: map[string]brain.PropFilter{"stage": {In: []any{nil}}}}),
		"a hand-built missing value":    brain.ValidateFilters(brain.Filter{Props: map[string]brain.PropFilter{"stage": {}}}),
		"a key with no usable letters":  decode(map[string]any{"!!": "x"}),
		"a key with a dash":             decode(map[string]any{"due-date": "x"}),
		"a hand-built key with a space": brain.ValidateFilters(brain.Filter{Props: map[string]brain.PropFilter{"due date": {Eq: "x"}}}),
		"a bad filter under a catalog":  brain.ValidateFiltersAgainst(brain.Filter{UpdatedAfter: "nope"}, nil),
	} {
		if !errors.Is(err, brain.ErrInvalid) {
			t.Errorf("%s = %v, want ErrInvalid", name, err)
		}
	}
}
