package brain

import (
	"strconv"
	"strings"
	"time"
)

// FilterPlan is the compiled form of a Filter. A backend evaluates it in
// process with Match, or translates its Conditions into its own query
// language (brain/postgres does so for SQL).
type FilterPlan struct {
	preds []filterPred
}

// FilterCondition is one condition of a FilterPlan; an object passes the
// plan when it meets every condition.
type FilterCondition struct {
	// Field is FieldKind, FieldTitle, FieldCreatedAt, FieldUpdatedAt, or a
	// property key when Property is set.
	Field    string
	Property bool
	// Op is OpEq (Values has one value), OpIn (any of Values), OpGte or
	// OpLt (against Time). Values keep their JSON types: a property matches
	// only a value of the same type (string, bool, or a number compared as
	// float64).
	Op     string
	Values []any
	Time   time.Time
}

// Filter condition fields and operators.
const (
	FieldKind      = "kind"
	FieldTitle     = "title"
	FieldCreatedAt = "created_at"
	FieldUpdatedAt = "updated_at"

	OpEq  = "eq"
	OpIn  = "in"
	OpGte = "gte"
	OpLt  = "lt"
)

// FilterValueKey is the canonical typed form of a scalar filter or property
// value: values a filter treats as equal (the same string, the same bool, or
// numbers equal as float64) have equal keys, and values of other types never
// do. A backend can index property values by key to match filters natively.
// A non-scalar value has no key.
func FilterValueKey(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return "s:" + x, true
	case bool:
		return "b:" + strconv.FormatBool(x), true
	}
	if f, ok := asFloat64(v); ok {
		return "n:" + strconv.FormatFloat(f, 'g', -1, 64), true
	}
	return "", false
}

// Conditions lists the plan's conditions.
func (p FilterPlan) Conditions() []FilterCondition {
	out := make([]FilterCondition, len(p.preds))
	for i, pred := range p.preds {
		c := FilterCondition{Field: pred.field, Op: pred.op, Time: pred.timeVal}
		if key, ok := strings.CutPrefix(pred.field, "prop:"); ok {
			c.Field, c.Property = key, true
		}
		switch pred.op {
		case OpEq:
			c.Values = []any{pred.want}
		case OpIn:
			c.Values = pred.wantIn
		}
		out[i] = c
	}
	return out
}

type filterPred struct {
	field   string // kind | title | created_at | updated_at | prop:<key>
	op      string // eq | in | gte | lt
	want    any
	wantIn  []any
	timeVal time.Time
}

// Compile validates f and compiles it.
func (f Filter) Compile() (FilterPlan, error) {
	if err := f.Validate(); err != nil {
		return FilterPlan{}, err
	}
	if f.empty() {
		return FilterPlan{}, nil
	}
	preds := make([]filterPred, 0, 6+len(f.Props))
	if f.Kind.set() {
		preds = append(preds, stringMatchPred(filterKind, f.Kind))
	}
	if f.Title.set() {
		preds = append(preds, stringMatchPred(filterTitle, f.Title))
	}
	for _, bound := range []struct {
		raw   string
		after bool
		field string
	}{
		{f.CreatedAfter, true, "created_at"},
		{f.CreatedBefore, false, "created_at"},
		{f.UpdatedAfter, true, "updated_at"},
		{f.UpdatedBefore, false, "updated_at"},
	} {
		if strings.TrimSpace(bound.raw) == "" {
			continue
		}
		t, _ := parseFilterTime(bound.raw) // ValidateFilters parsed it above
		op := "lt"
		if bound.after {
			op = "gte"
		}
		preds = append(preds, filterPred{field: bound.field, op: op, timeVal: t})
	}
	for key, pf := range f.Props {
		preds = append(preds, propPred("prop:"+key, pf)) // ValidateFilters checked key
	}
	return FilterPlan{preds: preds}, nil
}

func stringMatchPred(field string, s StringMatch) filterPred {
	if len(s.In) > 0 {
		in := make([]any, len(s.In))
		for i, v := range s.In {
			in[i] = v
		}
		return filterPred{field: field, op: "in", wantIn: in}
	}
	return filterPred{field: field, op: "eq", want: s.Eq}
}

func propPred(field string, pf PropFilter) filterPred {
	if len(pf.In) > 0 {
		return filterPred{field: field, op: "in", wantIn: pf.In}
	}
	return filterPred{field: field, op: "eq", want: pf.Eq}
}

// Match reports whether obj meets every condition.
func (p FilterPlan) Match(obj Object) bool {
	for _, pred := range p.preds {
		if !pred.match(obj) {
			return false
		}
	}
	return true
}

func (p filterPred) match(obj Object) bool {
	switch p.field {
	case filterKind:
		return matchWant(obj.Kind, p)
	case filterTitle:
		return matchWant(obj.Title, p)
	case "created_at":
		return matchTime(obj.CreatedAt, p)
	case "updated_at":
		return matchTime(obj.UpdatedAt, p)
	default:
		key := strings.TrimPrefix(p.field, "prop:")
		prop, ok := obj.Properties[key]
		if !ok {
			return false
		}
		return matchWant(prop, p)
	}
}

func matchWant(got any, p filterPred) bool {
	if p.op == "in" {
		return matchFilterValue(got, p.wantIn)
	}
	return matchFilterValue(got, p.want)
}

func matchTime(got time.Time, p filterPred) bool {
	if p.op == "gte" {
		return !got.Before(p.timeVal)
	}
	return got.Before(p.timeVal)
}

// validPropertyKey reports whether k is a non-empty run of letters, digits
// and underscores: a property key a filter can name, and safe in SQL.
func validPropertyKey(k string) bool { return k != "" && sanitizeJSONKey(k) == k }

func sanitizeJSONKey(k string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return -1
	}, k)
}
