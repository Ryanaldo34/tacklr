package brain

import (
	"fmt"
	"strings"
	"time"
)

// filterPlan is the single compiled form of Filter used by Memory and Postgres.
type filterPlan struct {
	preds []filterPred
}

type filterPred struct {
	field   string // kind | title | created_at | updated_at | prop:<key>
	op      string // eq | in | gte | lt
	want    any
	wantIn  []any
	timeVal time.Time
}

func compileFilters(f Filter) (filterPlan, error) {
	if err := ValidateFilters(f); err != nil {
		return filterPlan{}, err
	}
	if f.empty() {
		return filterPlan{}, nil
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
		t, err := parseFilterTime(bound.raw)
		if err != nil {
			return filterPlan{}, err
		}
		op := "lt"
		if bound.after {
			op = "gte"
		}
		preds = append(preds, filterPred{field: bound.field, op: op, timeVal: t})
	}
	for key, pf := range f.Props {
		col := sanitizeJSONKey(key)
		if col == "" {
			return filterPlan{}, fmt.Errorf("brain: filter %q is not a valid property key", key)
		}
		preds = append(preds, propPred("prop:"+col, pf))
	}
	return filterPlan{preds: preds}, nil
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

func (p filterPlan) match(obj Object) bool {
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
	switch p.op {
	case "gte":
		return !got.Before(p.timeVal)
	case "lt":
		return got.Before(p.timeVal)
	default:
		return false
	}
}

func (p filterPlan) sql(scope Scope, startArg int) (string, []any, error) {
	var b strings.Builder
	var args []any
	n := startArg
	add := func(frag string, v any) {
		args = append(args, v)
		b.WriteString(" AND ")
		b.WriteString(fmt.Sprintf(frag, n))
		n++
	}
	addIn := func(col string, vals []any) {
		if len(vals) == 0 {
			return
		}
		b.WriteString(" AND ")
		b.WriteString(col)
		b.WriteString(" IN (")
		for i, v := range vals {
			if i > 0 {
				b.WriteString(", ")
			}
			args = append(args, filterSQLValue(v))
			b.WriteString(fmt.Sprintf("$%d", n))
			n++
		}
		b.WriteString(")")
	}

	if !scope.Namespace.Empty() {
		raw, err := scope.Namespace.Value()
		if err != nil {
			return "", nil, fmt.Errorf("brain: marshal namespace: %w", err)
		}
		add("namespace @> $%d::jsonb", raw)
	}
	for _, pred := range p.preds {
		switch pred.field {
		case filterKind, filterTitle:
			col := pred.field
			if pred.op == "in" {
				addIn(col, pred.wantIn)
			} else {
				add(col+" = $%d", filterSQLValue(pred.want))
			}
		case "created_at", "updated_at":
			switch pred.op {
			case "gte":
				add(pred.field+" >= $%d", pred.timeVal)
			case "lt":
				add(pred.field+" < $%d", pred.timeVal)
			}
		default:
			col := strings.TrimPrefix(pred.field, "prop:")
			expr := "properties->>'" + col + "'"
			if pred.op == "in" {
				addIn(expr, pred.wantIn)
			} else {
				add(expr+" = $%d", filterSQLValue(pred.want))
			}
		}
	}
	return b.String(), args, nil
}

func filterSQLValue(v any) any {
	switch x := v.(type) {
	case string, bool:
		return x
	default:
		return fmt.Sprint(v)
	}
}

func objectMatchesFilters(obj Object, f Filter) bool {
	plan, err := compileFilters(f)
	if err != nil {
		return false
	}
	return plan.match(obj)
}

func filterSQL(scope Scope, filters Filter, startArg int) (string, []any, error) {
	plan, err := compileFilters(filters)
	if err != nil {
		return "", nil, err
	}
	return plan.sql(scope, startArg)
}
