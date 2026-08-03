package brain

import (
	"fmt"
	"strings"
	"time"
)

// filterPlan is the single compiled form of Filters used by Memory and Postgres.
// Compile once per query; Match for in-process stores, SQL for SQL stores.
type filterPlan struct {
	preds []filterPred
}

type filterPred struct {
	field string // kind | title | created_at | updated_at | prop:<key>
	op    string // eq | in | gte | lt
	// eq/in use scalars as strings for SQL text equality; Match uses original any.
	want    any
	wantIn  []any
	timeVal time.Time
}

// compileFilters validates and compiles filters. Empty/nil yields an empty plan.
func compileFilters(f Filters) (filterPlan, error) {
	if err := ValidateFilters(f); err != nil {
		return filterPlan{}, err
	}
	if len(f) == 0 {
		return filterPlan{}, nil
	}
	var preds []filterPred
	for key, val := range f {
		k := strings.TrimSpace(key)
		switch k {
		case filterCreatedAfter, filterUpdatedAfter:
			t, err := parseFilterTime(val)
			if err != nil {
				return filterPlan{}, fmt.Errorf("brain: filter %q: %w", k, err)
			}
			field := "created_at"
			if k == filterUpdatedAfter {
				field = "updated_at"
			}
			preds = append(preds, filterPred{field: field, op: "gte", timeVal: t})
		case filterCreatedBefore, filterUpdatedBefore:
			t, err := parseFilterTime(val)
			if err != nil {
				return filterPlan{}, fmt.Errorf("brain: filter %q: %w", k, err)
			}
			field := "created_at"
			if k == filterUpdatedBefore {
				field = "updated_at"
			}
			preds = append(preds, filterPred{field: field, op: "lt", timeVal: t})
		case filterKind, filterTitle:
			preds = append(preds, eqOrInPred(k, val))
		default:
			col := sanitizeJSONKey(k)
			if col == "" {
				return filterPlan{}, fmt.Errorf("brain: filter %q is not a valid property key", k)
			}
			preds = append(preds, eqOrInPred("prop:"+col, val))
		}
	}
	return filterPlan{preds: preds}, nil
}

func eqOrInPred(field string, val any) filterPred {
	if items, ok := val.([]any); ok {
		return filterPred{field: field, op: "in", wantIn: items}
	}
	return filterPred{field: field, op: "eq", want: val}
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

// sql builds " AND ..." clauses with placeholders starting at startArg.
// scope.Namespace is applied first when set.
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

	if scope.Namespace != nil {
		add("namespace_id = $%d", *scope.Namespace)
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
		// numbers and other scalars as text for JSONB ->> equality
		return fmt.Sprint(v)
	}
}

// objectMatchesFilters reports whether obj satisfies all filters (AND).
func objectMatchesFilters(obj Object, f Filters) bool {
	plan, err := compileFilters(f)
	if err != nil {
		return false
	}
	return plan.match(obj)
}

// filterSQL builds " AND ..." clauses with placeholders starting at startArg.
func filterSQL(scope Scope, filters Filters, startArg int) (string, []any, error) {
	plan, err := compileFilters(filters)
	if err != nil {
		return "", nil, err
	}
	return plan.sql(scope, startArg)
}
