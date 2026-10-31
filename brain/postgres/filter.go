package postgres

import (
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/brain"
)

// filterSQL compiles plan into a WHERE fragment that begins with AND, and its
// bound arguments from $startArg on. col maps a column (kind, title,
// properties, created_at, updated_at) to the SQL expression to test.
// Property keys are safe in SQL: Filter.Compile accepts only letters,
// digits and underscores.
func filterSQL(plan brain.FilterPlan, col func(string) string, startArg int) (string, []any) {
	var b strings.Builder
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", startArg+len(args)-1)
	}
	for _, c := range plan.Conditions() {
		expr := col(c.Field)
		if c.Property {
			expr = col("properties") + "->>'" + c.Field + "'"
		}
		b.WriteString(" AND ")
		switch c.Op {
		case brain.OpGte:
			b.WriteString(expr + " >= " + arg(c.Time))
		case brain.OpLt:
			b.WriteString(expr + " < " + arg(c.Time))
		case brain.OpIn:
			ph := make([]string, len(c.Values))
			for i, v := range c.Values {
				ph[i] = arg(sqlText(v))
			}
			b.WriteString(expr + " IN (" + strings.Join(ph, ", ") + ")")
		default:
			b.WriteString(expr + " = " + arg(sqlText(c.Values[0])))
		}
	}
	return b.String(), args
}

// sqlText is v as the text that properties->>'key' and the text columns
// compare against.
func sqlText(v any) any {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
