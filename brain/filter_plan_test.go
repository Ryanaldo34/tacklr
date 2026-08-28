package brain

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFilterPlan_listAndSQLParity(t *testing.T) {
	ns := MustNamespace("id", uuid.NewString())
	now := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	obj := Object{
		ID: uuid.New(), Kind: "Chunk", Title: "t",
		Namespace: ns, CreatedAt: now, UpdatedAt: now,
		Properties: map[string]any{"stage": "open"},
	}
	f := MustFilter(map[string]any{"kind": []any{"Chunk", "Doc"}, "stage": []any{"open", "closed"}})
	plan, err := compileFilters(f)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.match(obj) {
		t.Fatal("memory match list")
	}
	sql, args, err := plan.sql(Scope{Namespace: ns}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "kind IN") || !strings.Contains(sql, "properties->>'stage' IN") || !strings.Contains(sql, "namespace @>") {
		t.Fatalf("sql: %s", sql)
	}
	if len(args) < 3 {
		t.Fatalf("args: %v", args)
	}
}
