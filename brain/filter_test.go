package brain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestValidateFilters_andMatch(t *testing.T) {
	if err := ValidateFilters(nil); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFilters(Filters{"kind": "Document", "stage": "open"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFilters(Filters{"updated_after": "2024-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFilters(Filters{"created_before": "2024-06-01"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFilters(Filters{"tags": []any{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFilters(Filters{"updated_after": "nope"}); err == nil {
		t.Fatal("bad time")
	}
	if err := ValidateFilters(Filters{"": "x"}); err == nil {
		t.Fatal("empty key")
	}
	if err := ValidateFilters(Filters{"x": map[string]any{"nested": 1}}); err == nil {
		t.Fatal("nested map")
	}

	ns := uuid.New()
	now := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	obj := Object{
		ID: uuid.New(), Kind: "Document", Title: "T",
		NamespaceID: ns, UpdatedAt: now, CreatedAt: now,
		Properties: map[string]any{"stage": "negotiation", "amount": 10.0},
	}
	if !objectMatchesFilters(obj, Filters{"kind": "Document", "stage": "negotiation"}) {
		t.Fatal("should match")
	}
	if objectMatchesFilters(obj, Filters{"stage": "closed"}) {
		t.Fatal("should not match stage")
	}
	if !objectMatchesFilters(obj, Filters{"updated_after": "2024-01-01T00:00:00Z"}) {
		t.Fatal("updated_after")
	}
	if objectMatchesFilters(obj, Filters{"updated_before": "2024-01-01T00:00:00Z"}) {
		t.Fatal("updated_before")
	}
	if !objectMatchesFilters(obj, Filters{"amount": []any{10, 20}}) {
		t.Fatal("list match")
	}
	if objectMatchesFilters(obj, Filters{"title": "Other"}) {
		t.Fatal("title mismatch")
	}
	if !objectMatchesFilters(obj, Filters{"created_after": "2024-01-01", "created_before": "2025-01-01"}) {
		t.Fatal("created bounds")
	}
	if objectMatchesFilters(obj, Filters{"missing": "x"}) {
		t.Fatal("missing property")
	}

	// Numeric / bool / string scalar equality (memory filter path).
	obj.Properties["n"] = 3
	obj.Properties["b"] = true
	obj.Properties["s"] = "hi"
	if !objectMatchesFilters(obj, Filters{"n": 3, "b": true, "s": "hi"}) {
		t.Fatal("scalar eq")
	}
	if !objectMatchesFilters(obj, Filters{"n": int64(3)}) {
		t.Fatal("int64 eq")
	}
	if !objectMatchesFilters(obj, Filters{"n": float32(3)}) {
		t.Fatal("float32 eq")
	}
	if objectMatchesFilters(obj, Filters{"n": "nope"}) {
		t.Fatal("type mismatch")
	}
	// Empty list filter is invalid at validate; empty list match is false at runtime.
	if objectMatchesFilters(obj, Filters{"stage": []any{}}) {
		t.Fatal("empty list")
	}
	// Bad date values fail match (not validate path).
	if objectMatchesFilters(obj, Filters{"updated_after": "bogus"}) {
		t.Fatal("bad date match")
	}
	if err := ValidateFilters(Filters{"kind": nil}); err == nil {
		t.Fatal("nil value")
	}
	if err := ValidateFilters(Filters{"kind": []any{}}); err == nil {
		t.Fatal("empty list validate")
	}
	if _, err := parseFilterTime(""); err == nil {
		t.Fatal("empty time")
	}
}
