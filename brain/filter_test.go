package brain

import (
	"encoding/json"
	"strings"
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
	if err := ValidateFilters(Filters{"kind": nil}); err == nil {
		t.Fatal("nil value")
	}
	if err := ValidateFilters(Filters{"kind": []any{}}); err == nil {
		t.Fatal("empty list validate")
	}
	if _, err := parseFilterTime(""); err == nil {
		t.Fatal("empty time")
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
	if objectMatchesFilters(obj, Filters{"stage": []any{}}) {
		t.Fatal("empty list")
	}
	if objectMatchesFilters(obj, Filters{"updated_after": "bogus"}) {
		t.Fatal("bad date match")
	}
}

func TestCheckFieldValue_types(t *testing.T) {
	if err := checkFieldValue("ok", FieldTypeString); err != nil {
		t.Fatal(err)
	}
	if err := checkFieldValue(true, FieldTypeBoolean); err != nil {
		t.Fatal(err)
	}
	if err := checkFieldValue(json.Number("1.5"), FieldTypeNumber); err != nil {
		t.Fatal(err)
	}
	if err := checkFieldValue(int32(2), FieldTypeNumber); err != nil {
		t.Fatal(err)
	}
	if err := checkFieldValue(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), FieldTypeDateTime); err != nil {
		t.Fatal(err)
	}
	if err := checkFieldValue(time.Time{}, FieldTypeDateTime); err == nil {
		t.Fatal("zero time")
	}
	if err := checkFieldValue(3, FieldTypeString); err == nil || !strings.Contains(err.Error(), "string") {
		t.Fatalf("%v", err)
	}
	if err := checkFieldValue("x", FieldTypeBoolean); err == nil {
		t.Fatal("bool")
	}
	if err := checkFieldValue(struct{}{}, FieldTypeNumber); err == nil {
		t.Fatal("number")
	}
	if err := checkFieldValue(1, FieldType("nope")); err == nil {
		t.Fatal("unknown type")
	}
}
