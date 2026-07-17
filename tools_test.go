package tacklr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// ---- Handler types ----

type BasicArgs struct {
	Name string `json:"name" desc:"The name"`
	Age  int    `json:"age" desc:"The age"`
}

type TaggedArgs struct {
	Field    string  `json:"field_name" desc:"A field" enum:"a,b,c"`
	Optional *string `json:"optional,omitempty" desc:"Optional value"`
	Skipped  string  `json:"-"`
}

type NestedInner struct {
	Value string `json:"value"`
}

type NestedArgs struct {
	Inner  NestedInner    `json:"inner"`
	Items  []string       `json:"items"`
	Lookup map[string]int `json:"lookup"`
}

type WithTime struct {
	CreatedAt time.Time `json:"created_at" desc:"When created"`
}

type WithEmbedded struct {
	NestedInner
	Extra string `json:"extra"`
}

// ---- Handlers ----

func zeroArgsStringHandler() (string, error) { return "hello", nil }
func zeroArgsIntHandler() (int, error)       { return 42, nil }

func basicHandler(args BasicArgs) (string, error)       { return "ok", nil }
func taggedHandler(args TaggedArgs) (string, error)     { return "ok", nil }
func nestedHandler(args NestedArgs) (string, error)     { return "ok", nil }
func timeHandler(args WithTime) (string, error)         { return "ok", nil }
func embeddedHandler(args WithEmbedded) (string, error) { return "ok", nil }

type testErr struct{ msg string }

func (e testErr) Error() string { return e.msg }

func errHandler(args BasicArgs) (string, error) {
	return "", testErr{"boom"}
}

// ---- Tests ----

func TestInvoke(t *testing.T) {
	t.Run("returns raw string", func(t *testing.T) {
		tool := &Tool{Name: "zero", Handler: zeroArgsStringHandler}
		mustValidate(t, tool)
		got, err := tool.Invoke(context.Background(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != "hello" {
			t.Errorf("got %q, want %q", got, "hello")
		}
	})

	t.Run("marshals non-string return", func(t *testing.T) {
		tool := &Tool{Name: "zero_int", Handler: zeroArgsIntHandler}
		mustValidate(t, tool)
		got, err := tool.Invoke(context.Background(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != "42" {
			t.Errorf("got %q, want %q", got, "42")
		}
	})

	t.Run("unmarshals args and calls handler", func(t *testing.T) {
		h := func(args BasicArgs) (string, error) {
			if args.Name != "test" || args.Age != 10 {
				t.Errorf("unexpected args: %+v", args)
			}
			return "result", nil
		}
		tool := &Tool{Name: "handler", Handler: h}
		mustValidate(t, tool)
		got, err := tool.Invoke(context.Background(), `{"name":"test","age":10}`, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != "result" {
			t.Errorf("got %q, want %q", got, "result")
		}
	})

	t.Run("propagates handler error", func(t *testing.T) {
		tool := &Tool{Name: "err", Handler: errHandler}
		mustValidate(t, tool)
		_, err := tool.Invoke(context.Background(), `{"name":"x","age":1}`, nil)
		if err == nil || err.Error() != "boom" {
			t.Fatalf("got %v, want boom", err)
		}
	})

	t.Run("bad json args errors", func(t *testing.T) {
		tool := &Tool{Name: "basic", Handler: basicHandler}
		mustValidate(t, tool)
		_, err := tool.Invoke(context.Background(), `{bad`, nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestSchema(t *testing.T) {
	t.Run("basic fields with desc and required", func(t *testing.T) {
		tool := &Tool{Name: "basic", Handler: basicHandler}
		mustValidate(t, tool)
		s := asParams(t, tool)
		assertObject(t, s)
		props := s["properties"].(map[string]any)

		n := props["name"].(map[string]any)
		if n["type"] != "string" || n["description"] != "The name" {
			t.Errorf("name = %v", n)
		}
		a := props["age"].(map[string]any)
		if a["type"] != "integer" || a["description"] != "The age" {
			t.Errorf("age = %v", a)
		}

		assertRequired(t, s, "name", "age")
	})

	t.Run("tags: json rename, enum, skip, optional", func(t *testing.T) {
		tool := &Tool{Name: "tagged", Handler: taggedHandler}
		mustValidate(t, tool)
		s := asParams(t, tool)
		props := s["properties"].(map[string]any)

		if _, ok := props["Skipped"]; ok {
			t.Error(`json:"-" field should be skipped`)
		}

		f := props["field_name"].(map[string]any)
		if f["type"] != "string" || f["description"] != "A field" {
			t.Errorf("field = %v", f)
		}
		enumVals := f["enum"].([]string)
		if len(enumVals) != 3 || enumVals[0] != "a" || enumVals[1] != "b" || enumVals[2] != "c" {
			t.Errorf("enum = %v", enumVals)
		}

		assertRequired(t, s, "field_name")
		if contains(s["required"].([]string), "optional") {
			t.Error("optional (pointer+omitempty) should not be required")
		}
	})

	t.Run("nested structs, slices, and maps", func(t *testing.T) {
		tool := &Tool{Name: "nested", Handler: nestedHandler}
		mustValidate(t, tool)
		s := asParams(t, tool)
		props := s["properties"].(map[string]any)

		inner := props["inner"].(map[string]any)
		if inner["type"] != "object" {
			t.Errorf("inner type = %v", inner["type"])
		}
		innerProps := inner["properties"].(map[string]any)
		if innerProps["value"].(map[string]any)["type"] != "string" {
			t.Error("nested value type mismatch")
		}

		items := props["items"].(map[string]any)
		if items["type"] != "array" || items["items"].(map[string]any)["type"] != "string" {
			t.Errorf("items = %v", items)
		}

		lookup := props["lookup"].(map[string]any)
		if lookup["type"] != "object" || lookup["additionalProperties"].(map[string]any)["type"] != "integer" {
			t.Errorf("lookup = %v", lookup)
		}
	})

	t.Run("time.Time treated as string", func(t *testing.T) {
		tool := &Tool{Name: "time", Handler: timeHandler}
		mustValidate(t, tool)
		s := asParams(t, tool)
		created := s["properties"].(map[string]any)["created_at"].(map[string]any)
		if created["type"] != "string" || created["description"] != "When created" {
			t.Errorf("created_at = %v", created)
		}
	})

	t.Run("embedded struct flattened", func(t *testing.T) {
		tool := &Tool{Name: "embedded", Handler: embeddedHandler}
		mustValidate(t, tool)
		s := asParams(t, tool)
		props := s["properties"].(map[string]any)
		if _, ok := props["value"]; !ok {
			t.Error("embedded field not flattened")
		}
		if _, ok := props["extra"]; !ok {
			t.Error("own field missing")
		}
		assertRequired(t, s, "value", "extra")
	})
}

func TestAsJson(t *testing.T) {
	tool := &Tool{
		Name:        "get_weather",
		Description: "Get the weather",
		Handler:     basicHandler,
	}
	mustValidate(t, tool)
	def, err := tool.AsJson()
	if err != nil {
		t.Fatal(err)
	}

	if def["type"] != "function" {
		t.Errorf("type = %v", def["type"])
	}
	if def["name"] != "get_weather" {
		t.Errorf("name = %v", def["name"])
	}
	if def["description"] != "Get the weather" {
		t.Errorf("description = %v", def["description"])
	}
	if def["strict"] != true {
		t.Errorf("strict = %v", def["strict"])
	}
	if def["parameters"] == nil {
		t.Error("parameters missing")
	}
}

func TestToolsAsJson(t *testing.T) {
	tools := []*Tool{
		{Name: "a", Handler: zeroArgsStringHandler},
		{Name: "b", Handler: basicHandler},
	}
	for _, tool := range tools {
		mustValidate(t, tool)
	}
	raw, err := ToolsAsJson(tools)
	if err != nil {
		t.Fatal(err)
	}

	var parsed []map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 2 {
		t.Errorf("got %d tools, want 2", len(parsed))
	}
}

func TestToolsAsJsonWithNamespaces(t *testing.T) {
	tools := []*Tool{
		{Name: "standalone", Handler: zeroArgsStringHandler},
		{Name: "get_customer", Namespace: "crm", Handler: zeroArgsStringHandler},
	}
	for _, tool := range tools {
		mustValidate(t, tool)
	}

	raw, err := ToolsAsJson(tools)
	if err != nil {
		t.Fatal(err)
	}

	var parsed []map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 2 {
		t.Fatalf("got %d items, want 2", len(parsed))
	}
	if parsed[0]["type"] != "function" {
		t.Errorf("first item type = %v", parsed[0]["type"])
	}
	if parsed[0]["name"] != "standalone" {
		t.Errorf("first item name = %v, want standalone", parsed[0]["name"])
	}
	if parsed[1]["type"] != "function" {
		t.Errorf("second item type = %v", parsed[1]["type"])
	}
	if parsed[1]["name"] != "crm.get_customer" {
		t.Errorf("second item name = %v, want crm.get_customer", parsed[1]["name"])
	}
} // ---- Helpers ----

func assertObject(t *testing.T, s map[string]any) {
	t.Helper()
	if s["type"] != "object" || s["additionalProperties"] != false {
		t.Errorf("expected {type:object, additionalProperties:false}, got %v", s)
	}
}

func assertRequired(t *testing.T, s map[string]any, vals ...string) {
	t.Helper()
	req := s["required"].([]string)
	for _, v := range vals {
		if !contains(req, v) {
			t.Errorf("required %v missing %q", req, v)
		}
	}
}

func contains(slice []string, val string) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func mustValidate(t *testing.T, tool *Tool) {
	t.Helper()
	if err := tool.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func asParams(t *testing.T, tool *Tool) map[string]any {
	t.Helper()
	def, err := tool.AsJson()
	if err != nil {
		t.Fatalf("AsJson: %v", err)
	}
	return def["parameters"].(map[string]any)
}

func TestTool_Validate(t *testing.T) {
	t.Run("valid tool", func(t *testing.T) {
		tool := &Tool{
			Name:    "my_tool",
			Handler: zeroArgsStringHandler,
		}
		if err := tool.Validate(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("empty name", func(t *testing.T) {
		tool := &Tool{Handler: zeroArgsStringHandler}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "name") {
			t.Fatalf("got %v, want name error", err)
		}
	})

	t.Run("handler not a function", func(t *testing.T) {
		tool := &Tool{Name: "t", Handler: "not a func"}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "must be a function") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("handler wrong return count", func(t *testing.T) {
		tool := &Tool{Name: "t", Handler: func() string { return "" }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "must return") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("handler wrong return type", func(t *testing.T) {
		tool := &Tool{Name: "t", Handler: func() (string, string) { return "", "" }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "must return") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("handler arg not struct", func(t *testing.T) {
		tool := &Tool{Name: "t", Handler: func(s string) (string, error) { return s, nil }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "argument must be a struct") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("handler too many args", func(t *testing.T) {
		tool := &Tool{Name: "t", Handler: func(a, b BasicArgs) (string, error) { return "", nil }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "must accept 0 or 1") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("unsupported field type", func(t *testing.T) {
		type badArgs struct {
			Name string `json:"name"`
			Ch   chan int
		}
		tool := &Tool{Name: "t", Handler: func(args badArgs) (string, error) { return "", nil }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "not JSON-serializable") {
			t.Fatalf("got %v, want JSON-serializable error", err)
		}
	})

	t.Run("unsupported nested field type", func(t *testing.T) {
		type deep struct {
			Fn func() `json:"fn"`
		}
		type badNestedArgs struct {
			Name  string `json:"name"`
			Inner deep
		}
		tool := &Tool{Name: "t", Handler: func(args badNestedArgs) (string, error) { return "", nil }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "not JSON-serializable") {
			t.Fatalf("got %v, want JSON-serializable error", err)
		}
	})

	t.Run("pointer to unsupported type rejected", func(t *testing.T) {
		type ptrBadArgs struct {
			Fn *struct{ F func() }
		}
		tool := &Tool{Name: "t", Handler: func(args ptrBadArgs) (string, error) { return "", nil }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "not JSON-serializable") {
			t.Fatalf("got %v, want JSON-serializable error", err)
		}
	})

	t.Run("map with non-string key rejected", func(t *testing.T) {
		type mapArgs struct {
			Lookup map[int]string `json:"lookup"`
		}
		tool := &Tool{Name: "t", Handler: func(args mapArgs) (string, error) { return "", nil }}
		err := tool.Validate()
		if err == nil || !strings.Contains(err.Error(), "map key must be string") {
			t.Fatalf("got %v, want map key error", err)
		}
	})
}

func TestTypeToJSONSchema(t *testing.T) {
	t.Run("basic struct", func(t *testing.T) {
		type Simple struct {
			Name string `json:"name" desc:"The name"`
			Age  int    `json:"age" desc:"The age"`
		}
		s, err := TypeToJSONSchema(Simple{})
		if err != nil {
			t.Fatal(err)
		}
		assertObject(t, s)
		props := s["properties"].(map[string]any)
		n := props["name"].(map[string]any)
		if n["type"] != "string" || n["description"] != "The name" {
			t.Errorf("name = %v", n)
		}
		a := props["age"].(map[string]any)
		if a["type"] != "integer" || a["description"] != "The age" {
			t.Errorf("age = %v", a)
		}
		assertRequired(t, s, "name", "age")
	})

	t.Run("nested structs", func(t *testing.T) {
		type Inner struct {
			Value string `json:"value"`
		}
		type Outer struct {
			Inner Inner `json:"inner"`
		}
		s, err := TypeToJSONSchema(Outer{})
		if err != nil {
			t.Fatal(err)
		}
		props := s["properties"].(map[string]any)
		inner := props["inner"].(map[string]any)
		if inner["type"] != "object" {
			t.Errorf("inner type = %v", inner["type"])
		}
	})

	t.Run("embedded struct flattened", func(t *testing.T) {
		type Base struct {
			BaseName string `json:"base_name"`
		}
		type Extended struct {
			Base
			Extra string `json:"extra"`
		}
		s, err := TypeToJSONSchema(Extended{})
		if err != nil {
			t.Fatal(err)
		}
		props := s["properties"].(map[string]any)
		if _, ok := props["base_name"]; !ok {
			t.Error("embedded field not flattened")
		}
		if _, ok := props["extra"]; !ok {
			t.Error("own field missing")
		}
	})

	t.Run("nil value", func(t *testing.T) {
		_, err := TypeToJSONSchema(nil)
		if err == nil {
			t.Fatal("expected error for nil")
		}
	})

	t.Run("pointer type", func(t *testing.T) {
		type Simple struct {
			Name string `json:"name"`
		}
		s, err := TypeToJSONSchema(&Simple{})
		if err != nil {
			t.Fatal(err)
		}
		props := s["properties"].(map[string]any)
		if _, ok := props["name"]; !ok {
			t.Error("name field missing")
		}
	})

	t.Run("enum tag", func(t *testing.T) {
		type EnumStruct struct {
			Kind string `json:"kind" enum:"a,b,c"`
		}
		s, err := TypeToJSONSchema(EnumStruct{})
		if err != nil {
			t.Fatal(err)
		}
		props := s["properties"].(map[string]any)
		kind := props["kind"].(map[string]any)
		enumVals := kind["enum"].([]string)
		if len(enumVals) != 3 || enumVals[0] != "a" {
			t.Errorf("enum = %v", enumVals)
		}
	})
}
