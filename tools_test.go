package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

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

func zeroArgsStringHandler(ctx context.Context) (string, error)              { return "hello", nil }
func zeroArgsIntHandler(ctx context.Context) (int, error)                    { return 42, nil }
func basicHandler(ctx context.Context, args BasicArgs) (string, error)       { return "ok", nil }
func taggedHandler(ctx context.Context, args TaggedArgs) (string, error)     { return "ok", nil }
func nestedHandler(ctx context.Context, args NestedArgs) (string, error)     { return "ok", nil }
func timeHandler(ctx context.Context, args WithTime) (string, error)         { return "ok", nil }
func embeddedHandler(ctx context.Context, args WithEmbedded) (string, error) { return "ok", nil }

type testErr struct{ msg string }

func (e testErr) Error() string { return e.msg }

func errHandler(ctx context.Context, args BasicArgs) (string, error) {
	return "", testErr{"boom"}
}

func TestInvoke(t *testing.T) {
	t.Run("returns raw string", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "zero", Handler: zeroArgsStringHandler})
		res, err := tool.invoke(context.Background(), "", nil)
		got := res.output
		if err != nil {
			t.Fatal(err)
		}
		if got != "hello" {
			t.Errorf("got %q, want %q", got, "hello")
		}
	})

	t.Run("marshals non-string return", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "zero_int", Handler: zeroArgsIntHandler})
		res, err := tool.invoke(context.Background(), "", nil)
		got := res.output
		if err != nil {
			t.Fatal(err)
		}
		if got != "42" {
			t.Errorf("got %q, want %q", got, "42")
		}
	})

	t.Run("unmarshals args and calls handler", func(t *testing.T) {
		h := func(ctx context.Context, args BasicArgs) (string, error) {
			if args.Name != "test" || args.Age != 10 {
				t.Errorf("unexpected args: %+v", args)
			}
			return "result", nil
		}
		tool := NewTool(ToolConfig{Name: "handler", Handler: h})
		res, err := tool.invoke(context.Background(), `{"name":"test","age":10}`, nil)
		got := res.output
		if err != nil {
			t.Fatal(err)
		}
		if got != "result" {
			t.Errorf("got %q, want %q", got, "result")
		}
	})

	t.Run("handler that needs runtime fails closed without one", func(t *testing.T) {
		tool := NewTool(ToolConfig{
			Name: "needs_rt",
			Handler: func(ctx context.Context, rt HarnessRuntime) (string, error) {
				return "ok", nil
			},
		})
		_, err := tool.invoke(context.Background(), "", nil)
		if err == nil || !errors.Is(err, ErrFailed) {
			t.Fatalf("nil runtime: %v", err)
		}
	})

	t.Run("propagates handler error", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "err", Handler: errHandler})
		_, err := tool.invoke(context.Background(), `{"name":"x","age":1}`, nil)
		if err == nil || err.Error() != "boom" {
			t.Fatalf("got %v, want boom", err)
		}
	})

	t.Run("bad json args errors", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "basic", Handler: basicHandler})
		_, err := tool.invoke(context.Background(), `{bad`, nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	// Parent cancel is a distinct Invoke outcome from tool Timeout (covered via harness).
	t.Run("parent cancellation is not reported as tool timeout", func(t *testing.T) {
		tool := NewTool(ToolConfig{
			Name:    "slow",
			Timeout: time.Second,
			Handler: func(ctx context.Context) (string, error) {
				<-ctx.Done()
				return "", ctx.Err()
			},
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := tool.invoke(ctx, "", nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if errors.Is(err, ErrToolTimeout) {
			t.Fatalf("got ErrToolTimeout, want parent cancellation: %v", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})
}

func TestSchema(t *testing.T) {
	t.Run("basic fields with desc and required", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "basic", Handler: basicHandler})
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
		tool := NewTool(ToolConfig{Name: "tagged", Handler: taggedHandler})
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

		// Strict tool schemas list every property in required; optionals are T|null.
		assertRequired(t, s, "field_name", "optional")
		opt := props["optional"].(map[string]any)
		optType, ok := opt["type"].([]any)
		if !ok || len(optType) != 2 {
			t.Fatalf("optional type should be [string,null], got %v", opt["type"])
		}
	})

	t.Run("nested structs, slices, and maps", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "nested", Handler: nestedHandler})
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
		tool := NewTool(ToolConfig{Name: "time", Handler: timeHandler})
		s := asParams(t, tool)
		created := s["properties"].(map[string]any)["created_at"].(map[string]any)
		if created["type"] != "string" || created["description"] != "When created" {
			t.Errorf("created_at = %v", created)
		}
	})

	t.Run("embedded struct flattened", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "embedded", Handler: embeddedHandler})
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
	tool := NewTool(ToolConfig{
		Name:        "get_weather",
		Description: "Get the weather",
		Handler:     basicHandler,
	})
	def := tool.AsJson()

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
	// Nil parameters map → default empty object schema (strict tool host shape).
	empty := (&Tool{Name: "bare"}).AsJson()
	params, _ := empty["parameters"].(map[string]any)
	if params == nil || params["type"] != "object" {
		t.Fatalf("nil params default = %#v", empty["parameters"])
	}
}

// TestMakeSchemaNullable_outcomes covers each rewrite branch of makeSchemaNullable
// (used for strict optional fields). Cheap pure paths that lift library total cover.
// TestTypeToJSONSchema_structuredOutput is the public structured-output
// helper: omitempty fields stay optional (non-strict).
func TestTypeToJSONSchema_structuredOutput(t *testing.T) {
	type hostOut struct {
		Title string  `json:"title" desc:"Headline"`
		Note  *string `json:"note,omitempty" desc:"Optional note"`
		Count int     `json:"count"`
	}
	schema, err := TypeToJSONSchema(hostOut{})
	if err != nil {
		t.Fatal(err)
	}
	if schema["type"] != "object" {
		t.Fatalf("type = %v", schema["type"])
	}
	props, _ := schema["properties"].(map[string]any)
	title, _ := props["title"].(map[string]any)
	if title["type"] != "string" || title["description"] != "Headline" {
		t.Fatalf("title = %#v", title)
	}
	note, _ := props["note"].(map[string]any)
	if note["type"] != "string" || note["description"] != "Optional note" {
		t.Fatalf("note must stay a plain string (non-strict), got %#v", note)
	}
	req, _ := schema["required"].([]string)
	if len(req) != 2 || req[0] != "title" || req[1] != "count" {
		t.Fatalf("required = %v, want title and count only", req)
	}
	_, err = TypeToJSONSchema(nil)
	if err == nil || !strings.Contains(err.Error(), "nil value") {
		t.Fatalf("want nil-value error, got %v", err)
	}
}

func TestToolsAsJson(t *testing.T) {
	tools := []*Tool{
		NewTool(ToolConfig{Name: "a", Handler: zeroArgsStringHandler}),
		NewTool(ToolConfig{Name: "b", Handler: basicHandler}),
	}
	raw := ToolsAsJson(tools)

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
		NewTool(ToolConfig{Name: "standalone", Handler: zeroArgsStringHandler}),
		NewTool(ToolConfig{Name: "get_customer", Namespace: "crm", Handler: func(ctx context.Context) (string, error) {
			return "", nil
		}}),
	}

	raw := ToolsAsJson(tools)

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
}

type unregisteredOnCall struct{}

func (unregisteredOnCall) TypeName() string           { return "not_registered_on_call" }
func (unregisteredOnCall) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (unregisteredOnCall) Return([]byte) error        { return nil }
func (unregisteredOnCall) Error() string              { return "unregistered" }

func TestNewTool_validation(t *testing.T) {
	if NewTool(ToolConfig{Name: "my_tool", Handler: zeroArgsStringHandler}) == nil {
		t.Fatal("valid tool")
	}
	for _, c := range []struct {
		name    string
		cfg     ToolConfig
		wantSub string
	}{
		{"empty name", ToolConfig{Handler: zeroArgsStringHandler}, ""},
		{"not a function", ToolConfig{Name: "t", Handler: "not a func"}, "must be a function"},
		{"wrong return count", ToolConfig{Name: "t", Handler: func() string { return "" }}, "must return"},
		{"wrong return type", ToolConfig{Name: "t", Handler: func() (string, string) { return "", "" }}, "must return"},
		{"arg not struct", ToolConfig{Name: "t", Handler: func(ctx context.Context, s string) (string, error) { return s, nil }}, "must be a struct"},
		{"too many args", ToolConfig{Name: "t", Handler: func(ctx context.Context, a BasicArgs, b BasicArgs, c BasicArgs) (string, error) {
			return "", nil
		}}, ""}, // "too many parameters" or "unexpected parameter"
		{"unregistered on-call", ToolConfig{Name: "t", Handler: zeroArgsStringHandler, OnCall: []OnCallFunc{func(ToolInvocation) Interrupt {
			return unregisteredOnCall{}
		}}}, "not registered"},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic")
				}
				if c.wantSub != "" && !strings.Contains(fmt.Sprint(r), c.wantSub) {
					t.Fatalf("got %v", r)
				}
			}()
			NewTool(c.cfg)
		})
	}
}

func TestNewTool_pointerArgsAndNonStringResultAndBadArgs(t *testing.T) {
	type args struct {
		Name string `json:"name"`
	}
	tool := NewTool(ToolConfig{
		Name: "ptr_args",
		Handler: func(ctx context.Context, a *args) (string, error) {
			if a == nil || a.Name == "" {
				return "empty", nil
			}
			return a.Name, nil
		},
	})
	res, err := tool.invoke(context.Background(), `{"name":"x"}`, nil)
	got := res.output
	if err != nil || got != "x" {
		t.Fatalf("got %q %v", got, err)
	}
	// Type mismatch in JSON → invoke error.
	_, err = tool.invoke(context.Background(), `{"name":123}`, nil)
	if err == nil {
		t.Fatal("want unmarshal error")
	}

	// Non-string result marshaled to JSON.
	tool2 := NewTool(ToolConfig{
		Name: "struct_out",
		Handler: func(ctx context.Context) (struct {
			N int `json:"n"`
		}, error) {
			return struct {
				N int `json:"n"`
			}{N: 7}, nil
		},
	})
	res, err = tool2.invoke(context.Background(), "", nil)
	got = res.output
	if err != nil || !strings.Contains(got, "7") {
		t.Fatalf("got %q %v", got, err)
	}

	// Unmarshallable result type → error.
	tool3 := NewTool(ToolConfig{
		Name: "bad_out",
		Handler: func(ctx context.Context) (chan int, error) {
			return make(chan int), nil
		},
	})
	_, err = tool3.invoke(context.Background(), "", nil)
	if err == nil {
		t.Fatal("want marshal result error")
	}
}

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

func asParams(t *testing.T, tool *Tool) map[string]any {
	t.Helper()
	return tool.AsJson()["parameters"].(map[string]any)
}
