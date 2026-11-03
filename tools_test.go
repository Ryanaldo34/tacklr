package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
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
		got := res.Output
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
		got := res.Output
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
		got := res.Output
		if err != nil {
			t.Fatal(err)
		}
		if got != "result" {
			t.Errorf("got %q, want %q", got, "result")
		}
	})

	t.Run("unmarshals pointer args", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "ptr_args", Handler: func(ctx context.Context, a *BasicArgs) (string, error) { return a.Name, nil }})
		res, err := tool.invoke(context.Background(), `{"name":"x"}`, nil)
		if err != nil || res.Output != "x" {
			t.Fatalf("got %q %v", res.Output, err)
		}
	})

	t.Run("an arg of the wrong type errors", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "basic", Handler: basicHandler})
		if _, err := tool.invoke(context.Background(), `{"name":123}`, nil); err == nil || !strings.Contains(err.Error(), "unmarshal args") {
			t.Fatalf("got %v, want an unmarshal error", err)
		}
	})

	t.Run("a result that cannot be marshaled errors", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "bad_out", Handler: func(ctx context.Context) (chan int, error) { return make(chan int), nil }})
		if _, err := tool.invoke(context.Background(), "", nil); err == nil {
			t.Fatal("want marshal result error")
		}
	})

	t.Run("handler that needs runtime fails closed without one", func(t *testing.T) {
		tool := NewTool(ToolConfig{
			Name: "needs_rt",
			Handler: func(ctx context.Context, cc *CallContext) (string, error) {
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
		if err == nil || !errors.Is(err, ErrCorrection) || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("got %v, want ErrCorrection wrapping boom", err)
		}
	})

	t.Run("bad json args errors", func(t *testing.T) {
		tool := NewTool(ToolConfig{Name: "basic", Handler: basicHandler})
		_, err := tool.invoke(context.Background(), `{bad`, nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("a tool past its timeout is a correction to narrow the request", func(t *testing.T) {
		for name, handler := range map[string]func(context.Context) (string, error){
			"one that stops on its deadline": func(ctx context.Context) (string, error) { <-ctx.Done(); return "", ctx.Err() },
			"one that ignores its deadline":  func(context.Context) (string, error) { time.Sleep(50 * time.Millisecond); return "late", nil },
		} {
			tool := NewTool(ToolConfig{Name: "slow", Timeout: 5 * time.Millisecond, Handler: handler})

			_, err := tool.invoke(context.Background(), "", nil)

			if !errors.Is(err, ErrToolTimeout) {
				t.Errorf("%s = %v, want ErrToolTimeout", name, err)
			}
		}
	})

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
		if s["type"] != "object" || s["additionalProperties"] != false {
			t.Errorf("expected {type:object, additionalProperties:false}, got %v", s)
		}
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

	t.Run("numbers and a recursive type", func(t *testing.T) {
		type node struct {
			Weight   float64 `json:"weight"`
			Children []node  `json:"children"`
		}
		tool := NewTool(ToolConfig{Name: "tree", Handler: func(context.Context, node) (string, error) { return "", nil }})
		s := asParams(t, tool)
		props := s["properties"].(map[string]any)
		if props["weight"].(map[string]any)["type"] != "number" {
			t.Errorf("weight = %v, want a number", props["weight"])
		}
		// The schema of a recursive type stops at a fixed depth.
		for depth := 0; ; depth++ {
			items, ok := props["children"].(map[string]any)["items"].(map[string]any)
			if !ok || items["type"] != "object" {
				if depth == 0 {
					t.Fatalf("children = %v, want nested objects", props["children"])
				}
				break
			}
			props = items["properties"].(map[string]any)
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

// TestToolsAsJson is the wire shape of a tool list: each tool a strict
// function with its schema, a namespaced name joined with "__", and an
// empty list as [].
func TestToolsAsJson(t *testing.T) {
	raw := ToolsAsJson([]*Tool{
		NewTool(ToolConfig{Name: "get_weather", Description: "Get the weather", Handler: basicHandler}),
		NewTool(ToolConfig{Name: "greet", Namespace: "ado", Handler: zeroArgsStringHandler}),
	})
	var parsed []map[string]any

	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatal(err)
	}
	if ToolsAsJson(nil) != "[]" {
		t.Fatalf("empty = %s", ToolsAsJson(nil))
	}
	first := parsed[0]
	if len(parsed) != 2 || first["type"] != "function" || first["name"] != "get_weather" || first["description"] != "Get the weather" || first["strict"] != true || first["parameters"] == nil {
		t.Fatalf("tool = %v", first)
	}
	if parsed[1]["name"] != "ado__greet" {
		t.Fatalf("namespaced name = %v", parsed[1]["name"])
	}
	if params, _ := parsed[1]["parameters"].(map[string]any); params == nil || params["type"] != "object" {
		t.Fatalf("a tool with no arguments has parameters %#v, want an empty object schema", parsed[1]["parameters"])
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
		{"a parameter after the arguments that is not the runtime", ToolConfig{Name: "t", Handler: func(context.Context, BasicArgs, string) (string, error) { return "", nil }}, "want *CallContext"},
		{"a nil on-call", ToolConfig{Name: "t", Handler: zeroArgsStringHandler, OnCall: []OnCallFunc{nil}}, "must not be nil"},
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

func assertRequired(t *testing.T, s map[string]any, vals ...string) {
	t.Helper()
	req := s["required"].([]string)
	for _, v := range vals {
		if !slices.Contains(req, v) {
			t.Errorf("required %v missing %q", req, v)
		}
	}
}

func asParams(t *testing.T, tool *Tool) map[string]any {
	t.Helper()
	return tool.AsJson()["parameters"].(map[string]any)
}

func TestCorrection_emptyMsgAndAlreadyWrapped(t *testing.T) {
	if got := Correction(nil, "  "); !errors.Is(got, ErrCorrection) || got.Error() != ErrCorrection.Error() {
		t.Fatalf("empty: %v", got)
	}
	fromCause := Correction(vfs.ErrNotExist, "")
	if !errors.Is(fromCause, ErrCorrection) || !errors.Is(fromCause, vfs.ErrNotExist) {
		t.Fatalf("cause: %v", fromCause)
	}
	if strings.Contains(fromCause.Error(), "vfs:") || fromCause.Error() != "not found" {
		t.Fatalf("stripped: %q", fromCause.Error())
	}
	first := Correctionf(vfs.ErrUseHTML, "write HTML, not plain text")
	again := Correction(first, "other")
	if again.Error() != first.Error() || !errors.Is(again, vfs.ErrUseHTML) {
		t.Fatalf("already wrapped: %v", again)
	}
	var nilAE *correctionError
	if nilAE.Error() != ErrCorrection.Error() {
		t.Fatal("nil receiver")
	}
	if (&correctionError{}).Error() != ErrCorrection.Error() {
		t.Fatal("empty msg")
	}
}
