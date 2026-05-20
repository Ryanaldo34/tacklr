package core

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Tool struct {
	Name        string
	Description string
	Namespace   string
	Handler     any

	handlerType reflect.Type
}

var timeType = reflect.TypeOf(time.Time{})
var harnessRuntimeType = reflect.TypeOf(HarnessRuntime{})

func typeToJSONSchema(rt reflect.Type, depth int, skipTypes ...reflect.Type) map[string]any {
	if depth > 10 {
		return map[string]any{"type": "string"}
	}
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}

	shouldSkip := func(ft reflect.Type) bool {
		for _, st := range skipTypes {
			if ft == st {
				return true
			}
		}
		return false
	}

	if rt.Kind() == reflect.Struct {
		if rt == timeType {
			return map[string]any{"type": "string"}
		}

		properties := map[string]any{}
		required := []string{}

		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if !f.IsExported() {
				continue
			}

			ft := f.Type
			for ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if shouldSkip(ft) {
				continue
			}

			tag := f.Tag.Get("json")
			if tag == "-" {
				continue
			}

			name := f.Name
			opts := ""
			if tag != "" {
				parts := strings.SplitN(tag, ",", 2)
				if parts[0] != "" {
					name = parts[0]
				}
				if len(parts) > 1 {
					opts = parts[1]
				}
			}

			if f.Anonymous && name == f.Name && f.Type.Kind() == reflect.Struct {
				emb := typeToJSONSchema(f.Type, depth+1, skipTypes...)
				if p, ok := emb["properties"].(map[string]any); ok {
					for k, v := range p {
						properties[k] = v
					}
				}
				if r, ok := emb["required"].([]string); ok {
					required = append(required, r...)
				}
				continue
			}

			isOptional := f.Type.Kind() == reflect.Ptr || opts == "omitempty"
			schema := typeToJSONSchema(f.Type, depth+1, skipTypes...)

			if desc := f.Tag.Get("desc"); desc != "" {
				schema["description"] = desc
			}
			if enum := f.Tag.Get("enum"); enum != "" {
				schema["enum"] = strings.Split(enum, ",")
			}

			properties[name] = schema
			if !isOptional {
				required = append(required, name)
			}
		}

		result := map[string]any{
			"type":                 "object",
			"properties":           properties,
			"additionalProperties": false,
		}
		if len(required) > 0 {
			result["required"] = required
		}
		return result
	}

	if rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
		return map[string]any{
			"type":  "array",
			"items": typeToJSONSchema(rt.Elem(), depth+1, skipTypes...),
		}
	}

	if rt.Kind() == reflect.Map {
		return map[string]any{
			"type":                 "object",
			"additionalProperties": typeToJSONSchema(rt.Elem(), depth+1, skipTypes...),
		}
	}

	jsonType := "string"
	switch rt.Kind() {
	case reflect.String:
		jsonType = "string"
	case reflect.Bool:
		jsonType = "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		jsonType = "integer"
	case reflect.Float32, reflect.Float64:
		jsonType = "number"
	}
	return map[string]any{"type": jsonType}
}

func TypeToJSONSchema(v any) (map[string]any, error) {
	rt := reflect.TypeOf(v)
	if rt == nil {
		return nil, fmt.Errorf("cannot generate JSON schema from nil value")
	}
	return typeToJSONSchema(rt, 0), nil
}

func (t *Tool) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("tool name is required")
	}
	fnType := reflect.TypeOf(t.Handler)
	if fnType == nil || fnType.Kind() != reflect.Func {
		return fmt.Errorf("handler must be a function")
	}
	if fnType.NumOut() != 2 {
		return fmt.Errorf("handler must return (T, error)")
	}
	if !fnType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf("handler must return (T, error)")
	}
	if fnType.NumIn() > 1 {
		return fmt.Errorf("handler must accept 0 or 1 arguments")
	}
	if fnType.NumIn() == 1 && fnType.In(0).Kind() != reflect.Struct {
		return fmt.Errorf("handler argument must be a struct")
	}
	if fnType.NumIn() == 1 {
		var check func(t reflect.Type) error
		check = func(t reflect.Type) error {
			for t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			switch t.Kind() {
			case reflect.String, reflect.Bool,
				reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				return nil
			case reflect.Struct:
				if t == timeType || t == harnessRuntimeType {
					return nil
				}
				for i := 0; i < t.NumField(); i++ {
					f := t.Field(i)
					if !f.IsExported() {
						continue
					}
					if err := check(f.Type); err != nil {
						return fmt.Errorf("field %q: %w", f.Name, err)
					}
				}
				return nil
			case reflect.Map:
				if t.Key().Kind() != reflect.String {
					return fmt.Errorf("map key must be string, got %s", t.Key().Kind())
				}
				return check(t.Elem())
			case reflect.Slice, reflect.Array:
				return check(t.Elem())
			default:
				return fmt.Errorf("type %s is not JSON-serializable", t.Kind())
			}
		}
		if err := check(fnType.In(0)); err != nil {
			return fmt.Errorf("argument contains unsupported type: %w", err)
		}
	}
	t.handlerType = fnType
	return nil
}

func (t *Tool) Invoke(argsJson string, runtime *HarnessRuntime) (string, error) {
	handlerExecutor := reflect.ValueOf(t.Handler)

	var args []reflect.Value
	if t.handlerType.NumIn() == 1 {
		argType := t.handlerType.In(0)
		argPtr := reflect.New(argType)
		if err := json.Unmarshal([]byte(argsJson), argPtr.Interface()); err != nil {
			return "", fmt.Errorf("unmarshal args: %w", err)
		}
		// Inject runtime into args
		for i := 0; i < argType.NumField(); i++ {
			f := argType.Field(i)
			if !f.IsExported() {
				continue
			}
			ft := f.Type
			isPtr := ft.Kind() == reflect.Ptr
			if isPtr {
				ft = ft.Elem()
			}
			if ft == harnessRuntimeType {
				if runtime == nil {
					break
				}
				if isPtr {
					argPtr.Elem().Field(i).Set(reflect.ValueOf(runtime))
				} else {
					argPtr.Elem().Field(i).Set(reflect.ValueOf(runtime).Elem())
				}
				break
			}
		}

		args = append(args, argPtr.Elem())
	}
	results := handlerExecutor.Call(args)
	if err, ok := results[1].Interface().(error); ok && err != nil {
		return "", err
	}
	if results[0].Kind() == reflect.String {
		return results[0].String(), nil
	}
	b, err := json.Marshal(results[0].Interface())
	if err != nil {
		return "", fmt.Errorf("marshal result: %w", err)
	}
	return string(b), nil
}

func (t *Tool) AsJson() (map[string]any, error) {
	params := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{},
		"additionalProperties": false,
	}
	if t.handlerType.NumIn() == 1 {
		params = typeToJSONSchema(t.handlerType.In(0), 0, harnessRuntimeType)
	}

	def := map[string]any{
		"type":        "function",
		"name":        t.Name,
		"description": t.Description,
		"strict":      true,
		"parameters":  params,
	}

	return def, nil
}

// ToolNamespace provides metadata for a group of related tools.
type ToolNamespace struct {
	Name        string
	Description string
}

// ToolsAsJson serializes tools into a JSON array for the OpenAI API tools field.
// Standalone tools (Namespace == "") are serialized as function definitions.
// Tools with a Namespace are grouped into namespace wrappers.
// ToolsAsJson serializes tools into a JSON array for the API tools field.
// Standalone tools (Namespace == "") are serialized as function definitions.
// Tools with a Namespace are grouped into namespace wrappers.
func ToolsAsJson(tools []*Tool) (string, error) {
	if len(tools) == 0 {
		return "[]", nil
	}

	standalone := make([]*Tool, 0)
	grouped := map[string][]*Tool{}
	for _, t := range tools {
		if t.Namespace == "" {
			standalone = append(standalone, t)
		} else {
			grouped[t.Namespace] = append(grouped[t.Namespace], t)
		}
	}

	defs := make([]map[string]any, 0, len(standalone)+len(grouped))
	for _, t := range standalone {
		def, _ := t.AsJson()
		defs = append(defs, def)
	}
	for ns, group := range grouped {
		toolDefs := make([]map[string]any, len(group))
		for i, t := range group {
			toolDefs[i], _ = t.AsJson()
		}
		defs = append(defs, map[string]any{
			"type":        "namespace",
			"name":        ns,
			"description": "",
			"tools":       toolDefs,
		})
	}

	b, err := json.Marshal(defs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
