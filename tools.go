package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryanaldo34/tacklr/streaming"
)

type ToolPermission int

const (
	ReadPermission ToolPermission = iota
	WritePermission
	ExecutePermission
)

var ToolReadAccess = mapset.NewSet[ToolPermission](ReadPermission)
var ToolWriteAccess = mapset.NewSet[ToolPermission](WritePermission)
var ToolReadWriteAccess = mapset.NewSet[ToolPermission](ReadPermission, WritePermission)
var ToolExecuteAccess = mapset.NewSet[ToolPermission](ExecutePermission)
var ToolReadExecuteAccess = mapset.NewSet[ToolPermission](ReadPermission, ExecutePermission)
var ToolFullAccess = mapset.NewSet[ToolPermission](ReadPermission, WritePermission, ExecutePermission)

type ToolHandlerFunc func(ctx context.Context, args map[string]any, runtime *HarnessRuntime) (string, error)

type Tool struct {
	DisplayName string
	Name        string
	Description string
	Namespace   string
	Category    streaming.ToolCategory
	Access      mapset.Set[ToolPermission]

	handlerFunc func(ctx context.Context, args map[string]any, runtime *HarnessRuntime) (string, error)
	parameters  map[string]any
	strict      bool
}

type ToolConfig struct {
	Name        string
	Description string
	DisplayName string
	Namespace   string
	Category    streaming.ToolCategory
	Access      mapset.Set[ToolPermission]

	Handler any
}

type mcpToolConfig struct {
	Name        string
	Description string
	DisplayName string
	Namespace   string
	Schema      map[string]any
	Handler     ToolHandlerFunc
}

var timeType = reflect.TypeOf(time.Time{})
var harnessRuntimeType = reflect.TypeOf(HarnessRuntime{})
var ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()

func NewTool(cfg ToolConfig) *Tool {
	if cfg.Name == "" {
		panic("tool name is required")
	}

	fnType := reflect.TypeOf(cfg.Handler)
	if fnType == nil || fnType.Kind() != reflect.Func {
		panic(fmt.Sprintf("tool %q: handler must be a function", cfg.Name))
	}
	if fnType.NumOut() != 2 {
		panic(fmt.Sprintf("tool %q: handler must return (T, error)", cfg.Name))
	}
	if !fnType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic(fmt.Sprintf("tool %q: handler must return (T, error)", cfg.Name))
	}

	numIn := fnType.NumIn()
	if numIn > 3 {
		panic(fmt.Sprintf("tool %q: handler has too many parameters (%d)", cfg.Name, numIn))
	}

	idx := 0
	if numIn > 0 && fnType.In(0).Implements(ctxType) {
		idx = 1
	}

	var argsType reflect.Type
	var hasRuntime bool
	var runtimeIsPtr bool

	if idx < numIn {
		pType := fnType.In(idx)
		baseType := pType
		if baseType.Kind() == reflect.Ptr {
			baseType = baseType.Elem()
		}

		if baseType == harnessRuntimeType {
			hasRuntime = true
			runtimeIsPtr = pType.Kind() == reflect.Ptr
		} else if baseType.Kind() == reflect.Struct {
			argsType = baseType
			idx++
			if idx < numIn {
				rType := fnType.In(idx)
				rBase := rType
				if rBase.Kind() == reflect.Ptr {
					rBase = rBase.Elem()
				}
				if rBase == harnessRuntimeType {
					hasRuntime = true
					runtimeIsPtr = rType.Kind() == reflect.Ptr
				} else {
					panic(fmt.Sprintf("tool %q: unexpected parameter type %v", cfg.Name, rType))
				}
			}
		} else {
			panic(fmt.Sprintf("tool %q: handler parameter must be a struct or HarnessRuntime, got %v", cfg.Name, pType))
		}
	}

	t := &Tool{
		Name:        cfg.Name,
		DisplayName: cfg.DisplayName,
		Description: cfg.Description,
		Namespace:   cfg.Namespace,
		Category:    cfg.Category,
		Access:      cfg.Access,
		strict:      true,
	}
	if argsType != nil {
		t.parameters = typeToJSONSchema(argsType, 0)
	} else {
		t.parameters = map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		}
	}

	handlerValue := reflect.ValueOf(cfg.Handler)
	t.handlerFunc = func(ctx context.Context, args map[string]any, runtime *HarnessRuntime) (string, error) {
		var callArgs []reflect.Value

		callArgs = append(callArgs, reflect.ValueOf(ctx))

		if argsType != nil {
			argPtr := reflect.New(argsType)
			if len(args) > 0 {
				b, err := json.Marshal(args)
				if err != nil {
					return "", fmt.Errorf("marshal args: %w", err)
				}
				if err := json.Unmarshal(b, argPtr.Interface()); err != nil {
					return "", fmt.Errorf("unmarshal args: %w", err)
				}
			}
			callArgs = append(callArgs, argPtr.Elem())
		}

		if hasRuntime {
			if runtimeIsPtr {
				callArgs = append(callArgs, reflect.ValueOf(runtime))
			} else {
				callArgs = append(callArgs, reflect.ValueOf(*runtime))
			}
		}

		results := handlerValue.Call(callArgs)
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

	return t
}

func newMCPTool(cfg mcpToolConfig) *Tool {
	params := cfg.Schema
	if params == nil {
		params = map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		}
	}
	return &Tool{
		Name:        cfg.Name,
		DisplayName: cfg.DisplayName,
		Description: cfg.Description,
		Namespace:   cfg.Namespace,
		strict:      false,
		parameters:  params,
		handlerFunc: cfg.Handler,
	}
}

func (t *Tool) Invoke(ctx context.Context, argsJson string, runtime *HarnessRuntime) (string, error) {
	var args map[string]any
	if argsJson != "" {
		if err := json.Unmarshal([]byte(argsJson), &args); err != nil {
			return "", fmt.Errorf("unmarshal args for tool %q: %w", t.Name, err)
		}
	}
	return t.handlerFunc(ctx, args, runtime)
}

func (t *Tool) AsJson() (map[string]any, error) {
	params := t.parameters
	if params == nil {
		params = map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		}
	}
	def := map[string]any{
		"type":        "function",
		"name":        t.Name,
		"description": t.Description,
		"strict":      t.strict,
		"parameters":  params,
	}
	return def, nil
}

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

type ToolNamespace struct {
	Name        string
	Description string
}

func ToolsAsJson(tools []*Tool) (string, error) {
	if len(tools) == 0 {
		return "[]", nil
	}

	defs := make([]map[string]any, 0, len(tools))
	for _, t := range tools {
		def, err := t.AsJson()
		if err != nil {
			return "", fmt.Errorf("serialize tool %q: %w", t.Name, err)
		}
		if t.Namespace != "" {
			def["name"] = t.Namespace + "." + t.Name
		}
		defs = append(defs, def)
	}

	b, err := json.Marshal(defs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
