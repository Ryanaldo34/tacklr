package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Tool definitions, JSON schema, and post-tool ACM result hooks.

type ToolPermission uint8

const (
	ReadPermission ToolPermission = 1 << iota
	WritePermission
	ExecutePermission
)

// ToolAccess is an immutable permission bitmask. Zero allows nothing.
type ToolAccess uint8

const (
	ToolReadAccess        ToolAccess = ToolAccess(ReadPermission)
	ToolWriteAccess       ToolAccess = ToolAccess(WritePermission)
	ToolReadWriteAccess   ToolAccess = ToolAccess(ReadPermission | WritePermission)
	ToolExecuteAccess     ToolAccess = ToolAccess(ExecutePermission)
	ToolReadExecuteAccess ToolAccess = ToolAccess(ReadPermission | ExecutePermission)
	ToolFullAccess        ToolAccess = ToolAccess(ReadPermission | WritePermission | ExecutePermission)
)

// Allows reports whether a includes p.
func (a ToolAccess) Allows(p ToolPermission) bool { return a&ToolAccess(p) != 0 }

type ToolHandlerFunc func(ctx context.Context, args map[string]any, runtime HarnessRuntime) (string, error)

// toolCallResult is the internal success path for a single tool invoke.
type toolCallResult struct {
	output string
	disp   ToolOutcome
}

// Tool is a registered harness tool. Construct with NewTool(ToolConfig{...}).
// Fields are unexported; hosts read metadata through the getters below.
type Tool struct {
	displayName string
	name        string
	description string
	namespace   string
	category    streaming.ToolCategory
	access      ToolAccess
	// timeout is an optional per-invocation deadline. Zero means none.
	timeout time.Duration
	// onCall is the pre-invoke middleware stack. Each constructor may park.
	onCall []OnCallFunc

	handlerFunc func(ctx context.Context, args map[string]any, runtime HarnessRuntime) (toolCallResult, error)
	parameters  map[string]any
	strict      bool
}

// Name is the programmatic tool name presented to the model.
func (t *Tool) Name() string { return t.name }

// DisplayName is the optional human title from ToolConfig. Empty means unset;
// stream titles fall back to Name via ResolveToolTitle.
func (t *Tool) DisplayName() string { return t.displayName }

// Description is the model-facing tool description.
func (t *Tool) Description() string { return t.description }

// Namespace is the optional tool namespace (MCP server name, host grouping).
func (t *Tool) Namespace() string { return t.namespace }

// Category is the coarse streaming category for client presentation.
func (t *Tool) Category() streaming.ToolCategory { return t.category }

// Access is the permission bitmask for this tool.
func (t *Tool) Access() ToolAccess { return t.access }

// Timeout is the optional per-invocation deadline. Zero means none.
func (t *Tool) Timeout() time.Duration { return t.timeout }

type ToolConfig struct {
	Name        string
	Description string
	DisplayName string
	Namespace   string
	Category    streaming.ToolCategory
	Access      ToolAccess
	Timeout     time.Duration
	// OnCall is the pre-invoke middleware stack. Each constructor may park.
	// Return nil from a constructor to skip that layer. Types must be registered.
	OnCall []OnCallFunc

	// Handler is a Go function. Close over clients in the constructor that calls NewTool.
	// Optional parameters: context.Context, an args struct, HarnessRuntime.
	Handler any
}

// OnCallFunc builds a pre-invoke interrupt. Return nil to skip that layer.
type OnCallFunc func(ToolInvocation) Interrupt

type mcpToolConfig struct {
	Name        string
	Description string
	DisplayName string
	Namespace   string
	Schema      map[string]any
	Timeout     time.Duration
	Handler     ToolHandlerFunc
}

var timeType = reflect.TypeOf(time.Time{})
var harnessRuntimeType = reflect.TypeOf((*HarnessRuntime)(nil)).Elem()
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
	// Handlers are at most (ctx, args, runtime). Extra params are rejected below
	// when the third parameter is not HarnessRuntime (or via count when >3).
	if numIn > 3 {
		panic(fmt.Sprintf("tool %q: handler has too many parameters (%d)", cfg.Name, numIn))
	}

	idx := 0
	if numIn > 0 && fnType.In(0).Implements(ctxType) {
		idx = 1
	}

	var argsType reflect.Type
	var argsIsPtr bool
	var hasRuntime bool

	if idx < numIn {
		pType := fnType.In(idx)
		if pType == harnessRuntimeType {
			hasRuntime = true
		} else {
			baseType := pType
			if baseType.Kind() == reflect.Ptr {
				baseType = baseType.Elem()
			}
			if baseType.Kind() == reflect.Struct {
				argsType = baseType
				argsIsPtr = pType.Kind() == reflect.Ptr
				idx++
				if idx < numIn {
					rType := fnType.In(idx)
					if rType != harnessRuntimeType {
						panic(fmt.Sprintf("tool %q: unexpected parameter type %v (want HarnessRuntime)", cfg.Name, rType))
					}
					hasRuntime = true
				}
			} else {
				panic(fmt.Sprintf("tool %q: handler parameter must be a struct or HarnessRuntime, got %v", cfg.Name, pType))
			}
		}
	}

	t := &Tool{
		name:        cfg.Name,
		displayName: cfg.DisplayName,
		description: cfg.Description,
		namespace:   cfg.Namespace,
		category:    cfg.Category,
		access:      cfg.Access,
		timeout:     cfg.Timeout,
		onCall:      cfg.OnCall,
		strict:      true,
	}
	for _, ctor := range cfg.OnCall {
		if ctor == nil {
			continue
		}
		if sample := ctor(ToolInvocation{Tool: t}); sample != nil {
			if _, ok := interrupt.New(sample.TypeName()); !ok {
				panic(fmt.Sprintf("tool %q: OnCall type %q is not registered", cfg.Name, sample.TypeName()))
			}
		}
	}
	if argsType != nil {
		// strict:true tools require every properties key in required (OpenAI / DeepSeek).
		t.parameters = typeToJSONSchema(argsType, 0, true)
	} else {
		t.parameters = map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
			"required":             []string{},
		}
	}

	handlerValue := reflect.ValueOf(cfg.Handler)
	t.handlerFunc = func(ctx context.Context, args map[string]any, runtime HarnessRuntime) (toolCallResult, error) {
		var callArgs []reflect.Value

		callArgs = append(callArgs, reflect.ValueOf(ctx))

		if argsType != nil {
			argPtr := reflect.New(argsType)
			if len(args) > 0 {
				b, err := json.Marshal(args)
				if err != nil {
					return toolCallResult{}, fmt.Errorf("marshal args: %w", err)
				}
				if err := json.Unmarshal(b, argPtr.Interface()); err != nil {
					return toolCallResult{}, fmt.Errorf("unmarshal args: %w", err)
				}
			}
			if argsIsPtr {
				callArgs = append(callArgs, argPtr)
			} else {
				callArgs = append(callArgs, argPtr.Elem())
			}
		}

		if hasRuntime {
			if runtime == nil {
				return toolCallResult{}, fmt.Errorf("%w: tool %q requires a HarnessRuntime", ErrFailed, t.name)
			}
			callArgs = append(callArgs, reflect.ValueOf(runtime))
		}

		results := handlerValue.Call(callArgs)
		if err, ok := results[1].Interface().(error); ok && err != nil {
			return toolCallResult{}, err
		}
		if results[0].Kind() == reflect.String {
			return toolCallResult{output: results[0].String()}, nil
		}
		if br, ok := results[0].Interface().(ToolOutcome); ok {
			return toolCallResult{output: br.Output, disp: br}, nil
		}
		b, err := json.Marshal(results[0].Interface())
		if err != nil {
			return toolCallResult{}, fmt.Errorf("marshal result: %w", err)
		}
		return toolCallResult{output: string(b)}, nil
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
		name:        cfg.Name,
		displayName: cfg.DisplayName,
		description: cfg.Description,
		namespace:   cfg.Namespace,
		timeout:     cfg.Timeout,
		strict:      false,
		parameters:  params,
		handlerFunc: func(ctx context.Context, args map[string]any, runtime HarnessRuntime) (toolCallResult, error) {
			out, err := cfg.Handler(ctx, args, runtime)
			return toolCallResult{output: out}, err
		},
	}
}

// titleParamRE matches {param} placeholders (top-level JSON arg keys).
var titleParamRE = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// ResolveToolTitle fills {param} in DisplayName from top-level string args.
// Empty displayName → toolName. Missing/non-string args → empty slot.
func ResolveToolTitle(displayName, toolName, argsJSON string) string {
	if displayName == "" {
		return toolName
	}
	if !strings.Contains(displayName, "{") {
		return displayName
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return displayName
	}
	return titleParamRE.ReplaceAllStringFunc(displayName, func(m string) string {
		key := m[1 : len(m)-1]
		s, _ := args[key].(string)
		return s
	})
}

// invoke runs the tool handler. Only the harness tool runner should call this
// so interceptors, permissions, and effects apply.
func (t *Tool) invoke(ctx context.Context, argsJson string, runtime HarnessRuntime) (toolCallResult, error) {
	res, err := t.invokeRaw(ctx, argsJson, runtime)
	if err != nil {
		return res, presentToolError(t.name, err)
	}
	return res, nil
}

func (t *Tool) invokeRaw(ctx context.Context, argsJson string, runtime HarnessRuntime) (toolCallResult, error) {
	var args map[string]any
	if argsJson != "" {
		if err := json.Unmarshal([]byte(argsJson), &args); err != nil {
			return toolCallResult{}, Correctionf(ErrInvalid, "%s: arguments were not valid JSON. Check the tool schema and retry with a JSON object", t.name)
		}
	}

	if t.timeout <= 0 {
		return t.handlerFunc(ctx, args, runtime)
	}

	callCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	type outcome struct {
		res toolCallResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := t.handlerFunc(callCtx, args, runtime)
		done <- outcome{res: res, err: err}
	}()

	select {
	case res := <-done:
		if res.err != nil && ctx.Err() == nil && errors.Is(res.err, context.DeadlineExceeded) {
			return toolCallResult{}, Correctionf(ErrToolTimeout, "%s timed out. Retry with a smaller request, fewer URLs, or a narrower search", t.name)
		}
		return res.res, res.err
	case <-callCtx.Done():
		if err := ctx.Err(); err != nil {
			return toolCallResult{}, err
		}
		return toolCallResult{}, Correctionf(ErrToolTimeout, "%s timed out. Retry with a smaller request, fewer URLs, or a narrower search", t.name)
	}
}

// AsJson returns the OpenAI-style function tool definition for this tool.
// parameters is never nil on the returned map.
func (t *Tool) AsJson() map[string]any {
	params := t.parameters
	if params == nil {
		params = map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		}
	}
	return map[string]any{
		"type":        "function",
		"name":        t.name,
		"description": t.description,
		"strict":      t.strict,
		"parameters":  params,
	}
}

// typeToJSONSchema builds a JSON Schema for rt.
// When strict is true (OpenAI/DeepSeek function tools), every property name is
// listed in required, and omitempty/pointer fields are typed as T|null so the
// model may pass null for unused optionals.
func typeToJSONSchema(rt reflect.Type, depth int, strict bool, skipTypes ...reflect.Type) map[string]any {
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

			if f.Tag.Get("schema") == "-" {
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
				emb := typeToJSONSchema(f.Type, depth+1, strict, skipTypes...)
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

			isOptional := f.Type.Kind() == reflect.Ptr || strings.Contains(opts, "omitempty")
			schema := typeToJSONSchema(f.Type, depth+1, strict, skipTypes...)

			if desc := f.Tag.Get("desc"); desc != "" {
				schema["description"] = desc
			}
			if enum := f.Tag.Get("enum"); enum != "" {
				schema["enum"] = strings.Split(enum, ",")
			}

			if strict && isOptional {
				schema = makeSchemaNullable(schema)
			}

			properties[name] = schema
			if strict || !isOptional {
				required = append(required, name)
			}
		}

		result := map[string]any{
			"type":                 "object",
			"properties":           properties,
			"additionalProperties": false,
		}
		// Strict tool schemas always include required (may be empty).
		if strict || len(required) > 0 {
			result["required"] = required
		}
		return result
	}

	if rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
		return map[string]any{
			"type":  "array",
			"items": typeToJSONSchema(rt.Elem(), depth+1, strict, skipTypes...),
		}
	}

	if rt.Kind() == reflect.Map {
		return map[string]any{
			"type":                 "object",
			"additionalProperties": typeToJSONSchema(rt.Elem(), depth+1, strict, skipTypes...),
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

// makeSchemaNullable rewrites a schema type to accept null (strict optional fields).
func makeSchemaNullable(schema map[string]any) map[string]any {
	if schema == nil {
		return map[string]any{"type": []any{"string", "null"}}
	}
	switch t := schema["type"].(type) {
	case string:
		if t == "null" {
			return schema
		}
		schema["type"] = []any{t, "null"}
	case []any:
		hasNull := false
		for _, x := range t {
			if s, ok := x.(string); ok && s == "null" {
				hasNull = true
				break
			}
		}
		if !hasNull {
			schema["type"] = append(append([]any{}, t...), "null")
		}
	}
	return schema
}

// TypeToJSONSchema builds a JSON Schema for v.
// Prefer NewTool typed handlers for tools; this is mainly for structured model output.
func TypeToJSONSchema(v any) (map[string]any, error) {
	rt := reflect.TypeOf(v)
	if rt == nil {
		return nil, fmt.Errorf("cannot generate JSON schema from nil value")
	}
	// Non-strict: omitempty fields stay optional (structured outputs / hosts).
	return typeToJSONSchema(rt, 0, false), nil
}

// ToolsAsJson serializes tool definitions for model requests. An empty catalog
// is "[]". Namespaced tools are "namespace__name" (OpenAI rejects '.').
func ToolsAsJson(tools []*Tool) string {
	if len(tools) == 0 {
		return "[]"
	}

	defs := make([]map[string]any, 0, len(tools))
	for _, t := range tools {
		def := t.AsJson()
		if t.namespace != "" {
			def["name"] = t.namespace + "__" + t.name
		}
		defs = append(defs, def)
	}

	b, _ := json.Marshal(defs)
	return string(b)
}

// ToolResultEffect is applied once after a successful tool batch (no open interrupts).
type ToolResultEffect int

const (
	EffectNone ToolResultEffect = iota
	// EffectInstallPlanDocument sets the window to [user, plan document].
	EffectInstallPlanDocument
	// EffectHandoff rebuilds the window for the next open todos.
	EffectHandoff
)

// ToolOutcome is the single post-tool result: model-visible output plus a
// window effect. Plan builtins return this. Host hooks leave Output empty.
type ToolOutcome struct {
	Output string
	// Effect is merged for the batch and applied once at batch end.
	Effect ToolResultEffect
	// SuppressWindowMessage omits the tool Message from the window.
	// The client still receives StreamEventToolResult.
	SuppressWindowMessage bool
}

// ToolResultObservation is a successful tool result seen by a ToolResultHook.
type ToolResultObservation struct {
	Name     string
	ArgsJSON string
	Output   string
	Runtime  HarnessRuntime
}

// ToolResultHook runs after a successful host tool and before the tool result is emitted.
// Effects apply at batch end. Plan builtins return ToolOutcome instead.
type ToolResultHook func(ctx context.Context, obs ToolResultObservation) ToolOutcome

type toolResultHookRegistry struct {
	byName map[string]ToolResultHook
}

func newToolResultHookRegistry(hooks map[string]ToolResultHook) *toolResultHookRegistry {
	cp := make(map[string]ToolResultHook, len(hooks))
	for k, v := range hooks {
		cp[k] = v
	}
	return &toolResultHookRegistry{byName: cp}
}

func (r *toolResultHookRegistry) observe(ctx context.Context, obs ToolResultObservation) ToolOutcome {
	if r == nil {
		return ToolOutcome{}
	}
	hook := r.byName[obs.Name]
	if hook == nil {
		return ToolOutcome{}
	}
	return hook(ctx, obs)
}

type batchToolResultEffects struct {
	installPlan atomic.Bool
	handoff     atomic.Bool
	suppress    atomic.Bool
}

func (b *batchToolResultEffects) merge(d ToolOutcome) {
	switch d.Effect {
	case EffectInstallPlanDocument:
		b.installPlan.Store(true)
	case EffectHandoff:
		b.handoff.Store(true)
	}
	if d.SuppressWindowMessage {
		b.suppress.Store(true)
	}
}

// resolved prefers install over handoff when both appear in one batch.
func (b *batchToolResultEffects) resolved() ToolResultEffect {
	if b.installPlan.Load() {
		return EffectInstallPlanDocument
	}
	if b.handoff.Load() {
		return EffectHandoff
	}
	return EffectNone
}
