package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/message"
)

// Tool definitions, JSON schema, and tool outcomes.

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

type ToolHandlerFunc func(ctx context.Context, args map[string]any, cc *CallContext) (string, error)

// Tool is a registered harness tool. Construct with NewTool(ToolConfig{...}).
// Fields are unexported; hosts read metadata through the getters below.
type Tool struct {
	displayName string
	name        string
	description string
	namespace   string
	category    message.ToolCategory
	access      ToolAccess
	// timeout is an optional per-invocation deadline. Zero means none.
	timeout time.Duration
	// onCall is the pre-invoke middleware stack. Each constructor may park.
	onCall []OnCallFunc

	handlerFunc func(ctx context.Context, args map[string]any, cc *CallContext) (ToolOutcome, error)
	parameters  map[string]any
	strict      bool
}

// Name is the programmatic tool name presented to the model.
func (t *Tool) Name() string { return t.name }

// Namespace is the optional tool namespace (MCP server name, host grouping).
func (t *Tool) Namespace() string { return t.namespace }

// QualifiedName is the name the model calls the tool by: namespace__name
// for a namespaced tool, else its name.
func (t *Tool) QualifiedName() string {
	if t.namespace == "" {
		return t.name
	}
	return t.namespace + "__" + t.name
}

type ToolConfig struct {
	Name        string
	Description string
	DisplayName string
	Namespace   string
	Category    message.ToolCategory
	Access      ToolAccess
	Timeout     time.Duration
	// OnCall is the pre-invoke middleware stack. Each constructor may park.
	// Return nil from a constructor to skip that layer. Types must be registered.
	OnCall []OnCallFunc

	// Handler is a Go function. Close over clients in the constructor that calls NewTool.
	// Optional parameters: context.Context, an args struct, CallContext.
	Handler any
}

// OnCallFunc builds a check that runs before the handler and may park the
// call. Return nil to skip it.
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
var harnessRuntimeType = reflect.TypeOf((*CallContext)(nil))
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
	// Handlers take at most (ctx, args, runtime); anything else panics below.
	if numIn > 3 {
		panic(fmt.Sprintf("tool %q: handler has too many parameters (%d)", cfg.Name, numIn))
	}

	hasCtx := numIn > 0 && fnType.In(0).Implements(ctxType)
	idx := 0
	if hasCtx {
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
						panic(fmt.Sprintf("tool %q: unexpected parameter type %v (want *CallContext)", cfg.Name, rType))
					}
					hasRuntime = true
				}
			} else {
				panic(fmt.Sprintf("tool %q: handler parameter must be a struct or *CallContext, got %v", cfg.Name, pType))
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
			panic(fmt.Sprintf("tool %q: OnCall entries must not be nil", cfg.Name))
		}
		if sample := ctor(ToolInvocation{Tool: t}); sample != nil {
			if _, ok := NewInterrupt(sample.TypeName()); !ok {
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
	t.handlerFunc = func(ctx context.Context, args map[string]any, cc *CallContext) (ToolOutcome, error) {
		var callArgs []reflect.Value
		if hasCtx {
			callArgs = append(callArgs, reflect.ValueOf(ctx))
		}

		if argsType != nil {
			argPtr := reflect.New(argsType)
			if len(args) > 0 {
				// args were decoded from JSON, so they encode.
				b, _ := json.Marshal(args)
				if err := json.Unmarshal(b, argPtr.Interface()); err != nil {
					return ToolOutcome{}, fmt.Errorf("unmarshal args: %w", err)
				}
			}
			if argsIsPtr {
				callArgs = append(callArgs, argPtr)
			} else {
				callArgs = append(callArgs, argPtr.Elem())
			}
		}

		if hasRuntime {
			if cc == nil {
				return ToolOutcome{}, fmt.Errorf("%w: tool %q requires a CallContext", ErrFailed, t.name)
			}
			callArgs = append(callArgs, reflect.ValueOf(cc))
		}

		results := handlerValue.Call(callArgs)
		if err, ok := results[1].Interface().(error); ok && err != nil {
			return ToolOutcome{}, err
		}
		if results[0].Kind() == reflect.String {
			return ToolOutcome{Output: results[0].String()}, nil
		}
		if outcome, ok := results[0].Interface().(ToolOutcome); ok {
			return outcome, nil
		}
		b, err := json.Marshal(results[0].Interface())
		if err != nil {
			return ToolOutcome{}, fmt.Errorf("marshal result: %w", err)
		}
		return ToolOutcome{Output: string(b)}, nil
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
		handlerFunc: func(ctx context.Context, args map[string]any, cc *CallContext) (ToolOutcome, error) {
			out, err := cfg.Handler(ctx, args, cc)
			return ToolOutcome{Output: out}, err
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
func (t *Tool) invoke(ctx context.Context, argsJson string, cc *CallContext) (ToolOutcome, error) {
	res, err := t.invokeRaw(ctx, argsJson, cc)
	if err != nil {
		return res, presentToolError(t.name, err)
	}
	return res, nil
}

func (t *Tool) invokeRaw(ctx context.Context, argsJson string, cc *CallContext) (ToolOutcome, error) {
	var args map[string]any
	if argsJson != "" {
		if err := json.Unmarshal([]byte(argsJson), &args); err != nil {
			return ToolOutcome{}, Correctionf(ErrInvalid, "%s: arguments were not valid JSON. Check the tool schema and retry with a JSON object", t.name)
		}
	}

	if t.timeout <= 0 {
		return t.handlerFunc(ctx, args, cc)
	}

	callCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	type result struct {
		outcome ToolOutcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		outcome, err := t.handlerFunc(callCtx, args, cc)
		done <- result{outcome: outcome, err: err}
	}()

	select {
	case res := <-done:
		if res.err != nil && ctx.Err() == nil && errors.Is(res.err, context.DeadlineExceeded) {
			return ToolOutcome{}, Correctionf(ErrToolTimeout, "%s timed out. Retry with a smaller request, fewer URLs, or a narrower search", t.name)
		}
		return res.outcome, res.err
	case <-callCtx.Done():
		if err := ctx.Err(); err != nil {
			return ToolOutcome{}, err
		}
		return ToolOutcome{}, Correctionf(ErrToolTimeout, "%s timed out. Retry with a smaller request, fewer URLs, or a narrower search", t.name)
	}
}

// AsJson returns the OpenAI-style function tool definition for this tool.
// parameters is never nil on the returned map.
func (t *Tool) AsJson() map[string]any {
	return map[string]any{
		"type":        "function",
		"name":        t.name,
		"description": t.description,
		"strict":      t.strict,
		"parameters":  t.parameters,
	}
}

// typeToJSONSchema builds a JSON Schema for rt.
// When strict is true (OpenAI/DeepSeek function tools), every property name is
// listed in required, and omitempty/pointer fields are typed as T|null so the
// model may pass null for unused optionals.
func typeToJSONSchema(rt reflect.Type, depth int, strict bool) map[string]any {
	if depth > 10 {
		return map[string]any{"type": "string"}
	}
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
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
				emb := typeToJSONSchema(f.Type, depth+1, strict)
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
			schema := typeToJSONSchema(f.Type, depth+1, strict)

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
			"items": typeToJSONSchema(rt.Elem(), depth+1, strict),
		}
	}

	if rt.Kind() == reflect.Map {
		return map[string]any{
			"type":                 "object",
			"additionalProperties": typeToJSONSchema(rt.Elem(), depth+1, strict),
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

// makeSchemaNullable lets a strict optional field be null. A generated
// schema's type is one JSON type name.
func makeSchemaNullable(schema map[string]any) map[string]any {
	schema["type"] = []any{schema["type"], "null"}
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
		def["name"] = t.QualifiedName()
		defs = append(defs, def)
	}

	b, _ := json.Marshal(defs)
	return string(b)
}

// ToolResultEffect changes the window when the round of the call that asked
// for it commits. Higher values win when a round asks for two.
type ToolResultEffect int

const (
	EffectNone ToolResultEffect = iota
	// EffectHandoff rebuilds the window for the next open todos.
	EffectHandoff
	// EffectInstallPlanDocument sets the window to [user, plan document].
	EffectInstallPlanDocument
)

// ToolOutcome is a call's result: the model-visible output and the window
// effect it asks for. A handler may return it (the plan tools do); an
// interceptor may change it after next returns.
type ToolOutcome struct {
	Output string
	// Effect applies once, when the call's round commits.
	Effect ToolResultEffect
}

// correctionError is a model-facing tool failure. Error() is what the model sees
// (what failed and how to correct it). Unwrap is the harness cause.
type correctionError struct {
	msg   string
	cause error
}

func (e *correctionError) Error() string { return e.msg }

func (e *correctionError) Unwrap() error { return e.cause }

func (e *correctionError) Is(target error) bool { return target == ErrCorrection }

// Correction wraps cause with model-facing correction text. msg is Error();
// errors.Is matches ErrCorrection and cause. A nil/empty msg uses cause.Error().
func Correction(cause error, msg string) error {
	msg = strings.TrimSpace(msg)
	if msg == "" && cause != nil {
		msg = strings.TrimPrefix(cause.Error(), "vfs: ")
	}
	if msg == "" {
		return ErrCorrection
	}
	var a *correctionError
	if errors.As(cause, &a) {
		return cause
	}
	return &correctionError{msg: msg, cause: cause}
}

// Correctionf is Correction with fmt.Sprintf.
func Correctionf(cause error, format string, args ...any) error {
	return Correction(cause, fmt.Sprintf(format, args...))
}

// presentToolError makes a handler's error the model's to correct, named by
// the tool. A park, a cancel, a correction, and a service failure
// (ErrFailed) pass through as they are.
func presentToolError(name string, err error) error {
	var intr Interrupt
	if errors.As(err, &intr) || errors.Is(err, context.Canceled) || errors.Is(err, ErrCorrection) || errors.Is(err, ErrFailed) {
		return err
	}
	msg := strings.TrimPrefix(err.Error(), "vfs: ")
	if name != "" && !strings.HasPrefix(msg, name) {
		msg = name + ": " + msg
	}
	return Correction(err, msg)
}
