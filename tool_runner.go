package tacklr

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// ToolInvocation is a single tool call as it moves through the runner chain.
// Exported so consumers can implement custom ToolInterceptors via AgentOptions.
type ToolInvocation struct {
	Tool     *Tool
	ArgsJSON string
	Runtime  HarnessRuntime
}

// ToolCallFunc is the next step in the interceptor chain, or the final invoke.
type ToolCallFunc func(ctx context.Context, inv ToolInvocation) (string, error)

// ToolInterceptor gates or wraps a tool call. Call next to continue the chain;
// return without calling next to short-circuit with a result for the context window.
// Close over any state the interceptor needs.
//
// When AgentOptions.ToolInterceptors is nil, the harness installs its built-in
// chain (planning write lock, then permission gate). A non-nil slice replaces
// that chain entirely (empty disables interceptors).
type ToolInterceptor func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error)

type toolRunner struct {
	interceptors []ToolInterceptor
}

func newToolRunner(interceptors ...ToolInterceptor) *toolRunner {
	cp := make([]ToolInterceptor, len(interceptors))
	copy(cp, interceptors)
	return &toolRunner{interceptors: cp}
}

// Run executes the interceptor chain and the tool. Disposition comes only from
// the final tool invoke (BuiltinResult); interceptor short-circuits yield none.
func (r *toolRunner) Run(ctx context.Context, inv ToolInvocation) (string, ToolResultDisposition, error) {
	if inv.Tool == nil {
		return "", ToolResultDisposition{}, fmt.Errorf("%w", ErrToolNotFound)
	}

	var toolDisp ToolResultDisposition
	next := ToolCallFunc(func(ctx context.Context, inv ToolInvocation) (string, error) {
		res, err := inv.Tool.invoke(ctx, inv.ArgsJSON, inv.Runtime)
		toolDisp = res.disp
		return res.output, err
	})

	for i := len(r.interceptors) - 1; i >= 0; i-- {
		interceptor := r.interceptors[i]
		if interceptor == nil {
			continue
		}
		inner := next
		next = func(ctx context.Context, inv ToolInvocation) (string, error) {
			return interceptor(ctx, inv, inner)
		}
	}

	out, err := next(ctx, inv)
	return out, toolDisp, err
}

// toolPermissionGate raises a tool_permission interrupt when PermissionRequired
// is set. allow_always / reject_always choices are remembered for the session.
func toolPermissionGate(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if inv.Tool == nil || !inv.Tool.PermissionRequired {
		return next(ctx, inv)
	}

	name := inv.Tool.Name
	if permissionSetHas(inv.Runtime, permissionAlwaysDenyKey, name) {
		return "", fmt.Errorf("%w: tool %q is always rejected for this session", ErrToolPermissionDenied, name)
	}
	if permissionSetHas(inv.Runtime, permissionAlwaysAllowKey, name) {
		return next(ctx, inv)
	}

	initPayload, _ := json.Marshal(map[string]any{"toolName": name})
	intr, err := inv.Runtime.RaiseInterrupt("tool_permission", initPayload)
	if err != nil {
		return "", err
	}
	perm, ok := intr.(*interrupt.ToolPermissionInterrupt)
	if !ok || perm == nil {
		// Registered tool_permission factory always returns *ToolPermissionInterrupt.
		return "", fmt.Errorf("tool permission: unexpected interrupt type %T", intr)
	}

	switch perm.SelectedKind {
	case interrupt.PermissionAllowAlways:
		permissionRemember(inv.Runtime, permissionAlwaysAllowKey, name)
	case interrupt.PermissionRejectAlways:
		permissionRemember(inv.Runtime, permissionAlwaysDenyKey, name)
	}

	if !perm.Allowed {
		return "", fmt.Errorf("%w: user rejected tool %q", ErrToolPermissionDenied, name)
	}
	return next(ctx, inv)
}

const (
	permissionAlwaysAllowKey = "_permission_always_allow"
	permissionAlwaysDenyKey  = "_permission_always_deny"
)

func permissionSetHas(rt HarnessRuntime, key, toolName string) bool {
	v, ok := rt.StateGet(key)
	if !ok || v == nil {
		return false
	}
	switch s := v.(type) {
	case map[string]bool:
		return s[toolName]
	case map[string]any:
		b, _ := s[toolName].(bool)
		return b
	default:
		return false
	}
}

func permissionRemember(rt HarnessRuntime, key, toolName string) {
	set := make(map[string]bool)
	if v, ok := rt.StateGet(key); ok && v != nil {
		switch m := v.(type) {
		case map[string]bool:
			for k, b := range m {
				set[k] = b
			}
		case map[string]any:
			for k, raw := range m {
				b, _ := raw.(bool)
				set[k] = b
			}
		}
	}
	set[toolName] = true
	rt.StateSet(key, set)
}
