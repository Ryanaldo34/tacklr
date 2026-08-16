package tacklr

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// ToolInvocation is one tool call in the interceptor chain.
type ToolInvocation struct {
	Tool     *Tool
	ArgsJSON string
	Runtime  HarnessRuntime
}

// ToolCallFunc is the next interceptor step or the final tool invoke.
type ToolCallFunc func(ctx context.Context, inv ToolInvocation) (string, error)

// ToolInterceptor wraps a tool call. Call next to continue, or return early to
// short-circuit. Host interceptors on AgentOptions wrap outside the built-in
// planning lock and permission gate; they never replace that chain.
type ToolInterceptor func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error)

type toolRunner struct {
	interceptors []ToolInterceptor
}

func newToolRunner(interceptors ...ToolInterceptor) *toolRunner {
	cp := make([]ToolInterceptor, len(interceptors))
	copy(cp, interceptors)
	return &toolRunner{interceptors: cp}
}

// Run runs the interceptor chain and the tool.
// Disposition comes from the final invoke (BuiltinResult); short-circuits have none.
func (r *toolRunner) Run(ctx context.Context, inv ToolInvocation) (string, ToolResultDisposition, error) {
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

// toolPermissionGate raises tool_permission when PermissionRequired is set.
// allow_always and reject_always are stored for the session.
func toolPermissionGate(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if inv.Tool == nil || !inv.Tool.PermissionRequired {
		return next(ctx, inv)
	}

	name := inv.Tool.Name
	if inv.Runtime != nil && inv.Runtime.PermissionAlwaysDenied(name) {
		return "", fmt.Errorf("%w: tool %q is always rejected for this session", ErrToolPermissionDenied, name)
	}
	if inv.Runtime != nil && inv.Runtime.PermissionAlwaysAllowed(name) {
		return next(ctx, inv)
	}

	title := ResolveToolTitle(inv.Tool.DisplayName, name, inv.ArgsJSON)
	initPayload, _ := json.Marshal(map[string]any{
		"toolName": name,
		"title":    title,
	})
	intr, err := inv.Runtime.RaiseInterrupt("tool_permission", initPayload)
	if err != nil {
		return "", err
	}
	perm, ok := intr.(*interrupt.ToolPermissionInterrupt)
	if !ok {
		return "", fmt.Errorf("%w: unexpected permission interrupt type %T", ErrFailed, intr)
	}

	switch perm.SelectedKind {
	case interrupt.PermissionAllowAlways:
		if inv.Runtime != nil {
			inv.Runtime.RememberPermissionAllow(name)
		}
	case interrupt.PermissionRejectAlways:
		if inv.Runtime != nil {
			inv.Runtime.RememberPermissionDeny(name)
		}
	}

	if !perm.Allowed {
		return "", fmt.Errorf("%w: user rejected tool %q", ErrToolPermissionDenied, name)
	}
	return next(ctx, inv)
}
