package tacklr

import (
	"context"
	"fmt"
)

// ToolInvocation is a single tool call as it moves through the runner chain.
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
type ToolInterceptor func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error)

// ToolRunner executes tool calls through an ordered interceptor chain.
// The final step always invokes the tool.
type ToolRunner struct {
	interceptors []ToolInterceptor
}

// NewToolRunner builds a runner that applies interceptors in the given order
// (first interceptor is outermost).
func NewToolRunner(interceptors ...ToolInterceptor) *ToolRunner {
	cp := make([]ToolInterceptor, len(interceptors))
	copy(cp, interceptors)
	return &ToolRunner{interceptors: cp}
}

// DefaultToolInterceptors returns the harness default interceptor chain.
func DefaultToolInterceptors() []ToolInterceptor {
	return []ToolInterceptor{
		PlanningWriteLock,
	}
}

// Run passes the invocation through interceptors, then invokes the tool.
func (r *ToolRunner) Run(ctx context.Context, inv ToolInvocation) (string, error) {
	if inv.Tool == nil {
		return "", fmt.Errorf("%w", ErrToolNotFound)
	}

	next := ToolCallFunc(func(ctx context.Context, inv ToolInvocation) (string, error) {
		return inv.Tool.Invoke(ctx, inv.ArgsJSON, inv.Runtime)
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

	return next(ctx, inv)
}

// PlanningWriteLock denies tools that require Write access while no plan exists
// (planning mode: create_plan has not established a todo list yet).
func PlanningWriteLock(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if requiresWrite(inv.Tool) && len(inv.Runtime.PlanGet()) == 0 {
		return "", fmt.Errorf("%w: write tools are locked until create_plan establishes a todo list", ErrToolPermissionDenied)
	}
	return next(ctx, inv)
}

func requiresWrite(tool *Tool) bool {
	return tool != nil && tool.Access != nil && tool.Access.Contains(WritePermission)
}
