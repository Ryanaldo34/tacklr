package tacklr

import (
	"context"
)

// ToolInvocation is one tool call in the interceptor chain.
type ToolInvocation struct {
	Tool        *Tool
	ArgsJSON    string
	CallContext *CallContext
}

// ToolCallFunc is the next interceptor step or the final tool invoke. It
// returns the call's outcome: the model-visible output and its window
// effect.
type ToolCallFunc func(ctx context.Context, inv ToolInvocation) (ToolOutcome, error)

// ToolInterceptor wraps a tool call. Call next to continue, or return early
// to short-circuit. An interceptor may change the outcome next returns: its
// Output replaces what the model sees, and its Effect changes the window
// when the call's round commits. Host interceptors on AgentOptions wrap
// outside the built-in planning lock and OnCall middleware; they never
// replace that chain.
type ToolInterceptor func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (ToolOutcome, error)

type toolRunner struct {
	interceptors []ToolInterceptor
}

func newToolRunner(interceptors ...ToolInterceptor) *toolRunner {
	return &toolRunner{interceptors: interceptors}
}

// Run runs the interceptor chain and the tool.
func (r *toolRunner) Run(ctx context.Context, inv ToolInvocation) (ToolOutcome, error) {
	next := ToolCallFunc(func(ctx context.Context, inv ToolInvocation) (ToolOutcome, error) {
		return inv.Tool.invoke(ctx, inv.ArgsJSON, inv.CallContext)
	})
	for i := len(r.interceptors) - 1; i >= 0; i-- {
		interceptor, inner := r.interceptors[i], next
		next = func(ctx context.Context, inv ToolInvocation) (ToolOutcome, error) {
			return interceptor(ctx, inv, inner)
		}
	}
	return next(ctx, inv)
}

// ToolPermissionOnCall parks a tool_permission interrupt before the handler.
// The permission store's allow-always and reject-always answer it without a
// park.
func ToolPermissionOnCall(inv ToolInvocation) Interrupt {
	name := inv.Tool.QualifiedName()
	return &ToolPermissionInterrupt{
		ToolName: name,
		Title:    ResolveToolTitle(inv.Tool.displayName, name, inv.ArgsJSON),
		Options:  DefaultPermissionOptions(),
	}
}

// onCallMiddleware runs Tool.OnCall constructors in order. A layer the call
// completed before a park is not asked again.
func onCallMiddleware(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (ToolOutcome, error) {
	for _, ctor := range inv.Tool.onCall {
		if err := applyOnCallLayer(&inv, ctor); err != nil {
			return ToolOutcome{}, err
		}
	}
	return next(ctx, inv)
}

func applyOnCallLayer(inv *ToolInvocation, ctor OnCallFunc) error {
	cc := inv.CallContext
	intr := ctor(*inv)
	if intr == nil {
		return nil
	}
	typeName := intr.TypeName()
	if layer, ok := cc.layer(typeName); ok {
		inv.ArgsJSON = layer.Args
		return nil
	}
	if answered, ok := cc.takeAnswer(typeName); ok {
		denied := false
		if perm, ok := answered.(*ToolPermissionInterrupt); ok {
			cc.rememberAnswer(perm)
			denied = !perm.Allowed
		}
		return finishOnCallLayer(inv, typeName, denied)
	}
	if perm, ok := intr.(*ToolPermissionInterrupt); ok {
		switch cc.decision(perm.ToolName) {
		case permDenyAlways:
			return finishOnCallLayer(inv, typeName, true)
		case permAllowAlways:
			return finishOnCallLayer(inv, typeName, false)
		}
	}
	return cc.park(intr)
}

// finishOnCallLayer ends a layer: a denied call fails for the model to
// correct; an allowed one keeps the layer, so the call's run after a later
// park skips it.
func finishOnCallLayer(inv *ToolInvocation, typeName string, denied bool) error {
	if denied {
		return Correctionf(ErrToolPermissionDenied, "%s was rejected by the user. Do not retry this tool unless the task can proceed another way", inv.Tool.QualifiedName())
	}
	inv.CallContext.recordLayer(onCallLayer{TypeName: typeName, Args: inv.ArgsJSON})
	return nil
}
