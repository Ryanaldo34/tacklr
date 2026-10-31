package tacklr

import (
	"context"
	"fmt"
)

// ToolInvocation is one tool call in the interceptor chain.
type ToolInvocation struct {
	Tool     *Tool
	ArgsJSON string
	Runtime  HarnessRuntime
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
		return inv.Tool.invoke(ctx, inv.ArgsJSON, inv.Runtime)
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
// completed before a park (on its round call) is not asked again.
func onCallMiddleware(sm *sessionManager) ToolInterceptor {
	return func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (ToolOutcome, error) {
		if len(inv.Tool.onCall) == 0 {
			return next(ctx, inv)
		}
		if inv.Runtime == nil {
			return ToolOutcome{}, fmt.Errorf("%w: on-call interrupt requires a runtime", ErrFailed)
		}
		for _, ctor := range inv.Tool.onCall {
			if err := applyOnCallLayer(&inv, ctor, sm); err != nil {
				return ToolOutcome{}, err
			}
		}
		return next(ctx, inv)
	}
}

func applyOnCallLayer(inv *ToolInvocation, ctor OnCallFunc, sm *sessionManager) error {
	intr := ctor(*inv)
	if intr == nil {
		return nil
	}
	callID, typeName := inv.Runtime.CurrentToolCallID(), intr.TypeName()
	if layer, ok := sm.Layer(callID, typeName); ok {
		inv.ArgsJSON = layer.Args
		return nil
	}
	if answered, ok := sm.TakeAnswer(callID, typeName); ok {
		denied := false
		if perm, ok := answered.(*ToolPermissionInterrupt); ok {
			sm.Permissions.rememberAnswer(perm)
			denied = !perm.Allowed
		}
		return finishOnCallLayer(inv, typeName, denied, sm)
	}
	if perm, ok := intr.(*ToolPermissionInterrupt); ok {
		switch sm.Permissions.Decision(perm.ToolName) {
		case permDenyAlways:
			return finishOnCallLayer(inv, typeName, true, sm)
		case permAllowAlways:
			return finishOnCallLayer(inv, typeName, false, sm)
		}
	}
	return sm.Park(callID, intr)
}

// finishOnCallLayer ends a layer: a denied call fails for the model to
// correct; an allowed one keeps the layer on its round call, so the call's
// run after a later park skips it.
func finishOnCallLayer(inv *ToolInvocation, typeName string, denied bool, sm *sessionManager) error {
	if denied {
		return Correctionf(ErrToolPermissionDenied, "%s was rejected by the user. Do not retry this tool unless the task can proceed another way", inv.Tool.QualifiedName())
	}
	sm.RecordLayer(inv.Runtime.CurrentToolCallID(), onCallLayer{TypeName: typeName, Args: inv.ArgsJSON})
	return nil
}
