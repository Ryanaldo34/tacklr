package tacklr

import (
	"context"
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
// planning lock and OnCall middleware; they never replace that chain.
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
// Disposition comes from the final invoke (ToolOutcome); short-circuits have none.
func (r *toolRunner) Run(ctx context.Context, inv ToolInvocation) (string, ToolOutcome, error) {
	var toolDisp ToolOutcome
	next := ToolCallFunc(func(ctx context.Context, inv ToolInvocation) (string, error) {
		res, err := inv.Tool.invoke(ctx, inv.ArgsJSON, inv.Runtime)
		toolDisp = res.disp
		return res.output, err
	})

	for i := len(r.interceptors) - 1; i >= 0; i-- {
		interceptor := r.interceptors[i]
		inner := next
		next = func(ctx context.Context, inv ToolInvocation) (string, error) {
			return interceptor(ctx, inv, inner)
		}
	}

	out, err := next(ctx, inv)
	return out, toolDisp, err
}

func toolNameOf(inv ToolInvocation) string {
	return inv.Tool.name
}

// ToolPermissionOnCall parks a tool_permission interrupt before the handler.
// Session allow-always / reject-always are applied by on-call middleware.
func ToolPermissionOnCall(inv ToolInvocation) Interrupt {
	name := toolNameOf(inv)
	return &interrupt.ToolPermissionInterrupt{
		ToolName: name,
		Title:    ResolveToolTitle(inv.Tool.displayName, name, inv.ArgsJSON),
		Options:  interrupt.DefaultPermissionOptions(),
	}
}

// onCallMiddleware runs Tool.OnCall constructors in order. Permission memory
// and interrupt adopt live on sessionManager, not sessionRuntime.
func onCallMiddleware(sm *sessionManager) ToolInterceptor {
	return func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		if inv.Tool == nil || len(inv.Tool.onCall) == 0 {
			return next(ctx, inv)
		}
		if inv.Runtime == nil || sm == nil {
			return "", fmt.Errorf("%w: on-call interrupt requires a runtime", ErrFailed)
		}
		for _, ctor := range inv.Tool.onCall {
			if err := applyOnCallLayer(&inv, ctor, sm); err != nil {
				return "", err
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
	callID := inv.Runtime.CurrentToolCallID()
	if resolved, ok := sm.TakeResolved(callID); ok {
		denied := false
		if perm, ok := resolved.(*interrupt.ToolPermissionInterrupt); ok {
			rememberPermission(&sm.Permissions, perm)
			denied = !perm.Allowed
		}
		return finishOnCallLayer(inv, resolved.TypeName(), denied, sm)
	}
	if perm, ok := intr.(*interrupt.ToolPermissionInterrupt); ok {
		switch sm.Permissions.Decision(perm.ToolName) {
		case permDenyAlways:
			return finishOnCallLayer(inv, perm.TypeName(), true, sm)
		case permAllowAlways:
			return finishOnCallLayer(inv, perm.TypeName(), false, sm)
		}
	}
	if layer, ok := sm.OnCall.Get(callID, intr.TypeName()); ok {
		if layer.Denied {
			name := toolNameOf(*inv)
			return Correctionf(ErrToolPermissionDenied, "%s was rejected by the user. Do not retry this tool unless the task can proceed another way", name)
		}
		inv.ArgsJSON = layer.Args
		return nil
	}
	return sm.Park(callID, intr)
}

func finishOnCallLayer(inv *ToolInvocation, typeName string, denied bool, sm *sessionManager) error {
	sm.OnCall.Record(inv.Runtime.CurrentToolCallID(), typeName, onCallLayer{
		Args:   inv.ArgsJSON,
		Denied: denied,
	})
	if denied {
		name := toolNameOf(*inv)
		return Correctionf(ErrToolPermissionDenied, "%s was rejected by the user. Do not retry this tool unless the task can proceed another way", name)
	}
	return nil
}

func rememberPermission(perms *permissions, perm *interrupt.ToolPermissionInterrupt) {
	switch perm.SelectedKind {
	case interrupt.PermissionAllowAlways:
		perms.Remember(perm.ToolName, permAllowAlways)
	case interrupt.PermissionRejectAlways:
		perms.Remember(perm.ToolName, permDenyAlways)
	}
}
