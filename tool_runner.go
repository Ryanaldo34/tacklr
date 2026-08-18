package tacklr

import (
	"context"
	"fmt"

	"github.com/ryanaldo34/tacklr/internal/session"
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
		inner := next
		next = func(ctx context.Context, inv ToolInvocation) (string, error) {
			return interceptor(ctx, inv, inner)
		}
	}

	out, err := next(ctx, inv)
	return out, toolDisp, err
}

func rejectedOnCall(name string) error {
	return fmt.Errorf("%w: user rejected tool %q", ErrToolPermissionDenied, name)
}

func toolNameOf(inv ToolInvocation) string {
	return inv.Tool.Name
}

func toolDisplayOf(inv ToolInvocation) string {
	return inv.Tool.DisplayName
}

// ToolPermissionOnCall parks a tool_permission interrupt before the handler.
// Session allow-always / reject-always are applied by on-call middleware.
func ToolPermissionOnCall(inv ToolInvocation) Interrupt {
	name := toolNameOf(inv)
	return &interrupt.ToolPermissionInterrupt{
		ToolName: name,
		Title:    ResolveToolTitle(toolDisplayOf(inv), name, inv.ArgsJSON),
		Options:  interrupt.DefaultPermissionOptions(),
	}
}

// onCallMiddleware runs Tool.OnCall constructors in order. Permission memory
// and interrupt adopt live on SessionManager, not Runtime.
func onCallMiddleware(sm *session.SessionManager) ToolInterceptor {
	return func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		if inv.Tool == nil || len(inv.Tool.OnCall) == 0 {
			return next(ctx, inv)
		}
		if inv.Runtime == nil || sm == nil {
			return "", fmt.Errorf("%w: on-call interrupt requires a runtime", ErrFailed)
		}
		for _, ctor := range inv.Tool.OnCall {
			if err := applyOnCallLayer(&inv, ctor, sm); err != nil {
				return "", err
			}
		}
		return next(ctx, inv)
	}
}

func applyOnCallLayer(inv *ToolInvocation, ctor OnCallFunc, sm *session.SessionManager) error {
	intr := ctor(*inv)
	if intr == nil {
		return nil
	}
	callID := inv.Runtime.CurrentToolCallID()
	if layer, ok := sm.OnCall.Get(callID, intr.TypeName()); ok {
		if layer.Denied {
			return rejectedOnCall(toolNameOf(*inv))
		}
		inv.ArgsJSON = layer.Args
		return nil
	}
	if perm, ok := intr.(*interrupt.ToolPermissionInterrupt); ok {
		switch sm.Permissions.Decision(perm.ToolName) {
		case session.PermissionDenyAlways:
			return finishOnCallLayer(inv, perm.TypeName(), true, sm)
		case session.PermissionAllowAlways:
			return finishOnCallLayer(inv, perm.TypeName(), false, sm)
		}
	}
	resolved, err := sm.AdoptInterrupt(callID, intr)
	if err != nil {
		return err
	}
	denied := false
	if perm, ok := resolved.(*interrupt.ToolPermissionInterrupt); ok {
		rememberPermission(&sm.Permissions, perm)
		denied = !perm.Allowed
	}
	return finishOnCallLayer(inv, resolved.TypeName(), denied, sm)
}

func finishOnCallLayer(inv *ToolInvocation, typeName string, denied bool, sm *session.SessionManager) error {
	sm.OnCall.Record(inv.Runtime.CurrentToolCallID(), typeName, session.OnCallLayer{
		Args:   inv.ArgsJSON,
		Denied: denied,
	})
	if denied {
		return rejectedOnCall(toolNameOf(*inv))
	}
	return nil
}

func rememberPermission(perms *session.Permissions, perm *interrupt.ToolPermissionInterrupt) {
	switch perm.SelectedKind {
	case interrupt.PermissionAllowAlways:
		perms.Remember(perm.ToolName, session.PermissionAllowAlways)
	case interrupt.PermissionRejectAlways:
		perms.Remember(perm.ToolName, session.PermissionDenyAlways)
	}
}
