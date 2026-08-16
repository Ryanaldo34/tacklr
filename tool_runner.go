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
// Session allow-always skips the park; reject-always denies without a yield.
func ToolPermissionOnCall(inv ToolInvocation) Interrupt {
	name := toolNameOf(inv)
	if rt := inv.Runtime; rt != nil {
		if rt.PermissionAlwaysDenied(name) {
			return &interrupt.ToolPermissionInterrupt{
				ToolName:     name,
				SelectedKind: interrupt.PermissionRejectAlways,
				Allowed:      false,
			}
		}
		if rt.PermissionAlwaysAllowed(name) {
			return nil
		}
	}
	return &interrupt.ToolPermissionInterrupt{
		ToolName: name,
		Title:    ResolveToolTitle(toolDisplayOf(inv), name, inv.ArgsJSON),
		Options:  interrupt.DefaultPermissionOptions(),
	}
}

// onCallMiddleware runs Tool.OnCall constructors in order (FastAPI-style layers).
func onCallMiddleware(stages *session.OnCallStore) ToolInterceptor {
	return func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		if inv.Tool == nil || len(inv.Tool.OnCall) == 0 {
			return next(ctx, inv)
		}
		if inv.Runtime == nil {
			return "", fmt.Errorf("%w: on-call interrupt requires a runtime", ErrFailed)
		}
		for _, ctor := range inv.Tool.OnCall {
			if err := applyOnCallLayer(&inv, ctor, stages); err != nil {
				return "", err
			}
		}
		return next(ctx, inv)
	}
}

func applyOnCallLayer(inv *ToolInvocation, ctor OnCallFunc, stages *session.OnCallStore) error {
	intr := ctor(*inv)
	if intr == nil {
		return nil
	}
	callID := inv.Runtime.CurrentToolCallID()
	if stages != nil {
		if layer, ok := stages.Get(callID, intr.TypeName()); ok {
			if layer.Denied {
				return rejectedOnCall(toolNameOf(*inv))
			}
			inv.ArgsJSON = layer.Args
			return nil
		}
	}
	if perm, ok := intr.(*interrupt.ToolPermissionInterrupt); ok && perm.SelectedKind != "" {
		return finishOnCallLayer(inv, perm, stages)
	}
	resolved, err := inv.Runtime.AdoptInterrupt(intr)
	if err != nil {
		return err
	}
	return finishOnCallLayer(inv, resolved, stages)
}

func finishOnCallLayer(inv *ToolInvocation, resolved Interrupt, stages *session.OnCallStore) error {
	denied := false
	if perm, ok := resolved.(*interrupt.ToolPermissionInterrupt); ok {
		denied = perm.SelectedKind != "" && !perm.Allowed
		rememberOnCallSession(inv.Runtime, perm)
	}
	if stages != nil {
		stages.Record(inv.Runtime.CurrentToolCallID(), resolved.TypeName(), session.OnCallLayer{
			Args:   inv.ArgsJSON,
			Denied: denied,
		})
	}
	if denied {
		return rejectedOnCall(toolNameOf(*inv))
	}
	return nil
}

func rememberOnCallSession(rt HarnessRuntime, perm *interrupt.ToolPermissionInterrupt) {
	switch perm.SelectedKind {
	case interrupt.PermissionAllowAlways:
		rt.RememberPermissionAllow(perm.ToolName)
	case interrupt.PermissionRejectAlways:
		rt.RememberPermissionDeny(perm.ToolName)
	}
}
