package tacklr

import (
	"context"
	"fmt"
	"time"

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

func rejectedOnCall(name string) error {
	return fmt.Errorf("%w: user rejected tool %q", ErrToolPermissionDenied, name)
}

func toolNameOf(inv ToolInvocation) string {
	if inv.Tool == nil {
		return ""
	}
	return inv.Tool.Name
}

func toolDisplayOf(inv ToolInvocation) string {
	if inv.Tool == nil {
		return ""
	}
	return inv.Tool.DisplayName
}

// WriteApprovalOnCall parks a write_approval interrupt before the handler.
func WriteApprovalOnCall(inv ToolInvocation) Interrupt {
	name := toolNameOf(inv)
	return &interrupt.WriteApprovalInterrupt{
		ToolName: name,
		Title:    ResolveToolTitle(toolDisplayOf(inv), name, inv.ArgsJSON),
		Args:     inv.ArgsJSON,
	}
}

// ToolPermissionOnCall parks a tool_permission interrupt before the handler.
// Session allow-always skips the park; reject-always denies without a yield.
func ToolPermissionOnCall(inv ToolInvocation) Interrupt {
	name := toolNameOf(inv)
	if inv.Runtime != nil && inv.Runtime.PermissionAlwaysDenied(name) {
		return &interrupt.ToolPermissionInterrupt{
			ToolName:     name,
			SelectedKind: interrupt.PermissionRejectAlways,
			Allowed:      false,
		}
	}
	if inv.Runtime != nil && inv.Runtime.PermissionAlwaysAllowed(name) {
		return nil
	}
	return &interrupt.ToolPermissionInterrupt{
		ToolName: name,
		Title:    ResolveToolTitle(toolDisplayOf(inv), name, inv.ArgsJSON),
		Options:  interrupt.DefaultPermissionOptions(),
	}
}

// onCallStore is the session-backed OnCall stage + write-approval audit.
// session.Runtime implements it; it is not part of HarnessRuntime.
type onCallStore interface {
	OnCallStage(toolCallID, typeName string) (args string, denied bool, ok bool)
	RecordOnCallStage(toolCallID, typeName, args string, denied bool)
	RecordWriteApproval(WriteApprovalRecord)
}

type predecidedCall interface {
	Predecided() bool
}

// onCallMiddleware runs Tool.OnCall constructors in order (FastAPI-style layers).
// skipTypes omits those interrupt kinds (DisableWriteApproval uses write_approval).
func onCallMiddleware(skipTypes ...string) ToolInterceptor {
	skip := make(map[string]struct{}, len(skipTypes))
	for _, t := range skipTypes {
		skip[t] = struct{}{}
	}
	return func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		if inv.Tool == nil || len(inv.Tool.OnCall) == 0 {
			return next(ctx, inv)
		}
		if inv.Runtime == nil {
			return "", fmt.Errorf("%w: on-call interrupt requires a runtime", ErrFailed)
		}
		store, _ := inv.Runtime.(onCallStore)
		for _, ctor := range inv.Tool.OnCall {
			if ctor == nil {
				continue
			}
			if err := applyOnCallLayer(&inv, ctor, store, skip); err != nil {
				return "", err
			}
		}
		return next(ctx, inv)
	}
}

func applyOnCallLayer(inv *ToolInvocation, ctor OnCallFunc, store onCallStore, skip map[string]struct{}) error {
	intr := ctor(*inv)
	if intr == nil {
		return nil
	}
	if _, ok := skip[intr.TypeName()]; ok {
		return nil
	}
	callID := inv.Runtime.CurrentToolCallID()
	if store != nil {
		if args, denied, ok := store.OnCallStage(callID, intr.TypeName()); ok {
			if denied {
				return rejectedOnCall(toolNameOf(*inv))
			}
			if args != "" {
				inv.ArgsJSON = args
			}
			return nil
		}
	}
	if p, ok := intr.(predecidedCall); ok && p.Predecided() {
		return finishOnCallLayer(inv, intr, store)
	}
	resolved, err := inv.Runtime.AdoptInterrupt(intr)
	if err != nil {
		return err
	}
	return finishOnCallLayer(inv, resolved, store)
}

func finishOnCallLayer(inv *ToolInvocation, resolved Interrupt, store onCallStore) error {
	denied := false
	if eff, ok := resolved.(interrupt.CallEffect); ok {
		denied = eff.CallDenied()
	}
	rememberOnCallSession(inv.Runtime, resolved)
	if store != nil && resolved != nil {
		store.RecordOnCallStage(inv.Runtime.CurrentToolCallID(), resolved.TypeName(), inv.ArgsJSON, denied)
	}
	recordWriteApprovalIfNeeded(*inv, resolved)
	if denied {
		return rejectedOnCall(toolNameOf(*inv))
	}
	return nil
}

func rememberOnCallSession(rt HarnessRuntime, resolved Interrupt) {
	perm, ok := resolved.(*interrupt.ToolPermissionInterrupt)
	if !ok || rt == nil {
		return
	}
	switch perm.SelectedKind {
	case interrupt.PermissionAllowAlways:
		rt.RememberPermissionAllow(perm.ToolName)
	case interrupt.PermissionRejectAlways:
		rt.RememberPermissionDeny(perm.ToolName)
	}
}

func recordWriteApprovalIfNeeded(inv ToolInvocation, resolved Interrupt) {
	wa, ok := resolved.(*interrupt.WriteApprovalInterrupt)
	if !ok {
		return
	}
	store, ok := inv.Runtime.(onCallStore)
	if !ok {
		return
	}
	store.RecordWriteApproval(WriteApprovalRecord{
		ToolName:   toolNameOf(inv),
		ToolCallID: inv.Runtime.CurrentToolCallID(),
		Action:     wa.Action,
		Args:       inv.ArgsJSON,
		UnixTime:   time.Now().Unix(),
	})
}
