package tacklr

import (
	"context"
	"encoding/json"
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

// writeApprovalLog is the session-backed audit used by writeApprovalGate.
// session.Runtime implements it; it is not part of HarnessRuntime.
type writeApprovalLog interface {
	WriteApprovalFor(toolCallID string) (WriteApprovalRecord, bool)
	RecordWriteApproval(WriteApprovalRecord)
}

func rejectedWrite(name string) error {
	return fmt.Errorf("%w: user rejected write %q", ErrToolPermissionDenied, name)
}

// writeApprovalGate parks WritePermission tools until the host approves, edits, or rejects.
func writeApprovalGate(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if inv.Tool == nil || inv.Tool.Access == nil || !inv.Tool.Access.Contains(WritePermission) {
		return next(ctx, inv)
	}
	log, ok := inv.Runtime.(writeApprovalLog)
	if !ok {
		return "", fmt.Errorf("%w: write approval requires a runtime", ErrFailed)
	}

	name := inv.Tool.Name
	if rec, found := log.WriteApprovalFor(inv.Runtime.CurrentToolCallID()); found {
		if rec.Action == WriteApprovalReject {
			return "", rejectedWrite(name)
		}
		if rec.Action == WriteApprovalEdit {
			inv.ArgsJSON = rec.Args
		}
		return next(ctx, inv)
	}

	title := ResolveToolTitle(inv.Tool.DisplayName, name, inv.ArgsJSON)
	initPayload, _ := json.Marshal(map[string]any{
		"toolName": name,
		"title":    title,
		"args":     inv.ArgsJSON,
	})
	intr, err := inv.Runtime.RaiseInterrupt(WriteApprovalType, initPayload)
	if err != nil {
		return "", err
	}
	wa, ok := intr.(*interrupt.WriteApprovalInterrupt)
	if !ok {
		return "", fmt.Errorf("%w: unexpected write approval interrupt type %T", ErrFailed, intr)
	}

	args := inv.ArgsJSON
	if wa.Action == WriteApprovalEdit {
		args = wa.Args
		inv.ArgsJSON = args
	}
	log.RecordWriteApproval(WriteApprovalRecord{
		ToolName:   name,
		ToolCallID: inv.Runtime.CurrentToolCallID(),
		Action:     wa.Action,
		Args:       args,
		UnixTime:   time.Now().Unix(),
	})
	if wa.Action == WriteApprovalReject {
		return "", rejectedWrite(name)
	}
	return next(ctx, inv)
}
