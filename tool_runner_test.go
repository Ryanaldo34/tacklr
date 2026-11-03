package tacklr

import (
	"context"
	"errors"
	"testing"
)

// TestOnCallMiddleware_permissionThenInvoke: a permission-gated call parks
// tool_permission; after allow-once the handler runs, and the call runs
// again without a new park (the layer is on its round call).
func TestOnCallMiddleware_permissionThenInvoke(t *testing.T) {
	var calls int
	var seen string
	tool := NewTool(ToolConfig{
		Name:   "mutate",
		OnCall: []OnCallFunc{func(ToolInvocation) Interrupt { return nil }, ToolPermissionOnCall},
		Handler: func(ctx context.Context, args struct {
			Path string `json:"path"`
		}) (string, error) {
			calls++
			seen = args.Path
			return "ok", nil
		},
	})
	cc := callRuntime(newSessionManager(), "c1")
	runner := newToolRunner(onCallMiddleware)
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{"path":"/a"}`, CallContext: cc}

	_, err := runner.Run(t.Context(), inv)
	requireParked(t, err, "tool_permission")
	if err := cc.resume([]byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
	out, err := runner.Run(t.Context(), inv)
	if err != nil || calls != 1 || seen != "/a" || out.Output != "ok" {
		t.Fatalf("invoke: calls=%d seen=%q out=%q err=%v", calls, seen, out.Output, err)
	}
	out, err = runner.Run(t.Context(), inv)
	if err != nil || calls != 2 || seen != "/a" || out.Output != "ok" {
		t.Fatalf("re-entry: calls=%d seen=%q out=%q err=%v", calls, seen, out.Output, err)
	}
}

// TestOnCallMiddleware_twoLayersParkOneAfterTheOther: a call with two
// OnCall layers parks on the first, then on the second; each answer goes to
// its own layer, and the handler runs once.
func TestOnCallMiddleware_twoLayersParkOneAfterTheOther(t *testing.T) {
	var calls int
	choose := func(ToolInvocation) Interrupt {
		return &UserSelectionInterrupt{Question: "which?", Options: []UserChoice{{Title: "a"}, {Title: "b"}}}
	}
	tool := NewTool(ToolConfig{
		Name:    "two_layers",
		OnCall:  []OnCallFunc{ToolPermissionOnCall, choose},
		Handler: func(context.Context) (string, error) { calls++; return "ok", nil },
	})
	cc := callRuntime(newSessionManager(), "c1")
	runner := newToolRunner(onCallMiddleware)
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{}`, CallContext: cc}

	_, first := runner.Run(t.Context(), inv)
	firstAnswer := cc.resume([]byte(`{"optionId":"allow-once"}`))
	_, second := runner.Run(t.Context(), inv)
	secondAnswer := cc.resume([]byte(`{"selectionIdx":1}`))
	out, err := runner.Run(t.Context(), inv)

	requireParked(t, first, "tool_permission")
	requireParked(t, second, "user_selection_choice")
	if firstAnswer != nil || secondAnswer != nil || err != nil || out.Output != "ok" || calls != 1 {
		t.Fatalf("answers %v %v, run %+v %v after %d calls, want one run", firstAnswer, secondAnswer, out, err, calls)
	}
}

// TestOnCallMiddleware_permissionAnswersWithoutAPark: a host grant and a
// user's allow-always run the tool without a park; a user's reject-always
// denies it, even over a grant.
func TestOnCallMiddleware_permissionAnswersWithoutAPark(t *testing.T) {
	var calls int
	tool := NewTool(ToolConfig{
		Name:    "sensitive",
		OnCall:  []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) { calls++; return "secret", nil },
	})
	run := func(decision permissionDecision, granted bool) (ToolOutcome, error) {
		sm := newSessionManager()
		cc := callRuntime(sm, "c1")
		if granted {
			sm.Permissions.granted = map[string]bool{"sensitive": true}
		}
		if decision != permNone {
			sm.Permissions.set("sensitive", decision)
		}
		return newToolRunner(onCallMiddleware).Run(t.Context(), ToolInvocation{Tool: tool, ArgsJSON: `{}`, CallContext: cc})
	}

	allowed, allowErr := run(permAllowAlways, false)
	grantedOut, grantErr := run(permNone, true)
	_, denyErr := run(permDenyAlways, true)

	if allowErr != nil || grantErr != nil || allowed.Output != "secret" || grantedOut.Output != "secret" || calls != 2 {
		t.Fatalf("allow-always %v, granted %v, calls %d, want both to run", allowErr, grantErr, calls)
	}
	if !errors.Is(denyErr, ErrToolPermissionDenied) {
		t.Fatalf("reject-always over a grant = %v, want denied", denyErr)
	}
}

// TestToolInterceptor_changesTheOutcome: an interceptor replaces what the
// model sees and sets the window effect after next returns.
func TestToolInterceptor_changesTheOutcome(t *testing.T) {
	tool := NewTool(ToolConfig{Name: "big", Handler: func(context.Context) (string, error) { return "very long output", nil }})
	capOutput := func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (ToolOutcome, error) {
		out, err := next(ctx, inv)
		out.Output = out.Output[:4]
		return out, err
	}
	handoff := func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (ToolOutcome, error) {
		out, err := next(ctx, inv)
		out.Effect = EffectHandoff
		return out, err
	}

	out, err := newToolRunner(capOutput, handoff).Run(t.Context(), ToolInvocation{Tool: tool, ArgsJSON: `{}`})

	if err != nil || out.Output != "very" || out.Effect != EffectHandoff {
		t.Fatalf("outcome = %+v %v, want the capped output and a handoff", out, err)
	}
}

func requireParked(t *testing.T, err error, kind string) {
	t.Helper()
	var parked Interrupt
	if !errors.As(err, &parked) || parked.TypeName() != kind {
		t.Fatalf("park type=%v err=%v", parked, err)
	}
}
