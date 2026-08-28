package tacklr

import (
	"context"
	"errors"
	"testing"
)

// TestHarness_planningWriteLock_thenUnlockAfterCreatePlan: a WritePermission
// tool is denied until create_plan; the next call parks tool_permission and
// allow-once after reload runs the handler.
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
	rt, sm := onCallRuntime()
	runner := newToolRunner(onCallMiddleware(sm))
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{"path":"/a"}`, Runtime: rt}

	_, _, err := runner.Run(t.Context(), inv)
	requireParked(t, err, "tool_permission")
	if _, err := sm.Resume("c1", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
	out, _, err := runner.Run(t.Context(), inv)
	if err != nil || calls != 1 || seen != "/a" || out != "ok" {
		t.Fatalf("invoke: calls=%d seen=%q out=%q err=%v", calls, seen, out, err)
	}
	out, _, err = runner.Run(t.Context(), inv)
	if err != nil || calls != 2 || seen != "/a" || out != "ok" {
		t.Fatalf("re-entry: calls=%d seen=%q out=%q err=%v", calls, seen, out, err)
	}
}

// TestOnCallMiddleware_permissionMemory: allow-always skips the next park;
// reject-always denies without a yield.
func TestOnCallMiddleware_permissionMemory(t *testing.T) {
	var calls int
	tool := NewTool(ToolConfig{
		Name:   "sensitive",
		OnCall: []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			calls++
			return "secret", nil
		},
	})
	rt, sm := onCallRuntime()
	sm.Permissions.Remember("sensitive", permAllowAlways)
	runner := newToolRunner(onCallMiddleware(sm))
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{}`, Runtime: rt}
	out, _, err := runner.Run(t.Context(), inv)
	if err != nil || calls != 1 || out != "secret" {
		t.Fatalf("allow-always: calls=%d out=%q err=%v", calls, out, err)
	}

	denyRT, denySM := onCallRuntime()
	denySM.Permissions.Remember("sensitive", permDenyAlways)
	runner = newToolRunner(onCallMiddleware(denySM))
	denyInv := ToolInvocation{Tool: tool, ArgsJSON: `{}`, Runtime: denyRT}
	_, _, err = runner.Run(t.Context(), denyInv)
	if !errors.Is(err, ErrToolPermissionDenied) || calls != 1 {
		t.Fatalf("reject-always: calls=%d err=%v", calls, err)
	}
	_, _, err = runner.Run(t.Context(), denyInv)
	if !errors.Is(err, ErrToolPermissionDenied) || calls != 1 {
		t.Fatalf("denied re-entry: calls=%d err=%v", calls, err)
	}
}

// TestOnCallMiddleware_requiresRuntime: OnCall constructors need a runtime.
func TestOnCallMiddleware_requiresRuntime(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "gated",
		OnCall:  []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) { return "x", nil },
	})
	_, _, err := newToolRunner(onCallMiddleware(nil)).Run(t.Context(), ToolInvocation{
		Tool: tool, ArgsJSON: `{}`,
	})
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("err=%v", err)
	}
	rt, sm := onCallRuntime()
	rt = rt.WithToolCallID("")
	_, _, err = newToolRunner(onCallMiddleware(sm)).Run(t.Context(), ToolInvocation{
		Tool: tool, ArgsJSON: `{}`, Runtime: rt,
	})
	if err == nil {
		t.Fatal("empty tool call id must fail adopt")
	}
}

// TestHarness_writeTool_parksPermission: injected write parks tool_permission.
func TestOnCallMiddleware_emptyStackInvokes(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "hi", nil },
	})
	rt, sm := onCallRuntime()
	out, _, err := newToolRunner(onCallMiddleware(sm)).Run(t.Context(), ToolInvocation{
		Tool: tool, ArgsJSON: `{}`, Runtime: rt,
	})
	if err != nil || out != "hi" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func onCallRuntime() (toolRuntime, *sessionManager) {
	sm := newSessionManager()
	ch := make(chan StreamEvent, 8)
	go func() {
		for range ch {
		}
	}()
	return newToolRuntime(ch, sm, nil).WithToolCallID("c1"), sm
}

func requireParked(t *testing.T, err error, kind string) {
	t.Helper()
	var parked Interrupt
	if !errors.As(err, &parked) || parked.TypeName() != kind {
		t.Fatalf("park type=%v err=%v", parked, err)
	}
}
