package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestHarness_planningWriteLock_thenUnlockAfterCreatePlan: a WritePermission
// tool is denied until create_plan; the next call parks tool_permission and
// allow-once after reload runs the handler.
func TestHarness_planningWriteLock_thenUnlockAfterCreatePlan(t *testing.T) {
	var calls int
	writeTool := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		OnCall: []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			calls++
			return "mutated", nil
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			switch invokeCount {
			case 1:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "w1", CallID: "w1", Name: "mutate", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			case 2:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "p1", CallID: "p1", Name: "create_plan",
						Arguments: `{"plan":"P","todos":[{"title":"A","status":"pending","description":"d"}]}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			case 3:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "w2", CallID: "w2", Name: "mutate", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			}
		},
	}
	opts := AgentOptions{
		SessionID: "plan-then-write",
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Tools:     []*Tool{writeTool},
	}
	ah := mustNewAgent(t, opts)

	ch, err := ah.Run(context.Background(), "plan then write")
	if err != nil {
		t.Fatal(err)
	}
	// Pre-plan denial is a tool result; after create_plan the write parks.
	var locked bool
	var interruptID, kind string
	for ev := range ch {
		if ev.Type == StreamEventToolResult &&
			(strings.Contains(ev.Content, "locked") || strings.Contains(ev.Content, "permission denied")) {
			locked = true
		}
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
				Type        string `json:"type"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				t.Fatal(err)
			}
			interruptID, kind = payload.InterruptId, payload.Type
		}
	}
	if !locked {
		t.Fatal("expected write denial while planning (no plan yet)")
	}
	if calls != 0 || kind != "tool_permission" || interruptID == "" {
		t.Fatalf("park: calls=%d kind=%q id=%q", calls, kind, interruptID)
	}

	reloaded := reloadHarness(t, ah, opts)
	ch2, err := reloaded.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"allow-once"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var unlocked bool
	for ev := range ch2 {
		if ev.Type == StreamEventToolResult && ev.Content == "mutated" {
			unlocked = true
		}
	}
	if !unlocked || calls != 1 {
		t.Fatalf("allow after reload: unlocked=%v calls=%d", unlocked, calls)
	}
}

// TestHarness_toolPermission_allowAlwaysRemembers: first call parks for
// permission; allow-always resumes and runs the tool. After checkpoint reload
// the grant still skips the interrupt.
func TestHarness_toolPermission_allowAlwaysRemembers(t *testing.T) {
	var handlerCalls int
	tool := NewTool(ToolConfig{
		Name:   "sensitive",
		OnCall: []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			handlerCalls++
			return "secret-ok", nil
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			switch invokeCount {
			case 1, 3:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "s1", CallID: "s1", Name: "sensitive", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			}
		},
	}
	opts := AgentOptions{
		SessionID: "perm-reload",
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Tools:     []*Tool{tool},
	}
	ah := mustNewAgent(t, opts)

	ch1, err := ah.Run(context.Background(), "need secret")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range ch1 {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
				Type        string `json:"type"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
				if payload.Type != "tool_permission" {
					t.Fatalf("type = %q", payload.Type)
				}
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected tool_permission yield")
	}
	if handlerCalls != 0 {
		t.Fatal("handler must not run before permission")
	}

	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"allow-always"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}
	if handlerCalls != 1 {
		t.Fatalf("handlerCalls after allow = %d, want 1", handlerCalls)
	}

	reloaded := reloadHarness(t, ah, opts)

	ch3, err := reloaded.Run(context.Background(), "again")
	if err != nil {
		t.Fatal(err)
	}
	for range ch3 {
	}
	if handlerCalls != 2 {
		t.Fatalf("handlerCalls after second run = %d, want 2", handlerCalls)
	}
}

// TestHarness_toolPermission_rejectAlwaysRemembers: reject-always fails the tool
// and subsequent calls fail without another yield.
func TestHarness_toolPermission_rejectAlwaysRemembers(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "sensitive",
		OnCall:  []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) { return "nope", nil },
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			switch invokeCount {
			case 1, 3:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "s1", CallID: "s1", Name: "sensitive", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			}
		},
	}
	ah := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})

	ch1, err := ah.Run(context.Background(), "need secret")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range ch1 {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected permission yield")
	}

	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"reject-always"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}

	var denied int
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool && strings.Contains(m.Content, "permission denied") {
			denied++
		}
	}
	if denied < 1 {
		t.Fatal("expected permission denial after reject-always")
	}

	ch3, err := ah.Run(context.Background(), "again")
	if err != nil {
		t.Fatal(err)
	}
	for range ch3 {
	}
	denied = 0
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool && strings.Contains(m.Content, "permission denied") {
			denied++
		}
	}
	if denied < 2 {
		t.Fatalf("expected at least 2 denials in context, got %d", denied)
	}
}

// TestHarness_toolTimeout_surfacesAsToolResult: Timeout on the tool is enforced
// by the runner and reported as a tool result error in the turn.
func TestHarness_toolTimeout_surfacesAsToolResult(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "slow",
		Timeout: 40 * time.Millisecond,
		Handler: func(ctx context.Context) (string, error) {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(5 * time.Second):
				return "too late", nil
			}
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "t1", CallID: "t1", Name: "slow", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "after timeout", IsComplete: true}
		},
	}
	ah := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})

	start := time.Now()
	ch, err := ah.Run(context.Background(), "go slow")
	if err != nil {
		t.Fatal(err)
	}
	var toolResult string
	for ev := range ch {
		if ev.Type == StreamEventToolResult {
			toolResult = ev.Content
		}
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("turn took too long for a short tool timeout")
	}
	if !strings.Contains(toolResult, "timed out") && !strings.Contains(toolResult, "deadline") {
		t.Fatalf("tool result %q should report timeout", toolResult)
	}
	found := false
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool && (strings.Contains(m.Content, "timed out") || strings.Contains(m.Content, "deadline")) {
			found = true
		}
	}
	if !found {
		t.Fatal("timeout tool result missing from context window")
	}
}

// TestHarness_hostInterceptor_keepsPermissionGate: a host interceptor wraps
// outside the built-in gates and cannot disable the permission interrupt.
func TestHarness_hostInterceptor_keepsPermissionGate(t *testing.T) {
	var hostSaw string
	tool := NewTool(ToolConfig{
		Name:   "gated",
		Access: ToolWriteAccess,
		OnCall: []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			return "should-not-run", nil
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "c1", CallID: "c1", Name: "gated", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	ah := mustNewAgent(t, AgentOptions{
		Config:              Config{MaxWindowSize: 8192},
		Model:               strategy,
		Tools:               []*Tool{tool},
		disablePlanningLock: true,
		ToolInterceptors: []ToolInterceptor{
			func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
				hostSaw = inv.Tool.Name
				return next(ctx, inv)
			},
		},
	})
	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	_, kind := drainYield(t, ch)
	if hostSaw != "gated" {
		t.Fatalf("host interceptor saw %q", hostSaw)
	}
	if kind != "tool_permission" {
		t.Fatalf("permission gate type = %q", kind)
	}
}

// TestHarness_toolPermission_rejectOnceDeniesWrite: reject-once fails the
// tool; the handler does not run.
func TestHarness_toolPermission_rejectOnceDeniesWrite(t *testing.T) {
	var calls int
	tool := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		OnCall: []OnCallFunc{ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			calls++
			return "mutated", nil
		},
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "w1", CallID: "w1", Name: "mutate", Arguments: `{"path":"/a"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	ah := mustNewAgent(t, AgentOptions{
		Config:              Config{MaxWindowSize: 8192},
		Model:               strategy,
		Tools:               []*Tool{tool},
		disablePlanningLock: true,
	})
	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := drainYield(t, ch)
	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"optionId":"reject-once"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var denied bool
	for ev := range ch2 {
		if ev.Type == StreamEventToolResult && strings.Contains(ev.Content, "rejected") {
			denied = true
		}
	}
	if calls != 0 || !denied {
		t.Fatalf("reject: calls=%d denied=%v", calls, denied)
	}
}

func drainYield(t *testing.T, ch <-chan StreamEvent) (id, kind string) {
	t.Helper()
	for ev := range ch {
		if ev.Type != StreamEventInterrupt {
			continue
		}
		var payload struct {
			InterruptId string `json:"interruptId"`
			Type        string `json:"type"`
		}
		if err := json.Unmarshal(ev.Data, &payload); err != nil {
			t.Fatal(err)
		}
		return payload.InterruptId, payload.Type
	}
	t.Fatal("expected interrupt yield")
	return "", ""
}

func onCallRuntime() (session.Runtime, *session.SessionManager) {
	sm := session.NewSessionManager()
	ch := make(chan streaming.StreamEvent, 8)
	go func() {
		for range ch {
		}
	}()
	return session.NewRuntime(ch, sm).WithToolCallID("c1"), sm
}

func requireParked(t *testing.T, err error, kind string) {
	t.Helper()
	var parked Interrupt
	if !errors.As(err, &parked) || parked.TypeName() != kind {
		t.Fatalf("park type=%v err=%v", parked, err)
	}
}

// TestOnCallMiddleware_permissionThenInvoke: park, allow-once, then the
// handler runs with the original args.
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
	if _, err := sm.ReturnInterrupt("c1", []byte(`{"optionId":"allow-once"}`)); err != nil {
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
	sm.Permissions.Remember("sensitive", session.PermissionAllowAlways)
	runner := newToolRunner(onCallMiddleware(sm))
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{}`, Runtime: rt}
	out, _, err := runner.Run(t.Context(), inv)
	if err != nil || calls != 1 || out != "secret" {
		t.Fatalf("allow-always: calls=%d out=%q err=%v", calls, out, err)
	}

	denyRT, denySM := onCallRuntime()
	denySM.Permissions.Remember("sensitive", session.PermissionDenyAlways)
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
func TestHarness_writeTool_parksPermission(t *testing.T) {
	ctx := context.Background()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("write-perm", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	var n int
	ah := mustNewAgent(t, AgentOptions{
		Config:              Config{MaxWindowSize: 8192},
		disablePlanningLock: true,
		MountSession:        ms,
		Model: &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				n++
				if n == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "w1", CallID: "w1", Name: "write", Arguments: `{"path":"/work/a.txt","content":"hi"}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
					return
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			},
		},
	})
	ch, err := ah.Run(ctx, "write a file")
	if err != nil {
		t.Fatal(err)
	}
	_, kind := drainYield(t, ch)
	if kind != "tool_permission" {
		t.Fatalf("kind=%q", kind)
	}
}

// TestOnCallMiddleware_emptyStackInvokes: no OnCall layers means the handler runs.
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
