package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// TestToolRunner_interceptorChainOrder is the extension contract for custom
// interceptors: outermost first, short-circuit skips later stages and the tool.
// Nil interceptors in the chain are skipped.
func TestHarness_planningWriteLock_thenUnlockAfterCreatePlan(t *testing.T) {
	var calls int
	writeTool := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		OnCall: OnCalls(WriteApprovalOnCall),
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
	store := stores.NewInMemoryStore()
	opts := AgentOptions{
		SessionID: "plan-then-write",
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Store:     store,
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
	if calls != 0 || kind != WriteApprovalType || interruptID == "" {
		t.Fatalf("park: calls=%d kind=%q id=%q", calls, kind, interruptID)
	}

	ah.Close()
	reloaded, err := NewAgentFromSession(context.Background(), "plan-then-write", opts)
	if err != nil {
		t.Fatal(err)
	}
	ch2, err := reloaded.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"action":"approve"}`),
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
		t.Fatalf("approve after reload: unlocked=%v calls=%d", unlocked, calls)
	}
	recs := reloaded.WriteApprovals()
	if len(recs) != 1 || recs[0].Action != WriteApprovalApprove || recs[0].ToolName != "mutate" {
		t.Fatalf("audit = %+v", recs)
	}
}

// TestHarness_toolPermission_allowAlwaysRemembers: first call parks for
// permission; allow-always resumes and runs the tool. After Close +
// NewAgentFromSession the grant still skips the interrupt.
func TestHarness_toolPermission_allowAlwaysRemembers(t *testing.T) {
	var handlerCalls int
	tool := NewTool(ToolConfig{
		Name:   "sensitive",
		OnCall: OnCalls(ToolPermissionOnCall),
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
	store := stores.NewInMemoryStore()
	opts := AgentOptions{
		SessionID: "perm-reload",
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Store:     store,
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

	ah.Close()
	reloaded, err := NewAgentFromSession(context.Background(), "perm-reload", opts)
	if err != nil {
		t.Fatal(err)
	}

	ch3, err := reloaded.Run(context.Background(), "again")
	if err != nil {
		t.Fatal(err)
	}
	var sawInterrupt bool
	for ev := range ch3 {
		if ev.Type == StreamEventInterrupt {
			sawInterrupt = true
		}
	}
	if sawInterrupt {
		t.Fatal("allow-always should not re-raise permission interrupt")
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
		OnCall:  OnCalls(ToolPermissionOnCall),
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
	var sawInterrupt bool
	for ev := range ch3 {
		if ev.Type == StreamEventInterrupt {
			sawInterrupt = true
		}
	}
	if sawInterrupt {
		t.Fatal("reject-always should not re-raise permission interrupt")
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
		OnCall: OnCalls(WriteApprovalOnCall, ToolPermissionOnCall),
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
		DisablePlanningLock: true,
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
	id, kind := drainYield(t, ch)
	if hostSaw != "gated" {
		t.Fatalf("host interceptor saw %q", hostSaw)
	}
	if kind != WriteApprovalType {
		t.Fatalf("write approval type = %q", kind)
	}
	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"action":"approve"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, kind2 := drainYield(t, ch2)
	if kind2 != "tool_permission" {
		t.Fatalf("permission gate type = %q", kind2)
	}
}

// TestHarness_writeApproval_rejectDeniesWrite: reject fails the tool; the
// handler does not run; the audit records reject.
func TestHarness_writeApproval_rejectDeniesWrite(t *testing.T) {
	var calls int
	tool := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		OnCall: OnCalls(WriteApprovalOnCall),
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
		DisablePlanningLock: true,
	})
	ch, err := ah.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := drainYield(t, ch)
	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		id: []byte(`{"action":"reject"}`),
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
	recs := ah.WriteApprovals()
	if len(recs) != 1 || recs[0].Action != WriteApprovalReject || recs[0].ToolName != "mutate" {
		t.Fatalf("audit = %+v", recs)
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

func onCallRuntime() session.Runtime {
	ch := make(chan streaming.StreamEvent, 8)
	go func() {
		for range ch {
		}
	}()
	return session.NewRuntime(ch, session.NewSessionManager()).WithToolCallID("c1")
}

func requireParked(t *testing.T, err error, kind string) {
	t.Helper()
	var parked Interrupt
	if !errors.As(err, &parked) || parked.TypeName() != kind {
		t.Fatalf("park type=%v err=%v", parked, err)
	}
}

// TestOnCallMiddleware_writeThenPermission is the isolated middleware stack:
// write_approval then tool_permission, then the handler runs with the original args.
func TestOnCallMiddleware_writeThenPermission(t *testing.T) {
	var calls int
	var seen string
	tool := NewTool(ToolConfig{
		Name:   "mutate",
		OnCall: OnCalls(WriteApprovalOnCall, ToolPermissionOnCall),
		Handler: func(ctx context.Context, args struct {
			Path string `json:"path"`
		}) (string, error) {
			calls++
			seen = args.Path
			return "ok", nil
		},
	})
	rt := onCallRuntime()
	runner := newToolRunner(onCallMiddleware())
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{"path":"/a"}`, Runtime: rt}

	_, _, err := runner.Run(t.Context(), inv)
	requireParked(t, err, WriteApprovalType)
	if _, err := rt.ReturnInterrupt("c1", []byte(`{"action":"approve"}`)); err != nil {
		t.Fatal(err)
	}
	_, _, err = runner.Run(t.Context(), inv)
	requireParked(t, err, "tool_permission")
	if _, err := rt.ReturnInterrupt("c1", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
	out, _, err := runner.Run(t.Context(), inv)
	if err != nil || calls != 1 || seen != "/a" || out != "ok" {
		t.Fatalf("invoke: calls=%d seen=%q out=%q err=%v", calls, seen, out, err)
	}
}

// TestOnCallMiddleware_permissionMemory: allow-always skips the next park;
// reject-always denies without a yield.
func TestOnCallMiddleware_permissionMemory(t *testing.T) {
	var calls int
	tool := NewTool(ToolConfig{
		Name:   "sensitive",
		OnCall: OnCalls(ToolPermissionOnCall),
		Handler: func(ctx context.Context) (string, error) {
			calls++
			return "secret", nil
		},
	})
	rt := onCallRuntime()
	rt.RememberPermissionAllow("sensitive")
	runner := newToolRunner(onCallMiddleware())
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{}`, Runtime: rt}
	out, _, err := runner.Run(t.Context(), inv)
	if err != nil || calls != 1 || out != "secret" {
		t.Fatalf("allow-always: calls=%d out=%q err=%v", calls, out, err)
	}

	denyRT := onCallRuntime()
	denyRT.RememberPermissionDeny("sensitive")
	_, _, err = runner.Run(t.Context(), ToolInvocation{Tool: tool, ArgsJSON: `{}`, Runtime: denyRT})
	if !errors.Is(err, ErrToolPermissionDenied) || calls != 1 {
		t.Fatalf("reject-always: calls=%d err=%v", calls, err)
	}
}

// TestOnCallMiddleware_skipWriteStillParksPermission: DisableWriteApproval
// omits write_approval and still runs the permission layer.
func TestOnCallMiddleware_skipWriteStillParksPermission(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "gated",
		OnCall:  OnCalls(WriteApprovalOnCall, ToolPermissionOnCall),
		Handler: func(ctx context.Context) (string, error) { return "nope", nil },
	})
	rt := onCallRuntime()
	runner := newToolRunner(onCallMiddleware(WriteApprovalType))
	_, _, err := runner.Run(t.Context(), ToolInvocation{Tool: tool, ArgsJSON: `{}`, Runtime: rt})
	requireParked(t, err, "tool_permission")
}

// TestOnCallMiddleware_emptyStackInvokes: no OnCall layers means the handler runs.
func TestOnCallMiddleware_emptyStackInvokes(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "hi", nil },
	})
	rt := onCallRuntime()
	out, _, err := newToolRunner(onCallMiddleware()).Run(t.Context(), ToolInvocation{
		Tool: tool, ArgsJSON: `{}`, Runtime: rt,
	})
	if err != nil || out != "hi" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}
