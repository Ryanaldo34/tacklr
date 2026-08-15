package tacklr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/stores"
)

// TestToolRunner_interceptorChainOrder is the extension contract for custom
// interceptors: outermost first, short-circuit skips later stages and the tool.
// Nil interceptors in the chain are skipped.
func TestHarness_planningWriteLock_thenUnlockAfterCreatePlan(t *testing.T) {
	writeTool := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		Handler: func(ctx context.Context) (string, error) {
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
	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{writeTool},
	})

	ch, err := ah.Run(context.Background(), "plan then write")
	if err != nil {
		t.Fatal(err)
	}
	// Pre-plan denial is only on the stream; create_plan prunes the window.
	var locked, unlocked bool
	for ev := range ch {
		if ev.Type != StreamEventToolResult {
			continue
		}
		if strings.Contains(ev.Content, "locked") || strings.Contains(ev.Content, "permission denied") {
			locked = true
		}
		if ev.Content == "mutated" {
			unlocked = true
		}
	}
	if !locked {
		t.Fatal("expected write denial while planning (no plan yet)")
	}
	if !unlocked {
		t.Fatal("expected mutate success after create_plan")
	}
}

// TestHarness_toolPermission_allowAlwaysRemembers: first call parks for
// permission; allow-always resumes and runs the tool. After Close +
// NewAgentFromSession the grant still skips the interrupt.
func TestHarness_toolPermission_allowAlwaysRemembers(t *testing.T) {
	var handlerCalls int
	tool := NewTool(ToolConfig{
		Name:               "sensitive",
		PermissionRequired: true,
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
	ah := NewAgent(context.Background(), opts)

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
		Name:               "sensitive",
		PermissionRequired: true,
		Handler:            func(ctx context.Context) (string, error) { return "nope", nil },
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
	ah := NewAgent(context.Background(), AgentOptions{
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
	ah := NewAgent(context.Background(), AgentOptions{
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
