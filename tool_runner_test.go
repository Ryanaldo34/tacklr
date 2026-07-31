package tacklr

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestToolRunner_invokesToolWhenNoInterceptors(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name: "echo",
		Handler: func(ctx context.Context, args struct {
			Msg string `json:"msg"`
		}) (string, error) {
			return args.Msg, nil
		},
	})
	runner := NewToolRunner()
	got, err := runner.Run(context.Background(), ToolInvocation{
		Tool:     tool,
		ArgsJSON: `{"msg":"hi"}`,
		Runtime:  control.NewRuntime(nil, nil, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "hi" {
		t.Fatalf("got %q, want hi", got)
	}
}

func TestToolRunner_interceptorOrderAndShortCircuit(t *testing.T) {
	var order []string
	tool := NewTool(ToolConfig{
		Name: "t",
		Handler: func(ctx context.Context) (string, error) {
			order = append(order, "tool")
			return "ok", nil
		},
	})

	outer := func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		order = append(order, "outer-before")
		out, err := next(ctx, inv)
		order = append(order, "outer-after")
		return out, err
	}
	inner := func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		order = append(order, "inner-before")
		out, err := next(ctx, inv)
		order = append(order, "inner-after")
		return out, err
	}
	block := func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		order = append(order, "block")
		return "blocked", nil
	}

	t.Run("passes through in order", func(t *testing.T) {
		order = nil
		runner := NewToolRunner(outer, inner)
		got, err := runner.Run(context.Background(), ToolInvocation{Tool: tool})
		if err != nil {
			t.Fatal(err)
		}
		if got != "ok" {
			t.Fatalf("got %q", got)
		}
		want := []string{"outer-before", "inner-before", "tool", "inner-after", "outer-after"}
		if strings.Join(order, ",") != strings.Join(want, ",") {
			t.Fatalf("order = %v, want %v", order, want)
		}
	})

	t.Run("short circuit skips tool", func(t *testing.T) {
		order = nil
		runner := NewToolRunner(outer, block, inner)
		got, err := runner.Run(context.Background(), ToolInvocation{Tool: tool})
		if err != nil {
			t.Fatal(err)
		}
		if got != "blocked" {
			t.Fatalf("got %q, want blocked", got)
		}
		want := []string{"outer-before", "block", "outer-after"}
		if strings.Join(order, ",") != strings.Join(want, ",") {
			t.Fatalf("order = %v, want %v", order, want)
		}
	})
}

func TestPlanningWriteLock_deniesWriteToolsWithoutPlan(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()

	writeTool := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		Handler: func(ctx context.Context) (string, error) {
			return "mutated", nil
		},
	})
	readTool := NewTool(ToolConfig{
		Name:   "lookup",
		Access: ToolReadAccess,
		Handler: func(ctx context.Context) (string, error) {
			return "looked up", nil
		},
	})
	noAccessTool := NewTool(ToolConfig{
		Name: "plain",
		Handler: func(ctx context.Context) (string, error) {
			return "plain", nil
		},
	})

	runner := NewToolRunner(PlanningWriteLock)

	t.Run("write tool blocked without plan", func(t *testing.T) {
		_, err := runner.Run(context.Background(), ToolInvocation{Tool: writeTool, Runtime: rt})
		if !errors.Is(err, ErrToolPermissionDenied) {
			t.Fatalf("got %v, want ErrToolPermissionDenied", err)
		}
	})

	t.Run("read tool allowed without plan", func(t *testing.T) {
		got, err := runner.Run(context.Background(), ToolInvocation{Tool: readTool, Runtime: rt})
		if err != nil {
			t.Fatal(err)
		}
		if got != "looked up" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("tool without access allowed without plan", func(t *testing.T) {
		got, err := runner.Run(context.Background(), ToolInvocation{Tool: noAccessTool, Runtime: rt})
		if err != nil {
			t.Fatal(err)
		}
		if got != "plain" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("write tool allowed after plan exists", func(t *testing.T) {
		rt.PlanSet([]control.Todo{{Title: "step", Status: streaming.TodoStatusInProgress}})
		got, err := runner.Run(context.Background(), ToolInvocation{Tool: writeTool, Runtime: rt})
		if err != nil {
			t.Fatal(err)
		}
		if got != "mutated" {
			t.Fatalf("got %q", got)
		}
	})
}

func TestHarness_planningModeLocksWriteToolsInContext(t *testing.T) {
	writeTool := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		Handler: func(ctx context.Context) (string, error) {
			return "should not run", nil
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "c1", CallID: "c1", Name: "mutate", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "blocked and done", IsComplete: true}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{writeTool},
	})

	ch, err := ah.Run(context.Background(), "do a write")
	if err != nil {
		t.Fatal(err)
	}
	var toolResult string
	for ev := range ch {
		if ev.Type == StreamEventToolResult {
			toolResult = ev.Content
		}
	}
	if toolResult == "" {
		t.Fatal("expected tool result in stream")
	}
	if !strings.Contains(toolResult, "permission denied") && !strings.Contains(toolResult, "locked") {
		t.Fatalf("tool result %q should explain write lock", toolResult)
	}
	if strings.Contains(toolResult, "should not run") {
		t.Fatalf("write handler ran despite planning lock: %q", toolResult)
	}

	found := false
	for _, m := range ah.ContextWindow {
		if m != nil && m.Role == RoleTool && (strings.Contains(m.Content, "permission denied") || strings.Contains(m.Content, "locked")) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected permission denial in context window tool messages")
	}
}

func TestHarness_writeToolRunsAfterPlanCreated(t *testing.T) {
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
					{ID: "c1", CallID: "c1", Name: "create_plan",
						Arguments: `{"todos":[{"title":"A","status":"pending","description":"d"}]}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			case 2:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "c2", CallID: "c2", Name: "mutate", Arguments: `{}`},
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
	for range ch {
	}

	foundMutated := false
	for _, m := range ah.ContextWindow {
		if m != nil && m.Role == RoleTool && m.Content == "mutated" {
			foundMutated = true
			break
		}
	}
	if !foundMutated {
		t.Fatal("expected successful mutate tool result in context after plan was created")
	}
}
