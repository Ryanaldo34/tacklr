package tacklr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	mcpruntime "github.com/ryanaldo34/tacklr/internal/mcp"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
)

// drainEvents collects all events until the channel closes.
func drainEvents(events <-chan StreamEvent) []StreamEvent {
	var out []StreamEvent
	for ev := range events {
		out = append(out, ev)
	}
	return out
}

func hasEventType(events []StreamEvent, typ StreamEventType) bool {
	for _, ev := range events {
		if ev.Type == typ {
			return true
		}
	}
	return false
}

func hasErrorIs(events []StreamEvent, target error) bool {
	for _, ev := range events {
		if ev.Type == StreamEventError && ev.Error != nil && errors.Is(ev.Error, target) {
			return true
		}
	}
	return false
}

func hasToolResultContent(events []StreamEvent, substr string) bool {
	for _, ev := range events {
		if ev.Type == StreamEventToolResult && strings.Contains(ev.Content, substr) {
			return true
		}
	}
	return false
}

// TestRun_uninitializedHarnessFails: Run without constructor setup is rejected.
func TestRun_uninitializedHarnessFails(t *testing.T) {
	h := &AgentHarness{}
	_, err := h.Run(context.Background(), "hi")
	if err == nil || !strings.Contains(err.Error(), "uninitialized") {
		t.Fatalf("err = %v", err)
	}
}

// TestRun_skillsDirectoryMissing_failsRun: bad skill roots surface as a Run error.
func TestRun_skillsDirectoryMissing_failsRun(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{
			MaxWindowSize:    8192,
			SkillDirectories: []string{filepath.Join(t.TempDir(), "missing-skills")},
		},
		Model: &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "x", IsComplete: true}
			},
		},
	})
	_, err := h.Run(context.Background(), "hi")
	if err == nil || !strings.Contains(err.Error(), "skills") {
		t.Fatalf("err = %v", err)
	}
}

// TestRun_maxTurnRequests_emitsStopReason: a tool loop that would re-invoke the
// model stops with ErrMaxTurnRequests after the configured limit.
func TestRun_maxTurnRequests_emitsStopReason(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name: "ping",
		Handler: func(ctx context.Context) (string, error) {
			return "pong", nil
		},
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			// Always request another tool call so the turn would re-invoke forever.
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "c1", CallID: "c1", Name: "ping", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192, MaxTurnRequests: 1},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	events, err := h.Run(context.Background(), "loop")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasErrorIs(got, ErrMaxTurnRequests) {
		t.Fatalf("want ErrMaxTurnRequests in events, got %+v", summarizeEvents(got))
	}
}

// TestRun_invokeError_emitsErrorEvent: model Invoke failures end the turn with an error event.
func TestRun_invokeError_emitsErrorEvent(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{invokeErr: errors.New("provider down")},
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasEventType(got, StreamEventError) {
		t.Fatalf("want error event, got %+v", summarizeEvents(got))
	}
	var saw bool
	for _, ev := range got {
		if ev.Type == StreamEventError && ev.Error != nil && strings.Contains(ev.Error.Error(), "provider down") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want provider down in error, got %+v", summarizeEvents(got))
	}
}

// TestRun_modelStreamError_afterToolAnnounce_emitsFailedToolResults: incomplete
// tool announcements are closed with an error status when the model stream fails.
func TestRun_modelStreamError_afterToolAnnounce_emitsFailedToolResults(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "ann1", CallID: "ann1", Name: "missing_tool", Arguments: `{}`},
				},
				IsComplete: false,
			}
			ch <- LLMResponseChunk{
				Type:    StreamEventError,
				Content: "upstream failed",
			}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "model error") {
		t.Fatalf("want failed tool result for announced call, got %+v", summarizeEvents(got))
	}
}

// TestRun_incompleteToolCall_emitsFailedToolResult: announced but non-executable
// tool calls get a terminal tool_result so clients leave in_progress.
func TestRun_incompleteToolCall_emitsFailedToolResult(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "inc1", CallID: "inc1", Name: "ghost", Arguments: `{}`},
				},
				IsComplete: false,
			}
			// Stream ends without a complete executable function_call.
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "incomplete") {
		t.Fatalf("want incomplete tool result, got %+v", summarizeEvents(got))
	}
	if !hasEventType(got, StreamEventComplete) {
		t.Fatalf("want complete after incomplete tools closed, got %+v", summarizeEvents(got))
	}
}

// TestRun_unknownTool_surfacesToolResultError: missing tools produce an error tool_result, not a panic.
func TestRun_unknownTool_surfacesToolResultError(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "u1", CallID: "u1", Name: "does_not_exist", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "not found") && !hasToolResultContent(got, "does_not_exist") {
		t.Fatalf("want unknown tool error result, got %+v", summarizeEvents(got))
	}
}

// TestRun_toolCallKeyUsesCallIDWhenIDEmpty: CallID alone is enough for lifecycle ids.
func TestRun_toolCallKeyUsesCallIDWhenIDEmpty(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name: "echo",
		Handler: func(ctx context.Context) (string, error) {
			return "ok", nil
		},
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{CallID: "only-call-id", Name: "echo", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	var saw bool
	for _, ev := range got {
		if ev.Type == StreamEventToolResult && ev.MessageID == "only-call-id" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want tool_result message id from CallID, got %+v", summarizeEvents(got))
	}
}

// TestRun_modelErrorContentWithoutErrorField_becomesErrorEvent: content-only
// stream errors are still surfaced as error events to clients.
func TestRun_modelErrorContentWithoutErrorField_becomesErrorEvent(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Content: "plain failure text"}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	var saw bool
	for _, ev := range got {
		if ev.Type == StreamEventError && ev.Error != nil && strings.Contains(ev.Error.Error(), "plain failure text") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want error from content, got %+v", summarizeEvents(got))
	}
}

// TestRun_customInstructionsInSystemPrompt: creator instructions are present
// on the system prompt the model receives.
func TestRun_customInstructionsInSystemPrompt(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192, SystemPrompt: "Always greet formally."},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	drainEvents(events)
	strategy.mu.Lock()
	prompts := append([]string(nil), strategy.systemPrompts...)
	strategy.mu.Unlock()
	found := false
	for _, p := range prompts {
		if strings.Contains(p, "Always greet formally.") {
			found = true
		}
	}
	if !found {
		t.Fatalf("system prompts missing custom instructions: %v", prompts)
	}
}

// TestRun_mcpToolsDiscoveredAndInvokable: MCP configs inject callable tools
// into the turn, and Close runs discovery cleanup.
func TestRun_mcpToolsDiscoveredAndInvokable(t *testing.T) {
	var cleaned bool
	prev := discoverAllTools
	discoverAllTools = func(ctx context.Context, configs []mcp.MCPConfig, register mcpruntime.RegisterTool) func() {
		register("mcp_echo", "echo", "fake", nil, func(ctx context.Context, args map[string]any) (string, error) {
			return "from-mcp", nil
		})
		return func() { cleaned = true }
	}
	t.Cleanup(func() { discoverAllTools = prev })

	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "m1", CallID: "m1", Name: "mcp_echo", Namespace: "fake", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		MCPConfigs: []mcp.MCPConfig{
			{Name: "fake", Type: "http", URL: "http://127.0.0.1:1"},
		},
	})
	events, err := h.Run(context.Background(), "use mcp")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "from-mcp") {
		t.Fatalf("want MCP tool result, got %+v", summarizeEvents(got))
	}
	h.Close()
	if !cleaned {
		t.Fatal("expected MCP cleanup on Close")
	}
	// Second Close is a no-op.
	h.Close()
}

// TestRun_emptyToolInterceptorChain_allowsWriteWithoutPlan: replacing the
// built-in interceptor chain with an empty slice disables planning write lock.
func TestRun_emptyToolInterceptorChain_allowsWriteWithoutPlan(t *testing.T) {
	write := NewTool(ToolConfig{
		Name:   "mutate",
		Access: ToolWriteAccess,
		Handler: func(ctx context.Context) (string, error) {
			return "mutated", nil
		},
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "w1", CallID: "w1", Name: "mutate", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config:           Config{MaxWindowSize: 8192},
		Model:            strategy,
		Tools:            []*Tool{write},
		ToolInterceptors: []ToolInterceptor{}, // explicit empty replaces defaults
	})
	events, err := h.Run(context.Background(), "write")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "mutated") {
		t.Fatalf("write should succeed with empty interceptor chain, got %+v", summarizeEvents(got))
	}
}

// TestRun_readSkill_unknownName_toolError: unknown skill names fail the tool call.
func TestRun_readSkill_unknownName_toolError(t *testing.T) {
	dir := t.TempDir()
	skillDir := filepath.Join(dir, "demo")
	if err := os.Mkdir(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: demo\ndescription: d\n---\n\nDo the demo.\n"
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "s1", CallID: "s1", Name: "read_skill", Arguments: `{"name":"missing"}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192, SkillDirectories: []string{dir}},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "skill")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "unknown skill") {
		t.Fatalf("want unknown skill tool error, got %+v", summarizeEvents(got))
	}
}

// TestRun_checkpointSaveFailure_turnStillCompletes: store save errors are
// logged but do not block a successful complete event.
func TestRun_checkpointSaveFailure_turnStillCompletes(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  failSaveStore{},
	})
	h.SessionId = "sess-save-fail"
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasEventType(got, StreamEventComplete) {
		t.Fatalf("want complete despite save fail, got %+v", summarizeEvents(got))
	}
}

type failSaveStore struct{}

func (failSaveStore) SaveSession(context.Context, string, stores.SessionCheckpoint) error {
	return errors.New("save failed")
}
func (failSaveStore) LoadSession(context.Context, string) (stores.SessionCheckpoint, error) {
	return stores.SessionCheckpoint{}, stores.ErrSessionNotFound
}

// TestNewAgentFromSession_requiresStoreAndLoadsEmptyMaps: nil store is rejected;
// empty interrupt maps on a checkpoint still restore a runnable harness.
func TestNewAgentFromSession_requiresStoreAndLoadsEmptyMaps(t *testing.T) {
	_, err := NewAgentFromSession(context.Background(), "s", AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
	})
	if err == nil || !strings.Contains(err.Error(), "store") {
		t.Fatalf("nil store: %v", err)
	}

	store := testStore(t)
	cp, err := stores.NewCheckpoint([]*Message{{Role: RoleUser, Content: "hi"}}, nil, nil, map[string]any{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit nil maps on state after NewCheckpoint may still set nils.
	cp.State.InterruptToRequester = nil
	cp.State.PendingToolCalls = nil
	if err := store.SaveSession(context.Background(), "s1", *cp); err != nil {
		t.Fatal(err)
	}
	h, err := NewAgentFromSession(context.Background(), "s1", AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model: &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "resumed", IsComplete: true}
			},
		},
		Store: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := h.Run(context.Background(), "continue")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasEventType(got, StreamEventMessage) && !hasEventType(got, StreamEventComplete) {
		t.Fatalf("want resumed turn events, got %+v", summarizeEvents(got))
	}
}

// TestNewAgentFromSession_loadError: missing session fails construction.
func TestNewAgentFromSession_loadError(t *testing.T) {
	_, err := NewAgentFromSession(context.Background(), "missing", AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		Store:  testStore(t),
	})
	if err == nil {
		t.Fatal("want load error")
	}
}

// TestRun_watchdogRecordsToolResults: successful tool calls are recorded on the watchdog.
func TestRun_watchdogRecordsToolResults(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "tool-out", nil },
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "t1", CallID: "t1", Name: "echo", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	wd := &recordingWatchdog{}
	h := NewAgent(context.Background(), AgentOptions{
		Config:   Config{MaxWindowSize: 8192},
		Model:    strategy,
		Tools:    []*Tool{tool},
		WatchDog: wd,
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	drainEvents(events)
	if len(wd.toolResults) == 0 {
		t.Fatal("expected RecordToolResult")
	}
}

// TestRun_displayNameOnFunctionCall_stream: tools with DisplayName surface that
// name on streamed function_call events without breaking execution.
func TestRun_displayNameOnFunctionCall_stream(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:        "internal_name",
		DisplayName: "Friendly Name",
		Handler:     func(ctx context.Context) (string, error) { return "ran", nil },
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "d1", CallID: "d1", Name: "internal_name", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	var sawDisplay bool
	for _, ev := range got {
		if ev.Type == StreamEventFunctionCall {
			for _, tc := range ev.ToolCalls {
				if tc.Name == "Friendly Name" {
					sawDisplay = true
				}
			}
		}
	}
	if !sawDisplay {
		t.Fatalf("want display name on function_call stream, got %+v", summarizeEvents(got))
	}
	if !hasToolResultContent(got, "ran") {
		t.Fatalf("want tool executed under internal name, got %+v", summarizeEvents(got))
	}
}

// TestRun_cancelAfterToolAnnounce_closesAnnouncedTools: cancel while the model
// is still streaming closes announced tool calls as cancelled.
func TestRun_cancelAfterToolAnnounce_closesAnnouncedTools(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "can1", CallID: "can1", Name: "ghost", Arguments: `{}`},
				},
				IsComplete: false,
			}
			once.Do(func() { close(started) })
			<-ctx.Done()
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := h.Run(ctx, "hi")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("invoke did not start")
	}
	// Let the function_call event be delivered, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()
	got := drainEvents(events)
	if !hasErrorIs(got, context.Canceled) && !hasToolResultContent(got, "cancelled") {
		t.Fatalf("want cancel outcome, got %+v", summarizeEvents(got))
	}
}

func summarizeEvents(events []StreamEvent) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		s := string(ev.Type)
		if ev.Content != "" {
			s += ":" + ev.Content
		}
		if ev.Error != nil {
			s += ":" + ev.Error.Error()
		}
		out = append(out, s)
	}
	return out
}
