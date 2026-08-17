package tacklr

import (
	"context"
	"errors"
	"fmt"
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

// sequentialToolModel emits each tool-call batch on successive Invokes, then a final message.
func sequentialToolModel(batches ...[]ToolCall) *mockStrategy {
	var n int
	return &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n <= len(batches) {
				ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: batches[n-1], IsComplete: true}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
}

func toolCall(id, name, args string) ToolCall {
	return ToolCall{ID: id, CallID: id, Name: name, Arguments: args}
}

// runPrompt builds a harness, runs one prompt, drains events. opts may be nil.
func runPrompt(t *testing.T, model *mockStrategy, opts AgentOptions) (*AgentHarness, []StreamEvent) {
	t.Helper()
	if opts.Model == nil {
		opts.Model = model
	}
	if opts.Config.MaxWindowSize == 0 {
		opts.Config.MaxWindowSize = 8192
	}
	h := mustNewAgent(t, opts)
	t.Cleanup(h.Close)
	_ = h.SessionID() // exercise getter (empty when no store/session id set)
	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	return h, drainEvents(events)
}

func TestRunMessage_imageAcceptedOnlyWhenModelAllows(t *testing.T) {
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
	})
	t.Cleanup(h.Close)
	if _, err := h.RunMessage(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "requires a user message") {
		t.Fatalf("nil message: %v", err)
	}

	img := &Message{
		Content: "see this",
		ContentParts: []ContentPart{
			{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/png;base64,AAAA"}},
			{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/png;base64,BBBB"}},
			{Type: ContentTypeInputFile, FileData: &FileData{MIMEType: "", Filename: "skip"}},
		},
	}
	if _, err := h.RunMessage(context.Background(), img); err == nil || !strings.Contains(err.Error(), "unsupported content type") {
		t.Fatalf("text-only model: %v", err)
	}

	vision := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model: &mockStrategy{
			supportsMIMEFn: func(string) bool { return true },
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "a cat", IsComplete: true}
			},
		},
	})
	t.Cleanup(vision.Close)
	events, err := vision.RunMessage(context.Background(), img)
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	var saw bool
	for _, ev := range got {
		if ev.Type == StreamEventMessage && strings.Contains(ev.Content, "a cat") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("vision turn: %+v", summarizeEvents(got))
	}
}

func requireToolResult(t *testing.T, got []StreamEvent, substr string) {
	t.Helper()
	if !hasToolResultContent(got, substr) {
		t.Fatalf("want tool result containing %q, got %+v", substr, summarizeEvents(got))
	}
}

// TestRun_uninitializedHarnessFails: Run without constructor setup is rejected.
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
	h := mustNewAgent(t, AgentOptions{
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
	h := mustNewAgent(t, AgentOptions{
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
	h := mustNewAgent(t, AgentOptions{
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
					{ID: "", CallID: "", Name: "ghost", Arguments: `{}`},
					{ID: "inc1", CallID: "inc1", Name: "ghost", Arguments: `{}`},
				},
				IsComplete: false,
			}
			// Stream ends without a complete executable function_call.
		},
	}
	h := mustNewAgent(t, AgentOptions{
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
		invokeErrFn: func(context.Context, []*Message, []*Tool) error {
			if n >= 1 {
				return errors.New("after tools down")
			}
			return nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "u1", CallID: "u1", Name: "does_not_exist", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
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
	if !hasErrorIs(got, ErrModelAfterTools) {
		t.Fatalf("want after-tools model error, got %+v", summarizeEvents(got))
	}
}

// TestRun_functionCallRecordedBeforeToolResult: Azure-style pairing — the next
// model invoke sees an assistant function_call whose call_id matches the tool output.
func TestRun_functionCallRecordedBeforeToolResult(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "pong", nil },
	})
	var n int
	var second []*Message
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "fc_item", CallID: "call_abc123", Name: "echo", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			second = append([]*Message(nil), msgs...)
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
	})
	events, err := h.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)
	if n < 2 {
		t.Fatalf("want second model invoke, got %d", n)
	}
	var sawFC, sawOut bool
	for _, m := range second {
		if m.Role == RoleAssistant && len(m.ToolCalls) == 1 {
			tc := m.ToolCalls[0]
			if tc.CallID == "call_abc123" && tc.ID == "fc_item" {
				sawFC = true
			}
		}
		if m.Role == RoleTool && m.ToolCallID == "call_abc123" {
			sawOut = true
		}
	}
	if !sawFC || !sawOut {
		t.Fatalf("second invoke msgs missing paired function_call/tool: sawFC=%v sawOut=%v msgs=%+v", sawFC, sawOut, second)
	}
}

// TestRun_modelErrorAfterTools_tagsAndCheckpointsPairs: after a successful tool
// batch, a provider stream failure is marked ErrModelAfterTools and the durable
// window retains user + function_call + tool result for resume.
func TestRun_modelErrorAfterTools_tagsAndCheckpointsPairs(t *testing.T) {
	store := stores.NewInMemoryStore()
	tool := NewTool(ToolConfig{
		Name:    "echo",
		Handler: func(ctx context.Context) (string, error) { return "tool-ok", nil },
	})
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{
						{ID: "fc_echo", CallID: "call_echo", Name: "echo", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{
				Type:       StreamEventError,
				Error:      fmt.Errorf("provider HTTP 200: stream ended without a usable response; status=failed"),
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{tool},
		Store:  store,
	})
	h.sessionId = "sess-after-tools-ckpt"
	events, err := h.Run(context.Background(), "do the tool then die")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "tool-ok") {
		t.Fatalf("want successful tool result, got %+v", summarizeEvents(got))
	}
	var sawTagged bool
	for _, ev := range got {
		if ev.Type == StreamEventError && ev.Error != nil && errors.Is(ev.Error, ErrModelAfterTools) {
			sawTagged = true
		}
	}
	if !sawTagged {
		t.Fatalf("want ErrModelAfterTools, got %+v", summarizeEvents(got))
	}

	loaded, err := NewAgentFromSession(context.Background(), "sess-after-tools-ckpt", AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		Store:  store,
		Tools:  []*Tool{tool},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	var sawUser, sawFC, sawTool bool
	for _, m := range loaded.Messages() {
		if m == nil {
			continue
		}
		switch m.Role {
		case RoleUser:
			if m.Content == "do the tool then die" {
				sawUser = true
			}
		case RoleAssistant:
			if len(m.ToolCalls) == 1 && m.ToolCalls[0].CallID == "call_echo" {
				sawFC = true
			}
		case RoleTool:
			if m.ToolCallID == "call_echo" && m.Content == "tool-ok" {
				sawTool = true
			}
		}
	}
	if !sawUser || !sawFC || !sawTool {
		t.Fatalf("checkpoint missing pairs: user=%v fc=%v tool=%v window=%+v",
			sawUser, sawFC, sawTool, loaded.Messages())
	}
}

// TestRun_pairsOpenToolCallsBeforeTurn: a restored open function_call is paired
// with an error tool result so the next model turn sees a valid window.
func TestRun_pairsOpenToolCallsBeforeTurn(t *testing.T) {
	store := stores.NewInMemoryStore()
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type:       StreamEventError,
				Error:      fmt.Errorf("provider down"),
				IsComplete: true,
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  store,
	})
	h.sessionId = "sess-pair-open"
	h.restoreMessages([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, ToolCalls: []ToolCall{
			{CallID: "orphan", Name: "echo", Arguments: `{}`},
		}},
		{Role: RoleAssistant, ToolCalls: []ToolCall{
			{CallID: "good", Name: "echo", Arguments: `{}`},
		}},
		{Role: RoleTool, ToolCallID: "good", Content: "done"},
	})

	events, err := h.Run(context.Background(), "continue")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)

	loaded, err := NewAgentFromSession(context.Background(), "sess-pair-open", AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		Store:  store,
	})
	if err != nil {
		t.Fatal(err)
	}
	var sawGood, sawOrphanCall, sawOrphanResult bool
	for _, m := range loaded.Messages() {
		if m == nil {
			continue
		}
		if m.Role == RoleAssistant {
			for _, tc := range m.ToolCalls {
				if tc.CallID == "orphan" {
					sawOrphanCall = true
				}
			}
		}
		if m.Role == RoleTool {
			if m.ToolCallID == "good" && m.Content == "done" {
				sawGood = true
			}
			if m.ToolCallID == "orphan" {
				sawOrphanResult = true
			}
		}
	}
	if !sawOrphanCall || !sawOrphanResult {
		t.Fatalf("open tool call must be paired: call=%v result=%v window=%+v",
			sawOrphanCall, sawOrphanResult, loaded.Messages())
	}
	if !sawGood {
		t.Fatalf("paired tool result missing: %+v", loaded.Messages())
	}
}

// TestRun_toolCallKeyUsesCallIDWhenIDEmpty: CallID alone is enough for lifecycle ids.
func TestRun_customInstructionsInSystemPrompt(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := mustNewAgent(t, AgentOptions{
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

type testMCPCredentialResolver func(context.Context, string) (mcp.Credentials, error)

func (f testMCPCredentialResolver) ResolveMCP(ctx context.Context, ref string) (mcp.Credentials, error) {
	return f(ctx, ref)
}

// TestRun_mcpToolsDiscoveredAndInvokable: MCP configs inject callable tools
// into the turn, and Close runs discovery cleanup.
func TestRun_mcpToolsDiscoveredAndInvokable(t *testing.T) {
	var cleaned bool
	prev := discoverAllTools
	discoverAllTools = func(ctx context.Context, configs []mcp.MCPConfig, register mcpruntime.RegisterTool) func() {
		if len(configs) != 1 || len(configs[0].Headers) != 1 || configs[0].Headers[0].Value != "Bearer resolved" {
			t.Fatalf("resolved MCP configs = %#v", configs)
		}
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
	h := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		MCPConfigs: []mcp.MCPConfig{
			{Name: "fake", Type: "http", URL: "http://127.0.0.1:1", CredentialRef: "vault://fake"},
		},
		MCPCredentialResolver: testMCPCredentialResolver(func(_ context.Context, ref string) (mcp.Credentials, error) {
			if ref != "vault://fake" {
				t.Fatalf("credential ref = %q", ref)
			}
			return mcp.Credentials{Headers: []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer resolved"}}}, nil
		}),
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
	h := mustNewAgent(t, AgentOptions{
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

// TestRun_displayNameOnFunctionCall_stream: DisplayName templates fill Title;
// Name stays programmatic so execution keeps working.
func TestRun_cancelMidTool_pairsCancelledResultsInWindow(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	slow := NewTool(ToolConfig{
		Name: "slow",
		Handler: func(ctx context.Context) (string, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return "", ctx.Err()
		},
	})
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{
					{ID: "c_slow", CallID: "c_slow", Name: "slow", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	store := stores.NewInMemoryStore()
	h := mustNewAgent(t, AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Tools:     []*Tool{slow},
		Store:     store,
		SessionID: "steer-cancel-mid-tool",
	})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := h.Run(ctx, "run slow tool")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("tool did not start")
	}
	cancel()
	_ = drainEvents(events)

	// Reload from checkpoint — every tool_call must have a tool result.
	loaded, err := NewAgentFromSession(context.Background(), "steer-cancel-mid-tool", AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     strategy,
		Tools:     []*Tool{slow},
		Store:     store,
		SessionID: "steer-cancel-mid-tool",
	})
	if err != nil {
		t.Fatal(err)
	}
	var sawToolCancel bool
	hasCall := false
	for _, m := range loaded.Messages() {
		if m.Role == RoleAssistant && len(m.ToolCalls) > 0 {
			hasCall = true
		}
		if m.Role == RoleTool && m.ToolCallID == "c_slow" && strings.Contains(m.Content, "cancelled") {
			sawToolCancel = true
		}
	}
	if !hasCall {
		t.Fatalf("want assistant tool_calls in window, got %+v", loaded.Messages())
	}
	if !sawToolCancel {
		t.Fatalf("want cancelled tool result in checkpoint, got %+v", loaded.Messages())
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
	h := mustNewAgent(t, AgentOptions{
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
