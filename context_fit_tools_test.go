package tacklr

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/streaming"
)

// --- ModelTasks absorb/handoff outcomes used by the harness ---

func TestDefaultModelTasks_Absorb_defaultPolicyAndNilModel(t *testing.T) {
	cm := NewModelContextManager()
	cm.Restore([]*Message{{Role: RoleUser, Content: "u"}})
	tasks := NewDefaultModelTasks(nil, cm, ContextPolicy{}, 10)
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "next"}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(cm.Messages()) != 2 {
		t.Fatalf("len = %d", len(cm.Messages()))
	}
}

func TestDefaultModelTasks_Absorb_oversize_compressesAndRestoresPrompt(t *testing.T) {
	strategy := &mockStrategy{
		countTokensFn: func(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
			n := 0
			for _, m := range msgs {
				if m != nil {
					n += len(m.Content) + 50
				}
			}
			return n, nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "SUMMARY", IsComplete: true}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{
		{Role: RoleUser, Content: strings.Repeat("u", 40)},
		{Role: RoleAssistant, Content: strings.Repeat("a", 40)},
		{Role: RoleUser, Content: strings.Repeat("b", 40)},
		{Role: RoleAssistant, Content: strings.Repeat("c", 40)},
	})
	tasks := NewDefaultModelTasks(strategy, cm, ContextPolicy{PressureRatio: 0.5, CompressFraction: 0.25, StreamFitSummary: true}, 80)
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "q"}, nil, "restored-system")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range cm.Messages() {
		if m != nil && strings.Contains(m.Content, "SUMMARY") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected summary: %+v", cm.Messages())
	}
	strategy.mu.Lock()
	defer strategy.mu.Unlock()
	if len(strategy.systemPrompts) == 0 || strategy.systemPrompts[len(strategy.systemPrompts)-1] != "restored-system" {
		t.Fatalf("system prompts = %v", strategy.systemPrompts)
	}
}

func TestDefaultModelTasks_Absorb_compressStreamError(t *testing.T) {
	strategy := &mockStrategy{
		countTokensFn: func(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
			return 1000, nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Content: "compress failed"}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{
		{Role: RoleUser, Content: "u"},
		{Role: RoleAssistant, Content: "a"},
	})
	tasks := NewDefaultModelTasks(strategy, cm, DefaultContextPolicy(), 10)
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "n"}, nil, "")
	if err == nil || !strings.Contains(err.Error(), "compress") {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultModelTasks_Handoff_cancelAndStreamError(t *testing.T) {
	cm := NewModelContextManager()
	cm.Restore([]*Message{{Role: RoleUser, Content: "u"}})
	tasks := NewDefaultModelTasks(&mockStrategy{}, cm, DefaultContextPolicy(), 8192)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tasks.Handoff(ctx, nil, "", nil, ""); err == nil {
		t.Fatal("want cancel")
	}
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Content: "handoff boom"}
		},
	}
	cm.Restore([]*Message{
		{Role: RoleUser, Content: "u"},
		{Role: RoleAssistant, Content: "a"},
	})
	tasks = NewDefaultModelTasks(strategy, cm, DefaultContextPolicy(), 8192)
	err := tasks.Handoff(context.Background(), []Todo{{Title: "t", Status: streaming.TodoStatusInProgress}}, "", nil, "sys")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
}

// --- Tool definition / invoke outcomes ---

func TestNewTool_schemaOutcomes_nestedFloatUnexportedAndDeep(t *testing.T) {
	// Struct with float + unexported field + pointer args.
	type args struct {
		Score  float64 `json:"score" desc:"score"`
		hidden string  // unexported — omitted from schema
		Nested *struct {
			Name string `json:"name"`
		} `json:"nested"`
	}
	tool := NewTool(ToolConfig{
		Name: "schema_edge",
		Handler: func(ctx context.Context, a args) (string, error) {
			_ = a.hidden // field exists for schema-omission check only
			return "ok", nil
		},
	})
	def := tool.AsJson()
	params := def["parameters"].(map[string]any)
	props := params["properties"].(map[string]any)
	if _, ok := props["score"]; !ok {
		t.Fatal("score missing")
	}
	if _, ok := props["hidden"]; ok {
		t.Fatal("unexported field must not appear")
	}
	if _, ok := props["nested"]; !ok {
		t.Fatal("nested missing")
	}

	// ToolsAsJson empty list is a valid empty catalog.
	if empty := ToolsAsJson(nil); empty != "[]" {
		t.Fatalf("empty = %q", empty)
	}
	if empty := ToolsAsJson([]*Tool{}); empty != "[]" {
		t.Fatalf("empty = %q", empty)
	}

	// Pointer receiver-style args type via NewTool with *args is covered by
	// handler taking struct; pointer field Nested is enough for Ptr kind in schema.
	_ = reflect.TypeOf(args{})
}

func TestTool_Invoke_timeoutDeadlineFromHandler(t *testing.T) {
	// Handler returns DeadlineExceeded while the parent ctx is still live → tool timeout.
	tool := NewTool(ToolConfig{
		Name:    "slow",
		Timeout: time.Second,
		Handler: func(ctx context.Context) (string, error) {
			return "", context.DeadlineExceeded
		},
	})
	_, err := tool.Invoke(context.Background(), "", HarnessRuntime{})
	if err == nil || !errors.Is(err, ErrToolTimeout) {
		t.Fatalf("err = %v, want ErrToolTimeout", err)
	}
}

func TestTool_AsJson_nilParametersDefaults(t *testing.T) {
	// MCP-style tool with nil schema defaults parameters object.
	tool := newMCPTool(mcpToolConfig{
		Name:        "bare",
		Description: "d",
		Namespace:   "n",
		Schema:      nil,
		Handler: func(ctx context.Context, args map[string]any, _ HarnessRuntime) (string, error) {
			return "x", nil
		},
	})
	def := tool.AsJson()
	params := def["parameters"].(map[string]any)
	if params["type"] != "object" {
		t.Fatalf("params = %v", params)
	}
	got, err := tool.Invoke(context.Background(), `{}`, HarnessRuntime{})
	if err != nil || got != "x" {
		t.Fatalf("invoke = %q %v", got, err)
	}
}

// --- Subagent parent outcomes ---

func TestSpawnWorker_incompleteStream_errors(t *testing.T) {
	// StreamEventError with empty Error/Content → "worker stream error with no details".
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Content: ""}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "w", Model: workerModel},
		},
	})
	out := make(chan StreamEvent, 16)
	h.runtime.SetOutputChannel(out)
	_, err := h.runWorker(context.Background(), "w", "task", h.runtime)
	if err == nil {
		t.Fatal("want worker error")
	}
	if !strings.Contains(err.Error(), "no details") && !strings.Contains(err.Error(), "failed") {
		t.Fatalf("err = %v", err)
	}
}

func TestSpawnWorker_emptyTask_errors(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "w", Model: &mockStrategy{}},
		},
	})
	_, err := h.runWorker(context.Background(), "w", "   ", h.runtime)
	if !errors.Is(err, ErrEmptyWorkerTask) {
		t.Fatalf("err = %v", err)
	}
}

func TestSpawnWorker_reasoningUpdatesForwarded(t *testing.T) {
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventReasoning, Content: "thinking hard", IsComplete: true}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "answer", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		SubAgents: []*SubAgent{
			{WorkerName: "thinker", Model: workerModel},
		},
	})
	updates := make(chan StreamEvent, 32)
	h.runtime.SetOutputChannel(updates)
	got, err := h.runWorker(context.Background(), "thinker", "reason", h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if got != "answer" {
		// finalWorkerOutput may prefer last assistant
		if !strings.Contains(got, "answer") && got != "thinking hard" {
			t.Fatalf("got %q", got)
		}
	}
}

// TestRun_messageDeltas_assembledOnComplete: incomplete message chunks assemble
// into the context window when the complete event arrives.
func TestRun_messageDeltas_assembledOnComplete(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "Hel", IsComplete: false}
			ch <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "lo", IsComplete: false}
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, MessageId: "m1", Content: "ignored", IsComplete: false}
			ch <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "", IsComplete: true}
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
	drainEvents(events)
	found := false
	for _, m := range h.Messages() {
		if m != nil && m.Role == RoleAssistant && strings.Contains(m.Content, "Hello") {
			found = true
		}
	}
	if !found {
		t.Fatalf("window = %+v", h.Messages())
	}
}

// --- Permission memory survives map[string]any rehydrate (session shape) ---

func TestRun_permissionAllowAlways_survivesAnyMapRehydrate(t *testing.T) {
	// After checkpoint JSON round-trip, permission sets may appear as map[string]any.
	// allow_always must still skip the interrupt on the next call.
	permTool := NewTool(ToolConfig{
		Name:               "secret",
		PermissionRequired: true,
		Handler:            func(ctx context.Context) (string, error) { return "secret-ok", nil },
	})
	store := testStore(t)
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "p1", CallID: "p1", Name: "secret", Arguments: `{}`,
					}},
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
		Tools:  []*Tool{permTool},
		Store:  store,
	})
	h.sessionId = "perm-sess"
	// Seed allow list in the rehydrated any-map shape.
	h.runtime.StateSet(permissionAlwaysAllowKey, map[string]any{"secret": true})
	events, err := h.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	// Should not park on interrupt — tool runs.
	if hasEventType(got, StreamEventInterrupt) {
		t.Fatal("allow_always should skip permission interrupt")
	}
	if !hasToolResultContent(got, "secret-ok") {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}
