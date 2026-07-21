package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/control"
)

type mockStrategy struct {
	invokeFn  func(context.Context, []*Message, []*Tool, chan<- LLMResponseChunk)
	invokeErr error
	callNum   atomic.Int64
}

func (m *mockStrategy) WithApiKey(string) InferenceStrategy           { return m }
func (m *mockStrategy) WithModel(string) InferenceStrategy            { return m }
func (m *mockStrategy) WithURL(string) InferenceStrategy              { return m }
func (m *mockStrategy) WithReasoningLevel(string) InferenceStrategy   { return m }
func (m *mockStrategy) WithStructuredOutput(any) InferenceStrategy    { return m }
func (m *mockStrategy) SetSystemPrompt(string)                       {}
func (m *mockStrategy) Reset()                                        {}
func (m *mockStrategy) CompressContextWindow() error                  { return nil }
func (m *mockStrategy) MaxContextWindow() (int, error)                { return 0, nil }
func (m *mockStrategy) CountTokens(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
	return 0, nil
}
func (m *mockStrategy) Invoke(ctx context.Context, msgs []*Message, tools []*Tool) (chan LLMResponseChunk, error) {
	if m.invokeErr != nil {
		return nil, m.invokeErr
	}
	m.callNum.Add(1)
	ch := make(chan LLMResponseChunk)
	go func() {
		defer close(ch)
		m.invokeFn(ctx, msgs, tools, ch)
	}()
	return ch, nil
}

type recordingWatchdog struct {
	outputs     []*Message
	toolResults []*Message
	errors      []error
	thinking    []*Message
	tokenCounts []tokenCount
	toolCalls   []*Message
}

type tokenCount struct {
	input, output int
}

func (w *recordingWatchdog) RecordThinking(msg *Message) error {
	w.thinking = append(w.thinking, msg)
	return nil
}
func (w *recordingWatchdog) RecordOutput(msg *Message) error {
	w.outputs = append(w.outputs, msg)
	return nil
}
func (w *recordingWatchdog) RecordError(err error) error {
	w.errors = append(w.errors, err)
	return nil
}
func (w *recordingWatchdog) RecordTokens(input, output int) error {
	w.tokenCounts = append(w.tokenCounts, tokenCount{input, output})
	return nil
}
func (w *recordingWatchdog) RecordToolCalls(msg *Message) error {
	w.toolCalls = append(w.toolCalls, msg)
	return nil
}
func (w *recordingWatchdog) RecordToolResult(msg *Message) error {
	w.toolResults = append(w.toolResults, msg)
	return nil
}

func testStore(t *testing.T) *stores.InMemoryStore {
	t.Helper()
	return stores.NewInMemoryStore()
}

func TestAgentHarness_Run(t *testing.T) {
	validTool := &Tool{
		Name: "greet",
		Handler: func(args struct{ Name string }) (string, error) {
			return "Hello " + args.Name, nil
		},
	}
	if err := validTool.Validate(); err != nil {
		t.Fatal(err)
	}

	brokenTool := &Tool{
		Name:    "broken",
		Handler: func() (string, error) { return "", fmt.Errorf("handler error") },
	}
	if err := brokenTool.Validate(); err != nil {
		t.Fatal(err)
	}

	optionsJSON := `[{"title":"Option A","description":"First option","isRecommended":true},{"title":"Option B","description":"Second option","isRecommended":false}]`

	interruptTool := &Tool{
		Name: "ask_user",
		Handler: func(args struct {
			Runtime HarnessRuntime `json:"-"`
		}) (string, error) {
			intr, err := args.Runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			choice := intr.(*control.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	}
	if err := interruptTool.Validate(); err != nil {
		t.Fatal(err)
	}

	t.Run("single turn no tool calls", func(t *testing.T) {
		store := testStore(t)
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "Hello!", IsComplete: true}
			},
		}
		ah := &AgentHarness{
			Model:   strategy,
			Tools:   []*Tool{validTool},
			Store:   store,
		Runtime: HarnessRuntime{Store: store},
		}
		ch, err := ah.Run(context.Background(), "Hi")
		if err != nil {
			t.Fatal(err)
		}

		var messages []*Message
		for ev := range ch {
			switch ev.Type {
			case StreamEventMessage:
				messages = append(messages, &Message{Role: RoleAssistant, Content: ev.Content})
			case StreamEventComplete:
				// done
			}
		}

		if len(messages) != 1 {
			t.Fatalf("expected 1 assistant message, got %d", len(messages))
		}
		if messages[0].Content != "Hello!" {
			t.Errorf("content = %q", messages[0].Content)
		}
	})

	t.Run("full tool call loop", func(t *testing.T) {
		store := testStore(t)
		var invokeCount int
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				invokeCount++
				if invokeCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventMessage, Content: "Calling greet...", IsComplete: true}
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_1", CallID: "call_1", Name: "greet", Arguments: `{"Name":"World"}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "Done!", IsComplete: true}
			},
		}
		ah := &AgentHarness{
			Model:   strategy,
			Tools:   []*Tool{validTool},
			Store:   store,
		Runtime: HarnessRuntime{Store: store},
		}
		ch, err := ah.Run(context.Background(), "Say hello")
		if err != nil {
			t.Fatal(err)
		}

		var contentEvents int
		var functionCallEvents int
		var toolResultEvents int
		var toolResultEvent *StreamEvent
		for ev := range ch {
			switch ev.Type {
			case StreamEventMessage:
				contentEvents++
			case StreamEventFunctionCall:
				functionCallEvents++
			case StreamEventToolResult:
				toolResultEvents++
				if toolResultEvent == nil {
					ev := ev
					toolResultEvent = &ev
				}
			}
		}

		if invokeCount != 2 {
			t.Errorf("expected 2 Invoke calls, got %d", invokeCount)
		}
		if contentEvents != 3 {
			t.Errorf("expected 3 content events, got %d", contentEvents)
		}
		if functionCallEvents != 1 {
			t.Errorf("expected 1 function call event, got %d", functionCallEvents)
		}
		if toolResultEvents != 1 {
			t.Errorf("expected 1 tool result event, got %d", toolResultEvents)
		}
		if toolResultEvent == nil {
			t.Fatal("did not capture a tool result event")
		}
		if toolResultEvent.MessageID != "call_1" {
			t.Errorf("tool result MessageID = %q, want %q", toolResultEvent.MessageID, "call_1")
		}
		if len(toolResultEvent.ToolCalls) != 1 {
			t.Fatalf("tool result ToolCalls length = %d, want 1", len(toolResultEvent.ToolCalls))
		}
		got := toolResultEvent.ToolCalls[0]
		if got.CallID != "call_1" {
			t.Errorf("tool result ToolCalls[0].CallID = %q, want %q", got.CallID, "call_1")
		}
		if got.Name != "greet" {
			t.Errorf("tool result ToolCalls[0].Name = %q, want %q", got.Name, "greet")
		}
		if got.Arguments != `{"Name":"World"}` {
			t.Errorf("tool result ToolCalls[0].Arguments = %q", got.Arguments)
		}
		if got.ID != "call_1" {
			t.Errorf("tool result ToolCalls[0].ID = %q, want %q", got.ID, "call_1")
		}
	})

	t.Run("tool not found", func(t *testing.T) {
		store := testStore(t)
		var callCount int
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				callCount++
				if callCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_1", CallID: "call_1", Name: "nonexistent", Arguments: `{}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			},
		}
		ah := &AgentHarness{
			Model:   strategy,
			Store:   store,
		Runtime: HarnessRuntime{Store: store},
		}
		ch, err := ah.Run(context.Background(), "test")
		if err != nil {
			t.Fatal(err)
		}

		var foundToolResult bool
		var toolResultEv StreamEvent
		for ev := range ch {
			if ev.Type == StreamEventToolResult {
				foundToolResult = true
				toolResultEv = ev
			}
		}
		if !foundToolResult {
			t.Error("expected tool result event")
		}
		if !strings.Contains(toolResultEv.Content, "not found") {
			t.Errorf("tool result content = %q, want contains 'not found'", toolResultEv.Content)
		}
	})

	t.Run("tool handler error", func(t *testing.T) {
		store := testStore(t)
		var callCount int
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				callCount++
				if callCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_1", CallID: "call_1", Name: "broken", Arguments: `{}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			},
		}
		ah := &AgentHarness{
			Model:   strategy,
			Tools:   []*Tool{brokenTool},
			Store:   store,
		Runtime: HarnessRuntime{Store: store},
		}
		ch, err := ah.Run(context.Background(), "test")
		if err != nil {
			t.Fatal(err)
		}

		var foundToolResult bool
		for ev := range ch {
			if ev.Type == StreamEventToolResult {
				foundToolResult = true
			}
		}
		if !foundToolResult {
			t.Error("expected tool result despite handler error")
		}
	})

	t.Run("invoke returns error", func(t *testing.T) {
		store := testStore(t)
		strategy := &mockStrategy{
			invokeErr: fmt.Errorf("network error"),
		}
		ah := &AgentHarness{
			Model:   strategy,
			Store:   store,
		Runtime: HarnessRuntime{Store: store},
		}
		ch, err := ah.Run(context.Background(), "test")
		if err != nil {
			t.Fatal(err)
		}

		var foundError bool
		for ev := range ch {
			if ev.Type == StreamEventError {
				foundError = true
			}
		}
		if !foundError {
			t.Fatal("expected error event")
		}
	})

	t.Run("tool raises and resolves interrupt", func(t *testing.T) {
		store := testStore(t)
		var callCount int
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				callCount++
				if callCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_int", CallID: "call_int", Name: "ask_user", Arguments: `{}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
					return
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			},
		}

		ah := &AgentHarness{
			Model:   strategy,
			Tools:   []*Tool{interruptTool},
			Store:   store,
		Runtime: HarnessRuntime{Store: store},
		}
		ah.pendingToolCalls = make(map[string]pendingToolCall)
		ah.interruptToRequester = make(map[string]string)

		ch, err := ah.Run(context.Background(), "start")
		if err != nil {
			t.Fatal(err)
		}

		var interruptId string
		var foundInterrupt bool
		for ev := range ch {
			switch ev.Type {
			case StreamEventInterrupt:
				foundInterrupt = true
				var payload struct {
					InterruptId string          `json:"interruptId"`
					Data        json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(ev.Data, &payload); err != nil {
					t.Fatal(err)
				}
				interruptId = payload.InterruptId
			case StreamEventToolResult:
				t.Error("should not have tool result on first run")
			}
		}

		if !foundInterrupt {
			t.Fatal("expected interrupt event")
		}
		if interruptId == "" {
			t.Fatal("interruptId was not captured")
		}
		if callCount != 1 {
			t.Errorf("callCount after first run = %d, want 1", callCount)
		}
		if len(ah.pendingToolCalls) != 1 {
			t.Errorf("pendingToolCalls after first run = %d, want 1", len(ah.pendingToolCalls))
		}
		if !ah.pendingToolCalls["call_int"].InterruptActive {
			t.Error("pending tool call should have InterruptActive=true after first run")
		}

		// Consumer resolves the interrupt
		resolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptId)
		resolved, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
			interruptId: []byte(resolution),
		})
		if err != nil {
			t.Fatal(err)
		}

		var foundToolResult bool
		var foundDoneMessage bool
		var toolResultContent string
		for ev := range resolved {
			switch ev.Type {
			case StreamEventToolResult:
				foundToolResult = true
				toolResultContent = ev.Content
			case StreamEventMessage:
				if ev.Content == "done" {
					foundDoneMessage = true
				}
			}
		}

		if !foundToolResult {
			t.Fatal("expected tool result after resolving interrupt")
		}
		if !strings.Contains(toolResultContent, "Option A") {
			t.Errorf("tool result = %q, want contains 'Option A'", toolResultContent)
		}
		if !foundDoneMessage {
			t.Error("expected final 'done' assistant message after tool resumed")
		}
		if callCount != 2 {
			t.Errorf("callCount after resume = %d, want 2", callCount)
		}
		if len(ah.pendingToolCalls) != 0 {
			t.Errorf("pendingToolCalls after resume = %d, want 0", len(ah.pendingToolCalls))
		}
		if ah.Runtime.HasPendingInterrupt() {
			t.Error("runtime should have no pending interrupts after resume")
		}
	})
}

func TestReturnFromInterrupt_unknownUUID_returnsError(t *testing.T) {
	store := testStore(t)
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	ah := &AgentHarness{
		Model:                strategy,
		Store:                store,
		Runtime:              HarnessRuntime{Store: store},
		interruptToRequester: make(map[string]string),
		pendingToolCalls:     make(map[string]pendingToolCall),
	}

	resolved, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		"bogus-uuid": []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error for unknown interrupt UUID")
	}
	if !strings.Contains(err.Error(), "no tool call id found") {
		t.Errorf("error = %q, want contains 'no tool call id found'", err.Error())
	}
	if resolved != nil {
		t.Error("expected nil channel on error")
	}
}

func TestReturnFromInterrupt_invalidPayload_returnsError(t *testing.T) {
	store := testStore(t)
	optionsJSON := `[{"title":"A","description":"opt A","isRecommended":false}]`
	interruptTool := &Tool{
		Name: "ask_user",
		Handler: func(args struct {
			Runtime HarnessRuntime `json:"-"`
		}) (string, error) {
			intr, err := args.Runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			choice := intr.(*control.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	}
	if err := interruptTool.Validate(); err != nil {
		t.Fatal(err)
	}

	var callCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			callCount++
			if callCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "call_int", CallID: "call_int", Name: "ask_user", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			}
		},
	}

	ah := &AgentHarness{
		Model:                strategy,
		Tools:                []*Tool{interruptTool},
		Store:                store,
		Runtime:              HarnessRuntime{Store: store},
		pendingToolCalls:     make(map[string]pendingToolCall),
		interruptToRequester: make(map[string]string),
	}

	ch, err := ah.Run(context.Background(), "start")
	if err != nil {
		t.Fatal(err)
	}

	var interruptId string
	for ev := range ch {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string          `json:"interruptId"`
				Data        json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				t.Fatal(err)
			}
			interruptId = payload.InterruptId
		}
	}
	if interruptId == "" {
		t.Fatal("interruptId not captured")
	}

	// Send bad payload: out-of-bounds selectionIdx
	resolved, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptId: []byte(`{"interruptId":"` + interruptId + `","selectionIdx":99}`),
	})
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
	if !strings.Contains(err.Error(), "invalid payload") {
		t.Errorf("error = %q, want contains 'invalid payload'", err.Error())
	}
	if resolved != nil {
		t.Error("expected nil channel on error")
	}
}

func TestNewAgent(t *testing.T) {
	store := testStore(t)
	mockModel := &mockStrategy{}
	wd := &recordingWatchdog{}

	h := NewAgent(Config{
		MaxWindowSize: 4096,
		SystemPrompt:  "test prompt",
	}, mockModel, store, wd)

	if h.Model != InferenceStrategy(mockModel) {
		t.Error("Model not wired from arg")
	}
	if h.MaxWindowSize != 4096 {
		t.Errorf("MaxWindowSize = %d, want 4096", h.MaxWindowSize)
	}
	if h.SystemPrompt != "test prompt" {
		t.Errorf("SystemPrompt = %q, want 'test prompt'", h.SystemPrompt)
	}
	if h.Store != store {
		t.Error("Store not wired from arg")
	}
	if h.WatchDog != AgentWatchDog(wd) {
		t.Error("WatchDog not wired from arg")
	}
	if h.ContextWindow != nil {
		t.Error("ContextWindow should be nil on init")
	}
	if h.SessionId != "" {
		t.Errorf("SessionId = %q, want empty", h.SessionId)
	}
}

func TestWithStreamingStrategy(t *testing.T) {
	h := &AgentHarness{}
	returned := h.WithStreamingStrategy(nil)
	if returned != h {
		t.Error("WithStreamingStrategy should return *AgentHarness for chaining")
	}
	if h.streamingStrategy != nil {
		t.Error("streamingStrategy should be nil after setting nil")
	}
}

func TestFindTool_namespaceMatching(t *testing.T) {
	tools := []*Tool{
		{Name: "get_customer", Namespace: "crm"},
		{Name: "get_customer", Namespace: "email"},
		{Name: "get_weather", Namespace: ""},
	}
	h := &AgentHarness{Tools: tools}

	if h.findTool("get_customer", "crm") == nil {
		t.Error("expected to find tool in crm namespace")
	}
	if h.findTool("get_customer", "email") == nil {
		t.Error("expected to find tool in email namespace")
	}
	if h.findTool("get_customer", "wrong") != nil {
		t.Error("should not find tool in wrong namespace")
	}
	if h.findTool("get_weather", "") == nil {
		t.Error("expected to find tool with empty namespace")
	}
	if h.findTool("nonexistent", "") != nil {
		t.Error("should not find nonexistent tool")
	}
}

func TestRun_contextCancellation(t *testing.T) {
	mock := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{IsComplete: true, Content: "done", Type: StreamEventMessage}
		},
	}
	h := &AgentHarness{
		Model:         mock,
		MaxWindowSize: 8192,
		ContextWindow: []*Message{{Role: RoleUser, Content: "hi"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	events, err := h.Run(ctx, "test")
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	var evs []StreamEvent
	for ev := range events {
		evs = append(evs, ev)
	}

	if len(evs) == 0 {
		t.Fatal("expected at least one event")
	}
	hasError := false
	for _, ev := range evs {
		if ev.Type == StreamEventError {
			hasError = true
			if !errors.Is(ev.Error, context.Canceled) {
				t.Errorf("expected context.Canceled in error chain, got: %v", ev.Error)
			}
		}
	}
	if !hasError {
		t.Error("expected a StreamEventError event")
	}
}

func TestRun_watchdogInvoked(t *testing.T) {
	mock := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				IsComplete: true,
				Type:       StreamEventMessage,
				Content:    "assistant output",
			}
		},
	}
	wd := &recordingWatchdog{}
	h := &AgentHarness{
		Model:         mock,
		MaxWindowSize: 8192,
		WatchDog:      wd,
	}

	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}

	if len(wd.outputs) != 1 {
		t.Errorf("expected 1 RecordOutput call, got %d", len(wd.outputs))
	}
	if len(wd.outputs) > 0 && wd.outputs[0].Content != "assistant output" {
		t.Errorf("output content = %q", wd.outputs[0].Content)
	}
}

func TestRun_reasoningCapturedInContextWindow(t *testing.T) {
	mock := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{
				Type:       StreamEventReasoning,
				Content:    "Let me think about this...",
				MessageId:  "rs_1",
				IsComplete: true,
			}
			ch <- LLMResponseChunk{
				IsComplete: true,
				Type:       StreamEventMessage,
				Content:    "The answer is 42",
				MessageId:  "msg_1",
			}
			ch <- LLMResponseChunk{IsComplete: true}
		},
	}
	h := &AgentHarness{
		Model:         mock,
		MaxWindowSize: 8192,
	}

	events, err := h.Run(context.Background(), "what is the answer?")
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}

	if len(h.ContextWindow) < 3 {
		t.Fatalf("expected at least 3 messages in context window, got %d", len(h.ContextWindow))
	}

	var reasoningMsg, assistantMsg *Message
	for _, m := range h.ContextWindow {
		if m.Role == RoleReasoning && m.Content == "Let me think about this..." {
			reasoningMsg = m
		}
		if m.Role == RoleAssistant && m.Content == "The answer is 42" {
			assistantMsg = m
		}
	}
	if reasoningMsg == nil {
		t.Fatalf("did not find reasoning message in context window")
	}
	if reasoningMsg.MessageID != "rs_1" {
		t.Errorf("reasoning message_id = %q, want %q", reasoningMsg.MessageID, "rs_1")
	}
	if assistantMsg == nil {
		t.Fatalf("did not find assistant message in context window")
	}
	if assistantMsg.MessageID != "msg_1" {
		t.Errorf("assistant message_id = %q, want %q", assistantMsg.MessageID, "msg_1")
	}
}
