package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

type mockStrategy struct {
	invokeFn        func(context.Context, []*Message, []*Tool, chan<- LLMResponseChunk)
	invokeErr       error
	invokeErrFn     func(context.Context, []*Message, []*Tool) error
	countTokensFn   func(context.Context, []*Message, []*Tool) (int, error)
	supportsMIMEFn  func(string) bool
	systemPrompts   []string
	lastInvokeMsgs  []*Message
	lastInvokeTools []*Tool
	callNum         atomic.Int64
	mu              sync.Mutex
}

func (m *mockStrategy) ModelTelemetryIdentity() telemetry.ModelIdentity {
	return telemetry.ModelIdentity{Provider: "unknown", Model: "mock"}
}

func (m *mockStrategy) WithApiKey(string) InferenceStrategy         { return m }
func (m *mockStrategy) WithModel(string) InferenceStrategy          { return m }
func (m *mockStrategy) WithURL(string) InferenceStrategy            { return m }
func (m *mockStrategy) WithReasoningLevel(string) InferenceStrategy { return m }
func (m *mockStrategy) WithStructuredOutput(any) InferenceStrategy  { return m }
func (m *mockStrategy) SupportsMIME(mimeType string) bool {
	if m.supportsMIMEFn != nil {
		return m.supportsMIMEFn(mimeType)
	}
	// Tests default to text-only models.
	return streaming.IsTextMIME(mimeType)
}
func (m *mockStrategy) SetSystemPrompt(p string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.systemPrompts = append(m.systemPrompts, p)
}
func (m *mockStrategy) Reset()                       {}
func (m *mockStrategy) CompressContextWindow() error { return nil }
func (m *mockStrategy) MaxContextWindow() (int, error) {
	return 0, nil
}
func (m *mockStrategy) CountTokens(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
	if m.countTokensFn != nil {
		return m.countTokensFn(ctx, msgs, tools)
	}
	// Default 0 keeps existing tests under the window-pressure threshold.
	return 0, nil
}
func (m *mockStrategy) Invoke(ctx context.Context, msgs []*Message, tools []*Tool) (chan LLMResponseChunk, error) {
	if m.invokeErr != nil {
		return nil, m.invokeErr
	}
	if m.invokeErrFn != nil {
		if err := m.invokeErrFn(ctx, msgs, tools); err != nil {
			return nil, err
		}
	}
	m.callNum.Add(1)
	m.mu.Lock()
	m.lastInvokeMsgs = msgs
	m.lastInvokeTools = tools
	m.mu.Unlock()
	ch := make(chan LLMResponseChunk)
	go func() {
		defer close(ch)
		if m.invokeFn != nil {
			m.invokeFn(ctx, msgs, tools, ch)
		}
	}()
	return ch, nil
}

// contentTokenEstimate is a simple length-based token stand-in for window-pressure tests.
func contentTokenEstimate(msgs []*Message) int {
	n := 0
	for _, m := range msgs {
		if m != nil {
			n += len(m.Content)
		}
	}
	return n
}

type recordingWatchdog struct {
	mu          sync.Mutex
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
	w.mu.Lock()
	defer w.mu.Unlock()
	w.thinking = append(w.thinking, msg)
	return nil
}
func (w *recordingWatchdog) RecordOutput(msg *Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.outputs = append(w.outputs, msg)
	return nil
}
func (w *recordingWatchdog) RecordError(err error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errors = append(w.errors, err)
	return nil
}
func (w *recordingWatchdog) RecordTokens(input, output int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tokenCounts = append(w.tokenCounts, tokenCount{input, output})
	return nil
}
func (w *recordingWatchdog) RecordToolCalls(msg *Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.toolCalls = append(w.toolCalls, msg)
	return nil
}
func (w *recordingWatchdog) RecordToolResult(msg *Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.toolResults = append(w.toolResults, msg)
	return nil
}

func testStore(t *testing.T) *stores.InMemoryStore {
	t.Helper()
	return stores.NewInMemoryStore()
}

func TestAgentHarness_Run(t *testing.T) {
	validTool := NewTool(ToolConfig{
		Name: "greet",
		Handler: func(ctx context.Context, args struct{ Name string }) (string, error) {
			return "Hello " + args.Name, nil
		},
	})

	brokenTool := NewTool(ToolConfig{
		Name:    "broken",
		Handler: func(ctx context.Context) (string, error) { return "", fmt.Errorf("handler error") },
	})

	optionsJSON := `[{"title":"Option A","description":"First option","isRecommended":true},{"title":"Option B","description":"Second option","isRecommended":false}]`

	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime *HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			choice := intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	})

	t.Run("single turn no tool calls", func(t *testing.T) {
		store := testStore(t)
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "Hello!", IsComplete: true}
			},
		}
		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store, Tools: []*Tool{validTool}})
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
		ah.Close() // idempotent resource release after turn
	})

	t.Run("tool progress updates stream during turn", func(t *testing.T) {
		store := testStore(t)
		progress := NewTool(ToolConfig{
			Name: "progress_demo",
			Handler: func(ctx context.Context, _ struct{}, runtime *HarnessRuntime) (string, error) {
				runtime.EmitUpdate("starting")
				runtime.EmitUpdate("halfway")
				return "done-progress", nil
			},
		})
		var invokeCount int
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				invokeCount++
				if invokeCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "p1", CallID: "p1", Name: "progress_demo", Arguments: `{}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
					return
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "after progress", IsComplete: true}
			},
		}
		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store, Tools: []*Tool{progress}})
		ch, err := ah.Run(context.Background(), "progress")
		if err != nil {
			t.Fatal(err)
		}
		var updates int
		var toolOK bool
		for ev := range ch {
			if ev.Type == streaming.StreamEventToolUpdate {
				updates++
			}
			if ev.Type == StreamEventToolResult && strings.Contains(ev.Content, "done-progress") {
				toolOK = true
			}
		}
		// Unbuffered harness out + non-blocking EmitUpdate: only one may land if
		// the consumer is briefly busy; require at least one progress event.
		if updates < 1 {
			t.Fatalf("tool updates = %d, want >= 1", updates)
		}
		if !toolOK {
			t.Fatal("expected progress tool result")
		}
		ah.Close()
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
		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store, Tools: []*Tool{validTool}})
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
		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store})
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

	t.Run("incomplete function call gets terminal failed result", func(t *testing.T) {
		// Announced (streamed) but IsComplete=false must not leave clients stuck on in_progress.
		store := testStore(t)
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "fc_inc", CallID: "fc_inc", Name: "greet", Arguments: `{`},
				}, IsComplete: false}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "gave up", IsComplete: true}
			},
		}
		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store, Tools: []*Tool{validTool}})
		ch, err := ah.Run(context.Background(), "test")
		if err != nil {
			t.Fatal(err)
		}

		var sawFunctionCall bool
		var toolResults []StreamEvent
		var sawComplete bool
		for ev := range ch {
			switch ev.Type {
			case StreamEventFunctionCall:
				sawFunctionCall = true
			case StreamEventToolResult:
				toolResults = append(toolResults, ev)
			case StreamEventComplete:
				sawComplete = true
			}
		}
		if !sawFunctionCall {
			t.Error("expected function call announcement")
		}
		if len(toolResults) != 1 {
			t.Fatalf("tool results = %d, want 1 failed close", len(toolResults))
		}
		if toolResults[0].Content != "tool call incomplete" {
			t.Errorf("content = %q, want tool call incomplete", toolResults[0].Content)
		}
		if len(toolResults[0].ToolCalls) != 1 || toolResults[0].ToolCalls[0].Status != "error" {
			t.Errorf("expected error status on incomplete tool, got %+v", toolResults[0].ToolCalls)
		}
		if toolResults[0].MessageID != "fc_inc" {
			t.Errorf("MessageID = %q, want fc_inc", toolResults[0].MessageID)
		}
		if !sawComplete {
			t.Error("expected turn complete after closing incomplete tool")
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
		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store, Tools: []*Tool{brokenTool}})
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
		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store})
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

		ah := NewAgent(context.Background(), AgentOptions{Config: Config{}, Model: strategy, Store: store, Tools: []*Tool{interruptTool}})

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
				// data must be a nested JSON object (not base64 string from []byte marshal).
				if len(payload.Data) == 0 || payload.Data[0] != '{' {
					t.Fatalf("interrupt data must be JSON object, got %s", payload.Data)
				}
				var usi interrupt.UserSelectionInterrupt
				if err := json.Unmarshal(payload.Data, &usi); err != nil {
					t.Fatalf("unmarshal interrupt data into UserSelectionInterrupt: %v\ndata=%s", err, payload.Data)
				}
				if len(usi.Options) != 2 {
					t.Fatalf("options = %d, want 2", len(usi.Options))
				}
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
		if ah.session.HasPendingInterrupt() {
			t.Error("session should have no pending interrupts after resume")
		}
	})

	t.Run("complete todo triggers compression between turns", func(t *testing.T) {
		store := testStore(t)
		var invokeCount int
		var handoffHadNoTools bool
		var continueSawNudge bool

		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				invokeCount++
				if invokeCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_ct1", CallID: "call_ct1", Name: "complete_todo", Arguments: `{"title":"Task 1"}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
					return
				}
				if invokeCount == 2 {
					// Handoff is text-only: tools must not be offered (Azure fails otherwise).
					if tools == nil || len(tools) == 0 {
						handoffHadNoTools = true
					}
					events <- LLMResponseChunk{Type: StreamEventReasoning, MessageId: "rs_c", Content: "SECRET_REASONING", IsComplete: false}
					events <- LLMResponseChunk{Type: StreamEventReasoning, MessageId: "rs_c", IsComplete: true}
					// Streamed deltas then complete (empty content on done, like real SSE).
					events <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "msg_c", Content: "Mock compressed handoff. ", IsComplete: false}
					events <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "msg_c", Content: "Remaining: Task 2.", IsComplete: false}
					events <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "msg_c", IsComplete: true}
					// Earlier completed message must not win over the last one.
					return
				}
				// Post-compress continue turn should include the open-plan nudge.
				for _, m := range msgs {
					if m != nil && m.Role == RoleDeveloper && m.Content == continuePlanNudge {
						continueSawNudge = true
					}
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "All done!", IsComplete: true}
			},
		}

		ah := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 65536}, Model: strategy, Store: store, Tools: []*Tool{validTool}})
		ah.session.Plan().Set([]Todo{
			{Title: "Task 1", Status: streaming.TodoStatusInProgress},
			{Title: "Task 2", Status: streaming.TodoStatusPending},
		})

		ch, err := ah.Run(context.Background(), "Complete the first task")
		if err != nil {
			t.Fatal(err)
		}

		var events []StreamEvent
		for ev := range ch {
			events = append(events, ev)
		}

		if invokeCount != 3 {
			t.Errorf("expected 3 total invocations, got %d", invokeCount)
		}
		if !handoffHadNoTools {
			t.Error("handoff Invoke must not receive tools (Azure response.failed with tool schemas)")
		}
		if !continueSawNudge {
			t.Error("continue Invoke after compress should include continuePlanNudge (open todos remain)")
		}

		plan := ah.session.Plan().Get()
		if plan == nil {
			t.Fatal("plan should not be nil")
		}
		if plan[0].Status != streaming.TodoStatusCompleted {
			t.Errorf("Task 1 status = %q, want %q", plan[0].Status, streaming.TodoStatusCompleted)
		}
		if plan[1].Status != streaming.TodoStatusInProgress {
			t.Errorf("Task 2 status = %q, want %q", plan[1].Status, streaming.TodoStatusInProgress)
		}

		// Post-compress: [user, handoff, continue nudge]; next turn appends assistant.
		if len(ah.Messages()) != 4 {
			t.Errorf("expected 4 messages (user, handoff, continue nudge, final), got %d", len(ah.Messages()))
		} else {
			if ah.Messages()[0].Role != RoleUser {
				t.Errorf("first message role = %q, want user (original request must not be dropped)", ah.Messages()[0].Role)
			}
			if ah.Messages()[0].Content != "Complete the first task" {
				t.Errorf("first message content = %q, want original user prompt", ah.Messages()[0].Content)
			}
			if ah.Messages()[1].Role != RoleDeveloper {
				t.Errorf("handoff role = %q, want developer", ah.Messages()[1].Role)
			}
			if ah.Messages()[1].Content != "Mock compressed handoff. Remaining: Task 2." {
				t.Errorf("handoff content = %q, want last completed message full text only", ah.Messages()[1].Content)
			}
			if ah.Messages()[2].Role != RoleDeveloper {
				t.Errorf("nudge role = %q, want developer", ah.Messages()[2].Role)
			}
			if ah.Messages()[2].Content != continuePlanNudge {
				t.Errorf("nudge content = %q, want continuePlanNudge", ah.Messages()[2].Content)
			}
			if ah.Messages()[3].Role != RoleAssistant {
				t.Errorf("fourth message role = %q, want assistant", ah.Messages()[3].Role)
			}
			if !strings.Contains(ah.Messages()[3].Content, "All done!") {
				t.Errorf("final message content = %q, want contains 'All done!'", ah.Messages()[3].Content)
			}
		}

		var hasComplete bool
		for _, ev := range events {
			if ev.Type == StreamEventError {
				t.Errorf("unexpected error event: %v", ev.Error)
			}
			if ev.Type == StreamEventComplete {
				hasComplete = true
			}
		}
		if !hasComplete {
			t.Error("expected StreamEventComplete")
		}
	})

	t.Run("interrupt with completed todo parks turn with full history", func(t *testing.T) {
		// complete_todo + interrupt in the same batch: plan updates, interrupt
		// parks the turn, and handoff compress must not run (still one model invoke).
		store := testStore(t)
		var invokeCount int

		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				invokeCount++
				if invokeCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_comp1", CallID: "call_comp1", Name: "complete_todo", Arguments: `{"title":"Task 1"}`},
						{ID: "call_int1", CallID: "call_int1", Name: "ask_user", Arguments: `{}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
				}
			},
		}

		ah := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 65536}, Model: strategy, Store: store, Tools: []*Tool{interruptTool}})
		ah.session.Plan().Set([]Todo{
			{Title: "Task 1", Status: streaming.TodoStatusInProgress},
		})

		ch, err := ah.Run(context.Background(), "Start")
		if err != nil {
			t.Fatal(err)
		}

		var foundInterrupt bool
		var sawCompleteTodoResult bool
		for ev := range ch {
			if ev.Type == StreamEventInterrupt {
				foundInterrupt = true
			}
			if ev.Type == StreamEventToolResult && strings.Contains(ev.Content, "completed") {
				sawCompleteTodoResult = true
			}
			if ev.Type == StreamEventError {
				t.Errorf("unexpected error: %v", ev.Error)
			}
		}

		if !foundInterrupt {
			t.Fatal("expected interrupt event")
		}
		if !sawCompleteTodoResult {
			t.Error("expected complete_todo tool result before park")
		}
		if invokeCount != 1 {
			t.Errorf("invokeCount = %d, want 1 (no handoff compress invoke while interrupt pending)", invokeCount)
		}
		if len(ah.pendingToolCalls) == 0 {
			t.Error("expected pending tool calls on interrupt path")
		}

		// Window keeps the live turn (user + tool traffic), not a post-handoff reshape.
		if len(ah.Messages()) < 2 {
			t.Fatalf("context window too short: %d", len(ah.Messages()))
		}
		if ah.Messages()[0].Role != RoleUser || ah.Messages()[0].Content != "Start" {
			t.Errorf("first message = %+v, want original user prompt", ah.Messages()[0])
		}
		var hasDeveloperHandoff bool
		for _, m := range ah.Messages() {
			if m != nil && m.Role == RoleDeveloper {
				hasDeveloperHandoff = true
			}
		}
		if hasDeveloperHandoff {
			t.Error("parked interrupt turn should not replace history with a developer handoff")
		}

		plan := ah.session.Plan().Get()
		if plan == nil || len(plan) == 0 {
			t.Fatal("plan should not be nil or empty")
		}
		if plan[0].Status != streaming.TodoStatusCompleted {
			t.Errorf("Task 1 status = %q, want %q", plan[0].Status, streaming.TodoStatusCompleted)
		}
	})

	t.Run("regular tool call retains tool traffic in window", func(t *testing.T) {
		store := testStore(t)
		var invokeCount int

		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				invokeCount++
				if invokeCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_greet", CallID: "call_greet", Name: "greet", Arguments: `{"Name":"World"}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
					return
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "Done!", IsComplete: true}
			},
		}

		ah := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 65536}, Model: strategy, Store: store, Tools: []*Tool{validTool}})

		ch, err := ah.Run(context.Background(), "Greet the world")
		if err != nil {
			t.Fatal(err)
		}

		var toolResult string
		var finalMsg string
		for ev := range ch {
			switch ev.Type {
			case StreamEventError:
				t.Errorf("unexpected error: %v", ev.Error)
			case StreamEventToolResult:
				toolResult = ev.Content
			case StreamEventMessage:
				if ev.Content == "Done!" {
					finalMsg = ev.Content
				}
			}
		}

		if invokeCount != 2 {
			t.Errorf("expected 2 total invocations, got %d", invokeCount)
		}
		if toolResult != "Hello World" {
			t.Errorf("tool result = %q, want Hello World", toolResult)
		}
		if finalMsg != "Done!" {
			t.Errorf("final message = %q, want Done!", finalMsg)
		}

		var sawUser, sawTool, sawAssistant bool
		for _, m := range ah.Messages() {
			if m == nil {
				continue
			}
			switch m.Role {
			case RoleUser:
				if m.Content == "Greet the world" {
					sawUser = true
				}
			case RoleTool:
				if m.Content == "Hello World" {
					sawTool = true
				}
			case RoleAssistant:
				if m.Content == "Done!" {
					sawAssistant = true
				}
			case RoleDeveloper:
				t.Error("regular tool path should not insert developer handoff messages")
			}
		}
		if !sawUser || !sawTool || !sawAssistant {
			t.Errorf("window missing expected roles: user=%v tool=%v assistant=%v window=%+v",
				sawUser, sawTool, sawAssistant, ah.Messages())
		}
	})

	t.Run("complete last todo skips handoff", func(t *testing.T) {
		// Final complete_todo must not collapse context or spend a handoff model call.
		store := testStore(t)
		var invokeCount int
		var sawHandoffDeveloper bool

		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				invokeCount++
				if invokeCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_ct_last", CallID: "call_ct_last", Name: "complete_todo", Arguments: `{"title":"Only"}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
					return
				}
				for _, m := range msgs {
					if m != nil && m.Role == RoleDeveloper {
						sawHandoffDeveloper = true
					}
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "Plan finished.", IsComplete: true}
			},
		}

		ah := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 65536}, Model: strategy, Store: store, Tools: []*Tool{validTool}})
		ah.session.Plan().Set([]Todo{
			{Title: "Only", Status: streaming.TodoStatusInProgress},
		})

		ch, err := ah.Run(context.Background(), "Finish the plan")
		if err != nil {
			t.Fatal(err)
		}
		var sawToolDone, sawFinal bool
		for ev := range ch {
			if ev.Type == StreamEventToolResult && strings.Contains(ev.Content, "All todos completed") {
				sawToolDone = true
			}
			if ev.Type == StreamEventMessage && ev.Content == "Plan finished." {
				sawFinal = true
			}
		}
		if !sawToolDone {
			t.Error("want complete_todo all-done tool result")
		}
		if !sawFinal {
			t.Error("want final assistant wrap-up message")
		}
		// tool turn + wrap-up turn only (no handoff Invoke).
		if invokeCount != 2 {
			t.Errorf("invokeCount = %d, want 2 (no handoff model call)", invokeCount)
		}
		if sawHandoffDeveloper {
			t.Error("should not insert developer handoff when plan is fully done")
		}
	})

	t.Run("handoff invoke error soft-fails with fallback", func(t *testing.T) {
		// Mid-plan complete_todo must not die when the handoff model call fails
		// (Azure response.failed). We install a plan-derived fallback and continue.
		store := testStore(t)
		var invokeCount int
		var handoffAttempted bool

		strategy := &mockStrategy{
			invokeErrFn: func(_ context.Context, _ []*Message, tools []*Tool) error {
				invokeCount++
				if invokeCount == 1 {
					return nil
				}
				if invokeCount == 2 {
					handoffAttempted = true
					return fmt.Errorf("compression invoke failed")
				}
				return nil
			},
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				if invokeCount <= 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_ct_err", CallID: "call_ct_err", Name: "complete_todo", Arguments: `{"title":"Task 1"}`},
					}, IsComplete: true}
					return
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "continuing", IsComplete: true}
			},
		}

		ah := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 65536}, Model: strategy, Store: store, Tools: []*Tool{validTool}})
		ah.session.Plan().Set([]Todo{
			{Title: "Task 1", Status: streaming.TodoStatusInProgress},
			{Title: "Task 2", Status: streaming.TodoStatusPending},
		})

		ch, err := ah.Run(context.Background(), "Complete task 1")
		if err != nil {
			t.Fatal(err)
		}

		var foundComplete bool
		for ev := range ch {
			if ev.Type == StreamEventComplete {
				foundComplete = true
			}
			if ev.Type == StreamEventError {
				t.Fatalf("did not expect error after soft-fail handoff: %v %q", ev.Error, ev.Content)
			}
		}
		if !handoffAttempted {
			t.Error("handoff invoke should have been attempted")
		}
		if !foundComplete {
			t.Error("expected StreamEventComplete after soft-fail handoff")
		}
		foundFallback := false
		for _, m := range ah.Messages() {
			if m.Role == RoleDeveloper && strings.Contains(m.Content, "fallback") {
				foundFallback = true
			}
		}
		if !foundFallback {
			t.Fatalf("expected fallback handoff in window, got %+v", ah.Messages())
		}
	})
}

func TestNewAgent(t *testing.T) {
	store := testStore(t)
	mockModel := &mockStrategy{}
	wd := &recordingWatchdog{}

	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{
			MaxWindowSize: 4096,
			SystemPrompt:  "test prompt",
		},
		Model:    mockModel,
		Store:    store,
		WatchDog: wd,
	})

	if h.model != InferenceStrategy(mockModel) {
		t.Error("Model not wired from arg")
	}
	if h.maxWindowSize != 4096 {
		t.Errorf("MaxWindowSize = %d, want 4096", h.maxWindowSize)
	}
	if h.instructions != "test prompt" {
		t.Errorf("SystemPrompt = %q, want 'test prompt'", h.instructions)
	}
	if h.store != store {
		t.Error("Store not wired from arg")
	}
	if h.watchDog != AgentWatchDog(wd) {
		t.Error("WatchDog not wired from arg")
	}
	if len(h.Messages()) != 0 {
		t.Error("Messages should be empty on init")
	}
	if h.sessionId != "" {
		t.Errorf("SessionId = %q, want empty", h.sessionId)
	}
}

func TestRun_cancelMidStream_endsTurn(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			once.Do(func() { close(started) })
			for {
				select {
				case <-ctx.Done():
					return
				case ch <- LLMResponseChunk{
					Type:       StreamEventMessage,
					MessageId:  "m",
					Content:    "x",
					IsComplete: false,
				}:
				}
			}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 8192}, Model: strategy})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := h.Run(ctx, "stream please")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("invoke did not start")
	}
	select {
	case <-events:
	case <-time.After(2 * time.Second):
		t.Fatal("no stream events")
	}
	cancel()

	var sawCancel bool
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				if !sawCancel {
					t.Fatal("channel closed without cancelled error event")
				}
				return
			}
			if ev.Type == StreamEventError && errors.Is(ev.Error, context.Canceled) {
				sawCancel = true
			}
		case <-deadline:
			t.Fatal("turn did not end after cancel")
		}
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
	h := NewAgent(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 8192}, Model: mock})

	events, err := h.Run(context.Background(), "what is the answer?")
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}

	if len(h.Messages()) < 3 {
		t.Fatalf("expected at least 3 messages in context window, got %d", len(h.Messages()))
	}

	var reasoningMsg, assistantMsg *Message
	for _, m := range h.Messages() {
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

func TestRun_windowPressure_summarizesAndPreservesUser(t *testing.T) {
	// When token estimate exceeds 85% of MaxWindowSize, addToContext summarizes
	// older history, keeps the original user message, and continues the turn.
	store := testStore(t)
	const maxWindow = 40
	var invokeCount int
	var sawSummaryInvoke bool
	const summaryText = "SUMMARY_OF_HISTORY"

	strategy := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return contentTokenEstimate(msgs), nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			switch invokeCount {
			case 1:
				// Seed a reply already over MaxWindowSize so absorb uses the over-max loop.
				pad := strings.Repeat("x", 200)
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: pad, IsComplete: true}
			case 2:
				// This is the pressure-compress invoke (summarize staged history).
				sawSummaryInvoke = true
				events <- LLMResponseChunk{
					Type: StreamEventMessage, Content: summaryText, IsComplete: true,
					InputTokens: 11, OutputTokens: 7, ReasoningTokens: 3,
				}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "answer after compress", IsComplete: true}
			}
		},
	}

	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: maxWindow},
		Model:  strategy,
		Store:  store,
	})

	// First turn fills the window.
	ch1, err := ah.Run(context.Background(), "original user goal")
	if err != nil {
		t.Fatal(err)
	}
	for range ch1 {
	}

	// Second turn exceeds the pressure threshold and must compress.
	ch2, err := ah.Run(context.Background(), "follow-up that needs room")
	if err != nil {
		t.Fatal(err)
	}
	var final string
	for ev := range ch2 {
		if ev.Type == StreamEventError {
			t.Fatalf("unexpected error: %v %s", ev.Error, ev.Content)
		}
		if ev.Type == StreamEventMessage && ev.Content == "answer after compress" {
			final = ev.Content
		}
	}
	if !sawSummaryInvoke {
		t.Fatal("expected a summarization invoke under window pressure")
	}
	if final != "answer after compress" {
		t.Fatalf("final content = %q", final)
	}
	if len(ah.Messages()) == 0 || ah.Messages()[0].Role != RoleUser || ah.Messages()[0].Content != "original user goal" {
		t.Fatalf("first message must remain original user, got %+v", ah.Messages())
	}
	var sawSummary bool
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleAssistant && m.Content == summaryText {
			sawSummary = true
		}
	}
	if !sawSummary {
		t.Fatalf("expected summary assistant message in window, got %+v", ah.Messages())
	}
}

func TestRun_windowPressure_onToolResult_summarizes(t *testing.T) {
	// Tool results go through addToContext; pressure there must summarize without
	// hanging the turn (same deadlock class as user-prompt pressure).
	store := testStore(t)
	const maxWindow = 100
	const summaryText = "TOOL_PATH_SUMMARY"
	var invokeCount int
	var sawSummary bool

	greet := NewTool(ToolConfig{
		Name: "greet",
		Handler: func(ctx context.Context, args struct{ Name string }) (string, error) {
			// Large tool result to force pressure when appended.
			return strings.Repeat("R", 90), nil
		},
	})

	strategy := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return contentTokenEstimate(msgs), nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			// Invoke order: (1) tool call, (2) summary during tool-result addToContext,
			// (3) continue after tools.
			switch invokeCount {
			case 1:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "g1", CallID: "g1", Name: "greet", Arguments: `{"Name":"x"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			case 2:
				sawSummary = true
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: summaryText, IsComplete: true}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "after tool compress", IsComplete: true}
			}
		},
	}

	// Seed a near-full window so the large tool result tips pressure.
	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: maxWindow},
		Model:  strategy,
		Store:  store,
		Tools:  []*Tool{greet},
	})
	ah.restoreMessages([]*Message{
		{Role: RoleUser, Content: "start"},
		{Role: RoleAssistant, Content: strings.Repeat("a", 40)},
	})

	ch, err := ah.Run(context.Background(), "use greet")
	if err != nil {
		t.Fatal(err)
	}
	var final string
	for ev := range ch {
		if ev.Type == StreamEventError {
			t.Fatalf("error: %v %s", ev.Error, ev.Content)
		}
		if ev.Type == StreamEventMessage && ev.Content == "after tool compress" {
			final = ev.Content
		}
	}
	if !sawSummary {
		t.Fatal("expected summarization invoke when tool result exceeds window pressure")
	}
	if final != "after tool compress" {
		t.Fatalf("final = %q", final)
	}
	if ah.Messages()[0].Role != RoleUser || ah.Messages()[0].Content != "start" {
		t.Fatalf("must preserve original user, got %+v", ah.Messages()[0])
	}
}

func TestRun_countTokensError_emitsErrorEvent(t *testing.T) {
	strategy := &mockStrategy{
		countTokensFn: func(context.Context, []*Message, []*Tool) (int, error) {
			return 0, fmt.Errorf("token service unavailable")
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "should not run", IsComplete: true}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  testStore(t),
	})
	ch, err := ah.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	var sawErr bool
	for ev := range ch {
		if ev.Type == StreamEventError && strings.Contains(ev.Error.Error()+ev.Content, "token service unavailable") {
			sawErr = true
		}
		if ev.Type == StreamEventMessage && ev.Content == "should not run" {
			t.Error("model should not be invoked after CountTokens failure")
		}
	}
	if !sawErr {
		t.Fatal("expected StreamEventError from CountTokens failure")
	}
}

func TestRun_readSkill_returnsInstructions(t *testing.T) {
	skillsRoot := t.TempDir()
	skillDir := filepath.Join(skillsRoot, "research")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const instructions = "Always verify claims against primary sources."
	body := "---\nname: research-skill\ndescription: Research carefully\n---\n\n" + instructions + "\n"
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "call_skill", CallID: "call_skill", Name: "read_skill", Arguments: `{"name":"research-skill"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "applied skill", IsComplete: true}
		},
	}

	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192, SkillDirectories: []string{skillsRoot}},
		Model:  strategy,
		Store:  testStore(t),
	})

	ch, err := ah.Run(context.Background(), "research something")
	if err != nil {
		t.Fatal(err)
	}
	var toolResult string
	for ev := range ch {
		if ev.Type == StreamEventError {
			t.Fatalf("error: %v %s", ev.Error, ev.Content)
		}
		if ev.Type == StreamEventToolResult {
			toolResult = ev.Content
		}
	}
	if !strings.Contains(toolResult, instructions) {
		t.Fatalf("read_skill result = %q, want instructions body", toolResult)
	}
	if invokeCount != 2 {
		t.Errorf("invokeCount = %d, want 2", invokeCount)
	}
}

// stubModelTasks records Absorb/Handoff/Turn for AgentOptions.ModelTasks injection.
type stubModelTasks struct {
	cm           ContextManager
	absorbCalls  atomic.Int64
	handoffCalls atomic.Int64
	turnCalls    atomic.Int64
	turnFn       func(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error)
}

func (s *stubModelTasks) Turn(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error) {
	s.turnCalls.Add(1)
	if s.turnFn != nil {
		return s.turnFn(ctx, tools, systemPrompt)
	}
	ch := make(chan LLMResponseChunk, 1)
	ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
	close(ch)
	return ch, nil
}

func (s *stubModelTasks) Absorb(ctx context.Context, msg *Message, tools []*Tool, systemPrompt string) (AbsorbResult, error) {
	s.absorbCalls.Add(1)
	if msg != nil {
		s.cm.Add(msg)
	}
	return AbsorbResult{}, nil
}

func (s *stubModelTasks) Handoff(ctx context.Context, plan []Todo, planDoc string, tools []*Tool, systemPrompt string) error {
	s.handoffCalls.Add(1)
	s.cm.Replace([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleDeveloper, Content: "stub handoff"},
	})
	return nil
}

// TestNewAgent_injectsModelTasks: Run uses injected Absorb/Turn; complete_todo uses Handoff.
func TestNewAgentFromSession_resumesPendingToolInterrupt(t *testing.T) {
	store := testStore(t)
	interruptTool := NewTool(ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime *HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(
				`[{"title":"Yes","description":"","isRecommended":true},{"title":"No","description":"","isRecommended":false}]`,
			))
			if err != nil {
				return "", err
			}
			choice := intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice
			return "chose:" + choice.Title, nil
		},
	})

	var callCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			callCount++
			if callCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "call_park", CallID: "call_park", Name: "ask_user", Arguments: `{}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "resumed after reload", IsComplete: true}
		},
	}

	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  store,
		Tools:  []*Tool{interruptTool},
	})
	ah.sessionId = "sess-pending-resume"

	ch, err := ah.Run(context.Background(), "need a choice")
	if err != nil {
		t.Fatal(err)
	}
	var interruptId string
	for ev := range ch {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				t.Fatal(err)
			}
			interruptId = payload.InterruptId
		}
	}
	if interruptId == "" {
		t.Fatal("expected interrupt id from first run")
	}
	if len(ah.pendingToolCalls) != 1 {
		t.Fatalf("pending tools = %d, want 1", len(ah.pendingToolCalls))
	}

	// Process boundary: drop live harness, reload checkpoint.
	restored, err := NewAgentFromSession(context.Background(), "sess-pending-resume", AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  store,
		Tools:  []*Tool{interruptTool},
	})
	if err != nil {
		t.Fatalf("NewAgentFromSession: %v", err)
	}
	if len(restored.pendingToolCalls) != 1 {
		t.Fatalf("restored pending tools = %d, want 1", len(restored.pendingToolCalls))
	}
	if !restored.pendingToolCalls["call_park"].InterruptActive {
		t.Fatal("restored pending tool should still be InterruptActive")
	}
	if !restored.session.HasPendingInterrupt() {
		t.Fatal("restored session should have pending interrupt")
	}

	resolution := fmt.Sprintf(`{"interruptId":%q,"selectionIdx":0}`, interruptId)
	out, err := restored.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptId: []byte(resolution),
	})
	if err != nil {
		t.Fatalf("ReturnFromInterrupt: %v", err)
	}
	var toolResult, finalMsg string
	for ev := range out {
		switch ev.Type {
		case StreamEventToolResult:
			toolResult = ev.Content
		case StreamEventMessage:
			finalMsg = ev.Content
		case StreamEventError:
			t.Fatalf("error after resume: %v", ev.Error)
		}
	}
	if !strings.Contains(toolResult, "chose:Yes") {
		t.Fatalf("tool result = %q, want chose:Yes", toolResult)
	}
	if finalMsg != "resumed after reload" {
		t.Fatalf("final = %q", finalMsg)
	}
	if len(restored.pendingToolCalls) != 0 {
		t.Fatalf("pending after resume = %d, want 0", len(restored.pendingToolCalls))
	}
}
