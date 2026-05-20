package core

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
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
func (m *mockStrategy) WithResponseStrategy(string) InferenceStrategy { return m }
func (m *mockStrategy) SetSystemPrompt(string)                        {}
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

func mockConn(t *testing.T, expectCheckpoints int) pgxmock.PgxConnIface {
	t.Helper()
	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < expectCheckpoints; i++ {
		mock.ExpectExec("UPDATE postgres.session").
			WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	}
	return mock
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

	yieldTool := &Tool{
		Name: "prompt_user",
		Handler: func(args struct{}) (string, error) {
			return "", &YieldToConsumerError{
				Reason: "need user input",
				Data:   []byte(`{"prompt":"Who are you?"}`),
			}
		},
	}
	if err := yieldTool.Validate(); err != nil {
		t.Fatal(err)
	}

	t.Run("single turn no tool calls", func(t *testing.T) {
		conn := mockConn(t, 0)
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "Hello!", IsComplete: true}
			},
		}
		ah := &AgentHarness{
			Model:   strategy,
			Tools:   []*Tool{validTool},
			Runtime: HarnessRuntime{Store: conn},
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
		if err := conn.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("full tool call loop", func(t *testing.T) {
		conn := mockConn(t, 1)
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
			Runtime: HarnessRuntime{Store: conn},
		}
		ch, err := ah.Run(context.Background(), "Say hello")
		if err != nil {
			t.Fatal(err)
		}

		var contentEvents int
		var functionCallEvents int
		var toolResultEvents int
		for ev := range ch {
			switch ev.Type {
			case StreamEventMessage:
				contentEvents++
			case StreamEventFunctionCall:
				functionCallEvents++
			case StreamEventToolResult:
				toolResultEvents++
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
		if err := conn.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("tool not found", func(t *testing.T) {
		conn := mockConn(t, 1)
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
			Runtime: HarnessRuntime{Store: conn},
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
			t.Error("expected tool result event")
		}
		if err := conn.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("tool handler error", func(t *testing.T) {
		conn := mockConn(t, 1)
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
			Runtime: HarnessRuntime{Store: conn},
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
		if err := conn.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("invoke returns error", func(t *testing.T) {
		conn := mockConn(t, 0)
		strategy := &mockStrategy{
			invokeErr: fmt.Errorf("network error"),
		}
		ah := &AgentHarness{
			Model:   strategy,
			Runtime: HarnessRuntime{Store: conn},
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
		if err := conn.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("tool yields to consumer", func(t *testing.T) {
		conn := mockConn(t, 2)
		var callCount int
		strategy := &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
				callCount++
				if callCount == 1 {
					events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						{ID: "call_y", CallID: "call_y", Name: "prompt_user", Arguments: `{}`},
					}, IsComplete: true}
					events <- LLMResponseChunk{IsComplete: true}
					return
				}
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			},
		}
		ah := &AgentHarness{
			Model:   strategy,
			Tools:   []*Tool{yieldTool},
			Runtime: HarnessRuntime{Store: conn},
		}
		ch, err := ah.Run(context.Background(), "start")
		if err != nil {
			t.Fatal(err)
		}

		var foundYield bool
		var foundToolResult bool
		for ev := range ch {
			switch ev.Type {
			case StreamEventYield:
				foundYield = true
				if !bytes.Equal(ev.Data, []byte(`{"prompt":"Who are you?"}`)) {
					t.Errorf("yield data = %q", string(ev.Data))
				}
			case StreamEventToolResult:
				foundToolResult = true
			}
		}

		if !foundYield {
			t.Fatal("expected yield event")
		}
		if foundToolResult {
			t.Error("yield should not produce a tool result event")
		}
		if err := conn.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}
