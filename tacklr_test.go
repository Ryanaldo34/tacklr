package tacklr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

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

func (m *mockStrategy) SupportsMIME(mimeType string) bool {
	if m.supportsMIMEFn != nil {
		return m.supportsMIMEFn(mimeType)
	}
	// Tests default to text-only models.
	return IsTextMIME(mimeType)
}
func (m *mockStrategy) MaxContextWindow() (int, error) {
	return 8192, nil
}
func (m *mockStrategy) CountTokens(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
	if m.countTokensFn != nil {
		return m.countTokensFn(ctx, msgs, tools)
	}
	// Default 0 keeps existing tests under the window-pressure threshold.
	return 0, nil
}
func (m *mockStrategy) Invoke(ctx context.Context, msgs []*Message, tools []*Tool, systemPrompt string) (chan LLMResponseChunk, error) {
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
	if systemPrompt != "" {
		m.systemPrompts = append(m.systemPrompts, systemPrompt)
	}
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
}

func (w *recordingWatchdog) RecordOutput(msg *Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.outputs = append(w.outputs, msg)
	return nil
}
func (w *recordingWatchdog) RecordToolResult(msg *Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.toolResults = append(w.toolResults, msg)
	return nil
}

func reloadHarness(t *testing.T, h *TurnManager, opts AgentOptions) *TurnManager {
	t.Helper()
	cp, err := h.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	n, err := NewTurnManager(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.RestoreCheckpoint(*cp); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestNewTurnManager_nilModel_error(t *testing.T) {
	if _, err := NewTurnManager(context.Background(), AgentOptions{}); err == nil {
		t.Fatal("expected constructor error")
	}
}

func TestNewTurnManager(t *testing.T) {
	mockModel := &mockStrategy{}
	wd := &recordingWatchdog{}

	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{
			MaxWindowSize: 4096,
			SystemPrompt:  "test prompt",
		},
		Model:    mockModel,
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
	if h.watchDog != AgentWatchDog(wd) {
		t.Error("WatchDog not wired from arg")
	}
	if len(h.context.Messages()) != 0 {
		t.Error("window should be empty on init")
	}
	if h.sessionId != "" {
		t.Errorf("SessionId = %q, want empty", h.sessionId)
	}
}
