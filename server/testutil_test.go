package server

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/stores"
)

// mockInferenceStrategy is a controllable InferenceStrategy for tests.
type mockInferenceStrategy struct {
	invokeFn  func(context.Context, []*tacklr.Message, []*tacklr.Tool, chan<- tacklr.LLMResponseChunk)
	invokeErr error
	callNum   atomic.Int64
}

func (m *mockInferenceStrategy) WithApiKey(string) tacklr.InferenceStrategy         { return m }
func (m *mockInferenceStrategy) WithModel(string) tacklr.InferenceStrategy          { return m }
func (m *mockInferenceStrategy) WithURL(string) tacklr.InferenceStrategy            { return m }
func (m *mockInferenceStrategy) WithReasoningLevel(string) tacklr.InferenceStrategy { return m }
func (m *mockInferenceStrategy) WithStructuredOutput(any) tacklr.InferenceStrategy  { return m }
func (m *mockInferenceStrategy) SetSystemPrompt(string)                             {}
func (m *mockInferenceStrategy) Reset()                                             {}
func (m *mockInferenceStrategy) CompressContextWindow() error                       { return nil }
func (m *mockInferenceStrategy) MaxContextWindow() (int, error)                     { return 0, nil }
func (m *mockInferenceStrategy) CountTokens(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) (int, error) {
	return 0, nil
}
func (m *mockInferenceStrategy) Invoke(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) (chan tacklr.LLMResponseChunk, error) {
	if m.invokeErr != nil {
		return nil, m.invokeErr
	}
	m.callNum.Add(1)
	ch := make(chan tacklr.LLMResponseChunk)
	go func() {
		defer close(ch)
		if m.invokeFn != nil {
			m.invokeFn(ctx, msgs, tools, ch)
		}
	}()
	return ch, nil
}

func testStore(t *testing.T) *stores.InMemoryStore {
	t.Helper()
	return stores.NewInMemoryStore()
}

func newTestRegistry(store *stores.InMemoryStore, strategy tacklr.InferenceStrategy, tools []*tacklr.Tool) *Registry {
	r := NewRegistry(store, "default")
	r.Register("default", AgentSpec{
		Config: tacklr.Config{
			MaxWindowSize: 8192,
			SystemPrompt:  "test prompt",
		},
		Model:    strategy,
		Tools:    tools,
		WatchDog: nil,
	})
	return r
}

// recordedResult is one WriteResult call captured by recordingMessageWriter.
type recordedResult struct {
	ID     json.RawMessage
	Result any
}

// recordedError is one WriteError call captured by recordingMessageWriter.
type recordedError struct {
	ID  json.RawMessage
	Err error
}

// recordingMessageWriter is a MessageWriter that records all writes for assertions.
type recordingMessageWriter struct {
	mu      sync.Mutex
	Results []recordedResult
	Errors  []recordedError
	Frames  [][]byte
}

func (r *recordingMessageWriter) WriteResult(id json.RawMessage, result any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Results = append(r.Results, recordedResult{ID: append(json.RawMessage(nil), id...), Result: result})
	return nil
}

func (r *recordingMessageWriter) WriteError(id json.RawMessage, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Errors = append(r.Errors, recordedError{ID: append(json.RawMessage(nil), id...), Err: err})
	return nil
}

func (r *recordingMessageWriter) WriteFrame(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Frames = append(r.Frames, append([]byte(nil), data...))
	return nil
}

// framesAsMaps decodes recorded frames as JSON objects.
func (r *recordingMessageWriter) framesAsMaps(t *testing.T) []map[string]any {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]map[string]any, 0, len(r.Frames))
	for _, f := range r.Frames {
		var m map[string]any
		if err := json.Unmarshal(f, &m); err != nil {
			t.Fatalf("decode frame: %v\nframe: %s", err, f)
		}
		out = append(out, m)
	}
	return out
}
