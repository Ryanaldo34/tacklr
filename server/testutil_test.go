package server

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// mockInferenceStrategy is a controllable InferenceStrategy for tests.
type mockInferenceStrategy struct {
	invokeFn       func(context.Context, []*tacklr.Message, []*tacklr.Tool, chan<- tacklr.LLMResponseChunk)
	invokeErr      error
	supportsMIMEFn func(string) bool
	callNum        atomic.Int64
}

func (m *mockInferenceStrategy) MaxContextWindow() (int, error) { return 8192, nil }
func (m *mockInferenceStrategy) SupportsMIME(mimeType string) bool {
	if m.supportsMIMEFn != nil {
		return m.supportsMIMEFn(mimeType)
	}
	// Default: text-only. Multimodal tests opt in via supportsMIMEFn.
	return streaming.IsTextMIME(mimeType)
}
func (m *mockInferenceStrategy) CountTokens(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) (int, error) {
	return 0, nil
}
func (m *mockInferenceStrategy) Invoke(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, _ string) (chan tacklr.LLMResponseChunk, error) {
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

func mustAgent(t *testing.T, opts tacklr.AgentOptions) *tacklr.AgentHarness {
	t.Helper()
	h, err := tacklr.NewAgent(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func serveSSEHTTP(s *Server, w http.ResponseWriter, req *http.Request) {
	s.HTTPMux().ServeHTTP(w, req)
}

func testStore(t *testing.T) *stores.InMemoryStore {
	t.Helper()
	return stores.NewInMemoryStore()
}

func newTestRegistry(store *stores.InMemoryStore, strategy tacklr.InferenceStrategy, tools []*tacklr.Tool, opts ...RegistryOption) *Registry {
	r := NewRegistry(store, "default", append([]RegistryOption{WithVFSProjection(DirectProjection{})}, opts...)...)
	r.Register("default", AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{
				MaxWindowSize: 8192,
				SystemPrompt:  "test prompt",
			},
			Model: strategy,
			Tools: tools,
		},
	})
	return r
}

// recordingMessageWriter records MessageWriter traffic via shared testkit.
type recordingMessageWriter = testkit.RecordingWriter
