package server

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
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

func newTestRegistry(store *stores.InMemoryStore, strategy tacklr.InferenceStrategy, tools []*tacklr.Tool, opts ...inprocess.Option) *testKernel {
	_ = store
	return newTestKernel(strategy, tools, durable.AgentSpec{}, opts...)
}

type testKernel struct {
	Runtime durable.Runtime
	Catalog *durable.MemoryCatalog
}

func emptyTestKernel() *testKernel {
	cat := durable.NewCatalog("")
	return &testKernel{
		Runtime: inprocess.New(cat, inprocess.WithProjection(vfs.DirectProjection{})),
		Catalog: cat,
	}
}

func newTestKernel(strategy tacklr.InferenceStrategy, tools []*tacklr.Tool, spec durable.AgentSpec, opts ...inprocess.Option) *testKernel {
	if spec.Options.Model == nil {
		spec.Options.Model = strategy
	}
	if spec.Options.Config.MaxWindowSize == 0 {
		spec.Options.Config.MaxWindowSize = 8192
	}
	if spec.Options.Tools == nil {
		spec.Options.Tools = tools
	}
	if spec.Options.Config.SystemPrompt == "" {
		spec.Options.Config.SystemPrompt = "test prompt"
	}
	if spec.Options.Model == nil {
		spec.Options.Model = &mockInferenceStrategy{}
	}
	cat := durable.NewCatalog("default")
	cat.Register("default", spec)
	all := append([]inprocess.Option{inprocess.WithProjection(vfs.DirectProjection{})}, opts...)
	return &testKernel{Runtime: inprocess.New(cat, all...), Catalog: cat}
}

func (k *testKernel) Register(id string, spec durable.AgentSpec) {
	k.Catalog.Register(id, spec)
}

func (k *testKernel) DefaultAgent() string { return k.Catalog.DefaultID() }
func (k *testKernel) HasAgent(id string) bool {
	_, ok := k.Catalog.Lookup(id)
	return ok
}

func (k *testKernel) CancelSession(id string) {
	_ = k.Runtime.Cancel(context.Background(), durable.SessionID(id))
}

func serverFromKernel(k *testKernel, protocols ...Protocol) *Server {
	return NewServer(k.Runtime, k.Catalog, protocols...)
}

func newTestServer(strategy tacklr.InferenceStrategy, tools []*tacklr.Tool, protocols ...Protocol) *Server {
	k := newTestKernel(strategy, tools, durable.AgentSpec{})
	if len(protocols) == 0 {
		protocols = []Protocol{SSE}
	}
	return NewServer(k.Runtime, k.Catalog, protocols...)
}

// recordingMessageWriter records MessageWriter traffic via shared testkit.
type recordingMessageWriter = testkit.RecordingWriter
