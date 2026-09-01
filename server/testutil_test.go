package server

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	tacklrtemporal "github.com/ryanaldo34/tacklr/durable/temporal"
	"github.com/ryanaldo34/tacklr/internal/temporallive"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMain(m *testing.M) {
	shutdown, err := telemetry.Init(context.Background(), telemetry.Config{})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
	temporallive.Stop()
	os.Exit(code)
}

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
	return tacklr.IsTextMIME(mimeType)
}
func (m *mockInferenceStrategy) CountTokens(context.Context, []*tacklr.Message, []*tacklr.Tool) (int, error) {
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

type testRuntime struct {
	Runtime durable.Runtime
	Catalog *durable.MemoryCatalog
}

func newTestRuntime(t *testing.T, model tacklr.InferenceStrategy, spec durable.AgentSpec) *testRuntime {
	t.Helper()
	if spec.Options.Model == nil {
		spec.Options.Model = model
	}
	if spec.Options.Model == nil {
		spec.Options.Model = &mockInferenceStrategy{}
	}
	if spec.Options.Config.MaxWindowSize == 0 {
		spec.Options.Config.MaxWindowSize = 8192
	}
	if spec.Options.Config.SystemPrompt == "" {
		spec.Options.Config.SystemPrompt = "test prompt"
	}
	cat := durable.NewCatalog("default")
	cat.Register("default", spec)
	if testing.Short() || !temporallive.Available() {
		return &testRuntime{
			Runtime: inprocess.New(inprocess.Config{Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Projection: vfs.DirectProjection{}}),
			Catalog: cat,
		}
	}
	c, err := tacklrtemporal.Dial(client.Options{HostPort: temporallive.HostPort(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	tq := "tacklr-server-" + uuid.NewString()
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	tcfg := tacklrtemporal.Config{
		Catalog: cat, TaskQueue: tq, Snapshots: snaps, Fallback: log,
		Projection: vfs.DirectProjection{}, Secrets: durable.NewMemorySecretStorage(),
	}
	rt := tacklrtemporal.New(c, tcfg)
	w := tacklrtemporal.NewWorker(c, tcfg)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return &testRuntime{Runtime: rt, Catalog: cat}
}

func newEmptyRuntime() *testRuntime {
	cat := durable.NewCatalog("")
	return &testRuntime{
		Runtime: inprocess.New(inprocess.Config{Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Projection: vfs.DirectProjection{}}),
		Catalog: cat,
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	return NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil))
}

func (s *Server) inbound(ctx context.Context, body []byte, w MessageWriter) {
	_ = s.Protocols[0].HandleInbound(ctx, s.env(&Conn{Writer: w}), body)
}

type recordingMessageWriter = testkit.RecordingWriter
