package server

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	tacklrtemporal "github.com/ryanaldo34/tacklr/durable/temporal"
	"github.com/ryanaldo34/tacklr/internal/temporallive"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMain(m *testing.M) {
	code := m.Run()
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
	return streaming.IsTextMIME(mimeType)
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

type testKernel struct {
	Runtime durable.Runtime
	Catalog *durable.MemoryCatalog
}

func newTestKernel(t *testing.T, model tacklr.InferenceStrategy, spec durable.AgentSpec) *testKernel {
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
		return &testKernel{
			Runtime: inprocess.New(cat, inprocess.WithProjection(vfs.DirectProjection{})),
			Catalog: cat,
		}
	}
	c := temporallive.Client(t)
	tq := "tacklr-server-" + uuid.NewString()
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	rt := tacklrtemporal.New(c, tq, cat,
		tacklrtemporal.WithSnapshotStore(snaps),
		tacklrtemporal.WithEventLog(log),
	)
	w := tacklrtemporal.NewWorker(c, tq, tacklrtemporal.WorkerOptions{
		Catalog:    cat,
		Snapshots:  snaps,
		Fallback:   log,
		Projection: vfs.DirectProjection{},
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return &testKernel{Runtime: rt, Catalog: cat}
}

func newEmptyKernel() *testKernel {
	cat := durable.NewCatalog("")
	return &testKernel{
		Runtime: inprocess.New(cat, inprocess.WithProjection(vfs.DirectProjection{})),
		Catalog: cat,
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	k := newTestKernel(t, nil, durable.AgentSpec{})
	return NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil))
}

func (s *Server) inbound(ctx context.Context, body []byte, w MessageWriter) {
	_ = s.Protocols[0].HandleInbound(ctx, s.env(&Conn{Writer: w}), body)
}

type recordingMessageWriter = testkit.RecordingWriter
