package server

import (
	"context"
	"os"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
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
	os.Exit(code)
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
		spec.Options.Model = &testkit.ScriptedModel{}
	}
	if spec.Options.Config.MaxWindowSize == 0 {
		spec.Options.Config.MaxWindowSize = 8192
	}
	if spec.Options.Config.SystemPrompt == "" {
		spec.Options.Config.SystemPrompt = "test prompt"
	}
	cat := durable.NewCatalog("default")
	cat.Register("default", spec)
	return &testRuntime{
		Runtime: inprocess.New(inprocess.Config{Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Projection: vfs.DirectProjection{}}),
		Catalog: cat,
	}
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
