package telemetry

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
)

func TestDefaultInstrumentor_startTurn(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	SetTracerProvider(tp)
	t.Cleanup(func() { SetTracerProvider(nil) })

	ctx := BindTurnContext(context.Background(), "agent-a", "sess-1")
	ctx, span := DefaultInstrumentor().StartTurn(ctx, TurnAttrs{
		AgentID:   "agent-a",
		ThreadID:  "sess-1",
		SessionID: "sess-1",
		Kind:      TurnKindPrompt,
		Runtime:   RuntimeInProcess,
	})
	if AgentIDFromContext(ctx) != "agent-a" {
		t.Fatal(AgentIDFromContext(ctx))
	}
	EmitTurnReceived(ctx, TurnKindPrompt, 12, 0)
	EmitTurnReceived(ctx, TurnKindResume, 0, 2)
	span.End(OutcomeYield, nil)

	ended := sr.Ended()
	if len(ended) == 0 {
		t.Fatal("want turn span")
	}
	got := ended[len(ended)-1]
	if got.Name() != SpanTurn {
		t.Fatalf("name %s", got.Name())
	}
	attrs := map[string]string{}
	for _, a := range got.Attributes() {
		attrs[string(a.Key)] = a.Value.AsString()
	}
	if attrs[AttrRuntime] != RuntimeInProcess || attrs[AttrTurnKind] != TurnKindPrompt {
		t.Fatalf("attrs %+v", attrs)
	}
	if attrs[AttrOutcome] != OutcomeYield {
		t.Fatalf("outcome %s", attrs[AttrOutcome])
	}
}

func TestEnsureReplaySafeProvider(t *testing.T) {
	SetTracerProvider(nil)
	if IsReplaySafeProvider() {
		t.Fatal("noop is not replay-safe")
	}
	EnsureReplaySafeProvider()
	if !IsReplaySafeProvider() {
		t.Fatal("want replay-safe after ensure")
	}
	EnsureReplaySafeProvider() // idempotent
	tp := temporalotel.NewReplaySafeTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
}

func TestNewOTLPTransport_grpcConn(t *testing.T) {
	if _, err := newOTLPTransport("localhost:4317", "ftp", true); err == nil {
		t.Fatal("want unknown protocol")
	}
	tr, err := newOTLPTransport("localhost:4317", "grpc", true)
	if err != nil {
		t.Fatal(err)
	}
	if tr.conn == nil {
		t.Fatal("shared ClientConn")
	}
	if err := tr.close(); err != nil {
		t.Fatal(err)
	}
	if err := (*otlpTransport)(nil).close(); err != nil {
		t.Fatal(err)
	}
	httpTr, err := newOTLPTransport("localhost:4318", "http", true)
	if err != nil {
		t.Fatal(err)
	}
	if httpTr.httpClient == nil {
		t.Fatal("shared HTTP client")
	}
	if err := httpTr.close(); err != nil {
		t.Fatal(err)
	}
}

func TestInit_emptyEndpoint_replaySafe(t *testing.T) {
	shutdown, err := Init(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
		SetTracerProvider(nil)
	})
	if !IsReplaySafeProvider() {
		t.Fatal("Init with no endpoint should still install ReplaySafe")
	}
}
