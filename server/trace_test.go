package server

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func TestRunTurn_createsRootTurnSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, MessageId: "m1", Content: "hello", IsComplete: true}
		},
	}
	// Injected provider — does not require mutating the process-global OTEL provider.
	reg := newTestRegistry(testStore(t), strategy, nil, WithTracerProvider(tp))

	stream, err := reg.RunTurn(context.Background(), TurnRequest{
		AgentID:  "default",
		ThreadID: "thread-1",
		Prompt:   "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Events {
	}
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	spans := exp.GetSpans()
	var sawTurn bool
	for _, s := range spans {
		if s.Name == telemetry.SpanTurn {
			sawTurn = true
			break
		}
	}
	if !sawTurn {
		t.Fatalf("expected %s span among %d spans", telemetry.SpanTurn, len(spans))
	}
}
