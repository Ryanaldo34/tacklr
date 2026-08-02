package server

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// TestRunTurn_exportsTurnObservability: a completed registry turn is visible
// on injected trace and metric providers (outcome: host can scrape/observe the turn).
func TestRunTurn_exportsTurnObservability(t *testing.T) {
	spanExp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanExp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, MessageId: "m1", Content: "hello", IsComplete: true}
		},
	}
	reg := newTestRegistry(testStore(t), strategy, nil,
		WithTracerProvider(tp),
		WithMeterProvider(mp),
	)

	stream, err := reg.RunTurn(context.Background(), TurnRequest{
		AgentID:  "default",
		ThreadID: "thread-obs",
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

	var sawTurnSpan bool
	for _, s := range spanExp.GetSpans() {
		if s.Name == telemetry.SpanTurn {
			sawTurnSpan = true
			break
		}
	}
	if !sawTurnSpan {
		t.Fatalf("completed turn should export %s span", telemetry.SpanTurn)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var sawTurnMetric bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == telemetry.MetricTurnTotal {
				sawTurnMetric = true
			}
		}
	}
	if !sawTurnMetric {
		t.Fatalf("completed turn should export %s", telemetry.MetricTurnTotal)
	}
}
