package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type stubProviderErr struct {
	status int
	code   string
	msg    string
}

func (e stubProviderErr) Error() string { return e.msg }
func (e stubProviderErr) ProviderHTTPStatus() int {
	return e.status
}
func (e stubProviderErr) ProviderErrorCode() string { return e.code }

func TestModelSpan_lifecycleAndClassify(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp, err := MeterProviderFromPrometheusRegisterer(reg, "model-cov", "v0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	SetMeterProvider(mp)
	t.Cleanup(func() { SetMeterProvider(nil) })
	inst := MustInstruments(MeterFromProvider(mp))

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	SetTracerProvider(tp)
	t.Cleanup(func() { SetTracerProvider(nil) })

	id := NewModelIdentity("gpt-4o", "https://myresource.openai.azure.com/")
	if id.Provider != GenAIProviderAzure {
		t.Fatalf("azure: %s", id.Provider)
	}
	if NewModelIdentity("m", "https://api.openai.com/v1").Provider != GenAIProviderOpenAI {
		t.Fatal("openai")
	}
	if NewModelIdentity("m", "http://localhost:11434").Provider != GenAIProviderUnknown {
		t.Fatal("unknown")
	}

	ctx := ContextWithInstruments(context.Background(), inst)
	ctx = ContextWithAgentID(ctx, "agent-1")
	ctx = ContextWithModelIdentity(ctx, id)
	ctx = ContextWithAfterTools(ctx)

	ctx, span := StartModelSpan(ctx, "", 1, WindowShape{Messages: 3, ToolPairs: 1})
	span.End(nil, TokenUsage{Input: 10, Output: 5, Reasoning: 2})

	// Error classes
	cases := []struct {
		err    error
		class  string
		status int
		code   string
	}{
		{context.Canceled, ErrorClassCancelled, 0, ""},
		{context.DeadlineExceeded, ErrorClassTimeout, 0, ""},
		{stubProviderErr{status: 429, code: "rate", msg: "slow"}, ErrorClassProvider4xx, 429, "rate"},
		{stubProviderErr{status: 503, code: "x", msg: "down"}, ErrorClassProvider5xx, 503, "x"},
		{errors.New("max_output_tokens exceeded"), ErrorClassMaxTokens, 0, ""},
		{errors.New("context canceled by peer"), ErrorClassCancelled, 0, ""},
		{errors.New("upstream timeout"), ErrorClassTimeout, 0, ""},
		{errors.New("weird"), ErrorClassOther, 0, ""},
		{stubProviderErr{status: 400, code: strings.Repeat("c", 80), msg: "bad"}, ErrorClassProvider4xx, 400, strings.Repeat("c", 80)},
	}
	for _, tc := range cases {
		_, s := StartModelSpan(ctx, ModelPhaseHandoff, 0, WindowShape{})
		s.End(tc.err, TokenUsage{})
	}

	// Double end / nil
	_, s2 := StartModelSpan(ctx, ModelPhaseCompress, 2, WindowShape{})
	s2.End(nil, TokenUsage{})
	s2.End(errors.New("again"), TokenUsage{})
	(*ModelSpan)(nil).End(nil, TokenUsage{})

	// Empty identity defaults
	ctx2 := ContextWithInstruments(context.Background(), inst)
	ctx2 = ContextWithModelIdentity(ctx2, ModelIdentity{})
	_, s3 := StartModelSpan(ctx2, ModelPhaseTurn, 0, WindowShape{})
	s3.End(nil, TokenUsage{})

	if ClassifyErrorClass(nil, 0) != ErrorClassOK {
		t.Fatal("ok")
	}
}

func TestMeterContext_helpers(t *testing.T) {
	if MeterFromProvider(nil) == nil {
		t.Fatal("nil provider meter")
	}
	ctx := ContextWithMeter(context.Background(), Meter())
	if MeterFromContext(ctx) == nil {
		t.Fatal("meter from ctx")
	}
	if MeterFromContext(context.Background()) == nil {
		t.Fatal("fallback")
	}
	if ContextWithMeter(context.Background(), nil) == nil {
		t.Fatal("nil meter no-op")
	}
	if ContextWithInstruments(context.Background(), nil) == nil {
		t.Fatal("nil inst no-op")
	}
	// Force global instruments rebuild paths.
	SetMeterProvider(nil)
	_ = InstrumentsFromContext(context.Background())
	_ = InstrumentsFromContext(context.Background()) // second hit cached

	EmitEvent(context.Background(), "") // no-op empty name
	EmitEventSeverity(context.Background(), EventTurnEnded, log.SeverityError)

	InstallDefaultWithOTLP(nil, nil)
	InstallDefaultWithOTLP(slog.DiscardHandler, nil)
	InstallDefaultWithOTLP(nil, NewOTLPSlogHandler(""))
	InstallDefaultWithOTLP(slog.DiscardHandler, NewOTLPSlogHandler("svc"))
	// restore quiet default
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	// RecordBrain empty defaults
	reg := prometheus.NewRegistry()
	mp, err := MeterProviderFromPrometheusRegisterer(reg, "rb", "v")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	inst := MustInstruments(MeterFromProvider(mp))
	inst.RecordBrain(context.Background(), "a", "", "", "", false, 0)
	inst.RecordBrain(context.Background(), "a", BrainOpSearch, OutcomeOK, BrainDegradeNone, false, time.Millisecond)

	_ = time.Millisecond
}
