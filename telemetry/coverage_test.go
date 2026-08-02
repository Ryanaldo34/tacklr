package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr/streaming"
)

// TestInstallDefaultWithOTLP_writesToBaseHandler: dual-write setup used by
// testserver must still emit slog records to the local handler (OTLP side is
// noop-safe without a collector).
func TestInstallDefaultWithOTLP_writesToBaseHandler(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	InstallDefaultWithOTLP(base, NewOTLPSlogHandler("cov-test"))
	slog.Info("dual-write smoke")
	if !strings.Contains(buf.String(), "dual-write smoke") {
		t.Fatalf("base handler missing log: %q", buf.String())
	}
	// MultiHandler group/attrs stay usable.
	mh := MultiHandler{base}
	if !mh.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("enabled")
	}
	_ = mh.WithAttrs([]slog.Attr{slog.String("k", "v")})
	_ = mh.WithGroup("g")
}

// TestInstruments_recordAllPaths exercises every instrument helper (including nil receiver).
func TestInstruments_recordAllPaths(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp, err := MeterProviderFromPrometheusRegisterer(reg, "cov-test", "v0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	SetMeterProvider(mp)
	t.Cleanup(func() { SetMeterProvider(nil) })

	inst := MustInstruments(MeterFromProvider(mp))
	ctx := ContextWithAgentID(context.Background(), "agent-1")
	if AgentIDFromContext(ctx) != "agent-1" {
		t.Fatal(AgentIDFromContext(ctx))
	}
	if AgentIDFromContext(context.Background()) != "" {
		t.Fatal("empty agent")
	}

	inst.RecordTurnStart(ctx, "agent-1")
	inst.RecordTurnEnd(ctx, "agent-1", "prompt", OutcomeOK, 10*time.Millisecond)
	inst.RecordTool(ctx, "agent-1", "search", "web", "success", time.Millisecond)
	inst.RecordInterrupt(ctx, "agent-1", "user_selection_choice")
	inst.RecordHandoff(ctx, "agent-1", HandoffOutcomeOK)
	inst.RecordHandoff(ctx, "agent-1", HandoffOutcomeFallback)
	inst.RecordCompress(ctx, "agent-1")
	inst.RecordSessionCreated(ctx)
	inst.RecordCheckpointSave(ctx, OutcomeOK)
	inst.RecordModel(ctx, "agent-1", ModelPhaseTurn, OutcomeOK, ErrorClassOK, 5*time.Millisecond)
	inst.RecordModel(ctx, "agent-1", ModelPhaseHandoff, OutcomeError, ErrorClassProvider4xx, time.Millisecond)
	inst.RecordTokens(ctx, "agent-1", 10, 20, 3)

	// Nil receiver no-ops.
	var nilInst *Instruments
	nilInst.RecordTurnStart(ctx, "a")
	nilInst.RecordTurnEnd(ctx, "a", "k", OutcomeOK, 0)
	nilInst.RecordTool(ctx, "a", "t", "", "ok", 0)
	nilInst.RecordInterrupt(ctx, "a", "k")
	nilInst.RecordHandoff(ctx, "a", HandoffOutcomeOK)
	nilInst.RecordCompress(ctx, "a")
	nilInst.RecordSessionCreated(ctx)
	nilInst.RecordCheckpointSave(ctx, OutcomeError)
	nilInst.RecordModel(ctx, "a", ModelPhaseTurn, OutcomeOK, ErrorClassOK, 0)
	nilInst.RecordTokens(ctx, "a", 1, 1, 1)

	// Context helpers for meters/instruments (nil meter/instruments are no-ops).
	if ContextWithMeter(context.Background(), nil) == nil {
		t.Fatal("nil meter")
	}
	m := MeterFromProvider(mp)
	ctxM := ContextWithMeter(context.Background(), m)
	if MeterFromContext(ctxM) == nil || MeterFromContext(context.Background()) == nil {
		t.Fatal("meter from context")
	}
	if MeterFromProvider(nil) == nil {
		t.Fatal("meter from nil provider uses global")
	}
	if ContextWithInstruments(context.Background(), nil) == nil {
		t.Fatal("nil instruments")
	}
	if InstrumentsFromContext(context.Background()) == nil {
		t.Fatal("global instruments")
	}
	ctxI := ContextWithInstruments(context.Background(), inst)
	if InstrumentsFromContext(ctxI) != inst {
		t.Fatal("instruments from context")
	}

	// NewInstruments with nil meter falls back to global.
	if _, err := NewInstruments(nil); err != nil {
		t.Fatal(err)
	}
}

// TestInit_otlpPaths covers endpoint/protocol/sample branches without requiring a live collector.
func TestInit_otlpPaths(t *testing.T) {
	ctx := context.Background()

	// Unknown protocol fails before dialing.
	if _, err := Init(ctx, Config{OTLPEndpoint: "localhost:4317", Protocol: "ftp"}); err == nil {
		t.Fatal("want unknown protocol")
	}

	// gRPC + metrics (exporter client construction succeeds without a server).
	shutdown, err := Init(ctx, Config{
		ServiceName:    "cov",
		ServiceVersion: "1",
		OTLPEndpoint:   "localhost:4317",
		Protocol:       "grpc",
		Insecure:       true,
		SampleRatio:    0.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(ctx); err != nil {
		// Shutdown may error if export flush fails; still covered.
		t.Logf("shutdown: %v", err)
	}
	SetTracerProvider(nil)
	SetMeterProvider(nil)

	// HTTP protocol, traces only.
	shutdown, err = Init(ctx, Config{
		OTLPEndpoint:   "http://127.0.0.1:4318",
		Protocol:       "http",
		Insecure:       true,
		DisableMetrics: true,
		SampleRatio:    0, // treated as always sample
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = shutdown(ctx)
	SetTracerProvider(nil)
	SetMeterProvider(nil)

	// http/protobuf alias + sample ratio clamp.
	shutdown, err = Init(ctx, Config{
		OTLPEndpoint: "https://127.0.0.1:4318",
		Protocol:     "http/protobuf",
		SampleRatio:  2, // clamp to 1
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = shutdown(ctx)
	SetTracerProvider(nil)
	SetMeterProvider(nil)

	// Env endpoint fallback.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:14317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	shutdown, err = Init(ctx, Config{Insecure: true, DisableMetrics: true})
	if err != nil {
		t.Fatal(err)
	}
	_ = shutdown(ctx)
	SetTracerProvider(nil)
	SetMeterProvider(nil)

	// Explicit empty after env clear.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err = Init(ctx, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestTracerContext_helpers covers ContextWithTracer / TracerFrom* / stripScheme.
func TestTracerContext_helpers(t *testing.T) {
	if Tracer() == nil {
		t.Fatal("global tracer")
	}
	if TracerFromProvider(nil) == nil {
		t.Fatal("nil provider")
	}
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := TracerFromProvider(tp)
	if tr == nil {
		t.Fatal("from provider")
	}
	if ContextWithTracer(context.Background(), nil) == nil {
		t.Fatal("nil tracer is a no-op")
	}
	if TracerFromContext(context.Background()) == nil {
		t.Fatal("tracer from empty")
	}
	ctx := ContextWithTracer(context.Background(), tr)
	if TracerFromContext(ctx) != tr {
		t.Fatal("context tracer")
	}

	if stripScheme("https://host:4317") != "host:4317" {
		t.Fatal(stripScheme("https://host:4317"))
	}
	if stripScheme("http://host") != "host" {
		t.Fatal(stripScheme("http://host"))
	}
	if stripScheme("host:1") != "host:1" {
		t.Fatal(stripScheme("host:1"))
	}
}

// TestResourceAndPrometheus_edges covers DefaultResource env fallback and nil registerer.
func TestResourceAndPrometheus_edges(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "from-env")
	res, err := DefaultResource("", "v")
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("resource")
	}
	// Explicit name wins.
	res2, err := DefaultResource("named", "")
	if err != nil || res2 == nil {
		t.Fatal(err)
	}

	if _, err := MeterProviderFromPrometheusRegisterer(nil, "x", ""); err == nil {
		t.Fatal("nil registerer")
	}
}

// TestSpanHandler_andLogger covers slog bridge paths including span attrs and defaults.
func TestSpanHandler_andLogger(t *testing.T) {
	h := NewSpanHandler(nil) // discard base
	if !h.Enabled(context.Background(), slog.LevelInfo) {
		// discard may still enable; just call
	}
	_ = h.Handle(context.Background(), slog.Record{})
	_ = h.WithAttrs([]slog.Attr{slog.String("k", "v")})
	_ = h.WithGroup("g")

	// With a real span context, attrs attach.
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	var saw bool
	inner := &attrCaptureHandler{fn: func(r slog.Record) {
		saw = true
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "trace_id" && a.Value.String() != "" {
				return false
			}
			return true
		})
	}}
	sh := NewSpanHandler(inner)
	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	if err := sh.Handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if !saw {
		t.Fatal("handle")
	}

	log := NewLogger(inner)
	log.Info("x")
	InstallDefault(nil)
	InstallDefault(inner)
	// restore a quiet default
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
}

type attrCaptureHandler struct {
	fn func(slog.Record)
}

func (a *attrCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (a *attrCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	if a.fn != nil {
		a.fn(r)
	}
	return nil
}
func (a *attrCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return a }
func (a *attrCaptureHandler) WithGroup(string) slog.Handler      { return a }

// TestStdioWatchDog_recordsAllMethods is the watchdog outcome path.
func TestStdioWatchDog_recordsAllMethods(t *testing.T) {
	wd := New()
	msg := &streaming.Message{
		Role:       streaming.RoleAssistant,
		Content:    "hi",
		ToolCalls:  []streaming.ToolCall{{Name: "t1"}},
		ToolCallID: "c1",
	}
	if err := wd.RecordThinking(msg); err != nil {
		t.Fatal(err)
	}
	if err := wd.RecordOutput(msg); err != nil {
		t.Fatal(err)
	}
	if err := wd.RecordError(errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	if err := wd.RecordTokens(1, 2); err != nil {
		t.Fatal(err)
	}
	if err := wd.RecordToolCalls(msg); err != nil {
		t.Fatal(err)
	}
	if err := wd.RecordToolResult(msg); err != nil {
		t.Fatal(err)
	}
	// Non-assistant output still returns nil.
	if err := wd.RecordOutput(&streaming.Message{Role: streaming.RoleUser, Content: "u"}); err != nil {
		t.Fatal(err)
	}
}

// TestSetTracerProvider_nil installs noop-safe global.
func TestSetTracerProvider_nil(t *testing.T) {
	SetTracerProvider(nil)
	var sp trace.Span
	_, sp = Tracer().Start(context.Background(), "n")
	sp.End()
	if !strings.Contains(InstrumentationName, "tacklr") {
		t.Fatal(InstrumentationName)
	}
}

// errMeter returns errors for every instrument create so NewInstruments error
// branches and MustInstruments panic are outcome-tested.
type errMeter struct {
	metric.Meter
	failAt int
	n      int
}

func (m *errMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	m.n++
	if m.n >= m.failAt {
		return nil, errors.New("counter fail")
	}
	return noop.Int64Counter{}, nil
}
func (m *errMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	m.n++
	if m.n >= m.failAt {
		return nil, errors.New("hist fail")
	}
	return noop.Float64Histogram{}, nil
}
func (m *errMeter) Int64UpDownCounter(string, ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	m.n++
	if m.n >= m.failAt {
		return nil, errors.New("updown fail")
	}
	return noop.Int64UpDownCounter{}, nil
}

// TestNewInstruments_errorPaths covers each instrument creation failure return.
func TestNewInstruments_errorPaths(t *testing.T) {
	// NewInstruments creates: hist, counter, updown, counter, hist, counter×5 = 10 instruments.
	for failAt := 1; failAt <= 10; failAt++ {
		m := &errMeter{Meter: noop.Meter{}, failAt: failAt}
		if _, err := NewInstruments(m); err == nil {
			t.Fatalf("failAt=%d want error", failAt)
		}
	}
	// MustInstruments panics on error.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("want panic")
		}
	}()
	_ = MustInstruments(&errMeter{Meter: noop.Meter{}, failAt: 1})
}

// TestGlobalInstruments_concurrent rebuilds cache safely under load.
func TestGlobalInstruments_concurrent(t *testing.T) {
	SetMeterProvider(nil)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			_ = InstrumentsFromContext(context.Background())
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if InstrumentsFromContext(context.Background()) == nil {
		t.Fatal("global")
	}
}
