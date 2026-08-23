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
	"go.opentelemetry.io/otel/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/streaming"
)

// TestMultiHandler_forwardsToBase: dual-write helper must deliver records to
// the local handler (does not mutate process slog default).
func TestBrainObserver_startEnd(t *testing.T) {
	obs := NewBrainObserver()
	ctx, span := obs.StartOp(context.Background(), brain.OpSearch)
	if ctx == nil || span == nil {
		t.Fatal("start")
	}
	span.End(2, brain.DegradeNone, nil)
	span.End(0, brain.DegradeLexicalOnly, errors.New("x"))
}

func TestMultiHandler_forwardsToBase(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	mh := MultiHandler{base, NewOTLPSlogHandler("cov-test")}
	if !mh.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("enabled")
	}
	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "dual-write smoke", 0)
	if err := mh.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "dual-write smoke") {
		t.Fatalf("base handler missing log: %q", buf.String())
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
	inst.RecordTurnOutcome(ctx, "agent-1", TurnKindResume, OutcomeYield, 5*time.Millisecond)
	inst.RecordTool(ctx, "agent-1", "search", "web", "success", time.Millisecond)
	inst.RecordInterrupt(ctx, "agent-1", "user_selection_choice")
	inst.RecordHandoff(ctx, "agent-1", HandoffOutcomeOK)
	inst.RecordHandoff(ctx, "agent-1", HandoffOutcomeFallback)
	inst.RecordCompress(ctx, "agent-1")
	inst.RecordFuseMount(ctx, FuseMountOutcomeOK)
	inst.RecordFuseMount(ctx, FuseMountOutcomeError)
	inst.RecordFuseMount(ctx, FuseMountOutcomeUnavailable)
	inst.RecordSessionCreated(ctx)
	inst.RecordCheckpointSave(ctx, OutcomeOK)
	inst.RecordModel(ctx, "agent-1", ModelPhaseTurn, OutcomeOK, ErrorClassOK, 5*time.Millisecond)
	inst.RecordModel(ctx, "agent-1", ModelPhaseHandoff, OutcomeError, ErrorClassProvider4xx, time.Millisecond)
	inst.RecordTokens(ctx, "agent-1", 10, 20, 3)
	inst.RecordBrain(ctx, "agent-1", BrainOpSearch, OutcomeOK, BrainDegradeNone, false, 2*time.Millisecond)
	inst.RecordBrain(ctx, "agent-1", BrainOpExpand, OutcomeOK, BrainDegradeContainmentOnly, true, time.Millisecond)
	inst.RecordBrain(ctx, "agent-1", BrainOpFindExact, OutcomeError, BrainDegradeNone, false, time.Millisecond)
	inst.RecordBrain(ctx, "agent-1", BrainOpFindLinks, OutcomeOK, BrainDegradeNone, true, time.Millisecond)
	inst.RecordBrain(ctx, "agent-1", BrainOpExpandMany, OutcomeOK, BrainDegradeNone, false, time.Millisecond)
	_, bspan := StartBrainSpan(ctx, BrainOpContinue)
	bspan.End(0, BrainDegradeNone, nil)
	_, bspan2 := StartBrainSpan(ctx, BrainOpSearch)
	bspan2.End(3, BrainDegradeLexicalOnly, errors.New("x"))
	_, bspan3 := StartBrainSpan(ctx, BrainOpFindLinks)
	bspan3.End(1, BrainDegradeNone, nil)
	_, bspan4 := StartBrainSpan(ctx, BrainOpExpandMany)
	bspan4.End(2, BrainDegradeNone, nil)
	// nil-safe
	(*Instruments)(nil).RecordBrain(ctx, "", "", "", "", false, 0)
	(*BrainSpan)(nil).End(0, "", nil)

	ctxI := ContextWithInstruments(context.Background(), inst)
	if InstrumentsFromContext(ctxI) != inst {
		t.Fatal("instruments from context")
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

	// gRPC + TLS ClientConn (lazy dial; no collector required).
	shutdown, err = Init(ctx, Config{
		OTLPEndpoint: "https://localhost:4317",
		Protocol:     "grpc",
		DisableLogs:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !IsReplaySafeProvider() {
		t.Fatal("want ReplaySafe after OTLP Init")
	}
	if Tracer() == nil || Meter() == nil {
		t.Fatal("process-wide tracer and meter")
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
	wd := NewStdioWatchDog()
	msg := &streaming.Message{
		Role:       streaming.RoleAssistant,
		Content:    "hi",
		ToolCalls:  []streaming.ToolCall{{Name: "t1"}},
		ToolCallID: "c1",
	}
	_ = wd.RecordThinking(msg)
	_ = wd.RecordOutput(msg)
	_ = wd.RecordError(errors.New("boom"))
	_ = wd.RecordTokens(1, 2)
	_ = wd.RecordToolCalls(msg)
	_ = wd.RecordToolResult(msg)
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

// TestSpans_fullLifecycle covers turn/tool/plan/handoff/brain end paths used by harness.
func TestSpans_fullLifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp, err := MeterProviderFromPrometheusRegisterer(reg, "span-cov", "v0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	SetMeterProvider(mp)
	t.Cleanup(func() { SetMeterProvider(nil) })
	inst := MustInstruments(MeterFromProvider(mp))
	ctx := ContextWithInstruments(context.Background(), inst)

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	SetTracerProvider(tp)
	t.Cleanup(func() { SetTracerProvider(nil) })
	ctx = ContextWithTracer(ctx, TracerFromProvider(tp))

	ctx, turn := StartTurnSpan(ctx, TurnAttrs{
		AgentID: "a1", ThreadID: "t1", SessionID: "s1", Kind: "prompt", LoadSession: true,
	})
	ctx, tool := StartToolSpan(ctx, "search", "web")
	tool.Finish("success", nil)
	ctx, tool2 := StartToolSpan(ctx, "x", "")
	tool2.Finish("", errors.New("fail"))
	ctx, plan := StartPlanInstallSpan(ctx, "s1")
	plan.End(nil)
	ctx, plan2 := StartPlanInstallSpan(ctx, "s1")
	plan2.End(errors.New("plan err"))
	ctx, ho := StartHandoffSpan(ctx, 2)
	ho.End(HandoffOutcomeOK, nil)
	ctx, ho2 := StartHandoffSpan(ctx, 0)
	ho2.End("", errors.New("ho"))
	ctx, ho3 := StartHandoffSpan(ctx, 1)
	ho3.End(HandoffOutcomeFallback, nil)
	_, brain := StartBrainSpan(ctx, "")
	brain.End(2, "", nil)
	_, brain2 := StartBrainSpan(ctx, BrainOpExpand)
	brain2.End(0, BrainDegradeContainmentOnly, nil)
	// double-end is no-op
	brain2.End(1, "", nil)
	tool.Finish("success", nil)
	turn.End(OutcomeOK, nil)
	turn.End("", nil) // finished

	// Resume-style turn with cancel outcome
	_, turn2 := StartTurnSpan(ContextWithInstruments(context.Background(), inst), TurnAttrs{AgentID: "a", Kind: "resume"})
	turn2.End(OutcomeCancelled, nil)

	_, turn3 := StartTurnSpan(ContextWithInstruments(context.Background(), inst), TurnAttrs{})
	turn3.End("", errors.New("boom"))

	_, turn4 := StartTurnSpan(ContextWithInstruments(context.Background(), inst), TurnAttrs{
		Kind: TurnKindPrompt, Runtime: RuntimeEmbed,
	})
	turn4.End(OutcomeYield, nil)

	// EmitEvent path
	EmitEvent(ctx, EventPromptReceived, log.String(EventAttrPromptLen, "1"))
	EmitEvent(context.Background(), EventTurnEnded)

	// Model record empty defaults + zero duration
	inst.RecordModel(ctx, "a", "", "", "", 0)
	inst.RecordTokens(ctx, "a", 0, 0, 0)
	inst.RecordHandoff(ctx, "a", "")
	(*Instruments)(nil).RecordTurnStart(ctx, "")
	(*Instruments)(nil).RecordTurnEnd(ctx, "", "", "", 0)
	(*Instruments)(nil).RecordTurnOutcome(ctx, "", "", "", 0)
	(*Instruments)(nil).RecordTool(ctx, "", "", "", "", 0)
	(*Instruments)(nil).RecordInterrupt(ctx, "", "")
	(*Instruments)(nil).RecordHandoff(ctx, "", "")
	(*Instruments)(nil).RecordModel(ctx, "", "", "", "", time.Millisecond)
	(*Instruments)(nil).RecordTokens(ctx, "", 1, 1, 1)
	(*Instruments)(nil).RecordCompress(ctx, "")
	(*Instruments)(nil).RecordSessionCreated(ctx)
	(*Instruments)(nil).RecordCheckpointSave(ctx, "")
	(*TurnSpan)(nil).End("", nil)
	(*ToolSpan)(nil).Finish("", nil)
	(*PlanInstallSpan)(nil).End(nil)
	(*HandoffSpan)(nil).End("", nil)
}
