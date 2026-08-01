package telemetry

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestInit_emptyEndpoint_noop(t *testing.T) {
	shutdown, err := Init(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if shutdown == nil {
		t.Fatal("shutdown nil")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTracer_recordsTurnLifecycleSpans(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	SetTracerProviderForTest(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		SetTracerProviderForTest(nil)
	})

	ctx, span := Tracer().Start(context.Background(), SpanTurn,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaRegistry),
			attribute.String(AttrThreadID, "t1"),
			attribute.String(AttrTurnKind, "prompt"),
		),
	)
	span.AddEvent(EventPromptReceived, trace.WithAttributes(
		attribute.Int(EventAttrPromptLen, 2),
	))

	_, tool := Tracer().Start(ctx, SpanTool,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaHarness),
			attribute.String(AttrToolName, "create_plan"),
		),
	)
	tool.SetAttributes(
		attribute.String(AttrToolStatus, "success"),
		attribute.String(AttrOutcome, OutcomeOK),
	)
	tool.End()

	_, install := Tracer().Start(ctx, SpanPlanInstall,
		trace.WithAttributes(attribute.String(AttrArea, AreaContext)),
	)
	install.SetAttributes(attribute.String(AttrOutcome, OutcomeOK))
	install.End()

	_, handoff := Tracer().Start(ctx, SpanContextHandoff,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaModelTasks),
			attribute.Int(AttrOpenTodos, 2),
		),
	)
	handoff.SetAttributes(attribute.String(AttrOutcome, OutcomeOK))
	handoff.End()

	span.SetAttributes(attribute.String(AttrOutcome, OutcomeOK))
	span.AddEvent(EventTurnEnded, trace.WithAttributes(
		attribute.String(EventAttrOutcome, OutcomeOK),
	))
	span.End()

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	spans := exp.GetSpans()
	want := map[string]bool{
		SpanTurn:           false,
		SpanTool:           false,
		SpanPlanInstall:    false,
		SpanContextHandoff: false,
	}
	var sawPrompt bool
	for _, s := range spans {
		if _, ok := want[s.Name]; ok {
			want[s.Name] = true
		}
		if s.Name == SpanTurn {
			for _, ev := range s.Events {
				if ev.Name == EventPromptReceived {
					sawPrompt = true
				}
				// Lifecycle only — no mirrored slog noise.
				if ev.Name == "log" {
					t.Fatalf("unexpected log event on turn span")
				}
			}
		}
	}
	for name, ok := range want {
		if !ok {
			t.Fatalf("missing span %s among %+v", name, spans)
		}
	}
	if !sawPrompt {
		t.Fatal("missing prompt.received event")
	}
}

func TestSpanHandler_correlatesTraceIDsWithoutSpanEvents(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	SetTracerProviderForTest(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		SetTracerProviderForTest(nil)
	})

	var buf captureHandler
	log := slog.New(NewSpanHandler(&buf))
	ctx, span := Tracer().Start(context.Background(), SpanTurn)
	log.InfoContext(ctx, "hello turn", "area", "test")
	span.End()
	_ = tp.ForceFlush(context.Background())

	if !buf.got {
		t.Fatal("inner handler not called")
	}
	if buf.traceID == "" || buf.spanID == "" {
		t.Fatalf("expected trace_id/span_id on log, got trace=%q span=%q", buf.traceID, buf.spanID)
	}
	for _, s := range exp.GetSpans() {
		for _, ev := range s.Events {
			if ev.Name == "log" {
				t.Fatal("logs must not be mirrored as span events")
			}
		}
	}
}

type captureHandler struct {
	got     bool
	traceID string
	spanID  string
}

func (d *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (d *captureHandler) Handle(_ context.Context, r slog.Record) error {
	d.got = true
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "trace_id":
			d.traceID = a.Value.String()
		case "span_id":
			d.spanID = a.Value.String()
		}
		return true
	})
	return nil
}
func (d *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return d }
func (d *captureHandler) WithGroup(string) slog.Handler      { return d }
