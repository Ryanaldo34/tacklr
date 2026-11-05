package telemetry_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// observed is what the process-wide providers recorded during a test.
type observed struct {
	spans   *tracetest.SpanRecorder
	metrics *sdkmetric.ManualReader
	events  *eventRecorder
}

// observe installs recording trace, metric and log providers as the
// process-wide ones, and puts back no-op providers when the test ends.
func observe(t *testing.T) observed {
	t.Helper()
	o := observed{spans: tracetest.NewSpanRecorder(), metrics: sdkmetric.NewManualReader(), events: &eventRecorder{}}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(o.spans))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(o.metrics))
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(o.events)))
	telemetry.SetTracerProvider(tp)
	telemetry.SetMeterProvider(mp)
	otel.SetLoggerProvider(lp)
	t.Cleanup(func() {
		telemetry.SetTracerProvider(nil)
		telemetry.SetMeterProvider(nil)
		otel.SetLoggerProvider(lognoop.NewLoggerProvider())
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
		_ = lp.Shutdown(context.Background())
	})
	return o
}

// eventRecorder is an in-memory log exporter: it keeps each event name
// with its severity.
type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *eventRecorder) Export(_ context.Context, records []sdklog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range records {
		r.events = append(r.events, rec.EventName()+"/"+rec.Severity().String())
	}
	return nil
}

func (r *eventRecorder) Shutdown(context.Context) error   { return nil }
func (r *eventRecorder) ForceFlush(context.Context) error { return nil }

func (r *eventRecorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// count is the sum of the int64 counter name over the data points that
// carry every attribute of attrs.
func (o observed) count(t *testing.T, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := o.metrics.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != name || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if hasAll(dp.Attributes, attrs) {
					n += dp.Value
				}
			}
		}
	}
	return n
}

func hasAll(set attribute.Set, attrs []attribute.KeyValue) bool {
	for _, kv := range attrs {
		if v, ok := set.Value(kv.Key); !ok || v != kv.Value {
			return false
		}
	}
	return true
}

// span is the one ended span named name whose attributes include attrs.
func (o observed) span(t *testing.T, name string, attrs ...attribute.KeyValue) sdktrace.ReadOnlySpan {
	t.Helper()
	var found []sdktrace.ReadOnlySpan
	for _, s := range o.spans.Ended() {
		if s.Name() == name && hasAll(attribute.NewSet(s.Attributes()...), attrs) {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d %s spans with %v, want one", len(found), name, attrs)
	}
	return found[0]
}

// providerError is the error the real OpenAI client gives for a request
// the provider answers with status and an error code.
func providerError(t *testing.T, status int, code string) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"slow down","code":"` + code + `"}}`))
	}))
	t.Cleanup(srv.Close)
	model := openai.New(srv.Client()).WithApiKey("test-key").WithModel("gpt-4o").WithURL(srv.URL)
	ch, err := model.Invoke(t.Context(), tacklr.ModelRequest{Messages: []*message.Message{{Role: message.RoleUser, Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	for chunk := range ch {
		if chunk.Error != nil {
			return chunk.Error
		}
	}
	t.Fatal("the provider failure gave no error chunk")
	return nil
}

// TestModelSpan_recordsTheRequestAndItsOutcome: a model span names the
// provider from the API base URL, the model, the phase and the window. It
// ends with the outcome, the error class, the provider's HTTP status and
// error code (cut to 64 bytes), and the token usage; the model and token
// metrics count it under the agent. A request after tools emits the
// model.after_tools event. A span ends once: a second End keeps the first
// outcome.
func TestModelSpan_recordsTheRequestAndItsOutcome(t *testing.T) {
	o := observe(t)
	code := strings.Repeat("c", 80)
	rateLimited := providerError(t, http.StatusTooManyRequests, code)
	base := telemetry.ContextWithInstruments(telemetry.ContextWithAgentID(t.Context(), "agent-1"), telemetry.MustInstruments(telemetry.MeterFromProvider(nil)))
	azure := telemetry.ContextWithModelIdentity(base, telemetry.NewModelIdentity("gpt-4o", "https://res.openai.azure.com/"))

	_, ok := telemetry.StartModelSpan(telemetry.ContextWithAfterTools(azure), telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{Messages: 3, ToolPairs: 1})
	ok.End(nil, telemetry.TokenUsage{Input: 10, Output: 5, Reasoning: 2, Cached: 4, CacheWrite: 3})
	ok.End(errors.New("ended twice"), telemetry.TokenUsage{})
	_, limited := telemetry.StartModelSpan(telemetry.ContextWithModelIdentity(base, telemetry.NewModelIdentity("gpt-4o", "https://api.openai.com/v1")), telemetry.ModelPhaseHandoff, 0, telemetry.WindowShape{})
	limited.End(rateLimited, telemetry.TokenUsage{})
	_, cancelled := telemetry.StartModelSpan(telemetry.ContextWithModelIdentity(base, telemetry.NewModelIdentity("llama", "http://localhost:11434")), telemetry.ModelPhaseCompress, 2, telemetry.WindowShape{})
	cancelled.End(context.Canceled, telemetry.TokenUsage{})
	_, bare := telemetry.StartModelSpan(base, telemetry.ModelPhaseCompress, 0, telemetry.WindowShape{})
	bare.End(nil, telemetry.TokenUsage{})

	turn := o.span(t, telemetry.SpanModel, attribute.String(telemetry.AttrModelPhase, telemetry.ModelPhaseTurn))
	if want := []attribute.KeyValue{
		attribute.String(telemetry.AttrGenAIProviderName, telemetry.GenAIProviderAzure),
		attribute.String(telemetry.AttrGenAIRequestModel, "gpt-4o"),
		attribute.Int(telemetry.AttrModelSeq, 1),
		attribute.Int(telemetry.AttrContextMsgs, 3),
		attribute.Bool(telemetry.AttrAfterTools, true),
		attribute.String(telemetry.AttrOutcome, telemetry.OutcomeOK),
		attribute.Int(telemetry.AttrGenAIInputTokens, 10),
		attribute.Int(telemetry.AttrGenAIOutputTokens, 5),
		attribute.Int(telemetry.AttrGenAICachedTokens, 4),
		attribute.Int(telemetry.AttrGenAICacheWriteTokens, 3),
	}; !hasAll(attribute.NewSet(turn.Attributes()...), want) || turn.Status().Code != codes.Ok {
		t.Fatalf("turn span = %v %v, want %v and ok", turn.Attributes(), turn.Status(), want)
	}
	failed := o.span(t, telemetry.SpanModel,
		attribute.String(telemetry.AttrGenAIProviderName, telemetry.GenAIProviderOpenAI),
		attribute.String(telemetry.AttrOutcome, telemetry.OutcomeError),
		attribute.String(telemetry.AttrErrorClass, telemetry.ErrorClassProvider4xx),
		attribute.Int(telemetry.AttrHTTPStatus, http.StatusTooManyRequests),
		attribute.String(telemetry.AttrErrorCode, code[:64]))
	if failed.Status().Code != codes.Error {
		t.Fatalf("rate-limited span status = %v, want an error", failed.Status())
	}
	o.span(t, telemetry.SpanModel,
		attribute.String(telemetry.AttrGenAIProviderName, telemetry.GenAIProviderUnknown),
		attribute.String(telemetry.AttrGenAIOperationName, telemetry.GenAIOperationChat),
		attribute.String(telemetry.AttrOutcome, telemetry.OutcomeCancelled))
	if got := o.count(t, telemetry.MetricModelTotal, attribute.String(telemetry.LabelAgentID, "agent-1")); got != 4 {
		t.Fatalf("model total = %d, want each of the 4 requests counted once", got)
	}
	tokens := []int64{o.count(t, telemetry.MetricTokensInput), o.count(t, telemetry.MetricTokensOutput), o.count(t, telemetry.MetricTokensReasoning)}
	if tokens[0] != 10 || tokens[1] != 5 || tokens[2] != 2 {
		t.Fatalf("input, output, reasoning tokens = %v, want 10, 5, 2", tokens)
	}
	if events := o.events.names(); !strings.Contains(strings.Join(events, " "), telemetry.EventModelAfterTools+"/INFO") {
		t.Fatalf("events = %v, want %s", events, telemetry.EventModelAfterTools)
	}
}

// TestClassifyErrorClass_bucketsEachFailure: each failure falls in one
// closed error class, by its sentinel, its HTTP status, then its text.
func TestClassifyErrorClass_bucketsEachFailure(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		want   string
	}{
		{nil, 0, telemetry.ErrorClassOK},
		{context.Canceled, 0, telemetry.ErrorClassCancelled},
		{context.DeadlineExceeded, 0, telemetry.ErrorClassTimeout},
		{errors.New("slow down"), http.StatusTooManyRequests, telemetry.ErrorClassProvider4xx},
		{errors.New("down"), http.StatusServiceUnavailable, telemetry.ErrorClassProvider5xx},
		{errors.New("context canceled by peer"), 0, telemetry.ErrorClassCancelled},
		{errors.New("upstream timeout"), 0, telemetry.ErrorClassTimeout},
		{errors.New("max_output_tokens exceeded"), 0, telemetry.ErrorClassMaxTokens},
		{errors.New("weird"), 0, telemetry.ErrorClassOther},
	} {
		if got := telemetry.ClassifyErrorClass(tc.err, tc.status); got != tc.want {
			t.Errorf("ClassifyErrorClass(%v, %d) = %s, want %s", tc.err, tc.status, got, tc.want)
		}
	}
}

// TestSpans_recordToolPlanAndHandoffOutcomes: tool, plan-install and
// handoff spans end with their outcome and status; the tool and handoff
// metrics count each under the turn's agent; a checkpoint save counts by
// outcome. Instruments come from the context, else from the process-wide
// meter. Each span ends once, and only a named event is emitted.
func TestSpans_recordToolPlanAndHandoffOutcomes(t *testing.T) {
	o := observe(t)
	turn := telemetry.BindTurnContext(t.Context(), "agent-1", "")
	global := telemetry.ContextWithInstruments(t.Context(), nil)

	_, okTool := telemetry.StartToolSpan(turn, "grep", "vfs")
	okTool.Finish("success", nil)
	_, failedTool := telemetry.StartToolSpan(turn, "read", "vfs")
	failedTool.Finish("error", errors.New("boom"))
	failedTool.Finish("success", nil)
	_, okPlan := telemetry.StartPlanInstallSpan(turn, "s1")
	okPlan.End(nil)
	_, failedPlan := telemetry.StartPlanInstallSpan(turn, "s2")
	failedPlan.End(errors.New("install"))
	failedPlan.End(nil)
	_, okHandoff := telemetry.StartHandoffSpan(telemetry.ContextWithSessionID(turn, "s1"), 2)
	okHandoff.End(telemetry.HandoffOutcomeOK, nil)
	_, failedHandoff := telemetry.StartHandoffSpan(turn, 1)
	failedHandoff.End(telemetry.HandoffOutcomeError, errors.New("handoff"))
	failedHandoff.End(telemetry.HandoffOutcomeOK, nil)
	telemetry.RecordCheckpointAttempt(global, nil)
	telemetry.RecordCheckpointAttempt(global, errors.New("save failed"))
	telemetry.EmitEvent(global, "")
	telemetry.EmitEventSeverity(global, telemetry.EventProviderFailed, log.SeverityError)

	o.span(t, telemetry.SpanTool, attribute.String(telemetry.AttrToolName, "grep"), attribute.String(telemetry.AttrOutcome, telemetry.OutcomeOK))
	if s := o.span(t, telemetry.SpanTool, attribute.String(telemetry.AttrToolName, "read"), attribute.String(telemetry.AttrOutcome, telemetry.OutcomeError)); s.Status().Code != codes.Error {
		t.Fatalf("failed tool span status = %v, want an error", s.Status())
	}
	o.span(t, telemetry.SpanPlanInstall, attribute.String(telemetry.AttrSessionID, "s1"), attribute.String(telemetry.AttrOutcome, telemetry.OutcomeOK))
	o.span(t, telemetry.SpanPlanInstall, attribute.String(telemetry.AttrSessionID, "s2"), attribute.String(telemetry.AttrOutcome, telemetry.OutcomeError))
	o.span(t, telemetry.SpanContextHandoff, attribute.String(telemetry.AttrSessionID, "s1"), attribute.String(telemetry.AttrOutcome, telemetry.HandoffOutcomeOK))
	if s := o.span(t, telemetry.SpanContextHandoff, attribute.Int(telemetry.AttrOpenTodos, 1), attribute.String(telemetry.AttrOutcome, telemetry.HandoffOutcomeError)); s.Status().Code != codes.Error {
		t.Fatalf("failed handoff span status = %v, want an error", s.Status())
	}
	agent := attribute.String(telemetry.LabelAgentID, "agent-1")
	for _, tc := range []struct {
		name  string
		attrs []attribute.KeyValue
		want  int64
	}{
		{telemetry.MetricToolCalls, []attribute.KeyValue{agent, attribute.String(telemetry.LabelStatus, "success")}, 1},
		{telemetry.MetricToolCalls, []attribute.KeyValue{agent, attribute.String(telemetry.LabelStatus, "error")}, 1},
		{telemetry.MetricHandoffTotal, []attribute.KeyValue{agent, attribute.String(telemetry.LabelOutcome, telemetry.HandoffOutcomeOK)}, 1},
		{telemetry.MetricHandoffTotal, []attribute.KeyValue{agent, attribute.String(telemetry.LabelOutcome, telemetry.HandoffOutcomeError)}, 1},
		{telemetry.MetricCheckpointSave, []attribute.KeyValue{attribute.String(telemetry.LabelOutcome, telemetry.OutcomeOK)}, 1},
		{telemetry.MetricCheckpointSave, []attribute.KeyValue{attribute.String(telemetry.LabelOutcome, telemetry.OutcomeError)}, 1},
	} {
		if got := o.count(t, tc.name, tc.attrs...); got != tc.want {
			t.Errorf("%s %v = %d, want %d", tc.name, tc.attrs, got, tc.want)
		}
	}
	if events := o.events.names(); len(events) != 1 || events[0] != telemetry.EventProviderFailed+"/ERROR" {
		t.Fatalf("events = %v, want only the named provider failure at error severity", events)
	}
}
