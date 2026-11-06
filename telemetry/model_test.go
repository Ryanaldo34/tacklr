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
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

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
	otel.SetTextMapPropagator(propagation.TraceContext{})
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

// histogram is the count and sum of the histogram name over the data
// points that carry every attribute of attrs.
func (o observed) histogram(t *testing.T, name string, attrs ...attribute.KeyValue) (count uint64, sum float64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := o.metrics.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch h := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, dp := range h.DataPoints {
					if hasAll(dp.Attributes, attrs) {
						count, sum = count+dp.Count, sum+dp.Sum
					}
				}
			case metricdata.Histogram[int64]:
				for _, dp := range h.DataPoints {
					if hasAll(dp.Attributes, attrs) {
						count, sum = count+dp.Count, sum+float64(dp.Sum)
					}
				}
			}
		}
	}
	return count, sum
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

// TestModelSpan_recordsTheRequestAndItsOutcome: a model request is a
// client span named chat and its model, with the provider from the API
// base URL, the phase, and the session and agent of its step; its window
// and sequence are on its gen_ai.request log. It ends with its outcome,
// the provider-reported usage and, on failure, error.type, the HTTP status
// and the provider's error code (cut to 64 bytes). The GenAI metrics count
// its duration and its tokens by type. A span ends once.
func TestModelSpan_recordsTheRequestAndItsOutcome(t *testing.T) {
	o := observe(t)
	code := strings.Repeat("c", 80)
	rateLimited := providerError(t, http.StatusTooManyRequests, code)
	step := telemetry.Step(t.Context(), nil, "agent-1", "s1")
	azure := telemetry.ContextWithModelIdentity(step, telemetry.NewModelIdentity("gpt-4o", "https://res.openai.azure.com/"))

	_, ok := telemetry.StartModelSpan(telemetry.ContextWithAfterTools(azure), telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{Messages: 3, ToolPairs: 1})
	ok.End(nil, telemetry.TokenUsage{Input: 10, Output: 5, Reasoning: 2, Cached: 4, CacheWrite: 3})
	ok.End(errors.New("ended twice"), telemetry.TokenUsage{})
	_, limited := telemetry.StartModelSpan(telemetry.ContextWithModelIdentity(step, telemetry.NewModelIdentity("gpt-4o", "https://api.openai.com/v1")), telemetry.ModelPhaseHandoff, 0, telemetry.WindowShape{})
	limited.End(rateLimited, telemetry.TokenUsage{})
	_, cancelled := telemetry.StartModelSpan(telemetry.ContextWithModelIdentity(step, telemetry.NewModelIdentity("llama", "http://localhost:11434")), telemetry.ModelPhaseCompress, 2, telemetry.WindowShape{})
	cancelled.End(context.Canceled, telemetry.TokenUsage{})
	_, bare := telemetry.StartModelSpan(t.Context(), telemetry.ModelPhaseCompress, 0, telemetry.WindowShape{})
	bare.End(nil, telemetry.TokenUsage{})

	turn := o.span(t, "chat gpt-4o", attribute.String(telemetry.AttrModelPhase, telemetry.ModelPhaseTurn))
	if want := []attribute.KeyValue{
		attribute.String(telemetry.AttrGenAIOperationName, telemetry.OperationChat),
		attribute.String(telemetry.AttrGenAIProviderName, telemetry.GenAIProviderAzure),
		attribute.String(telemetry.AttrGenAIRequestModel, "gpt-4o"),
		attribute.String(telemetry.AttrGenAIConversationID, "s1"),
		attribute.String(telemetry.AttrGenAIAgentName, "agent-1"),
		attribute.String(telemetry.AttrOutcome, telemetry.OutcomeOK),
		attribute.Int(telemetry.AttrGenAIInputTokens, 10),
		attribute.Int(telemetry.AttrGenAIOutputTokens, 5),
		attribute.Int(telemetry.AttrGenAICachedTokens, 4),
		attribute.Int(telemetry.AttrGenAICacheWriteTokens, 3),
	}; !hasAll(attribute.NewSet(turn.Attributes()...), want) || turn.SpanKind() != trace.SpanKindClient {
		t.Fatalf("chat span = %v (%v), want %v as a client span", turn.Attributes(), turn.SpanKind(), want)
	}
	failed := o.span(t, "chat gpt-4o",
		attribute.String(telemetry.AttrGenAIProviderName, telemetry.GenAIProviderOpenAI),
		attribute.String(telemetry.AttrOutcome, telemetry.OutcomeError),
		attribute.String(telemetry.AttrErrorType, telemetry.ErrorClassProvider4xx),
		attribute.Int(telemetry.AttrHTTPStatus, http.StatusTooManyRequests),
		attribute.String(telemetry.AttrErrorCode, code[:64]))
	if failed.Status().Code != codes.Error {
		t.Fatalf("rate-limited span status = %v, want an error", failed.Status())
	}
	o.span(t, "chat llama", attribute.String(telemetry.AttrGenAIProviderName, telemetry.GenAIProviderUnknown), attribute.String(telemetry.AttrOutcome, telemetry.OutcomeCancelled))
	o.span(t, "chat", attribute.String(telemetry.AttrGenAIProviderName, telemetry.GenAIProviderUnknown))
	if n, _ := o.histogram(t, telemetry.MetricModelDuration); n != 4 {
		t.Fatalf("operation duration count = %d, want each of the 4 requests once", n)
	}
	agent := attribute.String(telemetry.LabelAgent, "agent-1")
	var tokens []float64
	for _, kind := range []string{"input", "output", "reasoning"} {
		_, sum := o.histogram(t, telemetry.MetricTokenUsage, agent, attribute.String(telemetry.AttrGenAITokenType, kind))
		tokens = append(tokens, sum)
	}
	if tokens[0] != 10 || tokens[1] != 5 || tokens[2] != 2 {
		t.Fatalf("input, output, reasoning tokens = %v, want 10, 5, 2", tokens)
	}
	if events := strings.Join(o.events.names(), " "); strings.Count(events, telemetry.EventModelRequest+"/INFO") != 4 {
		t.Fatalf("events = %v, want a %s log for each request", events, telemetry.EventModelRequest)
	}
}

// TestClassifyErrorClass_bucketsEachFailure: each failure falls in one
// closed error class, by its sentinel, its HTTP status, then its text.
func TestClassifyErrorClass_bucketsEachFailure(t *testing.T) {
	t.Parallel()
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

// TestSpans_recordToolHandoffAndPlanOutcomes: a tool call is an
// execute_tool span of its name, call id and type, with the session and
// agent of its step: it ends ok, as an error (whose text is on its
// tool.failed log), or parked (yield). A handoff span ends with its
// outcome; a plan install is a plan.installed log. The tool and handoff
// metrics count each under the agent; a checkpoint save, a FUSE mount and
// a connection count too. Each span ends once, and an event needs a name.
func TestSpans_recordToolHandoffAndPlanOutcomes(t *testing.T) {
	o := observe(t)
	step := telemetry.Step(t.Context(), nil, "agent-1", "s1")
	global := t.Context()

	_, okTool := telemetry.StartToolSpan(step, telemetry.Tool{Name: "grep", CallID: "call-1", Type: telemetry.ToolTypeFunction})
	okTool.Finish("success", nil)
	_, failedTool := telemetry.StartToolSpan(step, telemetry.Tool{Name: "read", Namespace: "docs", CallID: "call-2", Type: telemetry.ToolTypeExtension})
	failedTool.Finish("error", errors.New("boom"))
	failedTool.Finish("success", nil)
	_, parked := telemetry.StartToolSpan(step, telemetry.Tool{Name: "deploy", CallID: "call-3", Type: telemetry.ToolTypeFunction})
	parked.Finish("interrupt", nil)
	_, okHandoff := telemetry.StartHandoffSpan(step, 2)
	okHandoff.End(telemetry.HandoffOutcomeOK, nil)
	_, failedHandoff := telemetry.StartHandoffSpan(step, 1)
	failedHandoff.End(telemetry.HandoffOutcomeError, errors.New("handoff"))
	failedHandoff.End(telemetry.HandoffOutcomeOK, nil)
	telemetry.PlanInstalled(step, 3, 2, nil)
	telemetry.PlanInstalled(step, 3, 2, errors.New("install"))
	telemetry.RecordCheckpointAttempt(global, nil)
	telemetry.RecordCheckpointAttempt(global, errors.New("save failed"))
	telemetry.Metrics().RecordFuseMount(global, telemetry.FuseMountOutcomeError)
	telemetry.Metrics().RecordConnection(global, "/acp", 1)
	telemetry.EmitEvent(global, "")

	session := attribute.String(telemetry.AttrGenAIConversationID, "s1")
	o.span(t, "execute_tool grep", session, attribute.String(telemetry.AttrGenAIToolCallID, "call-1"), attribute.String(telemetry.AttrOutcome, telemetry.OutcomeOK))
	if s := o.span(t, "execute_tool read", attribute.String(telemetry.AttrToolNS, "docs"), attribute.String(telemetry.AttrGenAIToolType, telemetry.ToolTypeExtension), attribute.String(telemetry.AttrOutcome, telemetry.OutcomeError)); s.Status().Code != codes.Error {
		t.Fatalf("failed tool span status = %v, want an error", s.Status())
	}
	o.span(t, "execute_tool deploy", attribute.String(telemetry.AttrOutcome, telemetry.OutcomeYield))
	o.span(t, telemetry.SpanHandoff, session, attribute.String(telemetry.AttrOutcome, telemetry.HandoffOutcomeOK))
	if s := o.span(t, telemetry.SpanHandoff, attribute.String(telemetry.AttrOutcome, telemetry.HandoffOutcomeError)); s.Status().Code != codes.Error {
		t.Fatalf("failed handoff span status = %v, want an error", s.Status())
	}
	agent := attribute.String(telemetry.LabelAgent, "agent-1")
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
		{telemetry.MetricFuseMount, []attribute.KeyValue{attribute.String(telemetry.LabelOutcome, telemetry.FuseMountOutcomeError)}, 1},
		{telemetry.MetricConnections, []attribute.KeyValue{attribute.String(telemetry.LabelProtocol, "/acp")}, 1},
	} {
		if got := o.count(t, tc.name, tc.attrs...); got != tc.want {
			t.Errorf("%s %v = %d, want %d", tc.name, tc.attrs, got, tc.want)
		}
	}
	events := strings.Join(o.events.names(), " ")
	for _, want := range []string{telemetry.EventToolFailed + "/WARN", telemetry.EventPlanInstalled + "/INFO", telemetry.EventPlanInstalled + "/ERROR", telemetry.SpanHandoff + "/INFO"} {
		if !strings.Contains(events, want) {
			t.Errorf("events = %v, want %s", events, want)
		}
	}
}

// TestTurn_isASpanOfItsParentsTraceThatLinksWhatResumedIt: a turn's span
// is invoke_agent of its agent, a child of the span its carrier holds,
// with the session, its parent session, its kind and runtime, and a link
// to the span that resumed it. It ends ok, cancelled, or as an error whose
// text is on its turn.ended log; the turn metrics count it while it runs
// and by outcome once it ended. A context with no span has no carrier,
// and an empty carrier changes no context and links nothing.
func TestTurn_isASpanOfItsParentsTraceThatLinksWhatResumedIt(t *testing.T) {
	o := observe(t)
	parent, end := telemetry.Tracer().Start(t.Context(), "session/prompt")
	resumer, endResumer := telemetry.Tracer().Start(t.Context(), "session/request_permission")
	carrier, link := telemetry.Inject(parent), telemetry.Inject(resumer)
	end.End()
	endResumer.End()
	turn := telemetry.Turn{Agent: "researcher", Session: "child", Parent: "s1", Kind: "resume", Runtime: telemetry.RuntimeDurableObject}

	ctx, ok := telemetry.StartTurn(carrier.Context(context.Background()), turn, link, nil)
	telemetry.TurnStarted(ctx, 0, 1)
	ok.End(telemetry.OutcomeOK, nil)
	_, cancelled := telemetry.StartTurn(context.Background(), turn)
	cancelled.End(telemetry.OutcomeCancelled, context.Canceled)
	_, failed := telemetry.StartTurn(context.Background(), turn)
	failed.End(telemetry.OutcomeError, errors.New("model down"))
	none := telemetry.Inject(t.Context())
	_, linked := telemetry.Carrier(nil).Link()

	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range o.spans.Ended() {
		if s.Name() == "invoke_agent researcher" {
			for _, kv := range s.Attributes() {
				if kv.Key == telemetry.AttrOutcome {
					spans[kv.Value.AsString()] = s
				}
			}
		}
	}
	resumed := spans[telemetry.OutcomeOK]
	if resumed == nil || resumed.Parent().SpanID() != trace.SpanContextFromContext(parent).SpanID() || len(resumed.Links()) != 1 ||
		resumed.Links()[0].SpanContext.SpanID() != trace.SpanContextFromContext(resumer).SpanID() {
		t.Fatalf("resumed turn = %v, want a child of the prompt's span that links the resumer's", resumed)
	}
	if !hasAll(attribute.NewSet(resumed.Attributes()...), []attribute.KeyValue{
		attribute.String(telemetry.AttrGenAIOperationName, telemetry.OperationInvokeAgent),
		attribute.String(telemetry.AttrGenAIConversationID, "child"),
		attribute.String(telemetry.AttrParentSession, "s1"),
		attribute.String(telemetry.AttrRuntime, telemetry.RuntimeDurableObject),
	}) {
		t.Fatalf("turn attributes = %v", resumed.Attributes())
	}
	for _, outcome := range []string{telemetry.OutcomeCancelled, telemetry.OutcomeError} {
		if s := spans[outcome]; s == nil || s.Status().Code != codes.Error {
			t.Fatalf("%s turn = %v, want an error status", outcome, s)
		}
	}
	agent := attribute.String(telemetry.LabelAgent, "researcher")
	if active, total := o.count(t, telemetry.MetricTurnActive, agent), o.count(t, telemetry.MetricTurnTotal, agent); active != 0 || total != 3 {
		t.Fatalf("active = %d, total = %d; want none running and the 3 turns counted", active, total)
	}
	if events := strings.Join(o.events.names(), " "); !strings.Contains(events, telemetry.EventTurnStarted+"/INFO") || !strings.Contains(events, telemetry.EventTurnEnded+"/ERROR") {
		t.Fatalf("events = %v, want the turn's start and its failure", events)
	}
	if none != nil || linked {
		t.Fatalf("carrier of no span = %v, link of none = %v; want neither", none, linked)
	}
}

// TestRPC_spansAProtocolRequestFromItsRequestToItsAnswer: a request the
// server answers is a server span of its method, with its session; its
// error response sets the JSON-RPC code and error.type, and its duration
// is counted. A request the server sends the client is a client span that
// ends with the answer's stop reason. A span ends once.
func TestRPC_spansAProtocolRequestFromItsRequestToItsAnswer(t *testing.T) {
	o := observe(t)

	ctx, prompt := telemetry.StartRPC(t.Context(), trace.SpanKindServer, "session/prompt", "s1")
	_, ask := telemetry.StartRPC(prompt.Context(), trace.SpanKindClient, "session/request_permission", "s1", attribute.String(telemetry.AttrInterruptKind, "tool_permission"))
	ask.End(0, "end_turn")
	prompt.End(-32603, "")
	prompt.End(0, "end_turn")

	served := o.span(t, "session/prompt", attribute.String(telemetry.AttrGenAIConversationID, "s1"), attribute.Int(telemetry.AttrJSONRPCError, -32603))
	asked := o.span(t, "session/request_permission", attribute.String(telemetry.AttrStopReason, "end_turn"), attribute.String(telemetry.AttrInterruptKind, "tool_permission"))
	if served.SpanKind() != trace.SpanKindServer || served.Status().Code != codes.Error || asked.SpanKind() != trace.SpanKindClient ||
		asked.Parent().SpanID() != trace.SpanContextFromContext(ctx).SpanID() {
		t.Fatalf("served = %v %v, asked = %v under %v; want a failed server span and its client span", served.SpanKind(), served.Status(), asked.SpanKind(), asked.Parent())
	}
	if n, _ := o.histogram(t, telemetry.MetricRPCCallDuration, attribute.String(telemetry.AttrRPCMethod, "session/prompt"), attribute.String(telemetry.LabelErrorType, "-32603")); n != 1 {
		t.Fatalf("request duration count = %d, want the server request once", n)
	}
}

// TestTransport_carriesTheTraceToTheSystemItReaches: a request through the
// transport carries its context's trace, which the server it reaches
// continues; a request with no span carries none.
func TestTransport_carriesTheTraceToTheSystemItReaches(t *testing.T) {
	observe(t)
	got := make(chan trace.SpanContext, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got <- trace.SpanContextFromContext(telemetry.FromHeaders(r.Context(), r.Header))
	}))
	t.Cleanup(srv.Close)
	client := &http.Client{Transport: telemetry.Transport(nil)}
	send := func(ctx context.Context) trace.SpanContext {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return <-got
	}
	ctx, span := telemetry.Tracer().Start(t.Context(), "execute_tool search")
	defer span.End()

	traced, untraced := send(ctx), send(t.Context())

	if traced.TraceID() != span.SpanContext().TraceID() || untraced.IsValid() {
		t.Fatalf("traced = %v, untraced = %v; want the caller's trace, then none", traced.TraceID(), untraced)
	}
}
