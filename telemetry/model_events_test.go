package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestModelSpan_lifecycleAndAttrs covers Start/End model spans with static keys,
// cancel outcome, usage tokens, and GenAI identity from context.
func TestModelSpan_lifecycleAndAttrs(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	SetTracerProvider(tp)
	t.Cleanup(func() { SetTracerProvider(nil) })

	ctx := ContextWithModelIdentity(context.Background(), ModelIdentity{
		Provider:  GenAIProviderAzure,
		Model:     "gpt-test",
		Operation: GenAIOperationChat,
	})
	ctx = ContextWithAfterTools(ctx)
	if !AfterToolsFromContext(ctx) {
		t.Fatal("after tools")
	}
	if AfterToolsFromContext(context.Background()) {
		t.Fatal("empty ctx")
	}
	if ModelIdentityFromContext(ctx).Model != "gpt-test" {
		t.Fatal(ModelIdentityFromContext(ctx))
	}
	if ModelIdentityFromContext(context.TODO()).Model != "" {
		t.Fatal("empty identity")
	}

	ctx, span := StartModelSpan(ctx, ModelPhaseTurn, 3, WindowShape{Messages: 4, ToolPairs: 1})
	EndModelSpan(ctx, span, ModelPhaseTurn, nil, 0, "", TokenUsage{Input: 10, Output: 5, Reasoning: 2})

	ctx2, span2 := StartModelSpan(context.Background(), ModelPhaseHandoff, 0, WindowShape{})
	EndModelSpan(ctx2, span2, ModelPhaseHandoff, context.Canceled, 0, "", TokenUsage{})

	ctx3, span3 := StartModelSpan(context.Background(), ModelPhaseCompress, 1, WindowShape{})
	EndModelSpan(ctx3, span3, ModelPhaseCompress, errors.New("api error (status 400): bad"), 400, strings.Repeat("x", 80), TokenUsage{})

	EndModelSpan(context.Background(), nil, ModelPhaseTurn, nil, 0, "", TokenUsage{})

	ended := sr.Ended()
	if len(ended) < 3 {
		t.Fatalf("ended = %d", len(ended))
	}

	okSpan := ended[0]
	attrs := attrStrings(okSpan)
	for _, k := range []string{
		AttrModelPhase, AttrGenAIOperationName, AttrGenAIProviderName,
		AttrGenAIRequestModel, AttrAfterTools, AttrOutcome, AttrErrorClass,
		AttrGenAIInputTokens, AttrGenAIOutputTokens,
	} {
		if _, ok := attrs[k]; !ok {
			t.Fatalf("missing %s in %v", k, attrs)
		}
	}
	if attrs[AttrOutcome] != OutcomeOK || attrs[AttrErrorClass] != ErrorClassOK {
		t.Fatalf("%v", attrs)
	}
	if attrs[AttrAfterTools] != "true" {
		t.Fatalf("after_tools=%q", attrs[AttrAfterTools])
	}

	cAttrs := attrStrings(ended[1])
	if cAttrs[AttrOutcome] != OutcomeCancelled || cAttrs[AttrErrorClass] != ErrorClassCancelled {
		t.Fatalf("cancel attrs %v", cAttrs)
	}
	if ended[1].Status().Code != codes.Error {
		t.Fatalf("status %v", ended[1].Status())
	}

	eAttrs := attrStrings(ended[2])
	if eAttrs[AttrOutcome] != OutcomeError || eAttrs[AttrErrorClass] != ErrorClassProvider4xx {
		t.Fatalf("4xx attrs %v", eAttrs)
	}
	if code, ok := eAttrs[AttrErrorCode]; !ok || len(code) > 64 {
		t.Fatalf("error code %q", code)
	}
}

func attrStrings(st sdktrace.ReadOnlySpan) map[string]string {
	m := make(map[string]string)
	for _, a := range st.Attributes() {
		m[string(a.Key)] = a.Value.String()
	}
	return m
}

// TestClassifyErrorClass_buckets covers closed enum classification outcomes.
func TestClassifyErrorClass_buckets(t *testing.T) {
	cases := []struct {
		err    error
		status int
		want   string
	}{
		{nil, 0, ErrorClassOK},
		{context.Canceled, 0, ErrorClassCancelled},
		{context.DeadlineExceeded, 0, ErrorClassTimeout},
		{errors.New("x"), 404, ErrorClassProvider4xx},
		{errors.New("x"), 503, ErrorClassProvider5xx},
		{errors.New("context canceled by peer"), 0, ErrorClassCancelled},
		{errors.New("request timeout"), 0, ErrorClassTimeout},
		{errors.New("max_output_tokens"), 0, ErrorClassMaxTokens},
		{errors.New("status 429 from gateway"), 0, ErrorClassProvider4xx},
		{errors.New("status 502"), 0, ErrorClassProvider5xx},
		{errors.New("mystery"), 0, ErrorClassOther},
	}
	for _, tc := range cases {
		if got := ClassifyErrorClass(tc.err, tc.status); got != tc.want {
			t.Fatalf("ClassifyErrorClass(%v,%d)=%q want %q", tc.err, tc.status, got, tc.want)
		}
	}
}

// TestInferProviderName_closedEnum maps base URLs to provider enums.
func TestInferProviderName_closedEnum(t *testing.T) {
	if InferProviderName("https://my.openai.azure.com/v1") != GenAIProviderAzure {
		t.Fatal("azure")
	}
	if InferProviderName("https://api.openai.com/v1") != GenAIProviderOpenAI {
		t.Fatal("openai")
	}
	if InferProviderName("") != GenAIProviderUnknown {
		t.Fatal("empty")
	}
	if InferProviderName("https://llm.example.com") != GenAIProviderUnknown {
		t.Fatal("unknown host")
	}
}

// TestEmitEvent_andAfterTools is the Logs API event emit path (noop provider is fine).
func TestEmitEvent_andAfterTools(t *testing.T) {
	EmitEvent(context.TODO(), "")
	EmitEvent(context.Background(), EventPromptReceived, log.Int(EventAttrPromptLen, 3))
	EmitEventSeverity(context.Background(), EventProviderFailed, log.SeverityError, log.String("code", "x"))
	EmitModelAfterTools(context.Background())
	if Logger() == nil {
		t.Fatal("logger")
	}
}

// TestMultiHandlerAndOTLPSlog covers dual-write slog helpers used by testserver.
func TestMultiHandlerAndOTLPSlog(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	otlp := NewOTLPSlogHandler("test-scope")
	if otlp == nil {
		t.Fatal("otlp handler")
	}
	InstallDefaultWithOTLP(base, otlp)
	slog.Info("hello dual write", "k", 1)
	if !strings.Contains(buf.String(), "hello dual write") {
		t.Fatalf("stderr handler: %q", buf.String())
	}

	// MultiHandler group/attrs paths.
	mh := MultiHandler{base, nil, otlp}
	if !mh.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("enabled")
	}
	_ = mh.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "via multi", 0))
	_ = mh.WithAttrs([]slog.Attr{slog.String("a", "b")})
	_ = mh.WithGroup("g")

	// Empty install path.
	InstallDefaultWithOTLP(nil, nil)
	// Single handler path.
	InstallDefaultWithOTLP(base, nil)
}
