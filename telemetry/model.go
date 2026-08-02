package telemetry

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// WindowShape is a low-cardinality snapshot of the context window for model spans.
type WindowShape struct {
	Messages  int
	ToolPairs int
}

// ModelIdentity is static request identity for GenAI span attrs (set at span start).
type ModelIdentity struct {
	// Provider is a closed enum: azure.openai | openai | unknown.
	Provider string
	// Model is the deployment/model id (treat as low-cardinality config).
	Model string
	// Operation defaults to GenAIOperationChat when empty.
	Operation string
}

// modelAfterToolsKey marks Turn Invokes that follow a successful tool batch.
type modelAfterToolsKey struct{}

// ContextWithAfterTools marks the next model span as post-tool-batch.
func ContextWithAfterTools(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, modelAfterToolsKey{}, true)
}

// AfterToolsFromContext reports whether ContextWithAfterTools was set.
func AfterToolsFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(modelAfterToolsKey{}).(bool)
	return v
}

// modelIdentityKey carries optional ModelIdentity on context.
type modelIdentityKey struct{}

// ContextWithModelIdentity attaches static GenAI identity for model spans.
func ContextWithModelIdentity(ctx context.Context, id ModelIdentity) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, modelIdentityKey{}, id)
}

// ModelIdentityFromContext returns identity or zero.
func ModelIdentityFromContext(ctx context.Context) ModelIdentity {
	if ctx == nil {
		return ModelIdentity{}
	}
	id, _ := ctx.Value(modelIdentityKey{}).(ModelIdentity)
	return id
}

// InferProviderName maps a base URL to a closed provider enum.
func InferProviderName(baseURL string) string {
	u := strings.ToLower(baseURL)
	switch {
	case strings.Contains(u, "azure") || strings.Contains(u, "openai.azure.com") || strings.Contains(u, "cognitiveservices"):
		return GenAIProviderAzure
	case strings.Contains(u, "openai.com") || strings.Contains(u, "api.openai"):
		return GenAIProviderOpenAI
	case strings.TrimSpace(baseURL) == "":
		return GenAIProviderUnknown
	default:
		// Self-hosted / Foundry-compatible endpoints without azure in host.
		return GenAIProviderUnknown
	}
}

// ClassifyErrorClass maps HTTP status / error text to a closed error-class enum.
func ClassifyErrorClass(err error, httpStatus int) string {
	if err == nil {
		return ErrorClassOK
	}
	if errors.Is(err, context.Canceled) {
		return ErrorClassCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorClassTimeout
	}
	if httpStatus >= 400 && httpStatus < 500 {
		return ErrorClassProvider4xx
	}
	if httpStatus >= 500 {
		return ErrorClassProvider5xx
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "context canceled") || strings.Contains(msg, "cancelled"):
		return ErrorClassCancelled
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return ErrorClassTimeout
	case strings.Contains(msg, "max_output_tokens") || strings.Contains(msg, "max tokens") || strings.Contains(msg, "max_tokens"):
		return ErrorClassMaxTokens
	case strings.Contains(msg, "status 4"):
		return ErrorClassProvider4xx
	case strings.Contains(msg, "status 5"):
		return ErrorClassProvider5xx
	default:
		return ErrorClassOther
	}
}

// StartModelSpan starts a tacklr.model child span with static start attributes only
// (phase, seq, window shape, GenAI identity, after_tools). Outcome/usage attrs are
// set at end. Start time is stored on the returned context for duration metrics.
func StartModelSpan(ctx context.Context, phase string, seq int, shape WindowShape) (context.Context, trace.Span) {
	if phase == "" {
		phase = ModelPhaseTurn
	}
	id := ModelIdentityFromContext(ctx)
	op := id.Operation
	if op == "" {
		op = GenAIOperationChat
	}
	provider := id.Provider
	if provider == "" {
		provider = GenAIProviderUnknown
	}

	// Always set the same keys (static attribute schema) with closed-enum values.
	attrs := []attribute.KeyValue{
		attribute.String(AttrArea, AreaModelTasks),
		attribute.String(AttrModelPhase, phase),
		attribute.Int(AttrContextMsgs, shape.Messages),
		attribute.Int(AttrContextToolPairs, shape.ToolPairs),
		attribute.String(AttrGenAIOperationName, op),
		attribute.String(AttrGenAIProviderName, provider),
		attribute.Bool(AttrAfterTools, AfterToolsFromContext(ctx)),
	}
	if id.Model != "" {
		attrs = append(attrs, attribute.String(AttrGenAIRequestModel, id.Model))
	}
	if seq > 0 {
		attrs = append(attrs, attribute.Int(AttrModelSeq, seq))
	}

	started := time.Now()
	ctx, span := TracerFromContext(ctx).Start(ctx, SpanModel, trace.WithAttributes(attrs...))
	ctx = ContextWithModelStart(ctx, started)
	// Phase for end helpers that only have the span context.
	ctx = context.WithValue(ctx, modelPhaseKey{}, phase)
	return ctx, span
}

// TokenUsage is provider-reported token consumption for one model invoke.
type TokenUsage struct {
	Input     int
	Output    int
	Reasoning int
}

// EndModelSpan finishes a model span with outcome enums, optional usage, and metrics.
// Duration uses ContextWithModelStart when present; otherwise 0 (count still recorded).
func EndModelSpan(ctx context.Context, span trace.Span, phase string, err error, httpStatus int, errorCode string, usage TokenUsage) {
	started := ModelStartFromContext(ctx)
	EndModelSpanTimed(ctx, span, phase, err, httpStatus, errorCode, usage, started)
}

// EndModelSpanTimed is EndModelSpan with an explicit duration measurement.
func EndModelSpanTimed(ctx context.Context, span trace.Span, phase string, err error, httpStatus int, errorCode string, usage TokenUsage, started time.Time) {
	if span == nil {
		return
	}
	if phase == "" {
		if p, ok := ctx.Value(modelPhaseKey{}).(string); ok {
			phase = p
		}
		if phase == "" {
			phase = ModelPhaseTurn
		}
	}

	outcome := OutcomeOK
	errClass := ErrorClassOK
	if err != nil {
		errClass = ClassifyErrorClass(err, httpStatus)
		// Cancel is a first-class turn outcome (not a provider failure).
		if errClass == ErrorClassCancelled || errors.Is(err, context.Canceled) {
			outcome = OutcomeCancelled
		} else {
			outcome = OutcomeError
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	// End attrs use the same static keys every time (empty/zero when not applicable).
	endAttrs := []attribute.KeyValue{
		attribute.String(AttrOutcome, outcome),
		attribute.String(AttrErrorClass, errClass),
		attribute.Int(AttrHTTPStatus, httpStatus),
	}
	if errorCode != "" {
		endAttrs = append(endAttrs, attribute.String(AttrErrorCode, truncateCode(errorCode, 64)))
	}
	if usage.Input > 0 {
		endAttrs = append(endAttrs, attribute.Int(AttrGenAIInputTokens, usage.Input))
	}
	if usage.Output > 0 {
		endAttrs = append(endAttrs, attribute.Int(AttrGenAIOutputTokens, usage.Output))
	}
	span.SetAttributes(endAttrs...)
	span.End()

	dur := time.Duration(0)
	if !started.IsZero() {
		dur = time.Since(started)
	}
	InstrumentsFromContext(ctx).RecordModel(ctx, AgentIDFromContext(ctx), phase, outcome, errClass, dur)
	if usage.Input > 0 || usage.Output > 0 || usage.Reasoning > 0 {
		InstrumentsFromContext(ctx).RecordTokens(ctx, AgentIDFromContext(ctx), usage.Input, usage.Output, usage.Reasoning)
	}
}

// EmitModelAfterTools emits the model.after_tools log event (span-correlated).
func EmitModelAfterTools(ctx context.Context) {
	EmitEventSeverity(ctx, EventModelAfterTools, log.SeverityWarn,
		log.Bool(AttrAfterTools, true),
	)
}

func truncateCode(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n]
}

// modelStartKey carries span start time for accurate model duration metrics.
type modelStartKey struct{}

// modelPhaseKey carries phase set at StartModelSpan.
type modelPhaseKey struct{}

// ContextWithModelStart stores start time for EndModelSpan duration.
func ContextWithModelStart(ctx context.Context, start time.Time) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, modelStartKey{}, start)
}

// ModelStartFromContext returns start time or zero.
func ModelStartFromContext(ctx context.Context) time.Time {
	if ctx == nil {
		return time.Time{}
	}
	t, _ := ctx.Value(modelStartKey{}).(time.Time)
	return t
}
