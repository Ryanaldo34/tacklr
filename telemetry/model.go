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

// NewModelIdentity builds GenAI identity from deployment config (model id + API base URL).
func NewModelIdentity(model, baseURL string) ModelIdentity {
	return ModelIdentity{
		Provider:  inferProviderName(baseURL),
		Model:     model,
		Operation: GenAIOperationChat,
	}
}

type modelAfterToolsKey struct{}

// ContextWithAfterTools marks the next model span as after a tool batch (harness use).
func ContextWithAfterTools(ctx context.Context) context.Context {
	return context.WithValue(ctx, modelAfterToolsKey{}, true)
}

func afterToolsFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(modelAfterToolsKey{}).(bool)
	return v
}

type modelIdentityKey struct{}

// ContextWithModelIdentity attaches static GenAI identity for model spans.
func ContextWithModelIdentity(ctx context.Context, id ModelIdentity) context.Context {
	return context.WithValue(ctx, modelIdentityKey{}, id)
}

func modelIdentityFromContext(ctx context.Context) ModelIdentity {
	id, _ := ctx.Value(modelIdentityKey{}).(ModelIdentity)
	return id
}

// providerStatus is optional HTTP metadata on provider errors (errors.As in ModelSpan.End).
type providerStatus interface {
	ProviderHTTPStatus() int
	ProviderErrorCode() string
}

func inferProviderName(baseURL string) string {
	u := strings.ToLower(baseURL)
	switch {
	case strings.Contains(u, "azure") || strings.Contains(u, "openai.azure.com") || strings.Contains(u, "cognitiveservices"):
		return GenAIProviderAzure
	case strings.Contains(u, "openai.com") || strings.Contains(u, "api.openai"):
		return GenAIProviderOpenAI
	default:
		return GenAIProviderUnknown
	}
}

func classifyErrorClass(err error, httpStatus int) string {
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
	default:
		return ErrorClassOther
	}
}

// TokenUsage is provider-reported token consumption for one model invoke.
type TokenUsage struct {
	Input     int
	Output    int
	Reasoning int
}

// ModelSpan is an in-flight tacklr.model span. Call End once.
type ModelSpan struct {
	ctx      context.Context
	span     trace.Span
	phase    string
	started  time.Time
	finished bool
}

// StartModelSpan starts a model span. Emits model.after_tools when ContextWithAfterTools is set.
func StartModelSpan(ctx context.Context, phase string, seq int, shape WindowShape) (context.Context, *ModelSpan) {
	if phase == "" {
		phase = ModelPhaseTurn
	}
	id := modelIdentityFromContext(ctx)
	op := id.Operation
	if op == "" {
		op = GenAIOperationChat
	}
	provider := id.Provider
	if provider == "" {
		provider = GenAIProviderUnknown
	}
	afterTools := afterToolsFromContext(ctx)
	if afterTools {
		EmitEvent(ctx, EventModelAfterTools, log.Bool(AttrAfterTools, true))
	}

	attrs := []attribute.KeyValue{
		attribute.String(AttrArea, AreaModelTasks),
		attribute.String(AttrModelPhase, phase),
		attribute.Int(AttrContextMsgs, shape.Messages),
		attribute.Int(AttrContextToolPairs, shape.ToolPairs),
		attribute.String(AttrGenAIOperationName, op),
		attribute.String(AttrGenAIProviderName, provider),
		attribute.Bool(AttrAfterTools, afterTools),
	}
	if id.Model != "" {
		attrs = append(attrs, attribute.String(AttrGenAIRequestModel, id.Model))
	}
	if seq > 0 {
		attrs = append(attrs, attribute.Int(AttrModelSeq, seq))
	}

	started := time.Now()
	ctx, span := TracerFromContext(ctx).Start(ctx, SpanModel, trace.WithAttributes(attrs...))
	return ctx, &ModelSpan{ctx: ctx, span: span, phase: phase, started: started}
}

// End ends the model span with outcome, usage, and metrics.
// HTTP status and code come from err when it implements providerStatus.
func (m *ModelSpan) End(err error, usage TokenUsage) {
	if m == nil || m.finished {
		return
	}
	m.finished = true

	httpStatus, errorCode := 0, ""
	if err != nil {
		var ps providerStatus
		if errors.As(err, &ps) {
			httpStatus = ps.ProviderHTTPStatus()
			errorCode = ps.ProviderErrorCode()
		}
	}

	outcome := OutcomeOK
	errClass := ErrorClassOK
	if err != nil {
		errClass = classifyErrorClass(err, httpStatus)
		if errClass == ErrorClassCancelled || errors.Is(err, context.Canceled) {
			outcome = OutcomeCancelled
		} else {
			outcome = OutcomeError
		}
		m.span.RecordError(err)
		m.span.SetStatus(codes.Error, err.Error())
	} else {
		m.span.SetStatus(codes.Ok, "")
	}

	endAttrs := []attribute.KeyValue{
		attribute.String(AttrOutcome, outcome),
		attribute.String(AttrErrorClass, errClass),
		attribute.Int(AttrHTTPStatus, httpStatus),
	}
	if errorCode != "" {
		if len(errorCode) > 64 {
			errorCode = errorCode[:64]
		}
		endAttrs = append(endAttrs, attribute.String(AttrErrorCode, errorCode))
	}
	if usage.Input > 0 {
		endAttrs = append(endAttrs, attribute.Int(AttrGenAIInputTokens, usage.Input))
	}
	if usage.Output > 0 {
		endAttrs = append(endAttrs, attribute.Int(AttrGenAIOutputTokens, usage.Output))
	}
	m.span.SetAttributes(endAttrs...)
	m.span.End()

	inst := InstrumentsFromContext(m.ctx)
	agentID := AgentIDFromContext(m.ctx)
	inst.RecordModel(m.ctx, agentID, m.phase, outcome, errClass, time.Since(m.started))
	if usage.Input > 0 || usage.Output > 0 || usage.Reasoning > 0 {
		inst.RecordTokens(m.ctx, agentID, usage.Input, usage.Output, usage.Reasoning)
	}
}
