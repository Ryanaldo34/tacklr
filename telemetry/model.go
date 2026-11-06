package telemetry

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// WindowShape is a snapshot of the context window a model request sends.
// It changes with each request, so its model.request log carries it.
type WindowShape struct {
	Messages  int
	ToolPairs int
}

// ModelIdentity is the static identity of a model's requests.
type ModelIdentity struct {
	// Provider is a closed enum: azure.openai | openai | unknown.
	Provider string
	// Model is the deployment or model id (configuration, low cardinality).
	Model string
}

// NewModelIdentity builds GenAI identity from deployment config (model id + API base URL).
func NewModelIdentity(model, baseURL string) ModelIdentity {
	return ModelIdentity{Provider: inferProviderName(baseURL), Model: model}
}

func (id ModelIdentity) provider() string {
	if id.Provider == "" {
		return GenAIProviderUnknown
	}
	return id.Provider
}

type modelAfterToolsKey struct{}

// ContextWithAfterTools marks the next model request as the one after a
// reply's tool calls.
func ContextWithAfterTools(ctx context.Context) context.Context {
	return context.WithValue(ctx, modelAfterToolsKey{}, true)
}

func afterToolsFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(modelAfterToolsKey{}).(bool)
	return v
}

type modelIdentityKey struct{}

// ContextWithModelIdentity attaches the model's identity for its spans.
func ContextWithModelIdentity(ctx context.Context, id ModelIdentity) context.Context {
	return context.WithValue(ctx, modelIdentityKey{}, id)
}

func modelIdentityFromContext(ctx context.Context) ModelIdentity {
	id, _ := ctx.Value(modelIdentityKey{}).(ModelIdentity)
	return id
}

// ProviderStatus is HTTP metadata a provider error may carry. A model span
// reads it with errors.As when the request fails.
type ProviderStatus interface {
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

// ClassifyErrorClass buckets err into a closed error.class enum.
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
	default:
		return ErrorClassOther
	}
}

// TokenUsage is provider-reported token consumption for one model invoke.
type TokenUsage struct {
	Input      int
	Output     int
	Reasoning  int
	Cached     int
	CacheWrite int
}

// ModelSpan is an in-flight chat span. Call End once.
type ModelSpan struct {
	ctx      context.Context
	span     trace.Span
	id       ModelIdentity
	phase    string
	started  time.Time
	finished bool
}

// StartModelSpan starts the chat span of one model request (the GenAI
// semantic conventions' client span, chat {model}), and logs the request's
// dynamic shape: its sequence in the session, the window it sends, and
// whether it follows a reply's tool calls.
func StartModelSpan(ctx context.Context, phase string, seq int, shape WindowShape) (context.Context, *ModelSpan) {
	id := modelIdentityFromContext(ctx)
	attrs := append(identity(ctx),
		attribute.String(AttrGenAIOperationName, OperationChat),
		attribute.String(AttrGenAIProviderName, id.provider()),
		attribute.String(AttrModelPhase, phase),
	)
	if id.Model != "" {
		attrs = append(attrs, attribute.String(AttrGenAIRequestModel, id.Model))
	}
	started := time.Now()
	ctx, span := Tracer().Start(ctx, spanName(OperationChat, id.Model), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	EmitEvent(ctx, EventModelRequest,
		attribute.Int(EventAttrSeq, seq),
		attribute.Int(EventAttrMessages, shape.Messages),
		attribute.Int(EventAttrToolPairs, shape.ToolPairs),
		attribute.Bool(EventAttrAfterTools, afterToolsFromContext(ctx)),
	)
	return ctx, &ModelSpan{ctx: ctx, span: span, id: id, phase: phase, started: started}
}

// End ends the chat span with its outcome and, on failure, error.type (a
// closed class), the provider's HTTP status and error code; it records the
// provider-reported usage on the span and in the GenAI metrics.
func (m *ModelSpan) End(err error, usage TokenUsage) {
	if m.finished {
		return
	}
	m.finished = true

	outcome, errType := OutcomeOK, ""
	attrs := []attribute.KeyValue{}
	if err != nil {
		httpStatus, errorCode := 0, ""
		var ps ProviderStatus
		if errors.As(err, &ps) {
			httpStatus, errorCode = ps.ProviderHTTPStatus(), ps.ProviderErrorCode()
		}
		errType = ClassifyErrorClass(err, httpStatus)
		outcome = OutcomeError
		if errType == ErrorClassCancelled {
			outcome = OutcomeCancelled
		}
		attrs = append(attrs, attribute.String(AttrErrorType, errType))
		if httpStatus > 0 {
			attrs = append(attrs, attribute.Int(AttrHTTPStatus, httpStatus))
		}
		if errorCode != "" {
			attrs = append(attrs, attribute.String(AttrErrorCode, errorCode[:min(len(errorCode), 64)]))
		}
		m.span.RecordError(err)
		m.span.SetStatus(codes.Error, errType)
	}
	for _, u := range []struct {
		key string
		n   int
	}{
		{AttrGenAIInputTokens, usage.Input},
		{AttrGenAIOutputTokens, usage.Output},
		{AttrGenAICachedTokens, usage.Cached},
		{AttrGenAICacheWriteTokens, usage.CacheWrite},
	} {
		if u.n > 0 {
			attrs = append(attrs, attribute.Int(u.key, u.n))
		}
	}
	m.span.SetAttributes(append(attrs, attribute.String(AttrOutcome, outcome))...)
	m.span.End()
	Metrics().recordModel(m.ctx, AgentFromContext(m.ctx), m.id, m.phase, errType, time.Since(m.started), usage)
}
