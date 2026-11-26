package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metric names. The model's are the semantic conventions'
// (gen_ai.client.*); the rest are Tacklr's.
// Prometheus export writes '.' as '_'.
const (
	MetricTurnDuration   = "tacklr.turn.duration"
	MetricTurnTotal      = "tacklr.turn.total"
	MetricTurnActive     = "tacklr.turn.active"
	MetricToolCalls      = "tacklr.tool.calls"
	MetricToolDuration   = "tacklr.tool.duration"
	MetricInterruptTotal = "tacklr.interrupt.total"
	MetricHandoffTotal   = "tacklr.context.handoff.total"
	MetricCompressTotal  = "tacklr.context.compress.total"
	MetricCheckpointSave = "tacklr.checkpoint.save.total"
	MetricModelDuration  = "gen_ai.client.operation.duration"
	MetricTokenUsage     = "gen_ai.client.token.usage"
)

// Label keys: closed enums and configuration ids only, never free text or
// a session id.
const (
	LabelAgent      = AttrGenAIAgentName
	LabelTurnKind   = AttrTurnKind
	LabelOutcome    = "outcome"
	LabelTool       = AttrGenAIToolName
	LabelToolNS     = AttrToolNS
	LabelStatus     = "status"
	LabelKind       = "kind" // interrupt kind
	LabelModelPhase = AttrModelPhase
	LabelErrorType  = AttrErrorType
)

// Instruments holds cached metric instruments for one Meter.
type Instruments struct {
	turnDuration   metric.Float64Histogram
	turnTotal      metric.Int64Counter
	turnActive     metric.Int64UpDownCounter
	toolCalls      metric.Int64Counter
	toolDuration   metric.Float64Histogram
	interruptTotal metric.Int64Counter
	handoffTotal   metric.Int64Counter
	compressTotal  metric.Int64Counter
	checkpointSave metric.Int64Counter
	modelDuration  metric.Float64Histogram
	tokenUsage     metric.Int64Histogram
}

// MustInstruments builds instruments from m. Names are constants; the SDK
// only errors on invalid names, which would be a compile-time programmer error.
func MustInstruments(m metric.Meter) *Instruments {
	i := &Instruments{}
	i.turnDuration, _ = m.Float64Histogram(MetricTurnDuration,
		metric.WithDescription("Agent turn duration, from its start to its end or park"),
		metric.WithUnit("s"),
	)
	i.turnTotal, _ = m.Int64Counter(MetricTurnTotal,
		metric.WithDescription("Agent turns by outcome"),
	)
	i.turnActive, _ = m.Int64UpDownCounter(MetricTurnActive,
		metric.WithDescription("Agent turns running now"),
	)
	i.toolCalls, _ = m.Int64Counter(MetricToolCalls,
		metric.WithDescription("Tool invocations by name and status"),
	)
	i.toolDuration, _ = m.Float64Histogram(MetricToolDuration,
		metric.WithDescription("Tool invocation duration"),
		metric.WithUnit("s"),
	)
	i.interruptTotal, _ = m.Int64Counter(MetricInterruptTotal,
		metric.WithDescription("Interrupts raised (human-in-the-loop)"),
	)
	i.handoffTotal, _ = m.Int64Counter(MetricHandoffTotal,
		metric.WithDescription("Context handoffs after plan progress"),
	)
	i.compressTotal, _ = m.Int64Counter(MetricCompressTotal,
		metric.WithDescription("Context window compressions under pressure"),
	)
	i.checkpointSave, _ = m.Int64Counter(MetricCheckpointSave,
		metric.WithDescription("Session checkpoint save attempts"),
	)
	i.modelDuration, _ = m.Float64Histogram(MetricModelDuration,
		metric.WithDescription("GenAI operation duration"),
		metric.WithUnit("s"),
	)
	i.tokenUsage, _ = m.Int64Histogram(MetricTokenUsage,
		metric.WithDescription("Provider-reported tokens of each GenAI operation"),
		metric.WithUnit("{token}"),
	)
	return i
}

// turnStarted counts a running turn.
func (i *Instruments) turnStarted(ctx context.Context, agent, kind string) {
	i.turnActive.Add(ctx, 1, metric.WithAttributes(
		attribute.String(LabelAgent, agent),
		attribute.String(LabelTurnKind, kind),
	))
}

// turnEnded counts a turn that ended, by outcome, and its duration.
func (i *Instruments) turnEnded(ctx context.Context, agent, kind, outcome string, d time.Duration) {
	i.turnActive.Add(ctx, -1, metric.WithAttributes(
		attribute.String(LabelAgent, agent),
		attribute.String(LabelTurnKind, kind),
	))
	attrs := metric.WithAttributes(
		attribute.String(LabelAgent, agent),
		attribute.String(LabelTurnKind, kind),
		attribute.String(LabelOutcome, outcome),
	)
	i.turnTotal.Add(ctx, 1, attrs)
	i.turnDuration.Record(ctx, d.Seconds(), attrs)
}

func (i *Instruments) recordTool(ctx context.Context, agent, tool, namespace, status string, d time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String(LabelAgent, agent),
		attribute.String(LabelTool, tool),
		attribute.String(LabelToolNS, namespace),
		attribute.String(LabelStatus, status),
	)
	i.toolCalls.Add(ctx, 1, attrs)
	i.toolDuration.Record(ctx, d.Seconds(), attrs)
}

// RecordInterrupt counts a park of a tool call on an interrupt of kind.
func (i *Instruments) RecordInterrupt(ctx context.Context, agent, kind string) {
	i.interruptTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(LabelAgent, agent),
		attribute.String(LabelKind, kind),
	))
}

// recordHandoff records a context handoff. outcome is a closed enum
// (HandoffOutcomeOK | HandoffOutcomeFallback | HandoffOutcomeError).
func (i *Instruments) recordHandoff(ctx context.Context, agent, outcome string) {
	i.handoffTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(LabelAgent, agent),
		attribute.String(LabelOutcome, outcome),
	))
}

// recordModel records one GenAI operation's duration and its tokens.
func (i *Instruments) recordModel(ctx context.Context, agent string, id ModelIdentity, phase, errType string, d time.Duration, usage TokenUsage) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrGenAIOperationName, OperationChat),
		attribute.String(AttrGenAIProviderName, id.provider()),
		attribute.String(AttrGenAIRequestModel, id.Model),
		attribute.String(LabelAgent, agent),
		attribute.String(LabelModelPhase, phase),
	}
	if errType != "" {
		attrs = append(attrs, attribute.String(LabelErrorType, errType))
	}
	i.modelDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
	for _, t := range []struct {
		kind string
		n    int
	}{{"input", usage.Input}, {"output", usage.Output}, {"reasoning", usage.Reasoning}} {
		if t.n > 0 {
			i.tokenUsage.Record(ctx, int64(t.n), metric.WithAttributes(append(attrs, attribute.String(AttrGenAITokenType, t.kind))...))
		}
	}
}

// RecordCompress counts a collapse of the context window.
func (i *Instruments) RecordCompress(ctx context.Context, agent string) {
	i.compressTotal.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelAgent, agent)))
}

// RecordCheckpointSave counts a checkpoint save by outcome.
func (i *Instruments) RecordCheckpointSave(ctx context.Context, outcome string) {
	i.checkpointSave.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelOutcome, outcome)))
}

// agentContextKey carries the agent's name for the spans and metrics of a
// step without plumbing.
type agentContextKey struct{}

// ContextWithAgent attaches the agent's name.
func ContextWithAgent(ctx context.Context, agent string) context.Context {
	return context.WithValue(ctx, agentContextKey{}, agent)
}

// AgentFromContext is the agent's name, or "".
func AgentFromContext(ctx context.Context) string {
	s, _ := ctx.Value(agentContextKey{}).(string)
	return s
}

type sessionIDContextKey struct{}

// ContextWithSessionID attaches the session id (gen_ai.conversation.id).
func ContextWithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, sessionIDContextKey{}, sessionID)
}

// SessionIDFromContext returns the session id or "".
func SessionIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(sessionIDContextKey{}).(string)
	return s
}

// identity is the static attributes every span of a session carries: its
// conversation id and its agent, when ctx has them.
func identity(ctx context.Context) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if sid := SessionIDFromContext(ctx); sid != "" {
		attrs = append(attrs, attribute.String(AttrGenAIConversationID, sid))
	}
	if agent := AgentFromContext(ctx); agent != "" {
		attrs = append(attrs, attribute.String(AttrGenAIAgentName, agent))
	}
	return attrs
}
