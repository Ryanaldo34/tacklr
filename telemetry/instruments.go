package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metric names (OTel). Prometheus export sanitizes '.' → '_'.
const (
	MetricTurnDuration    = "tacklr.turn.duration"
	MetricTurnTotal       = "tacklr.turn.total"
	MetricTurnActive      = "tacklr.turn.active"
	MetricToolCalls       = "tacklr.tool.calls"
	MetricToolDuration    = "tacklr.tool.duration"
	MetricInterruptTotal  = "tacklr.interrupt.total"
	MetricHandoffTotal    = "tacklr.context.handoff.total"
	MetricCompressTotal   = "tacklr.context.compress.total"
	MetricSessionCreated  = "tacklr.session.created.total"
	MetricCheckpointSave  = "tacklr.checkpoint.save.total"
	MetricModelDuration   = "tacklr.model.duration"
	MetricModelTotal      = "tacklr.model.total"
	MetricTokensInput     = "tacklr.tokens.input"
	MetricTokensOutput    = "tacklr.tokens.output"
	MetricTokensReasoning = "tacklr.tokens.reasoning"
	MetricFuseMount       = "tacklr.fuse.mount.total"
)

// Label keys (low cardinality only — closed enums / config ids, never free text).
const (
	LabelAgentID    = "agent_id"
	LabelTurnKind   = "turn_kind"
	LabelOutcome    = "outcome"
	LabelTool       = "tool"
	LabelToolNS     = "tool_namespace"
	LabelStatus     = "status"
	LabelKind       = "kind" // interrupt kind
	LabelModelPhase = "model_phase"
	LabelErrorClass = "error_class"
)

// Instruments holds cached metric instruments for one Meter.
type Instruments struct {
	turnDuration    metric.Float64Histogram
	turnTotal       metric.Int64Counter
	turnActive      metric.Int64UpDownCounter
	toolCalls       metric.Int64Counter
	toolDuration    metric.Float64Histogram
	interruptTotal  metric.Int64Counter
	handoffTotal    metric.Int64Counter
	compressTotal   metric.Int64Counter
	sessionCreated  metric.Int64Counter
	checkpointSave  metric.Int64Counter
	modelDuration   metric.Float64Histogram
	modelTotal      metric.Int64Counter
	tokensInput     metric.Int64Counter
	tokensOutput    metric.Int64Counter
	tokensReasoning metric.Int64Counter
	fuseMountTotal  metric.Int64Counter
}

// MustInstruments builds instruments from m. Names are constants; the SDK
// only errors on invalid names, which would be a compile-time programmer error.
func MustInstruments(m metric.Meter) *Instruments {
	i := &Instruments{}
	i.turnDuration, _ = m.Float64Histogram(MetricTurnDuration,
		metric.WithDescription("End-to-end agent turn duration"),
		metric.WithUnit("s"),
	)
	i.turnTotal, _ = m.Int64Counter(MetricTurnTotal,
		metric.WithDescription("Total agent turns by outcome"),
	)
	i.turnActive, _ = m.Int64UpDownCounter(MetricTurnActive,
		metric.WithDescription("In-flight agent turns"),
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
	i.sessionCreated, _ = m.Int64Counter(MetricSessionCreated,
		metric.WithDescription("Sessions created via registry"),
	)
	i.checkpointSave, _ = m.Int64Counter(MetricCheckpointSave,
		metric.WithDescription("Session checkpoint save attempts"),
	)
	i.modelDuration, _ = m.Float64Histogram(MetricModelDuration,
		metric.WithDescription("Model invoke duration (turn | handoff | compress)"),
		metric.WithUnit("s"),
	)
	i.modelTotal, _ = m.Int64Counter(MetricModelTotal,
		metric.WithDescription("Model invokes by phase, outcome, and error class"),
	)
	i.tokensInput, _ = m.Int64Counter(MetricTokensInput,
		metric.WithDescription("Provider-reported input tokens"),
	)
	i.tokensOutput, _ = m.Int64Counter(MetricTokensOutput,
		metric.WithDescription("Provider-reported output tokens"),
	)
	i.tokensReasoning, _ = m.Int64Counter(MetricTokensReasoning,
		metric.WithDescription("Provider-reported reasoning tokens when present"),
	)
	i.fuseMountTotal, _ = m.Int64Counter(MetricFuseMount,
		metric.WithDescription("FUSE mount attempts by outcome (ok, error, unavailable)"),
	)
	return i
}

func (i *Instruments) RecordTurnStart(ctx context.Context, agentID string) {
	i.turnActive.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelAgentID, agentID)))
}

func (i *Instruments) RecordTurnEnd(ctx context.Context, agentID, turnKind, outcome string, d time.Duration) {
	i.turnActive.Add(ctx, -1, metric.WithAttributes(attribute.String(LabelAgentID, agentID)))
	i.RecordTurnOutcome(ctx, agentID, turnKind, outcome, d)
}

// RecordTurnOutcome increments turn totals and duration without touching the
// in-flight gauge. Temporal workflows use this: replay would double-count
// RecordTurnStart/RecordTurnEnd on the active gauge.
func (i *Instruments) RecordTurnOutcome(ctx context.Context, agentID, turnKind, outcome string, d time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelTurnKind, turnKind),
		attribute.String(LabelOutcome, outcome),
	)
	i.turnTotal.Add(ctx, 1, attrs)
	i.turnDuration.Record(ctx, d.Seconds(), attrs)
}

func (i *Instruments) RecordTool(ctx context.Context, agentID, tool, namespace, status string, d time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelTool, tool),
		attribute.String(LabelToolNS, namespace),
		attribute.String(LabelStatus, status),
	)
	i.toolCalls.Add(ctx, 1, attrs)
	i.toolDuration.Record(ctx, d.Seconds(), attrs)
}

func (i *Instruments) RecordInterrupt(ctx context.Context, agentID, kind string) {
	i.interruptTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelKind, kind),
	))
}

// RecordHandoff records a context handoff. outcome is a closed enum
// (HandoffOutcomeOK | HandoffOutcomeFallback | HandoffOutcomeError).
func (i *Instruments) RecordHandoff(ctx context.Context, agentID, outcome string) {
	i.handoffTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelOutcome, outcome),
	))
}

// RecordModel records one model invoke (duration + count). phase and errClass
// must be closed enums (ModelPhase* / ErrorClass*).
func (i *Instruments) RecordModel(ctx context.Context, agentID, phase, outcome, errClass string, d time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelModelPhase, phase),
		attribute.String(LabelOutcome, outcome),
		attribute.String(LabelErrorClass, errClass),
	)
	i.modelTotal.Add(ctx, 1, attrs)
	if d > 0 {
		i.modelDuration.Record(ctx, d.Seconds(), attrs)
	}
}

// RecordTokens adds provider-reported token counts (no high-cardinality labels).
func (i *Instruments) RecordTokens(ctx context.Context, agentID string, input, output, reasoning int) {
	attrs := metric.WithAttributes(attribute.String(LabelAgentID, agentID))
	if input > 0 {
		i.tokensInput.Add(ctx, int64(input), attrs)
	}
	if output > 0 {
		i.tokensOutput.Add(ctx, int64(output), attrs)
	}
	if reasoning > 0 {
		i.tokensReasoning.Add(ctx, int64(reasoning), attrs)
	}
}

func (i *Instruments) RecordCompress(ctx context.Context, agentID string) {
	i.compressTotal.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelAgentID, agentID)))
}

func (i *Instruments) RecordSessionCreated(ctx context.Context) {
	i.sessionCreated.Add(ctx, 1)
}

func (i *Instruments) RecordCheckpointSave(ctx context.Context, outcome string) {
	i.checkpointSave.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelOutcome, outcome)))
}

// Fuse mount outcomes for RecordFuseMount (closed enum).
const (
	FuseMountOutcomeOK          = "ok"
	FuseMountOutcomeError       = "error"
	FuseMountOutcomeUnavailable = "unavailable"
)

// RecordFuseMount increments tacklr.fuse.mount.total{outcome=...}.
func (i *Instruments) RecordFuseMount(ctx context.Context, outcome string) {
	i.fuseMountTotal.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelOutcome, outcome)))
}

// agentIDContextKey carries agent_id for child instrumentation without plumbing.
type agentIDContextKey struct{}

// ContextWithAgentID attaches agent_id for tool/handoff metrics.
func ContextWithAgentID(ctx context.Context, agentID string) context.Context {
	return context.WithValue(ctx, agentIDContextKey{}, agentID)
}

// AgentIDFromContext returns agent_id or "".
func AgentIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(agentIDContextKey{}).(string)
	return s
}

type sessionIDContextKey struct{}

// ContextWithSessionID attaches session_id for child spans.
func ContextWithSessionID(ctx context.Context, sessionID string) context.Context {
	if sessionID == "" {
		return ctx
	}
	return context.WithValue(ctx, sessionIDContextKey{}, sessionID)
}

// SessionIDFromContext returns session_id or "".
func SessionIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(sessionIDContextKey{}).(string)
	return s
}
