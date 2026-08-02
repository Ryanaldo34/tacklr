package telemetry

import (
	"context"
	"fmt"
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
}

// MustInstruments builds instruments from m. Panics only on programmer error
// from the SDK (invalid names); treated as init-time failure.
func MustInstruments(m metric.Meter) *Instruments {
	inst, err := NewInstruments(m)
	if err != nil {
		panic(fmt.Sprintf("telemetry instruments: %v", err))
	}
	return inst
}

// NewInstruments creates counters/histograms on m.
func NewInstruments(m metric.Meter) (*Instruments, error) {
	if m == nil {
		m = Meter()
	}
	i := &Instruments{}
	var err error

	i.turnDuration, err = m.Float64Histogram(MetricTurnDuration,
		metric.WithDescription("End-to-end agent turn duration"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	i.turnTotal, err = m.Int64Counter(MetricTurnTotal,
		metric.WithDescription("Total agent turns by outcome"),
	)
	if err != nil {
		return nil, err
	}
	i.turnActive, err = m.Int64UpDownCounter(MetricTurnActive,
		metric.WithDescription("In-flight agent turns"),
	)
	if err != nil {
		return nil, err
	}
	i.toolCalls, err = m.Int64Counter(MetricToolCalls,
		metric.WithDescription("Tool invocations by name and status"),
	)
	if err != nil {
		return nil, err
	}
	i.toolDuration, err = m.Float64Histogram(MetricToolDuration,
		metric.WithDescription("Tool invocation duration"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	i.interruptTotal, err = m.Int64Counter(MetricInterruptTotal,
		metric.WithDescription("Interrupts raised (human-in-the-loop)"),
	)
	if err != nil {
		return nil, err
	}
	i.handoffTotal, err = m.Int64Counter(MetricHandoffTotal,
		metric.WithDescription("Context handoffs after plan progress"),
	)
	if err != nil {
		return nil, err
	}
	i.compressTotal, err = m.Int64Counter(MetricCompressTotal,
		metric.WithDescription("Context window compressions under pressure"),
	)
	if err != nil {
		return nil, err
	}
	i.sessionCreated, err = m.Int64Counter(MetricSessionCreated,
		metric.WithDescription("Sessions created via registry"),
	)
	if err != nil {
		return nil, err
	}
	i.checkpointSave, err = m.Int64Counter(MetricCheckpointSave,
		metric.WithDescription("Session checkpoint save attempts"),
	)
	if err != nil {
		return nil, err
	}
	i.modelDuration, err = m.Float64Histogram(MetricModelDuration,
		metric.WithDescription("Model invoke duration (turn | handoff | compress)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	i.modelTotal, err = m.Int64Counter(MetricModelTotal,
		metric.WithDescription("Model invokes by phase, outcome, and error class"),
	)
	if err != nil {
		return nil, err
	}
	i.tokensInput, err = m.Int64Counter(MetricTokensInput,
		metric.WithDescription("Provider-reported input tokens"),
	)
	if err != nil {
		return nil, err
	}
	i.tokensOutput, err = m.Int64Counter(MetricTokensOutput,
		metric.WithDescription("Provider-reported output tokens"),
	)
	if err != nil {
		return nil, err
	}
	i.tokensReasoning, err = m.Int64Counter(MetricTokensReasoning,
		metric.WithDescription("Provider-reported reasoning tokens when present"),
	)
	if err != nil {
		return nil, err
	}
	return i, nil
}

func (i *Instruments) RecordTurnStart(ctx context.Context, agentID string) {
	if i == nil {
		return
	}
	i.turnActive.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelAgentID, agentID)))
}

func (i *Instruments) RecordTurnEnd(ctx context.Context, agentID, turnKind, outcome string, d time.Duration) {
	if i == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelTurnKind, turnKind),
		attribute.String(LabelOutcome, outcome),
	)
	i.turnActive.Add(ctx, -1, metric.WithAttributes(attribute.String(LabelAgentID, agentID)))
	i.turnTotal.Add(ctx, 1, attrs)
	i.turnDuration.Record(ctx, d.Seconds(), attrs)
}

func (i *Instruments) RecordTool(ctx context.Context, agentID, tool, namespace, status string, d time.Duration) {
	if i == nil {
		return
	}
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
	if i == nil {
		return
	}
	i.interruptTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelKind, kind),
	))
}

// RecordHandoff records a context handoff. outcome is a closed enum
// (HandoffOutcomeOK | HandoffOutcomeFallback | HandoffOutcomeError).
func (i *Instruments) RecordHandoff(ctx context.Context, agentID, outcome string) {
	if i == nil {
		return
	}
	if outcome == "" {
		outcome = HandoffOutcomeOK
	}
	i.handoffTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(LabelAgentID, agentID),
		attribute.String(LabelOutcome, outcome),
	))
}

// RecordModel records one model invoke (duration + count). phase and errClass
// must be closed enums (ModelPhase* / ErrorClass*).
func (i *Instruments) RecordModel(ctx context.Context, agentID, phase, outcome, errClass string, d time.Duration) {
	if i == nil {
		return
	}
	if phase == "" {
		phase = ModelPhaseTurn
	}
	if outcome == "" {
		outcome = OutcomeOK
	}
	if errClass == "" {
		errClass = ErrorClassOK
	}
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
	if i == nil {
		return
	}
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
	if i == nil {
		return
	}
	i.compressTotal.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelAgentID, agentID)))
}

func (i *Instruments) RecordSessionCreated(ctx context.Context) {
	if i == nil {
		return
	}
	i.sessionCreated.Add(ctx, 1)
}

func (i *Instruments) RecordCheckpointSave(ctx context.Context, outcome string) {
	if i == nil {
		return
	}
	i.checkpointSave.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelOutcome, outcome)))
}

// agentIDContextKey carries agent_id for child instrumentation without plumbing.
type agentIDContextKey struct{}

// ContextWithAgentID attaches agent_id for tool/handoff metrics.
func ContextWithAgentID(ctx context.Context, agentID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, agentIDContextKey{}, agentID)
}

// AgentIDFromContext returns agent_id or "".
func AgentIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	s, _ := ctx.Value(agentIDContextKey{}).(string)
	return s
}
