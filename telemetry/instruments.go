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
	MetricTurnDuration   = "tacklr.turn.duration"
	MetricTurnTotal      = "tacklr.turn.total"
	MetricTurnActive     = "tacklr.turn.active"
	MetricToolCalls      = "tacklr.tool.calls"
	MetricToolDuration   = "tacklr.tool.duration"
	MetricInterruptTotal = "tacklr.interrupt.total"
	MetricHandoffTotal   = "tacklr.context.handoff.total"
	MetricCompressTotal  = "tacklr.context.compress.total"
	MetricSessionCreated = "tacklr.session.created.total"
	MetricCheckpointSave = "tacklr.checkpoint.save.total"
)

// Label keys (low cardinality only).
const (
	LabelAgentID  = "agent_id"
	LabelTurnKind = "turn_kind"
	LabelOutcome  = "outcome"
	LabelTool     = "tool"
	LabelToolNS   = "tool_namespace"
	LabelStatus   = "status"
	LabelKind     = "kind" // interrupt kind
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
	sessionCreated metric.Int64Counter
	checkpointSave metric.Int64Counter
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

func (i *Instruments) RecordHandoff(ctx context.Context, agentID string) {
	if i == nil {
		return
	}
	i.handoffTotal.Add(ctx, 1, metric.WithAttributes(attribute.String(LabelAgentID, agentID)))
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
