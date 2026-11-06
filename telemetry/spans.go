package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// Tool is the static identity of one tool call's span.
type Tool struct {
	Name      string
	Namespace string
	// CallID is the model's id of the call (gen_ai.tool.call.id), to find
	// the span of one call.
	CallID string
	// Type is ToolTypeFunction for a host's tool, ToolTypeExtension for an
	// MCP server's.
	Type string
}

// ToolSpan is an in-flight execute_tool span. Call Finish once.
type ToolSpan struct {
	ctx      context.Context
	span     trace.Span
	start    time.Time
	tool     Tool
	finished bool
}

// StartToolSpan starts the execute_tool span of one tool call.
func StartToolSpan(ctx context.Context, t Tool) (context.Context, *ToolSpan) {
	attrs := append(identity(ctx),
		attribute.String(AttrGenAIOperationName, OperationExecuteTool),
		attribute.String(AttrGenAIToolName, t.Name),
		attribute.String(AttrGenAIToolCallID, t.CallID),
		attribute.String(AttrGenAIToolType, t.Type),
	)
	if t.Namespace != "" {
		attrs = append(attrs, attribute.String(AttrToolNS, t.Namespace))
	}
	ctx, span := Tracer().Start(ctx, spanName(OperationExecuteTool, t.Name), trace.WithAttributes(attrs...))
	return ctx, &ToolSpan{ctx: ctx, span: span, start: time.Now(), tool: t}
}

// Finish ends the tool span with status (success, error, interrupt) and
// counts the call. A failure's text goes to the span's tool.failed log.
func (t *ToolSpan) Finish(status string, err error) {
	if t.finished {
		return
	}
	t.finished = true
	outcome := OutcomeOK
	switch {
	case status == "interrupt":
		outcome = OutcomeYield
	case err != nil || status == "error":
		outcome = OutcomeError
		t.span.SetAttributes(attribute.String(AttrErrorType, ErrorClassOther))
		t.span.SetStatus(codes.Error, ErrorClassOther)
		if err != nil {
			EmitEventSeverity(t.ctx, EventToolFailed, log.SeverityWarn, attribute.String(EventAttrError, err.Error()))
		}
	}
	t.span.SetAttributes(attribute.String(AttrOutcome, outcome))
	t.span.End()
	Metrics().recordTool(t.ctx, AgentFromContext(t.ctx), t.tool.Name, t.tool.Namespace, status, time.Since(t.start))
}

// PlanInstalled logs the install of a plan document into the window, with
// how many todos it has and how many are open.
func PlanInstalled(ctx context.Context, todos, open int, err error) {
	attrs := []attribute.KeyValue{attribute.Int(EventAttrTodos, todos), attribute.Int(EventAttrOpenTodos, open)}
	severity := log.SeverityInfo
	if err != nil {
		attrs, severity = append(attrs, attribute.String(EventAttrError, err.Error())), log.SeverityError
	}
	EmitEventSeverity(ctx, EventPlanInstalled, severity, attrs...)
}

// HandoffSpan is an in-flight tacklr.context.handoff span. Call End once.
type HandoffSpan struct {
	ctx      context.Context
	span     trace.Span
	finished bool
}

// StartHandoffSpan starts a context-handoff span: the model request that
// writes the handoff is its child. openTodos goes on its log.
func StartHandoffSpan(ctx context.Context, openTodos int) (context.Context, *HandoffSpan) {
	ctx, span := Tracer().Start(ctx, SpanHandoff, trace.WithAttributes(identity(ctx)...))
	EmitEvent(ctx, SpanHandoff, attribute.Int(EventAttrOpenTodos, openTodos))
	return ctx, &HandoffSpan{ctx: ctx, span: span}
}

// End ends the handoff span and records the handoff metric.
// outcome is HandoffOutcomeOK, HandoffOutcomeFallback, or HandoffOutcomeError.
func (s *HandoffSpan) End(outcome string, err error) {
	if s.finished {
		return
	}
	s.finished = true
	if err != nil {
		s.span.RecordError(err)
		s.span.SetAttributes(attribute.String(AttrErrorType, ErrorClassOther))
		s.span.SetStatus(codes.Error, ErrorClassOther)
	}
	s.span.SetAttributes(attribute.String(AttrOutcome, outcome))
	s.span.End()
	Metrics().recordHandoff(s.ctx, AgentFromContext(s.ctx), outcome)
}
