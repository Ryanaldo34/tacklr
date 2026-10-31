package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ToolSpan is an in-flight tacklr.tool span. Call Finish once.
type ToolSpan struct {
	ctx      context.Context
	span     trace.Span
	start    time.Time
	name, ns string
	finished bool
}

// StartToolSpan starts a child tool span.
func StartToolSpan(ctx context.Context, name, namespace string) (context.Context, *ToolSpan) {
	start := time.Now()
	ctx, span := Tracer().Start(ctx, SpanTool,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaHarness),
			attribute.String(AttrToolName, name),
			attribute.String(AttrToolNS, namespace),
		),
	)
	return ctx, &ToolSpan{ctx: ctx, span: span, start: start, name: name, ns: namespace}
}

// Finish ends the tool span and records metrics.
// status is success, error, interrupt, or similar.
func (t *ToolSpan) Finish(status string, err error) {
	if t.finished {
		return
	}
	t.finished = true
	attrs := []attribute.KeyValue{
		attribute.String(AttrToolStatus, status),
	}
	if err != nil || status == "error" {
		attrs = append(attrs, attribute.String(AttrOutcome, OutcomeError))
		if err != nil {
			t.span.RecordError(err)
			t.span.SetStatus(codes.Error, ErrorClassOther)
		}
	} else {
		attrs = append(attrs, attribute.String(AttrOutcome, OutcomeOK))
		t.span.SetStatus(codes.Ok, "")
	}
	t.span.SetAttributes(attrs...)
	t.span.End()
	InstrumentsFromContext(t.ctx).RecordTool(
		t.ctx,
		AgentIDFromContext(t.ctx),
		t.name,
		t.ns,
		status,
		time.Since(t.start),
	)
}

// PlanInstallSpan is an in-flight tacklr.plan.install span. Call End once.
type PlanInstallSpan struct {
	span     trace.Span
	finished bool
}

// StartPlanInstallSpan starts a plan-document install span.
func StartPlanInstallSpan(ctx context.Context, sessionID string) (context.Context, *PlanInstallSpan) {
	ctx, span := Tracer().Start(ctx, SpanPlanInstall,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaContext),
			attribute.String(AttrSessionID, sessionID),
		),
	)
	return ctx, &PlanInstallSpan{span: span}
}

// End ends the span. err nil means ok; non-nil means error.
func (s *PlanInstallSpan) End(err error) {
	if s.finished {
		return
	}
	s.finished = true
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, ErrorClassOther)
		s.span.SetAttributes(attribute.String(AttrOutcome, OutcomeError))
	} else {
		s.span.SetStatus(codes.Ok, "")
		s.span.SetAttributes(attribute.String(AttrOutcome, OutcomeOK))
	}
	s.span.End()
}

// HandoffSpan is an in-flight tacklr.context.handoff span. Call End once.
type HandoffSpan struct {
	ctx      context.Context
	span     trace.Span
	finished bool
}

// StartHandoffSpan starts a context-handoff span. openTodos is remaining work.
func StartHandoffSpan(ctx context.Context, openTodos int) (context.Context, *HandoffSpan) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrArea, AreaModelTasks),
		attribute.Int(AttrOpenTodos, openTodos),
	}
	if sid := SessionIDFromContext(ctx); sid != "" {
		attrs = append(attrs, attribute.String(AttrSessionID, sid))
	}
	ctx, span := Tracer().Start(ctx, SpanContextHandoff, trace.WithAttributes(attrs...))
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
		s.span.SetStatus(codes.Error, ErrorClassOther)
	} else {
		s.span.SetStatus(codes.Ok, "")
	}
	s.span.SetAttributes(attribute.String(AttrOutcome, outcome))
	s.span.End()
	InstrumentsFromContext(s.ctx).RecordHandoff(s.ctx, AgentIDFromContext(s.ctx), outcome)
}
