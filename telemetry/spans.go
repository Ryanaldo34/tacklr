package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// TurnSpan is the root tacklr.turn span. Call End once.
type TurnSpan struct {
	ctx               context.Context
	span              trace.Span
	start             time.Time
	agentID, turnKind string
	finished          bool
}

// TurnAttrs are static attributes set when a turn starts.
type TurnAttrs struct {
	AgentID     string
	ThreadID    string
	SessionID   string
	Kind        string // prompt | resume
	LoadSession bool
}

// StartTurnSpan starts the root turn span and records turn-active.
// Uses TracerFromContext (set ContextWithTracer first).
func StartTurnSpan(ctx context.Context, a TurnAttrs) (context.Context, *TurnSpan) {
	if a.Kind == "" {
		a.Kind = "prompt"
	}
	ctx = ContextWithAgentID(ctx, a.AgentID)
	start := time.Now()
	ctx, span := TracerFromContext(ctx).Start(ctx, SpanTurn,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaRegistry),
			attribute.String(AttrAgentID, a.AgentID),
			attribute.String(AttrThreadID, a.ThreadID),
			attribute.String(AttrSessionID, a.SessionID),
			attribute.String(AttrTurnKind, a.Kind),
			attribute.Bool(AttrLoadSession, a.LoadSession),
		),
	)
	InstrumentsFromContext(ctx).RecordTurnStart(ctx, a.AgentID)
	return ctx, &TurnSpan{
		ctx:      ctx,
		span:     span,
		start:    start,
		agentID:  a.AgentID,
		turnKind: a.Kind,
	}
}

// End ends the turn span, emits turn.ended, and records metrics.
// outcome is OutcomeOK, OutcomeError, or OutcomeCancelled; empty derives from err.
func (t *TurnSpan) End(outcome string, err error) {
	if t == nil || t.finished {
		return
	}
	t.finished = true
	if outcome == "" {
		if err != nil {
			outcome = OutcomeError
		} else {
			outcome = OutcomeOK
		}
	}
	if t.span != nil {
		if err != nil {
			t.span.RecordError(err)
			t.span.SetStatus(codes.Error, err.Error())
		} else if outcome == OutcomeCancelled {
			t.span.SetStatus(codes.Error, outcome)
		} else {
			t.span.SetStatus(codes.Ok, "")
		}
		t.span.SetAttributes(attribute.String(AttrOutcome, outcome))
		t.span.End()
	}
	EmitEvent(t.ctx, EventTurnEnded, log.String(EventAttrOutcome, outcome))
	InstrumentsFromContext(t.ctx).RecordTurnEnd(
		t.ctx, t.agentID, t.turnKind, outcome, time.Since(t.start),
	)
}

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
	ctx, span := TracerFromContext(ctx).Start(ctx, SpanTool,
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
	if t == nil || t.finished {
		return
	}
	t.finished = true
	if t.span == nil {
		return
	}
	if status == "" {
		if err != nil {
			status = "error"
		} else {
			status = "success"
		}
	}
	attrs := []attribute.KeyValue{
		attribute.String(AttrToolStatus, status),
	}
	if err != nil || status == "error" {
		attrs = append(attrs, attribute.String(AttrOutcome, OutcomeError))
		if err != nil {
			t.span.RecordError(err)
			t.span.SetStatus(codes.Error, err.Error())
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
	ctx, span := TracerFromContext(ctx).Start(ctx, SpanPlanInstall,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaContext),
			attribute.String(AttrSessionID, sessionID),
		),
	)
	return ctx, &PlanInstallSpan{span: span}
}

// End ends the span. err nil means ok; non-nil means error.
func (s *PlanInstallSpan) End(err error) {
	if s == nil || s.finished {
		return
	}
	s.finished = true
	if s.span == nil {
		return
	}
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
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
	ctx, span := TracerFromContext(ctx).Start(ctx, SpanContextHandoff,
		trace.WithAttributes(
			attribute.String(AttrArea, AreaModelTasks),
			attribute.Int(AttrOpenTodos, openTodos),
		),
	)
	return ctx, &HandoffSpan{ctx: ctx, span: span}
}

// End ends the handoff span and records the handoff metric.
// outcome is HandoffOutcomeOK, HandoffOutcomeFallback, or HandoffOutcomeError.
func (s *HandoffSpan) End(outcome string, err error) {
	if s == nil || s.finished {
		return
	}
	s.finished = true
	if s.span == nil {
		return
	}
	if outcome == "" {
		if err != nil {
			outcome = HandoffOutcomeError
		} else {
			outcome = HandoffOutcomeOK
		}
	}
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	} else {
		s.span.SetStatus(codes.Ok, "")
	}
	s.span.SetAttributes(attribute.String(AttrOutcome, outcome))
	s.span.End()
	InstrumentsFromContext(s.ctx).RecordHandoff(s.ctx, AgentIDFromContext(s.ctx), outcome)
}
