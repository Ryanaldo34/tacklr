package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// Turn is the static identity of one turn's span: invoke_agent of the
// session's agent (a specialist's name for a specialist session).
type Turn struct {
	Agent   string
	Session string
	// Parent is the parent session of a child session.
	Parent string
	// Kind is prompt, resume or continue (a turn a runtime restarted).
	Kind    string
	Runtime string
}

// TurnSpan is a turn's running invoke_agent span. End it once.
type TurnSpan struct {
	ctx     context.Context
	span    trace.Span
	turn    Turn
	started time.Time
}

// StartTurn starts a turn's span as a child of the span in ctx (the
// client's request, or the parent session's turn), with a link to each
// span of links (the request that resumed it), and counts it as running.
// A durable runtime starts it once for each turn it runs, and records the
// span's context (Inject) so a replay of the turn reuses it without
// starting another.
func StartTurn(ctx context.Context, t Turn, links ...Carrier) (context.Context, *TurnSpan) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrGenAIOperationName, OperationInvokeAgent),
		attribute.String(AttrGenAIAgentName, t.Agent),
		attribute.String(AttrGenAIConversationID, t.Session),
		attribute.String(AttrTurnKind, t.Kind),
		attribute.String(AttrRuntime, t.Runtime),
	}
	if t.Parent != "" {
		attrs = append(attrs, attribute.String(AttrParentSession, t.Parent))
	}
	opts := []trace.SpanStartOption{trace.WithAttributes(attrs...)}
	for _, c := range links {
		if l, ok := c.Link(); ok {
			opts = append(opts, trace.WithLinks(l))
		}
	}
	ctx = ContextWithAgent(ContextWithSessionID(ctx, t.Session), t.Agent)
	ctx, span := Tracer().Start(ctx, spanName(OperationInvokeAgent, t.Agent), opts...)
	Metrics().turnStarted(ctx, t.Agent, t.Kind)
	return ctx, &TurnSpan{ctx: ctx, span: span, turn: t, started: time.Now()}
}

// End ends the turn's span with outcome (ok, error, cancelled, yield) and
// the error that ended it, and counts the turn by its outcome.
func (s *TurnSpan) End(outcome string, err error) {
	switch {
	case outcome == OutcomeCancelled:
		s.span.SetAttributes(attribute.String(AttrErrorType, ErrorClassCancelled))
		s.span.SetStatus(codes.Error, OutcomeCancelled)
	case err != nil:
		s.span.SetAttributes(attribute.String(AttrErrorType, ErrorClassOther))
		s.span.SetStatus(codes.Error, ErrorClassOther)
		// The error's text is dynamic: a log of the turn's span keeps it.
		EmitEventSeverity(s.ctx, EventTurnEnded, log.SeverityError, attribute.String(EventAttrError, err.Error()))
	}
	s.span.SetAttributes(attribute.String(AttrOutcome, outcome))
	s.span.End()
	Metrics().turnEnded(s.ctx, s.turn.Agent, s.turn.Kind, outcome, time.Since(s.started))
}

// TurnStarted logs a turn's start with its dynamic values: the prompt's
// length and how many answers it resumed with.
func TurnStarted(ctx context.Context, promptLen, resumes int) {
	EmitEvent(ctx, EventTurnStarted,
		attribute.Int(EventAttrPromptLength, promptLen),
		attribute.Int(EventAttrResumes, resumes),
	)
}

// spanName is a GenAI span's name: its operation and its target, when it
// has one.
func spanName(operation, target string) string {
	if target == "" {
		return operation
	}
	return operation + " " + target
}
