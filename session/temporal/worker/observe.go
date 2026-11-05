package worker

import (
	"context"

	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// startTurn opens tacklr.turn with Temporal's replay-safe Tracer (OTEL v2).
// Turn totals are recorded on the non-replay path only so the in-flight gauge
// is not double-counted (see Instruments.RecordTurnOutcome).
func startTurn(ctx workflow.Context, agentID string, sessionID session.ID, kind string) (workflow.Context, func(string, error)) {
	started := workflow.Now(ctx)
	ctx, span := startTurnSpan(ctx,
		attribute.String(telemetry.AttrArea, telemetry.AreaRuntime),
		attribute.String(telemetry.AttrRuntime, telemetry.RuntimeTemporal),
		attribute.String(telemetry.AttrAgentID, agentID),
		attribute.String(telemetry.AttrSessionID, string(sessionID)),
		attribute.String(telemetry.AttrThreadID, string(sessionID)),
		attribute.String(telemetry.AttrTurnKind, kind),
	)
	return ctx, func(outcome string, err error) {
		// A cancelled turn ends with context.Canceled; its status says
		// cancelled, not a failure of some other class.
		switch {
		case outcome == telemetry.OutcomeCancelled:
			span.SetStatus(codes.Error, telemetry.OutcomeCancelled)
		case err != nil:
			span.RecordError(err)
			span.SetStatus(codes.Error, telemetry.ErrorClassOther)
		default:
			span.SetStatus(codes.Ok, "")
		}
		span.SetAttributes(attribute.String(telemetry.AttrOutcome, outcome))
		span.End()
		if workflow.IsReplaying(ctx) {
			return
		}
		telemetry.InstrumentsFromContext(context.Background()).RecordTurnOutcome(
			context.Background(), agentID, kind, outcome, workflow.Now(ctx).Sub(started),
		)
	}
}

// startTurnSpan opens tacklr.turn with Temporal's replay-safe tracer when
// the host installed its provider (temporalotel.NewReplaySafeTracerProvider).
// Otherwise the span records nothing: a span of any other provider is not
// replay-safe, and tracing is the host's choice.
func startTurnSpan(ctx workflow.Context, attrs ...attribute.KeyValue) (workflow.Context, trace.Span) {
	if _, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider); !ok {
		return ctx, trace.SpanFromContext(context.Background())
	}
	return temporalotel.Tracer(telemetry.InstrumentationName).Start(ctx, telemetry.SpanTurn, trace.WithAttributes(attrs...))
}
