package temporal

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// startTurn opens the primary tacklr.turn span from workflow code using
// Temporal's replay-safe Tracer. Static attrs are set at start; End records
// outcome. No-op when the global provider is not ReplaySafe (tests without Init).
func startTurn(ctx workflow.Context, agentID string, sessionID durable.SessionID, kind string) (workflow.Context, func(string, error)) {
	if kind == "" {
		kind = telemetry.TurnKindPrompt
	}
	if !telemetry.IsReplaySafeProvider() {
		return ctx, func(string, error) {}
	}
	tracer := temporalotel.Tracer(telemetry.InstrumentationName)
	ctx, span := tracer.Start(ctx, telemetry.SpanTurn, trace.WithAttributes(
		attribute.String(telemetry.AttrArea, telemetry.AreaRuntime),
		attribute.String(telemetry.AttrRuntime, telemetry.RuntimeTemporal),
		attribute.String(telemetry.AttrAgentID, agentID),
		attribute.String(telemetry.AttrSessionID, string(sessionID)),
		attribute.String(telemetry.AttrThreadID, string(sessionID)),
		attribute.String(telemetry.AttrTurnKind, kind),
	))
	started := workflow.Now(ctx)
	kindCopy, agentCopy := kind, agentID
	return ctx, func(outcome string, err error) {
		if outcome == "" {
			if err != nil {
				outcome = telemetry.OutcomeError
			} else {
				outcome = telemetry.OutcomeOK
			}
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, telemetry.ErrorClassOther)
		} else if outcome == telemetry.OutcomeCancelled {
			span.SetStatus(codes.Error, telemetry.OutcomeCancelled)
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.SetAttributes(attribute.String(telemetry.AttrOutcome, outcome))
		if !workflow.IsReplaying(ctx) {
			// Workflow context is not context.Context. Bind the OTel span onto a
			// Go context so RecordTurnOutcome uses the SDK meter (exemplars)
			// instead of a disconnected Background context. Replay is skipped
			// because metric export is a side effect.
			metricCtx := trace.ContextWithSpan(context.Background(), span)
			telemetry.InstrumentsFromContext(metricCtx).RecordTurnOutcome(
				metricCtx, agentCopy, kindCopy, outcome, workflow.Now(ctx).Sub(started),
			)
		}
		span.End()
	}
}

func logInfo(ctx workflow.Context, msg string, keyvals ...any) {
	workflow.GetLogger(ctx).Info(msg, keyvals...)
}

func logError(ctx workflow.Context, msg string, keyvals ...any) {
	workflow.GetLogger(ctx).Error(msg, keyvals...)
}
