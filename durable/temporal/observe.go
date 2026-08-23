package temporal

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// startTurn opens tacklr.turn with Temporal's replay-safe Tracer (OTEL v2).
func startTurn(ctx workflow.Context, agentID string, sessionID durable.SessionID, kind string) (workflow.Context, func(string, error)) {
	if kind == "" {
		kind = telemetry.TurnKindPrompt
	}
	ctx, span := temporalotel.Tracer(telemetry.InstrumentationName).Start(ctx, telemetry.SpanTurn, trace.WithAttributes(
		attribute.String(telemetry.AttrArea, telemetry.AreaRuntime),
		attribute.String(telemetry.AttrRuntime, telemetry.RuntimeTemporal),
		attribute.String(telemetry.AttrAgentID, agentID),
		attribute.String(telemetry.AttrSessionID, string(sessionID)),
		attribute.String(telemetry.AttrThreadID, string(sessionID)),
		attribute.String(telemetry.AttrTurnKind, kind),
	))
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
		span.End()
	}
}

func logInfo(ctx workflow.Context, msg string, keyvals ...any) {
	workflow.GetLogger(ctx).Info(msg, keyvals...)
}

func logError(ctx workflow.Context, msg string, keyvals ...any) {
	workflow.GetLogger(ctx).Error(msg, keyvals...)
}
