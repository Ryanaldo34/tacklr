package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/log"
)

// BindTurnContext attaches agent/session identity so child spans and metrics
// pick up labels from context.
func BindTurnContext(ctx context.Context, agentID, sessionID string) context.Context {
	ctx = ContextWithAgentID(ctx, agentID)
	ctx = ContextWithSessionID(ctx, sessionID)
	return ContextWithInstruments(ctx, MustInstruments(Meter()))
}

// EmitTurnReceived logs prompt.received or resume.received with dynamic sizes.
func EmitTurnReceived(ctx context.Context, kind string, promptLen, resumeCount int) {
	if kind == TurnKindResume {
		EmitEvent(ctx, EventResumeReceived, log.Int(EventAttrResumeInterruptCount, resumeCount))
		return
	}
	EmitEvent(ctx, EventPromptReceived, log.Int(EventAttrPromptLen, promptLen))
}

// RecordCheckpointAttempt records one harness snapshot persist (ok or error).
func RecordCheckpointAttempt(ctx context.Context, err error) {
	outcome := OutcomeOK
	if err != nil {
		outcome = OutcomeError
	}
	InstrumentsFromContext(ctx).RecordCheckpointSave(ctx, outcome)
}
