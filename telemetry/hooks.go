package telemetry

import (
	"context"
)

// BindTurnContext attaches agent/session identity so child spans and metrics
// pick up labels from context.
func BindTurnContext(ctx context.Context, agentID, sessionID string) context.Context {
	ctx = ContextWithAgentID(ctx, agentID)
	ctx = ContextWithSessionID(ctx, sessionID)
	return ContextWithInstruments(ctx, MustInstruments(Meter()))
}

// RecordCheckpointAttempt records one harness snapshot persist (ok or error).
func RecordCheckpointAttempt(ctx context.Context, err error) {
	outcome := OutcomeOK
	if err != nil {
		outcome = OutcomeError
	}
	InstrumentsFromContext(ctx).RecordCheckpointSave(ctx, outcome)
}
