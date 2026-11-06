package telemetry

import (
	"context"
)

// Step is the context one step of a turn runs in: the turn's span (from
// turn, the context its runtime recorded), the session and its agent, so
// the step's spans are children of the turn's span and carry the session.
func Step(ctx context.Context, turn Carrier, agent, sessionID string) context.Context {
	return ContextWithAgent(ContextWithSessionID(turn.Context(ctx), sessionID), agent)
}

// RecordCheckpointAttempt records one harness snapshot persist (ok or error).
func RecordCheckpointAttempt(ctx context.Context, err error) {
	outcome := OutcomeOK
	if err != nil {
		outcome = OutcomeError
	}
	Metrics().RecordCheckpointSave(ctx, outcome)
}
