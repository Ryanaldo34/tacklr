package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/log"
)

// Instrumentor starts the primary tacklr.turn span. Durable wait-loops call
// this — the workflow (or in-process equivalent) is the instrumentor, not the
// activity/harness internals.
//
// context.Context runtimes (in-process, embedder, Azure/Lambda activities)
// implement this directly. Temporal workflows cannot: they must use the
// replay-safe adapter in durable/temporal (workflow.Context + OTEL v2 Tracer).
type Instrumentor interface {
	StartTurn(ctx context.Context, attrs TurnAttrs) (context.Context, *TurnSpan)
}

type defaultInstrumentor struct{}

// DefaultInstrumentor starts tacklr.turn via StartTurnSpan (global or
// context-attached Tracer).
func DefaultInstrumentor() Instrumentor { return defaultInstrumentor{} }

func (defaultInstrumentor) StartTurn(ctx context.Context, attrs TurnAttrs) (context.Context, *TurnSpan) {
	return StartTurnSpan(ctx, attrs)
}

// BindTurnContext attaches agent/session identity and the process Instruments
// so child spans (model, tool, brain) inherit metrics labels and ids.
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
