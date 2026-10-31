package tacklr

import (
	"context"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

func init() {
	RegisterDefaults()
	_ = adapters.RegisterCommon(vfs.DefaultContentRegistry())
}

// Drive is the turn-step API a durable runtime calls after NewTurnManager.
func (a *TurnManager) Drive() Engine { return turnDrive{a} }

// turnDrive keeps the step methods unexported on TurnManager.
type turnDrive struct{ a *TurnManager }

func (e turnDrive) AbsorbUser(ctx context.Context, user *Message, out chan StreamEvent) error {
	return e.a.absorbUser(ctx, user, out)
}

func (e turnDrive) RunInference(ctx context.Context, out chan StreamEvent) (InferenceStep, error) {
	return e.a.runInference(ctx, out)
}

func (e turnDrive) RunTools(ctx context.Context, seq int, resume map[string][]byte, state map[string]any, out chan StreamEvent) ([]ToolStep, error) {
	return e.a.runTools(ctx, seq, resume, state, out)
}

func (e turnDrive) RecordToolResult(ctx context.Context, tc ToolCall, output string, failed bool, out chan StreamEvent) error {
	return e.a.recordToolResult(ctx, tc, output, failed, out)
}
