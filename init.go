package tacklr

import (
	"context"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

func init() {
	interrupt.RegisterDefaults()
	_ = adapters.RegisterCommon(vfs.DefaultContentRegistry())
}

// Drive is the turn-step API in-process and Temporal adapters call after
// NewTurnManager.
func (a *TurnManager) Drive() Engine { return turnDrive{a} }

// turnDrive lives here so driver methods can stay unexported on TurnManager.
type turnDrive struct{ a *TurnManager }

func (e turnDrive) AbsorbUser(ctx context.Context, user *Message, out chan StreamEvent) error {
	return e.a.absorbUser(ctx, user, out)
}

func (e turnDrive) PendingToolCalls() []ToolCall {
	return e.a.runnableToolCalls()
}

func (e turnDrive) RecordToolResult(tc ToolCall, output string) {
	e.a.recordToolResult(tc, output)
}

func (e turnDrive) RunInference(ctx context.Context, st *TurnState, out chan StreamEvent) (InferenceStep, error) {
	return e.a.runInference(ctx, st, out)
}

func (e turnDrive) RunToolCall(ctx context.Context, tc ToolCall, out chan StreamEvent) (ToolStep, error) {
	return e.a.runToolCall(ctx, tc, out)
}

func (e turnDrive) ApplyResume(finishedInterrupts map[string][]byte) error {
	e.a.runMu.Lock()
	defer e.a.runMu.Unlock()
	return e.a.applyResume(finishedInterrupts)
}

func (e turnDrive) Messages() []*Message {
	return e.a.context.Messages()
}
