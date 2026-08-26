package tacklr

import (
	"context"

	"github.com/ryanaldo34/tacklr/internal/drive"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

func init() {
	interrupt.RegisterDefaults()
	if err := adapters.RegisterCommon(vfs.DefaultContentRegistry()); err != nil {
		panic("tacklr: register common content codecs: " + err.Error())
	}
	drive.Bind(func(h any) drive.Engine {
		ah, ok := h.(*AgentHarness)
		if !ok {
			panic("drive: harness is not *AgentHarness")
		}
		return harnessDrive{ah}
	})
}

// harnessDrive is the durable-runtime adapter. It lives here so driver
// methods can stay unexported on AgentHarness.
type harnessDrive struct{ a *AgentHarness }

func (e harnessDrive) AbsorbUser(ctx context.Context, user *streaming.Message, out chan streaming.StreamEvent) error {
	return e.a.absorbUser(ctx, user, out)
}

func (e harnessDrive) PendingToolCalls() []streaming.ToolCall {
	return e.a.runnableToolCalls()
}

func (e harnessDrive) RecordToolResult(tc streaming.ToolCall, output string) {
	e.a.recordToolResult(tc, output)
}

func (e harnessDrive) RunInference(ctx context.Context, st *drive.TurnState, out chan streaming.StreamEvent) (drive.InferenceStep, error) {
	return e.a.runInference(ctx, st, out)
}

func (e harnessDrive) RunToolCall(ctx context.Context, tc streaming.ToolCall, out chan streaming.StreamEvent) (drive.ToolStep, error) {
	return e.a.runToolCall(ctx, tc, out)
}

func (e harnessDrive) ApplyResume(finishedInterrupts map[string][]byte) error {
	e.a.runMu.Lock()
	defer e.a.runMu.Unlock()
	return e.a.applyResume(finishedInterrupts)
}

func (e harnessDrive) SetChildHost(h any) {
	if h == nil {
		e.a.childHost = nil
		return
	}
	host, ok := h.(childHost)
	if !ok {
		panic("drive: child host")
	}
	e.a.childHost = host
}
