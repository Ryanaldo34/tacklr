package drive

import (
	"context"

	"github.com/ryanaldo34/tacklr/streaming"
)

// InferenceStep is the result of one model invocation for the durable driver.
type InferenceStep struct {
	ToolCalls []streaming.ToolCall
	Complete  bool
}

// ToolStep is the result of one tool invocation for the durable driver.
// Interrupted means the tool parked; the driver must persist, publish yield,
// and wait for Resume. It must not block inside the tool function.
type ToolStep struct {
	Interrupted   bool
	InterruptID   string
	InterruptData []byte
}

// TurnState is per-slice counters for the durable inference loop.
type TurnState struct {
	ModelRequests int
	HadToolRound  bool
}

// Engine is the durable-runtime view of a harness. Hosts use Run /
// RunMessage / ReturnFromInterrupt; only in-repo drivers bind this.
type Engine interface {
	AbsorbUser(ctx context.Context, user *streaming.Message, out chan streaming.StreamEvent) error
	PendingToolCalls() []streaming.ToolCall
	RunInference(ctx context.Context, st *TurnState, out chan streaming.StreamEvent) (InferenceStep, error)
	RunToolCall(ctx context.Context, tc streaming.ToolCall, out chan streaming.StreamEvent) (ToolStep, error)
	ApplyResume(finishedInterrupts map[string][]byte) error
	// RecordToolResult appends a RoleTool message without executing (Temporal
	// after a child workflow already ran).
	RecordToolResult(tc streaming.ToolCall, output string)
	// SetChildHost installs nested-session child operations for tools.
	// Nil: child methods fail. h must implement tacklr childHost.
	SetChildHost(h any)
}

var bound func(any) Engine

// Bind installs the root-package adapter. tacklr.init calls this once.
func Bind(fn func(any) Engine) {
	if bound != nil {
		panic("drive: already bound")
	}
	bound = fn
}

// EngineOf returns the durable driver for a harness constructed by tacklr.
func EngineOf(h any) Engine {
	if bound == nil {
		panic("drive: not bound")
	}
	return bound(h)
}

const streamEventBuffer = 64

// PipeStreamEvents copies channel events to emit. Durable backends adapt
// emit callbacks to the harness chan StreamEvent API.
func PipeStreamEvents(emit func(streaming.StreamEvent)) (chan streaming.StreamEvent, func()) {
	out := make(chan streaming.StreamEvent, streamEventBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range out {
			if emit != nil {
				emit(ev)
			}
		}
	}()
	return out, func() {
		close(out)
		<-done
	}
}
