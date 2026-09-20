package tacklr

import (
	"context"
)

// InferenceStep is the result of one model invocation for the durable driver.
type InferenceStep struct {
	ToolCalls []ToolCall
	Complete  bool
}

// ToolStep is the result of one tool invocation for the durable driver.
// Interrupted means the tool parked; the driver must persist, publish yield,
// and wait for Resume. AwaitJobID means the tool is still open: the durable
// loop waits for that job, then RecordToolResult. It must not block inside
// the Temporal Tool activity.
type ToolStep struct {
	Interrupted   bool
	InterruptID   string
	InterruptData []byte
	AwaitJobID    string
}

// TurnState is per-slice counters for the durable inference loop.
type TurnState struct {
	ModelRequests int
	HadToolRound  bool
}

// Engine is the durable-runtime view of a TurnManager.
type Engine interface {
	AbsorbUser(ctx context.Context, user *Message, out chan StreamEvent) error
	PendingToolCalls() []ToolCall
	RunInference(ctx context.Context, st *TurnState, out chan StreamEvent) (InferenceStep, error)
	RunToolCall(ctx context.Context, tc ToolCall, out chan StreamEvent) (ToolStep, error)
	ApplyResume(finishedInterrupts map[string][]byte) error
	// RecordToolResult appends a RoleTool message without executing (Temporal
	// after a child workflow already ran).
	RecordToolResult(tc ToolCall, output string)
	Messages() []*Message
}

const streamEventBuffer = 64

// PipeStreamEvents copies channel events to emit. Durable backends adapt
// emit callbacks to the harness chan StreamEvent API.
func PipeStreamEvents(emit func(StreamEvent)) (chan StreamEvent, func()) {
	out := make(chan StreamEvent, streamEventBuffer)
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

// Action is the wait-loop leftover/HITL decision. In-process and Temporal
// adapters interpret this; they do not fork leftover-tool rules.
type Action int

const (
	ActionInfer Action = iota
	ActionRunTools
	ActionYield
	ActionComplete
	ActionWait
)

// Next chooses the next wait-loop step from leftover tools, park, inference
// completion, and remaining jobs.
func Next(runnable int, parked bool, inferComplete bool, jobsRemain bool) Action {
	if runnable > 0 {
		return ActionRunTools
	}
	if parked {
		return ActionYield
	}
	if inferComplete {
		if jobsRemain {
			return ActionWait
		}
		return ActionComplete
	}
	return ActionInfer
}
