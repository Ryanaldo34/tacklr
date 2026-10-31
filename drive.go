package tacklr

import (
	"context"
)

// InferenceStep is one model reply. Complete is a reply with no tool calls,
// and Result is its text. Otherwise the reply opened a round.
type InferenceStep struct {
	Complete bool
	Result   string
}

// ToolStep is one call RunTools ran. Interrupted means the call parked:
// the runtime yields the turn and waits for Resume. AwaitJobID means the
// call waits on that child session; RecordToolResult gives it its result.
// Starts and Cancels are the children the runtime starts and cancels.
type ToolStep struct {
	Call          ToolCall
	Interrupted   bool
	InterruptID   string
	InterruptData []byte
	AwaitJobID    string
	Starts        []JobStart
	Cancels       []string
}

// Engine is the session-runtime view of a TurnManager.
type Engine interface {
	// AbsorbUser commits a user message as a round. Calls of an open round
	// without a result are cancelled first.
	AbsorbUser(ctx context.Context, user *Message, out chan StreamEvent) error
	// RunInference calls the model once. A reply with tool calls opens a
	// round.
	RunInference(ctx context.Context, out chan StreamEvent) (InferenceStep, error)
	// RunTools is Tools step seq: it applies resume (the answers to a park),
	// runs the open round's runnable calls concurrently, and records them in
	// call order. It returns one step per call that step seq ran, from the
	// round, so a retried step reports what the first attempt did. The
	// first call in call order that parks parks the turn; any other parked
	// call stays runnable and runs after resume. The round commits when each
	// call has a result.
	RunTools(ctx context.Context, seq int, resume map[string][]byte, state map[string]any, out chan StreamEvent) ([]ToolStep, error)
	// RecordToolResult gives an awaited child's output to tc as its result,
	// with status error when the child failed, and commits the round when
	// that was its last result.
	RecordToolResult(ctx context.Context, tc ToolCall, output string, failed bool, out chan StreamEvent) error
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

// Action is the turn loop's next step.
type Action int

const (
	ActionInfer Action = iota
	ActionRunTools
	ActionYield
	ActionComplete
	ActionWait
)

// Next chooses the turn loop's next step: run an open round's calls, yield
// on a park, wait for jobs or complete once the model is done, else call the
// model.
func Next(runnable, parked, inferComplete, jobsRemain bool) Action {
	if runnable {
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
