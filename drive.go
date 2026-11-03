package tacklr

import (
	"encoding/json"
	"time"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"
)

// Reply is one model reply: its messages (text, reasoning, and the message
// that carries its calls) and its calls. A reply without calls is complete,
// and Result is its text.
type Reply struct {
	Messages []*message.Message `json:"messages"`
	Calls    []message.ToolCall `json:"calls,omitempty"`
	Result   string             `json:"result,omitempty"`
	// Timeouts are each call's tool timeout (ToolConfig.Timeout), by call
	// index; zero is none. A runtime bounds the call's step by it.
	Timeouts []time.Duration `json:"timeouts,omitempty"`
}

// Timeout is call i's tool timeout, zero when it has none.
func (r Reply) Timeout(i int) time.Duration {
	if i < len(r.Timeouts) {
		return r.Timeouts[i]
	}
	return 0
}

// ToolRun is one run of one call. Prior is the call's last outcome, when it
// parked or waited on a child: Answer is the answer to its park, Child the
// result of its child.
type ToolRun struct {
	Call   message.ToolCall   `json:"call"`
	Prior  *CallOutcome       `json:"prior,omitempty"`
	Answer []byte             `json:"answer,omitempty"`
	Child  *session.JobResult `json:"child,omitempty"`
	// Children are the session's running children in start order. The
	// runtime holds them and sets them before the run.
	Children []string `json:"children,omitempty"`
	// MaxChildren is how many children the session may run at once; zero
	// is no limit. The runtime sets it from its own limits.
	MaxChildren int `json:"maxChildren,omitempty"`
	// Timeout is the call's tool timeout (Reply.Timeout).
	Timeout time.Duration `json:"timeout,omitempty"`
}

// CallOutcome is what one run of a call produced: a result (Done), a park,
// or a wait on a child (AwaitID), and the children it starts or cancels.
// Record is the harness's own part (the result message, its window effect,
// the park, completed OnCall layers, and the shared-state writes the call
// made); only Commit and the call's next run read it. Stored says the
// runtime moved Record into the session's outcome store, under the call
// key, so its history does not hold the result; a step that reads the
// outcome loads it back.
type CallOutcome struct {
	Call    message.ToolCall `json:"call"`
	Done    bool             `json:"done,omitempty"`
	Park    *Park            `json:"park,omitempty"`
	AwaitID string           `json:"awaitId,omitempty"`
	Starts  []JobStart       `json:"starts,omitempty"`
	Cancels []string         `json:"cancels,omitempty"`
	Record  json.RawMessage  `json:"record,omitempty"`
	Stored  bool             `json:"stored,omitempty"`
}

// Park is a call's interrupt as the client sees it. InterruptID is the call
// id; Resume answers it by that id.
type Park struct {
	InterruptID string `json:"interruptId"`
	Data        []byte `json:"data,omitempty"`
}

// Round is a reply and an outcome per call, in call order. A round commits
// when each call has its result; a call without one then is cancelled.
type Round struct {
	Reply    Reply         `json:"reply"`
	Outcomes []CallOutcome `json:"outcomes,omitempty"`
}

// NewRound opens the round of a reply with calls.
func NewRound(reply Reply) *Round {
	return &Round{Reply: reply, Outcomes: make([]CallOutcome, len(reply.Calls))}
}

// Parked is the index of the call that waits on its park, or -1. A round
// has at most one: when several calls of a reply park, the turn keeps the
// first park in call order; the others keep no outcome and run again after
// resume, so the user answers one interrupt at a time.
func (r *Round) Parked() int {
	for i, o := range r.Outcomes {
		if o.Park != nil && !o.Done {
			return i
		}
	}
	return -1
}

// Complete reports whether each call has its result.
func (r *Round) Complete() bool {
	for _, o := range r.Outcomes {
		if !o.Done {
			return false
		}
	}
	return true
}

// CommitIn is what one Commit writes, in this order: host state, a round
// (its calls' state writes in call order), then user messages, each a round
// of its own.
type CommitIn struct {
	State map[string]any     `json:"state,omitempty"`
	Round *Round             `json:"round,omitempty"`
	Users []*message.Message `json:"users,omitempty"`
}

const streamEventBuffer = 64

// PipeEvents copies channel events to emit. Durable backends adapt
// emit callbacks to the harness chan session.Event API.
func PipeEvents(emit func(session.Event)) (chan session.Event, func()) {
	out := make(chan session.Event, streamEventBuffer)
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
