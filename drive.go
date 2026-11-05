package tacklr

import (
	"slices"
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

// Round is a reply and its calls, in call order. A round commits when each
// call has its result; a call without one then is cancelled.
type Round struct {
	Reply Reply  `json:"reply"`
	Calls []Call `json:"calls,omitempty"`
}

// NewRound opens the round of a reply with calls.
func NewRound(reply Reply) *Round {
	r := &Round{Reply: reply, Calls: make([]Call, len(reply.Calls))}
	for i, tc := range reply.Calls {
		r.Calls[i] = NewCall(tc, reply.Timeout(i))
	}
	return r
}

// Parked is the index of the call that waits on its park, or -1. A round
// has at most one: when several calls of a reply park, the turn keeps the
// first park in call order; the others run again after resume, so the user
// answers one interrupt at a time.
func (r *Round) Parked() int {
	return slices.IndexFunc(r.Calls, Call.Parked)
}

// Complete reports whether each call has its result.
func (r *Round) Complete() bool {
	return !slices.ContainsFunc(r.Calls, func(c Call) bool { return !c.Done })
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
