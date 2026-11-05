package loop

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// ErrSuspended is Run's error when a suspend stopped the session: the
// runtime restarts it from the Turn as Run left it.
var ErrSuspended = errors.New("session suspended")

// Run drives one session lifetime over d from the state in t, which the
// caller keeps reading (a status query) while Run changes it. A task
// session (Identity.Task) runs its first prompt, its task, and returns once
// that turn ended and is no longer parked; any other session returns when
// it is closed (t.Closed). Either way Run then stops the jobs that still
// run. A suspend returns ErrSuspended, and the jobs go on.
func Run(d Executor, t *Turn) (string, error) {
	r := &runner{turn: t}
	result, err := r.run(d)
	if r.suspended {
		return "", ErrSuspended
	}
	for _, j := range t.Jobs {
		if !j.Ended {
			d.Cancel(j.ID)
		}
	}
	return result, err
}

// runner is the state of one Run besides its Turn.
type runner struct {
	turn *Turn
	// running is set while a turn drives.
	running bool
	// suspended says a suspend stopped the session.
	suspended bool
}

func (s *runner) run(d Executor) (string, error) {
	if s.turn.Suspended {
		// The turn the runtime suspended goes on from where it stopped.
		d.Begin("continue", 0, 0)
		s.turn.Suspended = false
		s.drive(d, nil, nil)
		d.End()
	}
	for !s.turn.Closed && !s.suspended && !s.taskDone() {
		s.handle(s.next(d), d)
	}
	if s.turn.Task {
		return s.taskResult()
	}
	return s.turn.Result, nil
}

// taskDone reports whether a task session's turn ended: it completed or
// failed, and is not parked.
func (s *runner) taskDone() bool {
	return s.turn.Task && s.turn.Terminal != "" && !s.turn.Yielded
}

// next is the next wake to handle while no turn runs: a held one first,
// else the driver's, blocking.
func (s *runner) next(d Executor) session.Wake {
	if t := s.turn; len(t.held) > 0 {
		w := t.held[0]
		t.held = t.held[1:]
		return w
	}
	return d.Next(true)
}

// pump handles every wake the driver has queued, without blocking; with
// block, it first waits for one.
func (s *runner) pump(d Executor, block bool) {
	for w := d.Next(block); w.Kind != ""; w = d.Next(false) {
		s.handle(w, d)
	}
}

// taskResult is a child's outcome: its result, or the error its turn
// failed with, so the parent sees why.
func (s *runner) taskResult() (string, error) {
	if s.turn.Terminal != session.Failed {
		return s.turn.Result, nil
	}
	return "", s.turn.Err
}

// handle is what one wake does, given the session's state: the one place
// that decides it.
//
//   - A job's end, in any state, is its waiting call's result or, for a
//     background job, an inbox message (Turn.jobEnded).
//   - A prompt steers the turn when one runs (also while it waits on jobs)
//     and while it is parked; when idle it starts a turn (a worker session
//     runs its job).
//   - A cancel stops the running turn, fails a parked one, and stops the
//     session's jobs when idle.
//   - A resume or a close waits for a running turn to end; a resume then
//     runs the parked turn, a close ends the session.
//   - A suspend stops a running turn mid-way, or the idle session.
func (s *runner) handle(w session.Wake, d Executor) {
	t := s.turn
	switch w.Kind {
	case session.WakeJob:
		t.jobEnded(w.Job)
	case session.WakeSuspend:
		if s.running {
			t.Suspended = true
		} else {
			s.suspended = true
		}
	case session.WakeCancel:
		if s.running {
			t.cancelled = true
		} else {
			s.cancel(d)
		}
	case session.WakePrompt:
		switch {
		case s.running || t.Yielded:
			t.Steer(w.Prompt)
		case t.Worker != "":
			s.runWorker(d, UserMessage(w.Prompt.Text, w.Prompt.UserMessage).Content)
		default:
			s.applyQueued(w.Prompt)
			s.runTurn(d, session.WakePrompt, UserMessage(w.Prompt.Text, w.Prompt.UserMessage), nil, w.Prompt.Auth, w.Prompt.State)
		}
	case session.WakeResume:
		if s.running {
			t.held = append(t.held, w)
			return
		}
		t.Yielded = false
		s.runTurn(d, session.WakeResume, nil, w.Resume.Responses, w.Resume.Auth, w.Resume.State)
	case session.WakeClose:
		if s.running {
			t.held = append(t.held, w)
			return
		}
		t.Yielded = false
		t.Closed = true
	}
}

// applyQueued installs an MCP overlay on the next idle turn.
// A prompt that arrives while the turn is parked stays on NextMCP until then.
func (s *runner) applyQueued(p session.Prompt) {
	if p.MCPServers != nil {
		s.turn.MCP = p.MCPServers
	} else if s.turn.NextMCP != nil {
		s.turn.MCP = s.turn.NextMCP
	}
	s.turn.NextMCP = nil
}

// cancel stops child sessions. Idle cancel does not emit: that event would
// be the first record the next prompt's subscriber reads. Parked cancel
// aborts the turn, so it does emit.
func (s *runner) cancel(d Executor) {
	s.turn.cancelJobs(d)
	s.turn.Inbox = nil
	s.turn.NextMCP = nil
	if !s.turn.Yielded {
		return
	}
	s.turn.Yielded = false
	s.turn.State = nil
	s.turn.Terminal, s.turn.Err = session.Failed, context.Canceled
	d.Emit(session.FailureEvent(context.Canceled))
}

func (s *runner) runTurn(d Executor, kind string, user *message.Message, resume map[string][]byte, auth session.AuthContext, extra map[string]any) {
	n := 0
	if user != nil {
		n = len(user.Content)
	}
	d.Begin(kind, n, len(resume))
	s.turn.start(user, resume, auth, extra)
	s.drive(d, user, resume)
	d.End()
}

// drive runs the turn with this session handling the wakes that come in
// while it runs.
func (s *runner) drive(d Executor, user *message.Message, resume map[string][]byte) {
	s.running = true
	s.turn.drive(d, user, resume, func(block bool) { s.pump(d, block) })
	s.running = false
	// A turn that ended or parked has nothing to go on with, even when a
	// suspend came in with the wakes that ended it: the session handles
	// what it held (a close, a resume) instead of suspending.
	if s.turn.Terminal != "" || s.turn.Yielded {
		s.turn.Suspended = false
	}
	s.suspended = s.turn.Suspended
}

func (s *runner) runWorker(d Executor, task string) {
	d.Begin(session.WakePrompt, len(task), 0)
	defer d.End()
	out, err := d.Job(s.turn.Worker, task)
	if err != nil {
		s.turn.Err = err
		s.turn.Terminal = session.Failed
		s.turn.Result = err.Error()
		d.Emit(session.FailureEvent(err))
		return
	}
	s.turn.Err = nil
	s.turn.Result = out
	s.turn.Terminal = session.Complete
	d.Emit(session.Event{Type: session.EventComplete})
}

// UserMessage is the prompt text, or the host-supplied user message.
func UserMessage(text string, msg *message.Message) *message.Message {
	if msg != nil {
		return msg
	}
	return &message.Message{Role: message.RoleUser, Content: text}
}
