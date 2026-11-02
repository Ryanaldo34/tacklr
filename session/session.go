package session

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr"
)

// Wake kinds from the Driver mailbox. A prompt or resume is also the turn-span kind.
const (
	WakePrompt = "prompt"
	WakeResume = "resume"
	WakeCancel = "cancel"
	WakeClose  = "close"
	WakeChild  = "child"
)

// Wake is one mailbox event. Child is set when Kind is WakeChild.
type Wake struct {
	Kind   string
	Prompt PromptIn
	Resume ResumeIn
	Child  *tacklr.Message
}

// Session is one durable session lifetime.
// Task is set when a child workflow is started with a prompt: that session
// runs one turn and returns. A parent has an empty Task and waits until Close.
type Session struct {
	Turn   Turn
	Task   string
	Closed bool
}

// Run drives the session until it returns. A parent returns when Close is
// signaled. A child returns when its task turn is no longer parked.
func (s *Session) Run(d Driver) (string, error) {
	if s.Turn.Worker != "" && s.Task != "" {
		s.runWorker(d, s.Task)
		return s.Turn.Result, s.Turn.Err
	}
	if s.Task != "" {
		s.runTurn(d, WakePrompt, UserMessage(s.Task, nil), nil, AuthContext{}, nil)
		for s.Turn.Yielded && !s.Closed {
			s.dispatch(d.Recv(), d)
		}
		return s.taskResult()
	}
	for !s.Closed {
		s.dispatch(d.Recv(), d)
	}
	return s.Turn.Result, nil
}

func (s *Session) taskResult() (string, error) {
	if s.Turn.Terminal != SessionFailed {
		return s.Turn.Result, nil
	}
	if s.Turn.Result != "" {
		return s.Turn.Result, errors.New(s.Turn.Result)
	}
	return "", errors.New("child failed")
}

func (s *Session) dispatch(w Wake, d Driver) {
	switch w.Kind {
	case WakeClose:
		s.Turn.Yielded = false
		s.Closed = true
	case WakeCancel:
		s.cancel(d)
	case WakePrompt:
		if s.Turn.Yielded {
			s.Turn.Steer(w.Prompt)
			return
		}
		if s.Turn.Worker != "" {
			user := UserMessage(w.Prompt.Text, w.Prompt.UserMessage)
			task := ""
			if user != nil {
				task = user.Content
			}
			s.runWorker(d, task)
			return
		}
		s.applyQueued(w.Prompt)
		s.runTurn(d, WakePrompt, UserMessage(w.Prompt.Text, w.Prompt.UserMessage), nil, w.Prompt.Auth, w.Prompt.State)
	case WakeResume:
		s.Turn.Yielded = false
		s.runTurn(d, WakeResume, nil, w.Resume.Responses, w.Resume.Auth, w.Resume.State)
	}
}

// applyQueued installs an MCP overlay on the next idle turn.
// A prompt that arrives while the turn is parked stays on NextMCP until then.
func (s *Session) applyQueued(p PromptIn) {
	if p.MCPServers != nil {
		s.Turn.MCP = p.MCPServers
	} else if s.Turn.NextMCP != nil {
		s.Turn.MCP = s.Turn.NextMCP
	}
	s.Turn.NextMCP = nil
}

// cancel stops child sessions. Idle cancel does not emit: that event would
// be the first record the next prompt's subscriber reads. Parked cancel
// aborts the turn, so it does emit.
func (s *Session) cancel(d Driver) {
	d.CancelAll()
	s.Turn.Inbox = nil
	s.Turn.NextMCP = nil
	if !s.Turn.Yielded {
		return
	}
	s.Turn.Yielded = false
	s.Turn.State = nil
	s.Turn.Terminal = SessionFailed
	msg := context.Canceled.Error()
	d.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
}

func (s *Session) runTurn(d Driver, kind string, user *tacklr.Message, resume map[string][]byte, auth AuthContext, extra map[string]any) {
	n := 0
	if user != nil {
		n = len(user.Content)
	}
	d.Begin(kind, n, len(resume))
	s.Turn.Run(d, user, resume, auth, extra)
	d.End()
}

func (s *Session) runWorker(d Driver, task string) {
	out, err := d.Job(s.Turn.Worker, task)
	if err != nil {
		s.Turn.Err = err
		s.Turn.Terminal = SessionFailed
		msg := err.Error()
		s.Turn.Result = msg
		d.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
		return
	}
	s.Turn.Err = nil
	s.Turn.Result = out
	s.Turn.Terminal = SessionComplete
	d.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventComplete})
}

// UserMessage is the prompt text, or the host-supplied user message.
func UserMessage(text string, msg *tacklr.Message) *tacklr.Message {
	if msg != nil {
		return msg
	}
	return &tacklr.Message{Role: tacklr.RoleUser, Content: text}
}
