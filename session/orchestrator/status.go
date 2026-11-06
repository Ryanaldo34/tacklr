package orchestrator

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// Outcome is how the turn that ran last ended, for its span and metrics:
// parked (yield), cancelled, failed (error), or ok.
func (t *State) Outcome() string {
	switch {
	case t.Yielded:
		return telemetry.OutcomeYield
	case errors.Is(t.Err, context.Canceled):
		return telemetry.OutcomeCancelled
	case t.Terminal == session.Failed:
		return telemetry.OutcomeError
	}
	return telemetry.OutcomeOK
}

// Status is the session's status as the State holds it: running (waiting
// while parked), or the terminal state its turn or a close set. A runtime
// answers a status query with it.
func (t *State) Status() session.Status {
	id := t.Identity
	st := session.Status{
		ID:         t.SessionID,
		Parent:     id.Parent,
		Specialist: id.Specialist,
		State:      session.Running,
		Waiting:    t.Yielded,
		Result:     t.Result,
	}
	if id.Worker != "" {
		st.Kind = session.KindWorker
		st.Specialist = id.Worker
	} else if id.Specialist != "" {
		st.Kind = session.KindSpecialist
	}
	if t.Terminal != "" {
		st.State = t.Terminal
		st.Waiting = false
	} else if t.Closed {
		st.State = session.Complete
		st.Waiting = false
	}
	return st
}
