package orchestrator

import "github.com/ryanaldo34/tacklr/session"

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
