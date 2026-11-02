package adapter

import (
	"fmt"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
)

// OverlaySpecialist copies the named specialist onto the parent agent.
func OverlaySpecialist(parent tacklr.AgentOptions, specialist string) (tacklr.AgentOptions, error) {
	spec := tacklr.FindSpecialist(parent.Specialists, specialist)
	if spec == nil {
		return tacklr.AgentOptions{}, fmt.Errorf("%w: specialist %q", tacklr.ErrNotFound, specialist)
	}
	out := parent.WithSpecialist(spec)
	return tacklr.BindTurn(out, "", nil), nil
}

// JobSteer is the RoleUser inbox text when a non-blocking job reaches
// complete or failed. It is a new message, not a second RoleTool for the
// schedule call_id.
func JobSteer(id, name, body string, failed bool) *tacklr.Message {
	verb := "completed"
	if failed {
		verb = "failed"
	}
	return &tacklr.Message{
		Role:    tacklr.RoleUser,
		Content: fmt.Sprintf("Job %s (%s) %s:\n%s", id, name, verb, body),
	}
}

// ChildJobMessage is JobSteer for a nested session job.
func ChildJobMessage(st session.SessionStatus) *tacklr.Message {
	body := st.Result
	failed := st.State == session.SessionFailed
	if failed && body == "" && st.Err != nil {
		body = st.Err.Error()
	}
	return JobSteer(string(st.ID), st.Specialist, body, failed)
}
