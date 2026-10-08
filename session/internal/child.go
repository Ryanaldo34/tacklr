package adapter

import (
	"fmt"
	"strings"

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

// NormalizeSpawn trims spawn_specialist arguments.
// Specialist is required; empty task allowed for idempotent retry of the same callID.
func NormalizeSpawn(specialist, task string) (string, string, error) {
	specialist = strings.TrimSpace(specialist)
	task = strings.TrimSpace(task)
	if specialist == "" {
		return "", "", fmt.Errorf("specialist is required: %w", tacklr.ErrInvalid)
	}
	return specialist, task, nil
}

// HasSpecialist reports whether the agent defines this assistant.
func HasSpecialist(agent tacklr.AgentOptions, name string) bool {
	return tacklr.FindSpecialist(agent.Specialists, name) != nil
}

// UnknownChild is cancel/wait with an id that is not this session's job.
func UnknownChild(id string) error {
	return fmt.Errorf("job %q is unknown; call list_children and use an id from that list: %w", id, tacklr.ErrNotFound)
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
