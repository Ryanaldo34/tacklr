package adapter

import (
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
)

// OverlaySpecialist copies the parent catalog spec and applies the named Specialist.
func OverlaySpecialist(parent durable.AgentSpec, specialist string) (durable.AgentSpec, error) {
	spec := tacklr.FindSpecialist(parent.Options.Specialists, specialist)
	if spec == nil {
		return durable.AgentSpec{}, fmt.Errorf("%w: specialist %q", durable.ErrAgentNotFound, specialist)
	}
	out := parent
	out.Name = spec.Name
	out.Options = parent.Options.WithSpecialist(spec)
	out.Options.SessionID = ""
	out.Options.MountSession = nil
	out.Options.SkillsSession = nil
	return out, nil
}

// ChildState is the tool-facing running/completed/failed for a session.
func ChildState(st durable.SessionState) string {
	switch st {
	case durable.SessionComplete:
		return tacklr.JobCompleted
	case durable.SessionFailed:
		return tacklr.JobFailed
	default:
		return tacklr.JobRunning
	}
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

// HasSpecialist reports whether agentID's catalog spec names a specialist.
func HasSpecialist(cat durable.Catalog, agentID, name string) bool {
	if cat == nil {
		return false
	}
	spec, ok := cat.Lookup(agentID)
	return ok && tacklr.FindSpecialist(spec.Options.Specialists, name) != nil
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
func ChildJobMessage(st durable.SessionStatus) *tacklr.Message {
	body := st.Result
	failed := st.State == durable.SessionFailed
	if failed && body == "" && st.Err != nil {
		body = st.Err.Error()
	}
	return JobSteer(string(st.ID), st.Specialist, body, failed)
}
