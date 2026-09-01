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
		return tacklr.ChildCompleted
	case durable.SessionFailed:
		return tacklr.ChildFailed
	default:
		return tacklr.ChildRunning
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

// UnknownChild is get_child/cancel_child with an id that is not this session's child.
func UnknownChild(id string) error {
	return fmt.Errorf("job %q is unknown; call list_children and use an id from that list: %w", id, tacklr.ErrNotFound)
}

// ChildrenNudge is injected when inference would complete while children remain.
func ChildrenNudge(rows []durable.SessionStatus) string {
	if len(rows) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(280 + 48*len(rows))
	b.WriteString("Automated harness nudge: This turn still has child sessions whose results have not been collected:\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "- id=%s status=%s\n", r.ID, ChildState(r.State))
	}
	b.WriteString("The turn cannot finish while children remain. Continue useful work if possible. Otherwise call get_child with block=true to wait for and collect each result. Use cancel_child only when a child is no longer needed.")
	return b.String()
}
