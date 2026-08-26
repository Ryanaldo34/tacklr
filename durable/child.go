package durable

import (
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr"
)

// ChildSessionID is the stable id for a spawn_specialist child session.
// Same shape as embed workerSessionID: {parent}/w/{worker}/{call}.
func ChildSessionID(parent SessionID, specialist, callID string) SessionID {
	return SessionID(fmt.Sprintf("%s/w/%s/%s", parent, strings.TrimSpace(specialist), strings.TrimSpace(callID)))
}

// OverlaySpecialist copies the parent catalog spec and applies the named Specialist.
// Nested specialists stay on the child spec so grandchild spawn is the same path.
func OverlaySpecialist(parent AgentSpec, specialist string) (AgentSpec, error) {
	spec := tacklr.FindSpecialist(parent.Options.Specialists, specialist)
	if spec == nil {
		return AgentSpec{}, fmt.Errorf("%w: specialist %q", ErrAgentNotFound, specialist)
	}
	out := parent
	out.Name = spec.Name
	out.Options = parent.Options.WithSpecialist(spec)
	out.Options.SessionID = ""
	out.Options.MountSession = nil
	return out, nil
}

func parentFacingState(st SessionStatus) string {
	if st.State == SessionComplete {
		return "completed"
	}
	if st.State == SessionFailed {
		return "failed"
	}
	return "running"
}

// ChildrenNudge is injected when inference would complete while children remain.
func ChildrenNudge(rows []SessionStatus) string {
	if len(rows) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Automated harness nudge: This turn still has child sessions whose results have not been collected:\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "- id=%s status=%s\n", r.ID, parentFacingState(r))
	}
	b.WriteString("The turn cannot finish while children remain. Continue useful work if possible. Otherwise call get_child with block=true to wait for and collect each result. Use cancel_child only when a child is no longer needed.")
	return b.String()
}
