package durable

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr"
)

// ChildSessionID is the stable id for a spawn_specialist child session.
// Same shape as embed workerSessionID: {parent}/w/{worker}/{call}.
func ChildSessionID(parent SessionID, specialist, callID string) SessionID {
	specialist = strings.TrimSpace(specialist)
	callID = strings.TrimSpace(callID)
	if parent == "" {
		return SessionID(fmt.Sprintf("w/%s/%s", specialist, callID))
	}
	return SessionID(fmt.Sprintf("%s/w/%s/%s", parent, specialist, callID))
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

// SpawnCall is the JSON body of spawn_specialist.
type SpawnCall struct {
	Task       string
	Specialist string
	Block      bool
}

// ParseSpawnCall reads spawn_specialist arguments. Block defaults to true.
func ParseSpawnCall(arguments string) (SpawnCall, error) {
	var raw struct {
		Task       string `json:"task_description_and_context"`
		Specialist string `json:"specialist"`
		Block      *bool  `json:"block"`
	}
	if err := json.Unmarshal([]byte(arguments), &raw); err != nil {
		return SpawnCall{}, fmt.Errorf("%w: spawn_specialist arguments: %w", tacklr.ErrInvalid, err)
	}
	out := SpawnCall{
		Task:       strings.TrimSpace(raw.Task),
		Specialist: strings.TrimSpace(raw.Specialist),
		Block:      raw.Block == nil || *raw.Block,
	}
	if out.Specialist == "" {
		return SpawnCall{}, fmt.Errorf("specialist is required: %w", tacklr.ErrInvalid)
	}
	if out.Task == "" {
		return SpawnCall{}, fmt.Errorf("task_description_and_context is required: %w", tacklr.ErrInvalid)
	}
	return out, nil
}

// ParseChildCall reads child_id and optional block from get_child / cancel_child.
func ParseChildCall(arguments string) (id string, block bool, err error) {
	var raw struct {
		ChildID string `json:"child_id"`
		Block   bool   `json:"block"`
	}
	if err := json.Unmarshal([]byte(arguments), &raw); err != nil {
		return "", false, fmt.Errorf("%w: child arguments: %w", tacklr.ErrInvalid, err)
	}
	id = strings.TrimSpace(raw.ChildID)
	if id == "" {
		return "", false, fmt.Errorf("child_id is required: %w", tacklr.ErrInvalid)
	}
	return id, raw.Block, nil
}

// FormatChildList is the list_children text for child sessions.
func FormatChildList(rows []SessionStatus) string {
	if len(rows) == 0 {
		return "No child sessions."
	}
	var b strings.Builder
	b.WriteString("Child sessions:\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "- id=%s specialist=%s status=%s\n", r.ID, r.Specialist, parentFacingState(r))
	}
	b.WriteString("Use get_child to collect a result (block=true to wait), or cancel_child to stop a child.")
	return b.String()
}

// FormatChild is the get_child text for one child.
func FormatChild(st SessionStatus) string {
	status := parentFacingState(st)
	var b strings.Builder
	fmt.Fprintf(&b, "id=%s specialist=%s status=%s\n", st.ID, st.Specialist, status)
	switch st.State {
	case SessionComplete:
		b.WriteString(st.Result)
	case SessionFailed:
		if st.Err != nil {
			b.WriteString(st.Err.Error())
		} else {
			b.WriteString("failed")
		}
	default:
		b.WriteString("Still running. Call get_child again later, or set block=true to wait until finished.")
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func parentFacingState(st SessionStatus) string {
	switch st.State {
	case SessionComplete:
		return "completed"
	case SessionFailed:
		return "failed"
	case SessionUnknown:
		return "unknown"
	default:
		return "running"
	}
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

// ScheduledChildMessage is the spawn_specialist block=false tool result.
func ScheduledChildMessage(id SessionID, specialist string) string {
	return fmt.Sprintf("Child %s scheduled (specialist=%s). Use list_children to poll, get_child to collect its result (block=true to wait), or cancel_child to stop it.", id, specialist)
}
