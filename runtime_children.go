package tacklr

import (
	"context"
	"fmt"
	"strings"
)

// ChildID is the id of the child a tool call starts: a specialist session
// or a named Runtime job. It comes from the call, so a call that runs again
// names the same child. The harness and every runtime use it.
func ChildID(parent, name, callID string, specialist bool) string {
	kind := "j"
	if specialist {
		kind = "w"
	}
	return fmt.Sprintf("%s/%s/%s/%s", parent, kind, strings.TrimSpace(name), strings.TrimSpace(callID))
}

// JobStart is a child a tool call asks the runtime to start. The runtime
// starts and cancels children from the call's outcome.
type JobStart struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Task string `json:"task"`
	// Specialist is a nested session; otherwise a named worker.
	Specialist bool `json:"specialist,omitempty"`
}

func formatJobs(rows []Job) string {
	if len(rows) == 0 {
		return "No jobs."
	}
	var b strings.Builder
	b.Grow(80 + 64*len(rows))
	b.WriteString("Jobs:\n")
	for _, j := range rows {
		fmt.Fprintf(&b, "- id=%s name=%s status=%s\n", j.ID, j.Name, j.State)
	}
	b.WriteString("Results arrive as later messages. Use cancel_child to stop a job.")
	return b.String()
}

func spawnSpecialist(ctx context.Context, args spawnSpecialistArgs, runtime *HarnessRuntime) (string, error) {
	spec := strings.TrimSpace(args.Specialist)
	task := strings.TrimSpace(args.TaskDescriptionAndContext)
	if task == "" {
		return "", Correctionf(ErrInvalid, "spawn_specialist: task_description_and_context is required. Describe the worker's goal and constraints")
	}
	if spec == "" {
		return "", Correctionf(ErrInvalid, "spawn_specialist: specialist is required. Pass a name from the available specialists")
	}
	wait := args.Block == nil || *args.Block
	job, err := runtime.startJob(JobRequest{Name: spec, Task: task, Wait: wait}, true)
	if err != nil {
		// The name and task are checked above, so the only failure is an
		// unknown specialist.
		return "", Correctionf(ErrNotFound, "spawn_specialist: that specialist is not registered. Pass a name from the available specialists")
	}
	if wait {
		// The child's output becomes this call's result once it finishes.
		return "", nil
	}
	return fmt.Sprintf("Job %s scheduled (name=%s). The result arrives as a later message. Use list_children to inspect, or cancel_child to stop it.", job.ID, job.Name), nil
}

func listChildren(_ context.Context, _ listChildrenArgs, runtime *HarnessRuntime) (string, error) {
	return formatJobs(runtime.Jobs()), nil
}

func cancelChild(ctx context.Context, args cancelChildArgs, runtime *HarnessRuntime) (string, error) {
	id := strings.TrimSpace(args.ChildID)
	if id == "" {
		return "", Correctionf(ErrInvalid, "cancel_child: child_id is required. Call list_children and pass a child_id from that list")
	}
	if err := runtime.CancelJob(ctx, id); err != nil {
		return "", Correctionf(ErrNotFound, "cancel_child: that job is unknown. Call list_children, then cancel_child with an id from that list")
	}
	return fmt.Sprintf("Job %s cancelled and removed.", id), nil
}
