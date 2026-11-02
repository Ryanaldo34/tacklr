package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// JobHost is what a durable runtime knows about a session's jobs. A tool
// call's outcome carries the children it asks to start or cancel, and the
// runtime starts and cancels them from that outcome. Nil host: job methods
// fail.
type JobHost interface {
	// Resolve validates job and returns the child it names. The id comes
	// from callID, so a call that runs again names the same child.
	Resolve(job JobRequest, callID string) (JobStart, error)
	// Jobs is the session's running children.
	Jobs() []Job
}

// JobStart is a child a tool call asks the runtime to start.
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

func unknownJobErr(name string, err error) error {
	if errors.Is(err, ErrNotFound) {
		return Correctionf(ErrNotFound, "%s: that job is unknown. Call list_children, then cancel_child with an id from that list", name)
	}
	return err
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
		return "", spawnSpecialistErr(err)
	}
	if wait {
		// The child's output becomes this call's result once it finishes.
		return "", nil
	}
	return fmt.Sprintf("Job %s scheduled (name=%s). The result arrives as a later message. Use list_children to inspect, or cancel_child to stop it.", job.ID, job.Name), nil
}

func spawnSpecialistErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return Correctionf(ErrNotFound, "spawn_specialist: that specialist is not registered. Pass a name from the available specialists")
	}
	return err
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
		return "", unknownJobErr("cancel_child", err)
	}
	return fmt.Sprintf("Job %s cancelled and removed.", id), nil
}
