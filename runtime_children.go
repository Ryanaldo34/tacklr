package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// JobHost is the session-side implementation of HarnessRuntime job methods.
// Durable runtimes bind nested sessions and named workers; nil host means
// jobs are unavailable.
type JobHost interface {
	RunSpecialist(ctx context.Context, name, task, callID string) (string, error)
	Schedule(ctx context.Context, job JobRequest, callID string) (Job, error)
	Jobs() []Job
	CancelJob(ctx context.Context, id string) error
}

// toolRuntime is the HarnessRuntime passed to tools: session emit/state/interrupt
// plus job operations. Durable drivers replace the host; nil host errors on schedule.
type toolRuntime struct {
	sessionRuntime
	host JobHost
}

func newToolRuntime(ch chan StreamEvent, sm *sessionManager, host JobHost) toolRuntime {
	return toolRuntime{sessionRuntime: newSessionRuntime(ch, sm), host: host}
}

func (t toolRuntime) WithToolCallID(id string) toolRuntime {
	t.sessionRuntime = t.sessionRuntime.WithToolCallID(id)
	return t
}

func (t toolRuntime) requireHost() (JobHost, error) {
	if t.host == nil {
		return nil, fmt.Errorf("jobs are not available: %w", ErrFailed)
	}
	return t.host, nil
}

func (t toolRuntime) Schedule(ctx context.Context, job JobRequest) (Job, error) {
	host, err := t.requireHost()
	if err != nil {
		return Job{}, err
	}
	return host.Schedule(ctx, job, t.CurrentToolCallID())
}

func (t toolRuntime) Jobs() []Job {
	if t.host == nil {
		return nil
	}
	return t.host.Jobs()
}

func (t toolRuntime) CancelJob(ctx context.Context, id string) error {
	host, err := t.requireHost()
	if err != nil {
		return err
	}
	return host.CancelJob(ctx, id)
}

func (t toolRuntime) RunSpecialist(ctx context.Context, name, task string) (string, error) {
	host, err := t.requireHost()
	if err != nil {
		return "", err
	}
	return host.RunSpecialist(ctx, name, task, t.CurrentToolCallID())
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

func spawnSpecialist(ctx context.Context, args spawnSpecialistArgs, runtime HarnessRuntime) (string, error) {
	spec := strings.TrimSpace(args.Specialist)
	task := strings.TrimSpace(args.TaskDescriptionAndContext)
	if task == "" {
		return "", Correctionf(ErrInvalid, "spawn_specialist: task_description_and_context is required. Describe the worker's goal and constraints")
	}
	if spec == "" {
		return "", Correctionf(ErrInvalid, "spawn_specialist: specialist is required. Pass a name from the available specialists")
	}
	block := args.Block == nil || *args.Block
	if !block {
		job, err := runtime.Schedule(ctx, JobRequest{Name: spec, Task: task})
		if err != nil {
			return "", spawnSpecialistErr(err)
		}
		return fmt.Sprintf("Job %s scheduled (name=%s). The result arrives as a later message. Use list_children to inspect, or cancel_child to stop it.", job.ID, job.Name), nil
	}
	out, err := runtime.RunSpecialist(ctx, spec, task)
	if err != nil {
		return "", spawnSpecialistErr(err)
	}
	return out, nil
}

func spawnSpecialistErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return Correctionf(ErrNotFound, "spawn_specialist: that specialist is not registered. Pass a name from the available specialists")
	}
	if errors.Is(err, ErrInvalid) {
		return Correctionf(ErrInvalid, "spawn_specialist: specialist is required. Pass a name from the available specialists")
	}
	return unknownJobErr("spawn_specialist", err)
}

func listChildren(_ context.Context, _ listChildrenArgs, runtime HarnessRuntime) (string, error) {
	return formatJobs(runtime.Jobs()), nil
}

func cancelChild(ctx context.Context, args cancelChildArgs, runtime HarnessRuntime) (string, error) {
	id := strings.TrimSpace(args.ChildID)
	if id == "" {
		return "", Correctionf(ErrInvalid, "cancel_child: child_id is required. Call list_children and pass a child_id from that list")
	}
	if err := runtime.CancelJob(ctx, id); err != nil {
		return "", unknownJobErr("cancel_child", err)
	}
	return fmt.Sprintf("Job %s cancelled and removed.", id), nil
}
