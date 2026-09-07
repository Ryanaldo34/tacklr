package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// JobHost is the session-side implementation of HarnessRuntime job methods.
// Durable runtimes bind nested sessions and named workers; nil host means
// jobs are unavailable.
type JobHost interface {
	Schedule(ctx context.Context, job JobRequest, callID string) (Job, error)
	Jobs() []Job
	CancelJob(ctx context.Context, id string) error
	// WaitJob waits until the job is terminal. A *interrupt.ChildWaiting error
	// is the Temporal unpaired-tool seam (workflow waits; parent does not yield).
	WaitJob(ctx context.Context, id string) (Job, error)
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

func (t toolRuntime) WaitJob(ctx context.Context, id string) (Job, error) {
	host, err := t.requireHost()
	if err != nil {
		return Job{}, err
	}
	job, err := host.WaitJob(ctx, id)
	if err != nil {
		var waiting *interrupt.ChildWaiting
		if errors.As(err, &waiting) {
			_, err := t.Park(interrupt.TypeChildWaiting, []byte(`{}`))
			return Job{}, err
		}
		return job, err
	}
	return job, nil
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
	block := args.Block == nil || *args.Block
	job, err := runtime.Schedule(ctx, JobRequest{Name: spec, Task: task})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", Correctionf(ErrNotFound, "spawn_specialist: that specialist is not registered. Pass a name from the available specialists")
		}
		if errors.Is(err, ErrInvalid) {
			return "", Correctionf(ErrInvalid, "spawn_specialist: specialist is required. Pass a name from the available specialists")
		}
		return "", err
	}
	if !block {
		return fmt.Sprintf("Job %s scheduled (name=%s). The result arrives as a later message. Use list_children to inspect, or cancel_child to stop it.", job.ID, job.Name), nil
	}
	job, err = runtime.WaitJob(ctx, job.ID)
	if err != nil {
		return "", unknownJobErr("spawn_specialist", err)
	}
	return job.Result, nil
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
