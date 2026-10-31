package tacklr

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// JobHost is what a durable runtime knows about a session's jobs. The
// harness records what each tool call asks for on its round, and the
// runtime starts and cancels children from the Tools step's outputs. Nil
// host: job methods fail.
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

// callJobs is what one tool call asked the runtime to do with children.
// Copies of a call's runtime share it; a tool may call from goroutines.
type callJobs struct {
	mu      sync.Mutex
	starts  []JobStart
	cancels []string
	await   string
}

// start records s once.
func (j *callJobs) start(s JobStart) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !slices.ContainsFunc(j.starts, func(o JobStart) bool { return o.ID == s.ID }) {
		j.starts = append(j.starts, s)
	}
}

// toolRuntime is the HarnessRuntime passed to tools: session emit, state
// and interrupts, plus job operations recorded for the call.
type toolRuntime struct {
	sessionRuntime
	host JobHost
	jobs *callJobs
}

func newToolRuntime(ch chan StreamEvent, sm *sessionManager, host JobHost) toolRuntime {
	return toolRuntime{sessionRuntime: newSessionRuntime(ch, sm), host: host, jobs: &callJobs{}}
}

func (t toolRuntime) WithToolCallID(id string) toolRuntime {
	t.sessionRuntime = t.sessionRuntime.WithToolCallID(id)
	return t
}

func (t toolRuntime) resolve(job JobRequest) (JobStart, error) {
	if t.host == nil {
		return JobStart{}, fmt.Errorf("jobs are not available: %w", ErrFailed)
	}
	return t.host.Resolve(job, t.CurrentToolCallID())
}

func (t toolRuntime) Schedule(_ context.Context, job JobRequest) (Job, error) {
	s, err := t.resolve(job)
	if err != nil {
		return Job{}, err
	}
	if !t.running(s.ID) {
		t.jobs.start(s)
	}
	return Job{ID: s.ID, Name: s.Name, State: JobRunning}, nil
}

func (t toolRuntime) RunSpecialist(_ context.Context, name, task string) error {
	s, err := t.resolve(JobRequest{Name: name, Task: task})
	if err != nil {
		return err
	}
	if !s.Specialist {
		return fmt.Errorf("%w: %s is not a specialist", ErrNotFound, name)
	}
	if !t.running(s.ID) {
		t.jobs.start(s)
	}
	t.jobs.mu.Lock()
	t.jobs.await = s.ID
	t.jobs.mu.Unlock()
	return nil
}

// Jobs is the session's running children and the ones this call started
// (a call records a start only for a child that is not running yet).
func (t toolRuntime) Jobs() []Job {
	var out []Job
	if t.host != nil {
		out = t.host.Jobs()
	}
	t.jobs.mu.Lock()
	defer t.jobs.mu.Unlock()
	for _, s := range t.jobs.starts {
		out = append(out, Job{ID: s.ID, Name: s.Name, State: JobRunning})
	}
	return out
}

func (t toolRuntime) CancelJob(_ context.Context, id string) error {
	if t.host == nil {
		return fmt.Errorf("jobs are not available: %w", ErrFailed)
	}
	if !slices.ContainsFunc(t.Jobs(), func(j Job) bool { return j.ID == id }) {
		return fmt.Errorf("job %q is unknown: %w", id, ErrNotFound)
	}
	t.jobs.mu.Lock()
	defer t.jobs.mu.Unlock()
	t.jobs.cancels = append(t.jobs.cancels, id)
	return nil
}

// running reports a child the runtime already runs for the session.
func (t toolRuntime) running(id string) bool {
	return slices.ContainsFunc(t.host.Jobs(), func(j Job) bool { return j.ID == id })
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
	// The child's result becomes this call's result once it finishes.
	if err := runtime.RunSpecialist(ctx, spec, task); err != nil {
		return "", spawnSpecialistErr(err)
	}
	return "", nil
}

func spawnSpecialistErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return Correctionf(ErrNotFound, "spawn_specialist: that specialist is not registered. Pass a name from the available specialists")
	}
	return err
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
