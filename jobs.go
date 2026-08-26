package tacklr

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Background job statuses surfaced by the job tools.
const (
	jobStatusRunning     = "running"
	jobStatusCompleted   = "completed"
	jobStatusFailed      = "failed"
	jobStatusInterrupted = "interrupted"
)

// workerRun is the single live lifecycle record for synchronous and
// asynchronous spawn_specialist execution. Durable interrupt state lives in the
// typed session parks module; process handles remain intentionally ephemeral.
type workerRun struct {
	id         string
	specialist string
	task       string

	mu           sync.Mutex
	status       string
	result       string
	err          error
	worker       *AgentHarness
	cancel       context.CancelFunc
	childIntr    interrupt.Interrupt
	childIntrIDs []string
	done         chan struct{} // closed when status leaves running
}

func (j *workerRun) snapshot() (status, result string, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.status, j.result, j.err
}

func (j *workerRun) setTerminal(status, result string, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status != jobStatusRunning {
		return
	}
	j.status = status
	j.result = result
	j.err = err
	close(j.done)
}

func (j *workerRun) setInterrupted(intr interrupt.Interrupt, ids []string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status != jobStatusRunning {
		return
	}
	j.status = jobStatusInterrupted
	j.childIntr = intr
	j.childIntrIDs = ids
	close(j.done)
}

type listJobsArgs struct{}

type getJobArgs struct {
	ChildID string `json:"child_id" desc:"Child session id returned by spawn_specialist when block is false"`
	Block   bool   `json:"block" desc:"Wait for a running child before returning its result. Defaults to false."`
}

type cancelJobArgs struct {
	ChildID string `json:"child_id" desc:"Child session id returned by spawn_specialist when block is false"`
}

func (a *AgentHarness) listJobsTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "list_children",
		DisplayName: "List children",
		Description: "Non-blocking overview of child sessions (running, completed, failed). Status stays running while a child waits for user input. Use get_child to collect a result or wait, and cancel_child to stop work that is no longer needed.",
		Category:    streaming.ToolCategoryExecute,
		Handler:     listChildren,
	})
}

func (a *AgentHarness) getJobTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "get_child",
		DisplayName: "Get child {child_id}",
		Description: "Get one child session. By default this is non-blocking: a running child returns its current status (including while waiting for user input), while a terminal child returns and consumes its result. Set block=true to wait until it finishes, or to park this call if the child needs user input.",
		Category:    streaming.ToolCategoryExecute,
		Handler:     getChild,
	})
}

func (a *AgentHarness) cancelJobTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "cancel_child",
		DisplayName: "Cancel child {child_id}",
		Description: "Cancel and remove a child session that is no longer needed. Completed and failed children are discarded without returning their result.",
		Category:    streaming.ToolCategoryExecute,
		Handler:     cancelChild,
	})
}

// jobsHost is the embed childHost: in-memory workers on this harness.
type jobsHost struct{ a *AgentHarness }

func (j jobsHost) SpawnChild(ctx context.Context, specialist, task, callID string) (string, error) {
	return j.a.spawnJob(ctx, specialist, task, callID)
}

func (j jobsHost) Children() []Child {
	jobs := j.a.copyJobs()
	out := make([]Child, 0, len(jobs))
	for _, job := range jobs {
		status, result, err := job.snapshot()
		c := Child{ID: job.id, Specialist: job.specialist, State: jobFacingStatus(status)}
		switch status {
		case jobStatusCompleted:
			c.State = ChildCompleted
			c.Result = result
		case jobStatusFailed:
			c.State = ChildFailed
			c.Result = result
			if c.Result == "" && err != nil {
				c.Result = err.Error()
			}
		}
		out = append(out, c)
	}
	return out
}

func (j jobsHost) CancelChild(ctx context.Context, id string) error {
	_, err := j.a.cancelJob(ctx, id)
	return err
}

func (j jobsHost) AwaitChild(ctx context.Context, id, callID string) (Child, bool, error) {
	return j.a.awaitJob(ctx, id, callID)
}

func (a *AgentHarness) toolUpdate(callID string) func(string) {
	return func(msg string) {
		if msg == "" || callID == "" {
			return
		}
		ch, _ := a.liveOut.Load().(chan StreamEvent)
		if ch == nil {
			return
		}
		select {
		case ch <- StreamEvent{Type: streaming.StreamEventToolUpdate, Content: msg, MessageID: callID}:
		default:
		}
	}
}

func (a *AgentHarness) spawnJob(_ context.Context, specialist, task, callID string) (string, error) {
	specialist = strings.TrimSpace(specialist)
	task = strings.TrimSpace(task)
	if callID == "" {
		return "", fmt.Errorf("tool call id is required: %w", ErrInvalid)
	}
	if a.getJob(callID) != nil {
		return callID, nil
	}
	if _, err := a.scheduleBackgroundWorker(specialist, task, callID, nil); err != nil {
		return "", err
	}
	return callID, nil
}

func (a *AgentHarness) awaitJob(ctx context.Context, id, callID string) (Child, bool, error) {
	if id == "" {
		return Child{}, false, fmt.Errorf("child_id is required; call list_children and pass a child_id from that list: %w", ErrInvalid)
	}
	j := a.getJob(id)
	if j == nil {
		return Child{}, false, fmt.Errorf("job %q is unknown; call list_children and use an id from that list: %w", id, ErrNotFound)
	}
	status, _, _ := j.snapshot()
	if status == jobStatusRunning {
		select {
		case <-ctx.Done():
			if j.cancel != nil {
				j.cancel()
			}
			return Child{}, false, ctx.Err()
		case <-j.done:
		}
	}
	status, result, jobErr := j.snapshot()
	switch status {
	case jobStatusCompleted:
		a.removeJob(id)
		return Child{ID: id, Specialist: j.specialist, State: ChildCompleted, Result: result}, false, nil
	case jobStatusFailed:
		a.removeJob(id)
		if result == "" && jobErr != nil {
			result = jobErr.Error()
		}
		if jobErr == nil {
			jobErr = fmt.Errorf("worker %q failed: %w", j.specialist, ErrFailed)
		}
		return Child{ID: id, Specialist: j.specialist, State: ChildFailed, Result: result}, false, jobErr
	case jobStatusInterrupted:
		result, err := a.resumeInterruptedJob(ctx, j, callID, a.toolUpdate(callID))
		if err != nil {
			return Child{}, false, err
		}
		return Child{ID: id, Specialist: j.specialist, State: ChildCompleted, Result: result}, false, nil
	default:
		return Child{}, false, fmt.Errorf("job %q: unexpected status %q: %w", id, status, ErrFailed)
	}
}

func (a *AgentHarness) copyJobs() []*workerRun {
	a.jobsMu.Lock()
	defer a.jobsMu.Unlock()
	out := make([]*workerRun, 0, len(a.jobs))
	for _, j := range a.jobs {
		out = append(out, j)
	}
	slices.SortFunc(out, func(a, b *workerRun) int { return strings.Compare(a.id, b.id) })
	return out
}

func jobFacingStatus(status string) string {
	if status == jobStatusInterrupted {
		return jobStatusRunning
	}
	return status
}

func (a *AgentHarness) backgroundChildrenNudge() string {
	jobs := a.copyJobs()
	if len(jobs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Automated harness nudge: This turn still has child sessions whose results have not been collected:\n")
	for _, j := range jobs {
		status, _, _ := j.snapshot()
		fmt.Fprintf(&b, "- id=%s status=%s\n", j.id, jobFacingStatus(status))
	}
	b.WriteString("The turn cannot finish while children remain. Continue useful work if possible. Otherwise call get_child with block=true to wait for and collect each result. Use cancel_child only when a child is no longer needed.")
	return b.String()
}

func (a *AgentHarness) getJob(id string) *workerRun {
	a.jobsMu.Lock()
	defer a.jobsMu.Unlock()
	return a.jobs[id]
}

func (a *AgentHarness) registerJob(j *workerRun) {
	a.jobsMu.Lock()
	defer a.jobsMu.Unlock()
	if a.jobs == nil {
		a.jobs = make(map[string]*workerRun)
	}
	a.jobs[j.id] = j
}

func (a *AgentHarness) removeJob(id string) {
	a.jobsMu.Lock()
	defer a.jobsMu.Unlock()
	delete(a.jobs, id)
}

// scheduleBackgroundWorker starts a worker on the harness jobs context and
// returns immediately with a schedule message.
func (a *AgentHarness) scheduleBackgroundWorker(specialist, task, jobID string, runtime HarnessRuntime) (string, error) {
	spec, ok := a.specialists[specialist]
	if !ok {
		return "", fmt.Errorf("specialist %q: %w", specialist, ErrNotFound)
	}
	if strings.TrimSpace(task) == "" {
		return "", fmt.Errorf("worker %q: empty task: %w", specialist, ErrInvalid)
	}
	if jobID == "" {
		return "", fmt.Errorf("worker %q: empty job id: %w", specialist, ErrInvalid)
	}
	if existing := a.getJob(jobID); existing != nil {
		return "", fmt.Errorf("job %q: already exists: %w", jobID, ErrInvalid)
	}

	a.jobsMu.Lock()
	parentCtx := a.jobsCtx
	a.jobsMu.Unlock()
	workerCtx, cancel := context.WithCancel(parentCtx)

	worker, err := a.newWorkerHarness(workerCtx, specialist, jobID, spec)
	if err != nil {
		cancel()
		return "", fmt.Errorf("worker %q: %w", specialist, err)
	}

	j := &workerRun{
		id:         jobID,
		specialist: specialist,
		task:       task,
		status:     jobStatusRunning,
		worker:     worker,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	a.registerJob(j)

	logAttrs := []any{
		"area", "subagent",
		"session_id", a.sessionId,
		"worker", specialist,
		"child_id", jobID,
		"background", true,
	}
	slog.Info("scheduling background worker", logAttrs...)
	if runtime != nil {
		runtime.EmitUpdate(fmt.Sprintf("Child %s scheduled (specialist=%s)", jobID, specialist))
	}

	go a.runBackgroundJob(workerCtx, j)
	a.toolUpdate(jobID)(fmt.Sprintf("Specialist %q started", specialist))

	return fmt.Sprintf("Child %s scheduled (specialist=%s). Use list_children to poll, get_child to collect its result (block=true to wait), or cancel_child to stop it.", jobID, specialist), nil
}

func (a *AgentHarness) runBackgroundJob(ctx context.Context, j *workerRun) {
	start := time.Now()
	logAttrs := []any{
		"area", "subagent",
		"session_id", a.sessionId,
		"worker", j.specialist,
		"child_id", j.id,
		"background", true,
	}
	defer func() {
		if r := recover(); r != nil {
			j.setTerminal(jobStatusFailed, "", fmt.Errorf("worker %q panic: %v: %w", j.specialist, r, ErrFailed))
			j.cancel()
			if j.worker != nil {
				j.worker.Close()
			}
		}
	}()

	events, err := j.worker.Run(ctx, j.task)
	if err != nil {
		j.setTerminal(jobStatusFailed, "", fmt.Errorf("%w: starting worker %q: %w", ErrFailed, j.specialist, err))
		j.worker.Close()
		slog.Error("failed to start background worker", append(logAttrs, "error", err)...)
		return
	}

	drained, drainErr := drainWorkerEvents(ctx, j.specialist, events, a.toolUpdate(j.id))
	elapsed := time.Since(start).Round(time.Millisecond)

	if ctx.Err() != nil {
		j.setTerminal(jobStatusFailed, "", ctx.Err())
		j.worker.Close()
		slog.Info("background worker cancelled", append(logAttrs, "elapsed", elapsed, "error", ctx.Err())...)
		return
	}
	if drainErr != nil {
		j.setTerminal(jobStatusFailed, "", fmt.Errorf("%w: worker %q: %w", ErrFailed, j.specialist, drainErr))
		j.worker.Close()
		slog.Warn("background worker failed", append(logAttrs, "elapsed", elapsed, "error", drainErr)...)
		return
	}
	if drained.completed {
		result := finalWorkerOutput(j.worker.Messages(), drained.lastAssistant)
		if result == "" {
			j.setTerminal(jobStatusFailed, "", fmt.Errorf("worker %q: no output: %w", j.specialist, ErrFailed))
			j.worker.Close()
			slog.Warn("background worker produced no output", append(logAttrs, "elapsed", elapsed)...)
			return
		}
		j.setTerminal(jobStatusCompleted, result, nil)
		j.worker.Close()
		slog.Info("background worker completed", append(logAttrs, "elapsed", elapsed, "output_length", len(result))...)
		return
	}

	childIntrIDs, childIntr := collectChildInterrupts(j.worker, drained.interruptIDs)
	if childIntr == nil {
		j.setTerminal(jobStatusFailed, "", fmt.Errorf("worker %q: incomplete: %w", j.specialist, ErrFailed))
		j.worker.Close()
		slog.Warn("background worker incomplete", append(logAttrs, "elapsed", elapsed)...)
		return
	}

	parkMeta, err := workerParkMeta(j.worker, j.specialist, j.task, childIntrIDs)
	if err != nil {
		slog.Error("failed to checkpoint background worker", append(logAttrs, "error", err)...)
	}
	a.setPark(j.id, parkMeta, j.worker)
	j.setInterrupted(childIntr, childIntrIDs)
	slog.Info("background worker interrupted", append(logAttrs,
		"elapsed", elapsed,
		"child_interrupts", len(childIntrIDs),
	)...)
}

func (a *AgentHarness) resumeInterruptedJob(ctx context.Context, j *workerRun, callID string, emit func(string)) (string, error) {
	getID := callID
	j.mu.Lock()
	intr := j.childIntr
	specialist := j.specialist
	j.mu.Unlock()
	if intr == nil {
		a.removeJob(j.id)
		return "", fmt.Errorf("job %q: interrupted without interrupt object: %w", j.id, ErrFailed)
	}

	resolved, err := a.session.AdoptInterrupt(getID, intr)
	if err != nil {
		return "", err
	}
	if resolved == nil {
		return "", fmt.Errorf("job %q: adopt returned nil: %w", j.id, ErrFailed)
	}

	spec, ok := a.specialists[specialist]
	if !ok {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("worker %q: %w", specialist, ErrNotFound)
	}
	meta := a.getParkMeta(j.id)
	if meta == nil {
		a.removeJob(j.id)
		return "", fmt.Errorf("job %q: parked worker state is missing: %w", j.id, ErrNotFound)
	}

	worker, err := a.attachParkedWorker(ctx, j.id, *meta, spec)
	if err != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("worker %q: %w", specialist, err)
	}

	if emit != nil {
		emit(fmt.Sprintf("Job %s resumed", j.id))
	}
	events, err := worker.ReturnFromInterrupt(ctx, a.childResolutionPayloads(getID, meta))
	if err != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("%w: resuming worker %q: %w", ErrFailed, specialist, err)
	}

	drained, drainErr := drainWorkerEvents(ctx, specialist, events, emit)
	if ctx.Err() != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", ctx.Err()
	}
	if drainErr != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("%w: worker %q: %w", ErrFailed, specialist, drainErr)
	}
	if drained.completed {
		result := finalWorkerOutput(worker.Messages(), drained.lastAssistant)
		a.clearPark(j.id)
		a.removeJob(j.id)
		if result == "" {
			return "", fmt.Errorf("worker %q: no output: %w", specialist, ErrFailed)
		}
		return result, nil
	}

	// Re-interrupt: re-park under job id and bubble onto get_child again.
	childIntrIDs, childIntr := collectChildInterrupts(worker, drained.interruptIDs)
	if childIntr == nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("worker %q: incomplete: %w", specialist, ErrFailed)
	}
	parkMeta, err := workerParkMeta(worker, specialist, j.task, childIntrIDs)
	if err != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("checkpoint interrupted worker %q: %w", specialist, err)
	}
	a.setPark(j.id, parkMeta, worker)
	j.mu.Lock()
	j.status = jobStatusInterrupted
	j.childIntr = childIntr
	j.childIntrIDs = childIntrIDs
	j.done = make(chan struct{})
	close(j.done)
	j.mu.Unlock()

	_, err = a.session.AdoptInterrupt(getID, childIntr)
	return "", err
}

func (a *AgentHarness) cancelJob(ctx context.Context, jobID string) (string, error) {
	if jobID == "" {
		return "", fmt.Errorf("child_id is required; call list_children and pass a child_id from that list: %w", ErrInvalid)
	}
	j := a.getJob(jobID)
	if j == nil {
		return "", fmt.Errorf("job %q is unknown; call list_children and use an id from that list: %w", jobID, ErrNotFound)
	}

	status, _, _ := j.snapshot()
	if j.cancel != nil {
		j.cancel()
	}
	if status == jobStatusRunning {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-j.done:
		}
	}
	if status == jobStatusInterrupted {
		a.clearPark(jobID)
	} else if j.worker != nil {
		j.worker.Close()
	}
	a.removeJob(jobID)
	return fmt.Sprintf("Child %s cancelled and removed.", jobID), nil
}

// cancelBackgroundJobs stops detached workers. Called from Close and when the
// original Run/ReturnFromInterrupt context is cancelled (client stop).
func (a *AgentHarness) cancelBackgroundJobs() {
	a.jobsCancelMu.Lock()
	defer a.jobsCancelMu.Unlock()

	a.jobsMu.Lock()
	if a.jobsCancel != nil {
		a.jobsCancel()
	}
	jobs := make([]*workerRun, 0, len(a.jobs))
	for _, j := range a.jobs {
		jobs = append(jobs, j)
	}
	a.jobsMu.Unlock()
	for _, j := range jobs {
		if j.cancel != nil {
			j.cancel()
		}
		select {
		case <-j.done:
		case <-time.After(2 * time.Second):
			slog.Warn("background child did not finish on cancel", "child_id", j.id, "specialist", j.specialist)
		}
	}
	a.jobsMu.Lock()
	a.jobsCtx, a.jobsCancel = context.WithCancel(context.Background())
	a.jobsMu.Unlock()
}
