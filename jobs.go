package tacklr

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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

// backgroundJob is one detached spawn_worker. Live only; not checkpointed.
// ponytail: in-memory registry — durable job bag when cross-process resume matters.
type backgroundJob struct {
	id         string
	workerName string
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

func (j *backgroundJob) snapshot() (status, result string, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.status, j.result, j.err
}

func (j *backgroundJob) setTerminal(status, result string, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status != jobStatusRunning {
		return
	}
	j.status = status
	j.result = result
	j.err = err
	select {
	case <-j.done:
	default:
		close(j.done)
	}
}

func (j *backgroundJob) setInterrupted(intr interrupt.Interrupt, ids []string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status != jobStatusRunning {
		return
	}
	j.status = jobStatusInterrupted
	j.childIntr = intr
	j.childIntrIDs = ids
	select {
	case <-j.done:
	default:
		close(j.done)
	}
}

type listJobsArgs struct{}

type getJobArgs struct {
	JobID string `json:"job_id" desc:"Job id returned by spawn_worker when block is false"`
	Block bool   `json:"block" desc:"Wait for a running job before returning its result. Defaults to false."`
}

type cancelJobArgs struct {
	JobID string `json:"job_id" desc:"Job id returned by spawn_worker when block is false"`
}

func (a *AgentHarness) listJobsTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "list_jobs",
		DisplayName: "List jobs",
		Description: "Non-blocking overview of background worker jobs and their statuses (running, completed, failed, interrupted). Use get_job to collect one result or wait for it, and cancel_job to stop work that is no longer needed.",
		Category:    streaming.ToolCategoryExecute,
		Handler: func(ctx context.Context, _ listJobsArgs, _ HarnessRuntime) (string, error) {
			return a.formatJobList(), nil
		},
	})
}

func (a *AgentHarness) getJobTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "get_job",
		DisplayName: "Get job {job_id}",
		Description: "Get one background job. By default this is non-blocking: a running job returns its current status, while a terminal job returns and consumes its result. Set block=true to wait until a running job finishes and then return its result. Interrupted jobs request the required user input only when block=true.",
		Category:    streaming.ToolCategoryExecute,
		Handler: func(ctx context.Context, args getJobArgs, runtime HarnessRuntime) (string, error) {
			return a.readJob(ctx, strings.TrimSpace(args.JobID), args.Block, runtime)
		},
	})
}

func (a *AgentHarness) cancelJobTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "cancel_job",
		DisplayName: "Cancel job {job_id}",
		Description: "Cancel and remove a background worker job that is no longer needed. Completed and failed jobs are discarded without returning their result.",
		Category:    streaming.ToolCategoryExecute,
		Handler: func(ctx context.Context, args cancelJobArgs, _ HarnessRuntime) (string, error) {
			return a.cancelJob(ctx, strings.TrimSpace(args.JobID))
		},
	})
}

func (a *AgentHarness) formatJobList() string {
	a.jobsMu.Lock()
	ids := make([]string, 0, len(a.jobs))
	for id := range a.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	type row struct{ id, worker, status string }
	rows := make([]row, 0, len(ids))
	for _, id := range ids {
		j := a.jobs[id]
		status, _, _ := j.snapshot()
		rows = append(rows, row{id: j.id, worker: j.workerName, status: status})
	}
	a.jobsMu.Unlock()

	if len(rows) == 0 {
		return "No background jobs."
	}
	var b strings.Builder
	b.WriteString("Background jobs:\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "- id=%s worker=%s status=%s\n", r.id, r.worker, r.status)
	}
	b.WriteString("Use get_job to collect a result (block=true to wait), or cancel_job to stop a job.")
	return b.String()
}

func (a *AgentHarness) backgroundJobsNudge() string {
	a.jobsMu.Lock()
	ids := make([]string, 0, len(a.jobs))
	for id := range a.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	type row struct{ id, status string }
	rows := make([]row, 0, len(ids))
	for _, id := range ids {
		status, _, _ := a.jobs[id].snapshot()
		rows = append(rows, row{id: id, status: status})
	}
	a.jobsMu.Unlock()

	if len(rows) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Automated harness nudge: This turn still has background jobs whose results have not been collected:\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "- id=%s status=%s\n", r.id, r.status)
	}
	b.WriteString("The turn cannot finish while jobs remain. Continue useful work if possible. Otherwise call get_job with block=true to wait for and collect each result. Use cancel_job only when a job is no longer needed.")
	return b.String()
}

func (a *AgentHarness) formatJob(jobID string) (string, error) {
	if jobID == "" {
		return "", fmt.Errorf("job_id is required: %w", ErrInvalid)
	}
	j := a.getJob(jobID)
	if j == nil {
		return "", fmt.Errorf("job %q: %w", jobID, ErrNotFound)
	}
	status, _, _ := j.snapshot()
	var b strings.Builder
	fmt.Fprintf(&b, "id=%s worker=%s status=%s\n", j.id, j.workerName, status)
	switch status {
	case jobStatusInterrupted:
		b.WriteString("Interrupted awaiting user input. Call get_job with block=true to resolve and continue.")
	case jobStatusRunning:
		b.WriteString("Still running. Call get_job again later, or set block=true to wait until finished.")
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (a *AgentHarness) getJob(id string) *backgroundJob {
	a.jobsMu.Lock()
	defer a.jobsMu.Unlock()
	return a.jobs[id]
}

func (a *AgentHarness) registerJob(j *backgroundJob) {
	a.jobsMu.Lock()
	defer a.jobsMu.Unlock()
	if a.jobs == nil {
		a.jobs = make(map[string]*backgroundJob)
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
func (a *AgentHarness) scheduleBackgroundWorker(workerName, task, jobID string, runtime HarnessRuntime) (string, error) {
	spec, ok := a.subagents[workerName]
	if !ok {
		return "", fmt.Errorf("worker %q: %w", workerName, ErrNotFound)
	}
	if strings.TrimSpace(task) == "" {
		return "", fmt.Errorf("worker %q: empty task: %w", workerName, ErrInvalid)
	}
	if jobID == "" {
		return "", fmt.Errorf("worker %q: empty job id: %w", workerName, ErrInvalid)
	}
	if existing := a.getJob(jobID); existing != nil {
		return "", fmt.Errorf("job %q: already exists: %w", jobID, ErrInvalid)
	}

	parentCtx := a.jobsCtx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(parentCtx)

	worker, err := a.newWorkerHarness(workerCtx, workerName, jobID, spec)
	if err != nil {
		cancel()
		return "", fmt.Errorf("worker %q: %w", workerName, err)
	}

	j := &backgroundJob{
		id:         jobID,
		workerName: workerName,
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
		"worker", workerName,
		"job_id", jobID,
		"background", true,
	}
	slog.Info("scheduling background worker", logAttrs...)
	runtime.EmitUpdate(fmt.Sprintf("Job %s scheduled (worker=%s)", jobID, workerName))

	go a.runBackgroundJob(workerCtx, j)

	return fmt.Sprintf("Job %s scheduled (worker=%s). Use list_jobs to poll, get_job to collect its result (block=true to wait), or cancel_job to stop it.", jobID, workerName), nil
}

func (a *AgentHarness) runBackgroundJob(ctx context.Context, j *backgroundJob) {
	start := time.Now()
	logAttrs := []any{
		"area", "subagent",
		"session_id", a.sessionId,
		"worker", j.workerName,
		"job_id", j.id,
		"background", true,
	}
	defer func() {
		if r := recover(); r != nil {
			j.setTerminal(jobStatusFailed, "", fmt.Errorf("worker %q panic: %v: %w", j.workerName, r, ErrFailed))
			j.cancel()
			if j.worker != nil {
				j.worker.Close()
			}
		}
	}()

	events, err := j.worker.Run(ctx, j.task)
	if err != nil {
		j.setTerminal(jobStatusFailed, "", fmt.Errorf("starting worker %q: %w: %w", j.workerName, ErrFailed, err))
		j.worker.Close()
		slog.Error("failed to start background worker", append(logAttrs, "error", err)...)
		return
	}

	drained, drainErr := drainWorkerEvents(ctx, j.workerName, events, nil)
	elapsed := time.Since(start).Round(time.Millisecond)

	if ctx.Err() != nil {
		j.setTerminal(jobStatusFailed, "", ctx.Err())
		j.worker.Close()
		slog.Info("background worker cancelled", append(logAttrs, "elapsed", elapsed, "error", ctx.Err())...)
		return
	}
	if drainErr != nil {
		j.setTerminal(jobStatusFailed, "", fmt.Errorf("worker %q failed: %w: %w", j.workerName, ErrFailed, drainErr))
		j.worker.Close()
		slog.Warn("background worker failed", append(logAttrs, "elapsed", elapsed, "error", drainErr)...)
		return
	}
	if drained.completed {
		result := finalWorkerOutput(j.worker.Messages(), drained.lastAssistant)
		if result == "" {
			j.setTerminal(jobStatusFailed, "", fmt.Errorf("worker %q: no output: %w", j.workerName, ErrFailed))
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
		j.setTerminal(jobStatusFailed, "", fmt.Errorf("worker %q: incomplete: %w", j.workerName, ErrFailed))
		j.worker.Close()
		slog.Warn("background worker incomplete", append(logAttrs, "elapsed", elapsed)...)
		return
	}

	if j.worker.store != nil && j.worker.sessionId != "" {
		if err := j.worker.persistSession(ctx); err != nil {
			slog.Error("failed to checkpoint background worker", append(logAttrs, "error", err)...)
		}
	}
	parkMeta := parkedWorkerMeta{
		WorkerName:        j.workerName,
		WorkerSessionID:   j.worker.sessionId,
		Task:              j.task,
		ChildInterruptIDs: childIntrIDs,
	}
	a.setPark(j.id, parkMeta, j.worker)
	j.setInterrupted(childIntr, childIntrIDs)
	slog.Info("background worker interrupted", append(logAttrs,
		"elapsed", elapsed,
		"child_interrupts", len(childIntrIDs),
	)...)
}

func (a *AgentHarness) readJob(ctx context.Context, jobID string, block bool, runtime HarnessRuntime) (string, error) {
	if jobID == "" {
		return "", fmt.Errorf("job_id is required: %w", ErrInvalid)
	}
	j := a.getJob(jobID)
	if j == nil {
		return "", fmt.Errorf("job %q: %w", jobID, ErrNotFound)
	}

	status, _, _ := j.snapshot()
	if status == jobStatusRunning && !block {
		return a.formatJob(jobID)
	}
	if status == jobStatusInterrupted && !block {
		return a.formatJob(jobID)
	}
	if status == jobStatusRunning {
		runtime.EmitUpdate(fmt.Sprintf("Awaiting job %s", jobID))
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-j.done:
		}
	}

	status, result, jobErr := j.snapshot()
	switch status {
	case jobStatusCompleted:
		a.removeJob(jobID)
		return result, nil
	case jobStatusFailed:
		a.removeJob(jobID)
		if jobErr == nil {
			jobErr = fmt.Errorf("worker %q failed: %w", j.workerName, ErrFailed)
		}
		return "", jobErr
	case jobStatusInterrupted:
		return a.resumeInterruptedJob(ctx, j, runtime)
	default:
		return "", fmt.Errorf("job %q: unexpected status %q: %w", jobID, status, ErrFailed)
	}
}

func (a *AgentHarness) resumeInterruptedJob(ctx context.Context, j *backgroundJob, runtime HarnessRuntime) (string, error) {
	getID := runtime.CurrentToolCallID()
	j.mu.Lock()
	intr := j.childIntr
	workerName := j.workerName
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

	spec, ok := a.subagents[workerName]
	if !ok {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("worker %q: %w", workerName, ErrNotFound)
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
		return "", fmt.Errorf("worker %q: %w", workerName, err)
	}

	runtime.EmitUpdate(fmt.Sprintf("Job %s resumed", j.id))
	events, err := worker.ReturnFromInterrupt(ctx, a.childResolutionPayloads(getID, meta))
	if err != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("resuming worker %q: %w: %w", workerName, ErrFailed, err)
	}

	drained, drainErr := drainWorkerEvents(ctx, workerName, events, runtime.EmitUpdate)
	if ctx.Err() != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", ctx.Err()
	}
	if drainErr != nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("worker %q failed: %w: %w", workerName, ErrFailed, drainErr)
	}
	if drained.completed {
		result := finalWorkerOutput(worker.Messages(), drained.lastAssistant)
		a.clearPark(j.id)
		a.removeJob(j.id)
		if result == "" {
			return "", fmt.Errorf("worker %q: no output: %w", workerName, ErrFailed)
		}
		return result, nil
	}

	// Re-interrupt: re-park under job id and bubble onto get_job again.
	childIntrIDs, childIntr := collectChildInterrupts(worker, drained.interruptIDs)
	if childIntr == nil {
		a.clearPark(j.id)
		a.removeJob(j.id)
		return "", fmt.Errorf("worker %q: incomplete: %w", workerName, ErrFailed)
	}
	if worker.store != nil && worker.sessionId != "" {
		if err := worker.persistSession(ctx); err != nil {
			a.clearPark(j.id)
			a.removeJob(j.id)
			return "", fmt.Errorf("checkpoint interrupted worker %q: %w", workerName, err)
		}
	}
	parkMeta := parkedWorkerMeta{
		WorkerName:        workerName,
		WorkerSessionID:   worker.sessionId,
		Task:              j.task,
		ChildInterruptIDs: childIntrIDs,
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
		return "", fmt.Errorf("job_id is required: %w", ErrInvalid)
	}
	j := a.getJob(jobID)
	if j == nil {
		return "", fmt.Errorf("job %q: %w", jobID, ErrNotFound)
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
	return fmt.Sprintf("Job %s cancelled and removed.", jobID), nil
}

// cancelBackgroundJobs cancels detached workers. Called from Close.
func (a *AgentHarness) cancelBackgroundJobs() {
	if a.jobsCancel != nil {
		a.jobsCancel()
	}
	a.jobsMu.Lock()
	jobs := make([]*backgroundJob, 0, len(a.jobs))
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
			slog.Warn("background job did not finish on Close", "job_id", j.id, "worker", j.workerName)
		}
	}
}
