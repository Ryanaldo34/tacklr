package temporal

import (
	"context"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
)

// childRun is one job tracked by the parent: a child session or a RunJob activity.
type childRun struct {
	id     durable.SessionID
	spec   string
	fut    workflow.Future
	child  workflow.ChildWorkflowFuture
	cancel workflow.CancelFunc
	done   bool
	result string
	err    string
}

func startChild(ctx, sessionCtx workflow.Context, parent durable.SessionID, agentID, specialist, worker, task string, childID durable.SessionID, mounts []durable.MountRecipe, in workflowInput) (childRun, error) {
	cctx := workflow.WithChildOptions(sessionCtx, workflow.ChildWorkflowOptions{
		WorkflowID:        string(childID),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	name := specialist
	if worker != "" {
		name = worker
	}
	fut := workflow.ExecuteChildWorkflow(cctx, SessionWorkflow, workflowInput{
		SessionID:           childID,
		AgentID:             agentID,
		Parent:              parent,
		Specialist:          specialist,
		Worker:              worker,
		Prompt:              task,
		Mounts:              mounts,
		TurnLocalityTimeout: in.TurnLocalityTimeout,
		ActivityTimeout:     in.ActivityTimeout,
		HeartbeatTimeout:    in.HeartbeatTimeout,
		ActivityAttempts:    in.ActivityAttempts,
	})
	var exec workflow.Execution
	if err := fut.GetChildWorkflowExecution().Get(ctx, &exec); err != nil {
		return childRun{}, err
	}
	return childRun{id: childID, spec: name, fut: fut, child: fut}, nil
}

func findChild(spawned []childRun, id durable.SessionID) int {
	return slices.IndexFunc(spawned, func(c childRun) bool { return c.id == id })
}

func dropChild(spawned *[]childRun, id durable.SessionID) {
	*spawned = slices.DeleteFunc(*spawned, func(c childRun) bool { return c.id == id })
}

func spawnedIDs(spawned []childRun) []durable.SessionID {
	out := make([]durable.SessionID, len(spawned))
	for i, c := range spawned {
		out[i] = c.id
	}
	return out
}

func markChildDone(spawned *[]childRun, id durable.SessionID, result string, err error) *tacklr.Message {
	i := findChild(*spawned, id)
	if i < 0 {
		return nil
	}
	c := (*spawned)[i]
	dropChild(spawned, id)
	st := durable.SessionStatus{ID: c.id, Specialist: c.spec, Result: result, State: durable.SessionComplete}
	if err != nil {
		st.State = durable.SessionFailed
		st.Result = err.Error()
	}
	return adapter.ChildJobMessage(st)
}

func harvestReadyChildren(ctx workflow.Context, spawned *[]childRun, inbox *[]*tacklr.Message) {
	for {
		if len(*spawned) == 0 {
			return
		}
		var gotID durable.SessionID
		var gotResult string
		var gotErr error
		ready := false
		pending := 0
		s := workflow.NewSelector(ctx)
		for _, c := range *spawned {
			if c.done {
				continue
			}
			pending++
			id := c.id
			s.AddFuture(c.fut, func(f workflow.Future) {
				var result string
				err := f.Get(ctx, &result)
				gotID, gotResult, gotErr, ready = id, result, err, true
			})
		}
		if pending == 0 {
			return
		}
		s.AddDefault(func() {})
		s.Select(ctx)
		if !ready {
			return
		}
		if msg := markChildDone(spawned, gotID, gotResult, gotErr); msg != nil {
			*inbox = adapter.AppendMessages(*inbox, msg)
		}
	}
}

func cancelOne(ctx workflow.Context, spawned *[]childRun, id durable.SessionID) {
	i := findChild(*spawned, id)
	if i < 0 {
		return
	}
	var exec workflow.Execution
	c := (*spawned)[i]
	if c.child != nil {
		if err := c.child.GetChildWorkflowExecution().Get(ctx, &exec); err == nil {
			_ = workflow.RequestCancelExternalWorkflow(ctx, exec.ID, exec.RunID).Get(ctx, nil)
		}
	}
	if c.cancel != nil {
		c.cancel()
	}
	dropChild(spawned, id)
}

func applyChildIntent(
	ctx, sessionCtx workflow.Context,
	spawned *[]childRun,
	tout toolOutput,
	in workflowInput,
	agentID string,
	mounts []durable.MountRecipe,
) error {
	if tout.CancelID != "" {
		cancelOne(ctx, spawned, tout.CancelID)
	}
	if tout.JobID == "" || tout.JobName == "" || findChild(*spawned, tout.JobID) >= 0 {
		return nil
	}
	spec, worker := "", ""
	if tout.Child {
		spec = tout.JobName
	} else {
		worker = tout.JobName
	}
	c, err := startChild(ctx, sessionCtx, in.SessionID, agentID, spec, worker, tout.JobTask, tout.JobID, mounts, in)
	if err != nil {
		return err
	}
	*spawned = append(*spawned, c)
	return nil
}

func waitChildTool(
	ctx workflow.Context,
	spawned *[]childRun,
	id durable.SessionID,
	cancelCh workflow.ReceiveChannel,
	cancelSpawned func(),
) (string, error) {
	for {
		i := findChild(*spawned, id)
		if i < 0 {
			return "", durable.ErrSessionNotFound
		}
		if (*spawned)[i].done {
			break
		}
		cancelled := false
		s := workflow.NewSelector(ctx)
		s.AddFuture((*spawned)[i].fut, func(f workflow.Future) {
			var result string
			err := f.Get(ctx, &result)
			j := findChild(*spawned, id)
			if j < 0 {
				return
			}
			(*spawned)[j].done = true
			(*spawned)[j].result = result
			if err != nil {
				(*spawned)[j].err = err.Error()
			}
		})
		s.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			cancelSpawned()
			cancelled = true
		})
		s.Select(ctx)
		if cancelled {
			return "", workflow.ErrCanceled
		}
	}
	i := findChild(*spawned, id)
	if i < 0 {
		return "", durable.ErrSessionNotFound
	}
	c := (*spawned)[i]
	dropChild(spawned, id)
	if c.err != "" {
		return c.err, nil
	}
	return c.result, nil
}

// activityChildren is the Tool-activity JobHost. It records this call's
// schedule/cancel/wait; the workflow starts, cancels, or waits via Runtime.
type activityChildren struct {
	parent   durable.SessionID
	agentID  string
	catalog  durable.Catalog
	jobs     map[string]durable.JobHandler
	known    []durable.SessionID
	jobID    durable.SessionID
	jobName  string
	jobTask  string
	child    bool
	cancelID durable.SessionID
	awaitID  durable.SessionID
}

func (a *activityChildren) Schedule(_ context.Context, job tacklr.JobRequest, callID string) (tacklr.Job, error) {
	name, task, err := adapter.NormalizeSpawn(job.Name, job.Task)
	if err != nil {
		return tacklr.Job{}, err
	}
	session := adapter.HasSpecialist(a.catalog, a.agentID, name)
	var id durable.SessionID
	if session {
		id = durable.ChildSessionID(a.parent, name, callID)
	} else {
		if _, ok := a.jobs[name]; !ok {
			return tacklr.Job{}, fmt.Errorf("%w: %s", tacklr.ErrNotFound, name)
		}
		id = durable.JobID(a.parent, name, callID)
	}
	if slices.Contains(a.known, id) || a.jobID == id {
		return tacklr.Job{ID: string(id), Name: name, State: tacklr.JobRunning}, nil
	}
	if task == "" {
		if session {
			return tacklr.Job{}, fmt.Errorf("task_description_and_context is required: %w", tacklr.ErrInvalid)
		}
		return tacklr.Job{}, fmt.Errorf("task is required: %w", tacklr.ErrInvalid)
	}
	a.jobID, a.jobName, a.jobTask, a.child = id, name, task, session
	return tacklr.Job{ID: string(id), Name: name, State: tacklr.JobRunning}, nil
}

func (a *activityChildren) Jobs() []tacklr.Job {
	out := make([]tacklr.Job, 0, len(a.known)+1)
	seen := map[durable.SessionID]bool{}
	for _, id := range a.known {
		seen[id] = true
		out = append(out, tacklr.Job{ID: string(id), State: tacklr.JobRunning})
	}
	if a.jobID != "" && !seen[a.jobID] {
		out = append(out, tacklr.Job{ID: string(a.jobID), Name: a.jobName, State: tacklr.JobRunning})
	}
	return out
}

func (a *activityChildren) CancelJob(_ context.Context, id string) error {
	sid := durable.SessionID(id)
	if sid != a.jobID && !slices.Contains(a.known, sid) {
		return adapter.UnknownChild(id)
	}
	a.cancelID = sid
	return nil
}

func (a *activityChildren) RunSpecialist(_ context.Context, name, task, callID string) (string, error) {
	if !adapter.HasSpecialist(a.catalog, a.agentID, name) {
		return "", fmt.Errorf("%w: %s", tacklr.ErrNotFound, name)
	}
	job, err := a.Schedule(context.Background(), tacklr.JobRequest{Name: name, Task: task}, callID)
	if err != nil {
		return "", err
	}
	a.awaitID = durable.SessionID(job.ID)
	return "", &tacklr.JobWaitError{ID: job.ID}
}
