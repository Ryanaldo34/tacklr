package temporal

import (
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
)

// childRun is one job tracked by the parent: a child session or a RunJob activity.
type childRun struct {
	id     session.SessionID
	spec   string
	fut    workflow.Future
	child  workflow.ChildWorkflowFuture
	cancel workflow.CancelFunc
	done   bool
	result string
	err    string
}

func startChild(ctx, sessionCtx workflow.Context, parent session.SessionID, specialist, worker, task string, childID session.SessionID, mounts []session.MountRecipe, in workflowInput) (childRun, error) {
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
	// The child-started event has to be in this workflow's history before we
	// continue. Otherwise the parent can finish before the child is scheduled.
	var exec workflow.Execution
	if err := fut.GetChildWorkflowExecution().Get(ctx, &exec); err != nil {
		return childRun{}, err
	}
	return childRun{id: childID, spec: name, fut: fut, child: fut}, nil
}

func findChild(spawned []childRun, id session.SessionID) int {
	return slices.IndexFunc(spawned, func(c childRun) bool { return c.id == id })
}

func dropChild(spawned *[]childRun, id session.SessionID) {
	*spawned = slices.DeleteFunc(*spawned, func(c childRun) bool { return c.id == id })
}

func markChildDone(spawned *[]childRun, id session.SessionID, result string, err error) *tacklr.Message {
	i := findChild(*spawned, id)
	if i < 0 {
		return nil
	}
	c := (*spawned)[i]
	dropChild(spawned, id)
	st := session.SessionStatus{ID: c.id, Specialist: c.spec, Result: result, State: session.SessionComplete}
	if err != nil {
		st.State = session.SessionFailed
		st.Result = err.Error()
	}
	return adapter.ChildJobMessage(st)
}

func cancelOne(ctx workflow.Context, spawned *[]childRun, id session.SessionID) {
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
