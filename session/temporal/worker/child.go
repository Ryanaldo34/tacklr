package worker

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"
)

// startChild starts the job childID as a child session and waits until it
// started. A child already started under childID runs on: starting it
// again is not an error.
func startChild(ctx workflow.Context, parent session.ID, specialist, worker, task string, childID session.ID, mounts []session.MountRecipe, in protocol.Input) error {
	cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: string(childID),
		// Abandon: a parent's continue-as-new applies the close policy, and
		// its jobs must outlive that. The parent cancels them itself when it
		// ends.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	})
	fut := workflow.ExecuteChildWorkflow(cctx, protocol.Workflow, protocol.Input{
		SessionID:          childID,
		Parent:             parent,
		Specialist:         specialist,
		Worker:             worker,
		Prompt:             task,
		Mounts:             mounts,
		WorkerQueueTimeout: in.WorkerQueueTimeout,
		ActivityTimeout:    in.ActivityTimeout,
		HeartbeatTimeout:   in.HeartbeatTimeout,
		ActivityAttempts:   in.ActivityAttempts,
		MaxToolCalls:       in.MaxToolCalls,
		MaxChildren:        in.MaxChildren,
	})
	// The child-started event has to be in this workflow's history before we
	// continue. Otherwise the parent can finish before the child is scheduled.
	var exec workflow.Execution
	if err := fut.GetChildWorkflowExecution().Get(ctx, &exec); err != nil && !temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return err
	}
	return nil
}
