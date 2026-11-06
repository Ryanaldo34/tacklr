package worker

import (
	"go.temporal.io/sdk/contrib/workflowstreams"

	"github.com/ryanaldo34/tacklr/session/orchestrator"
)

const (
	// signalChildDone carries a job's result to its parent when the job
	// ends: the one way a result arrives. Jobs are abandoned on the parent's
	// close, so this also reaches a parent that continued as new.
	signalChildDone = "tacklr_child_done"
)

// carried is a session's state across continue-as-new: the turn's carry
// (orchestrator.Carry), the worker that holds its harness, and its event stream.
type carried struct {
	Carry     orchestrator.Carry
	TaskQueue string
	LiveURL   string
	Stream    *workflowstreams.WorkflowStreamState
}
