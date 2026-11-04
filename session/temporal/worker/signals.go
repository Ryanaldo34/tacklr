package worker

import (
	"go.temporal.io/sdk/contrib/workflowstreams"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/loop"
)

const (
	// signalChildDone carries a job's result to its parent when the job
	// ends: the one way a result arrives. Jobs are abandoned on the parent's
	// close, so this also reaches a parent that continued as new.
	signalChildDone = "tacklr_child_done"
)

// childDone is a job's result as its parent records it: Err is set when
// the job failed.
type childDone struct {
	ID     session.ID
	Result string
	Err    string
}

// wake is d as the session's mailbox has it.
func (d childDone) wake() session.Wake {
	res := session.JobResult{Output: d.Result}
	if d.Err != "" {
		res = session.JobResult{Output: d.Err, Failed: true}
	}
	return session.Wake{Kind: session.WakeJob, Job: session.JobEnd{ID: d.ID, Result: res}}
}

// carried is a session's state across continue-as-new: everything the
// workflow holds that is not in the snapshot. A run continues with no wake
// queued and no step running: idle, or in a turn that waits on children
// (Suspended), which the next run continues. Children carry by id and,
// when one already finished, its result.
type carried struct {
	Mounts    []session.MountRecipe
	Inbox     []*message.Message
	NextMCP   []session.MCPServer
	State     map[string]any
	Round     *tacklr.Round
	Rev       session.Revision
	Result    string
	Terminal  session.State
	Yielded   bool
	Finished  bool
	Suspended bool
	Jobs      []loop.Job
	TaskQueue string
	LiveURL   string
	Stream    *workflowstreams.WorkflowStreamState
}
