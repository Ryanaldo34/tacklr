package temporal

import (
	"context"
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
)

// childRun is one child workflow tracked by the parent.
type childRun struct {
	id      durable.SessionID
	spec    string
	fut     workflow.ChildWorkflowFuture
	waiting bool
	done    bool
	result  string
	err     string
}

func startChild(ctx, sessionCtx workflow.Context, parent durable.SessionID, agentID, specialist, task string, childID durable.SessionID, auth durable.AuthContext, mounts []durable.MountRecipe, locality time.Duration) (childRun, error) {
	cctx := workflow.WithChildOptions(sessionCtx, workflow.ChildWorkflowOptions{
		WorkflowID:        string(childID),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	fut := workflow.ExecuteChildWorkflow(cctx, SessionWorkflow, WorkflowInput{
		SessionID:           childID,
		AgentID:             agentID,
		Parent:              parent,
		Specialist:          specialist,
		Prompt:              task,
		Auth:                auth,
		Mounts:              mounts,
		TurnLocalityTimeout: locality,
	})
	var exec workflow.Execution
	if err := fut.GetChildWorkflowExecution().Get(ctx, &exec); err != nil {
		return childRun{}, err
	}
	return childRun{id: childID, spec: specialist, fut: fut}, nil
}

func findChild(spawned []childRun, id durable.SessionID) int {
	return slices.IndexFunc(spawned, func(c childRun) bool { return c.id == id })
}

func dropChild(spawned *[]childRun, id durable.SessionID) {
	*spawned = slices.DeleteFunc(*spawned, func(c childRun) bool { return c.id == id })
}

func childOps(spawned []childRun) []durable.ChildOp {
	out := make([]durable.ChildOp, len(spawned))
	for i, c := range spawned {
		st := durable.SessionRunning
		if c.done {
			if c.err != "" {
				st = durable.SessionFailed
			} else {
				st = durable.SessionComplete
			}
		}
		out[i] = durable.ChildOp{ID: c.id, Specialist: c.spec, State: durable.ChildState(st), Result: c.result}
	}
	return out
}

func cancelOne(ctx workflow.Context, spawned *[]childRun, id durable.SessionID) {
	i := findChild(*spawned, id)
	if i < 0 {
		return
	}
	var exec workflow.Execution
	if err := (*spawned)[i].fut.GetChildWorkflowExecution().Get(ctx, &exec); err == nil {
		_ = workflow.RequestCancelExternalWorkflow(ctx, exec.ID, exec.RunID).Get(ctx, nil)
	}
	dropChild(spawned, id)
}

func reconcileChildren(
	ctx, sessionCtx workflow.Context,
	spawned *[]childRun,
	ops []durable.ChildOp,
	in WorkflowInput,
	agentID string,
	auth durable.AuthContext,
	mounts []durable.MountRecipe,
) error {
	keep := make(map[durable.SessionID]bool, len(ops))
	for _, op := range ops {
		if op.Cancel {
			cancelOne(ctx, spawned, op.ID)
			continue
		}
		keep[op.ID] = true
		if findChild(*spawned, op.ID) >= 0 {
			continue
		}
		if op.Task == "" || op.Specialist == "" {
			continue
		}
		c, err := startChild(ctx, sessionCtx, in.SessionID, agentID, op.Specialist, op.Task, op.ID, auth, mounts, in.TurnLocalityTimeout)
		if err != nil {
			return err
		}
		*spawned = append(*spawned, c)
	}
	*spawned = slices.DeleteFunc(*spawned, func(c childRun) bool { return !keep[c.id] })
	return nil
}

func awaitID(ops []durable.ChildOp) durable.SessionID {
	for _, op := range ops {
		if op.Await {
			return op.ID
		}
	}
	return ""
}

func waitChildTool(
	ctx workflow.Context,
	spawned *[]childRun,
	parks *map[string]durable.SessionID,
	callID string,
	id durable.SessionID,
	cancelCh workflow.ReceiveChannel,
	cancelSpawned func(),
) (string, string, error) {
	waitCh := workflow.GetSignalChannel(ctx, signalChildWaiting)
	for {
		i := findChild(*spawned, id)
		if i < 0 {
			return "", "", durable.ErrSessionNotFound
		}
		if (*spawned)[i].done || (*spawned)[i].waiting {
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
			(*spawned)[j].waiting = false
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
		s.AddReceive(waitCh, func(c workflow.ReceiveChannel, more bool) {
			var w durable.SessionID
			c.Receive(ctx, &w)
			if j := findChild(*spawned, w); j >= 0 {
				(*spawned)[j].waiting = true
			}
		})
		s.Select(ctx)
		if cancelled {
			return "", "", workflow.ErrCanceled
		}
	}
	i := findChild(*spawned, id)
	if i < 0 {
		return "", "", durable.ErrSessionNotFound
	}
	c := (*spawned)[i]
	if c.waiting && !c.done {
		if *parks == nil {
			*parks = map[string]durable.SessionID{}
		}
		(*parks)[callID] = c.id
		return "", callID, nil
	}
	dropChild(spawned, id)
	if c.err != "" {
		return c.err, "", nil
	}
	return c.result, "", nil
}

// activityChildren is the Tool-activity childHost. It records intent; the
// workflow starts/cancels/waits after the activity returns.
type activityChildren struct {
	parent durable.SessionID
	ops    []durable.ChildOp
}

func (a *activityChildren) SpawnChild(_ context.Context, specialist, task, callID string) (string, error) {
	specialist, task, err := durable.NormalizeSpawn(specialist, task)
	if err != nil {
		return "", err
	}
	id := durable.ChildSessionID(a.parent, specialist, callID)
	if i := findOp(a.ops, id); i >= 0 {
		return string(id), nil
	}
	if task == "" {
		return "", fmt.Errorf("task_description_and_context is required: %w", tacklr.ErrInvalid)
	}
	a.ops = append(a.ops, durable.ChildOp{
		ID: id, Specialist: specialist, Task: task, State: tacklr.ChildRunning,
	})
	return string(id), nil
}

func (a *activityChildren) Children() []tacklr.Child {
	out := make([]tacklr.Child, 0, len(a.ops))
	for _, op := range a.ops {
		if op.Cancel {
			continue
		}
		state := op.State
		if state == "" {
			state = tacklr.ChildRunning
		}
		out = append(out, tacklr.Child{ID: string(op.ID), Specialist: op.Specialist, State: state, Result: op.Result})
	}
	return out
}

func (a *activityChildren) CancelChild(_ context.Context, id string) error {
	sid := durable.SessionID(id)
	i := findOp(a.ops, sid)
	if i < 0 {
		return durable.UnknownChild(id)
	}
	a.ops[i].Cancel = true
	return nil
}

func (a *activityChildren) AwaitChild(_ context.Context, id, _ string) (tacklr.Child, bool, error) {
	sid := durable.SessionID(id)
	i := findOp(a.ops, sid)
	if i < 0 {
		return tacklr.Child{}, false, durable.UnknownChild(id)
	}
	op := a.ops[i]
	switch op.State {
	case tacklr.ChildCompleted:
		a.ops = append(a.ops[:i], a.ops[i+1:]...)
		return tacklr.Child{ID: id, Specialist: op.Specialist, State: tacklr.ChildCompleted, Result: op.Result}, false, nil
	case tacklr.ChildFailed:
		a.ops = append(a.ops[:i], a.ops[i+1:]...)
		err := fmt.Errorf("%s: %w", op.Result, tacklr.ErrFailed)
		if op.Result == "" {
			err = fmt.Errorf("failed: %w", tacklr.ErrFailed)
		}
		return tacklr.Child{ID: id, Specialist: op.Specialist, State: tacklr.ChildFailed, Result: op.Result}, false, err
	}
	a.ops[i].Await = true
	return tacklr.Child{}, true, nil
}

func findOp(ops []durable.ChildOp, id durable.SessionID) int {
	return slices.IndexFunc(ops, func(op durable.ChildOp) bool { return op.ID == id })
}
