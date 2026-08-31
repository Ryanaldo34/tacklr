package temporal

import (
	"context"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/interrupt"
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

func startChild(ctx, sessionCtx workflow.Context, parent durable.SessionID, agentID, specialist, task string, childID durable.SessionID, mounts []durable.MountRecipe, in WorkflowInput) (childRun, error) {
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
	return childRun{id: childID, spec: specialist, fut: fut}, nil
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

func spawnedNudge(spawned []childRun) string {
	if len(spawned) == 0 {
		return ""
	}
	rows := make([]durable.SessionStatus, len(spawned))
	for i, c := range spawned {
		rows[i] = durable.SessionStatus{ID: c.id, Specialist: c.spec, State: durable.SessionRunning}
	}
	return durable.ChildrenNudge(rows)
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

func applyChildIntent(
	ctx, sessionCtx workflow.Context,
	spawned *[]childRun,
	tout ToolOutput,
	in WorkflowInput,
	agentID string,
	mounts []durable.MountRecipe,
) error {
	if tout.CancelID != "" {
		cancelOne(ctx, spawned, tout.CancelID)
	}
	if tout.SpawnID == "" || tout.SpawnSpec == "" || tout.SpawnTask == "" {
		return nil
	}
	if findChild(*spawned, tout.SpawnID) >= 0 {
		return nil
	}
	c, err := startChild(ctx, sessionCtx, in.SessionID, agentID, tout.SpawnSpec, tout.SpawnTask, tout.SpawnID, mounts, in)
	if err != nil {
		return err
	}
	*spawned = append(*spawned, c)
	return nil
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

// activityChildren is the Tool-activity childHost. It records this call's
// spawn/cancel/await; the workflow runs those on Runtime (ExecuteChildWorkflow).
type activityChildren struct {
	parent    durable.SessionID
	agentID   string
	catalog   durable.Catalog
	known     []durable.SessionID
	spawnID   durable.SessionID
	spawnSpec string
	spawnTask string
	cancelID  durable.SessionID
	awaitID   durable.SessionID
}

func (a *activityChildren) SpawnChild(_ context.Context, specialist, task, callID string) (string, error) {
	specialist, task, err := durable.NormalizeSpawn(specialist, task)
	if err != nil {
		return "", err
	}
	if a.catalog != nil {
		if spec, ok := a.catalog.Lookup(a.agentID); ok {
			if _, err := durable.OverlaySpecialist(spec, specialist); err != nil {
				return "", fmt.Errorf("%w: %w", tacklr.ErrNotFound, err)
			}
		}
	}
	id := durable.ChildSessionID(a.parent, specialist, callID)
	if slices.Contains(a.known, id) || a.spawnID == id {
		return string(id), nil
	}
	if task == "" {
		return "", fmt.Errorf("task_description_and_context is required: %w", tacklr.ErrInvalid)
	}
	a.spawnID = id
	a.spawnSpec = specialist
	a.spawnTask = task
	return string(id), nil
}

func (a *activityChildren) Children() []tacklr.Child {
	out := make([]tacklr.Child, 0, len(a.known)+1)
	seen := map[durable.SessionID]bool{}
	for _, id := range a.known {
		seen[id] = true
		out = append(out, tacklr.Child{ID: string(id), State: tacklr.ChildRunning})
	}
	if a.spawnID != "" && !seen[a.spawnID] {
		out = append(out, tacklr.Child{ID: string(a.spawnID), Specialist: a.spawnSpec, State: tacklr.ChildRunning})
	}
	return out
}

func (a *activityChildren) CancelChild(_ context.Context, id string) error {
	sid := durable.SessionID(id)
	if sid != a.spawnID && !slices.Contains(a.known, sid) {
		return durable.UnknownChild(id)
	}
	a.cancelID = sid
	return nil
}

func (a *activityChildren) AwaitChild(_ context.Context, id, _ string) (tacklr.Child, error) {
	sid := durable.SessionID(id)
	if sid != a.spawnID && !slices.Contains(a.known, sid) {
		return tacklr.Child{}, durable.UnknownChild(id)
	}
	a.awaitID = sid
	return tacklr.Child{}, &interrupt.ChildWaiting{Kind: interrupt.TypeChildWaiting}
}
