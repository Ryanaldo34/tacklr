package temporal

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// SessionWorkflow is the Temporal type name for the session wait loop.
// StartWorker registers it. Hosts call session.Runtime, not this function.
func SessionWorkflow(ctx workflow.Context, in workflowInput) (string, error) {
	logger := workflow.GetLogger(ctx)
	if _, err := workflowstreams.NewWorkflowStream(ctx, nil); err != nil {
		logger.Error("workflow stream", "error", err)
	}

	var spawned []childRun
	wakeCh := workflow.GetSignalChannel(ctx, signalWake)
	childWaitCh := workflow.GetSignalChannel(ctx, signalChildWaiting)

	sess := &session.Session{
		Task: in.Prompt,
		Turn: session.Turn{
			SessionID:  in.SessionID,
			Specialist: in.Specialist,
			Worker:     in.Worker,
			Parent:     in.Parent,
			Mounts:     session.ApplyAuth(in.Mounts, session.AuthContext{}),
			MCP:        in.MCPServers,
			Seed:       in.State,
		},
	}
	ex := &wfExec{
		ctx:         ctx,
		sessionCtx:  ctx,
		in:          in,
		turn:        &sess.Turn,
		opts:        activityOptions(in),
		wakeCh:      wakeCh,
		childWaitCh: childWaitCh,
		spawned:     &spawned,
	}
	_ = workflow.SetQueryHandler(ctx, queryStatus, func() (session.SessionStatus, error) {
		st := session.SessionStatus{
			ID:         in.SessionID,
			Parent:     in.Parent,
			Specialist: in.Specialist,
			State:      session.SessionRunning,
			Waiting:    sess.Turn.Yielded,
			Result:     sess.Turn.Result,
		}
		if in.Worker != "" {
			st.Kind = session.SessionKindWorker
			st.Specialist = in.Worker
		} else if in.Specialist != "" {
			st.Kind = session.SessionKindSpecialist
		}
		if sess.Turn.Terminal != "" {
			st.State = sess.Turn.Terminal
			st.Waiting = false
		} else if sess.Closed {
			st.State = session.SessionComplete
			st.Waiting = false
		}
		return st, nil
	})
	_ = workflow.SetQueryHandler(ctx, queryChildren, func() ([]session.SessionID, error) {
		out := make([]session.SessionID, len(spawned))
		for i, c := range spawned {
			out[i] = c.id
		}
		return out, nil
	})
	return sess.Run(ex, ex, ex)
}

func activityOptions(in workflowInput) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: cmp.Or(in.ActivityTimeout, defaultActivityTimeout),
		HeartbeatTimeout:    cmp.Or(in.HeartbeatTimeout, defaultHeartbeatTimeout),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: cmp.Or(in.ActivityAttempts, defaultActivityAttempts),
		},
	}
}

// wfExec is the Temporal driver for one session. It implements session.Step,
// session.Mailbox, and session.Ledger.
//
// Every wake arrives on wakeCh and moves into queue in arrival order. Readers
// take from the front of queue, so a cancel sent before a prompt or resume is
// always handled before it.
type wfExec struct {
	ctx         workflow.Context
	sessionCtx  workflow.Context
	pinned      bool
	open        bool
	end         func(string, error)
	in          workflowInput
	turn        *session.Turn
	opts        workflow.ActivityOptions
	wakeCh      workflow.ReceiveChannel
	childWaitCh workflow.ReceiveChannel
	queue       []session.Wake
	spawned     *[]childRun
}

var (
	_ session.Step    = (*wfExec)(nil)
	_ session.Mailbox = (*wfExec)(nil)
	_ session.Ledger  = (*wfExec)(nil)
)

func (w *wfExec) Begin(kind string, promptLen, resumes int) {
	if w.open {
		return
	}
	w.open = true
	w.sessionCtx, w.pinned = openTurnLocality(w.ctx, w.in.TurnLocalityTimeout, 2*time.Second)
	logInfo(w.ctx, "turn start",
		"kind", kind, "session_id", w.in.SessionID,
		"prompt_len", promptLen, "resume_count", resumes,
	)
	_, w.end = startTurn(w.sessionCtx, "", w.in.SessionID, kind)
}

func (w *wfExec) End() {
	if !w.open {
		return
	}
	outcome := telemetry.OutcomeOK
	if w.turn.Yielded {
		outcome = telemetry.OutcomeYield
	} else if errors.Is(w.turn.Err, context.Canceled) {
		outcome = telemetry.OutcomeCancelled
	} else if w.turn.Terminal == session.SessionFailed {
		outcome = telemetry.OutcomeError
	}
	if w.end != nil {
		w.end(outcome, w.turn.Err)
	}
	if w.pinned {
		workflow.CompleteSession(w.sessionCtx)
		w.pinned = false
	}
	w.sessionCtx = w.ctx
	w.open = false
	if w.turn.Yielded && w.in.Parent != "" {
		_ = workflow.SignalExternalWorkflow(w.ctx, string(w.in.Parent), "", signalChildWaiting, w.in.SessionID).Get(w.ctx, nil)
	}
}

func (w *wfExec) Infer(in session.InferenceInput) (session.InferenceOutput, error) {
	var act *activities
	var out session.InferenceOutput
	err := w.waitActivity(act.Inference, in, &out)
	return out, err
}

func (w *wfExec) Tool(in session.ToolInput) (session.ToolOutput, error) {
	var act *activities
	var out session.ToolOutput
	err := w.waitActivity(act.Tool, in, &out)
	return out, err
}

func (w *wfExec) Commit(in session.CommitInput) error {
	var act *activities
	return w.waitActivity(act.CommitToolOutput, in, nil)
}

func (w *wfExec) Emit(ev tacklr.StreamEvent) {
	var act *activities
	// The turn already chose its outcome. A failed publish must not run the model again.
	_ = w.waitActivity(act.EmitEvent, emitEventInput{SessionID: w.in.SessionID, Event: ev}, nil)
}

func (w *wfExec) Job(name, task string) (string, error) {
	var act *activities
	var out string
	err := w.waitActivity(act.RunJob, runJobInput{Name: name, Task: task}, &out)
	return out, err
}

func (w *wfExec) waitActivity(activity any, in, out any) error {
	actCtx := workflow.WithActivityOptions(w.sessionCtx, w.opts)
	cctx, cancelAct := workflow.WithCancel(actCtx)
	defer cancelAct()
	fut := workflow.ExecuteActivity(cctx, activity, in)
	var err error
	done := false
	w.fill()
	cancelled := w.takeCancel()
	for !done && !cancelled {
		sel := workflow.NewSelector(w.ctx)
		sel.AddFuture(fut, func(f workflow.Future) {
			err = f.Get(w.ctx, out)
			done = true
		})
		sel.AddReceive(w.wakeCh, func(c workflow.ReceiveChannel, more bool) {
			w.receive(c)
			cancelled = w.takeCancel()
		})
		sel.AddReceive(cctx.Done(), func(c workflow.ReceiveChannel, more bool) {
			c.Receive(w.ctx, nil)
			cancelled = true
		})
		sel.Select(w.ctx)
	}
	if !done {
		cancelAct()
		err = fut.Get(w.ctx, out)
	}
	if err == nil || turnCanceled(w.ctx, err) {
		if err != nil {
			return context.Canceled
		}
		return nil
	}
	return errors.New(failureText(err))
}

// Prompts returns the prompts at the front of the queue. A prompt queued after
// a cancel or another wake stays queued until that wake is handled.
func (w *wfExec) Prompts() []session.PromptIn {
	w.fill()
	var out []session.PromptIn
	for len(w.queue) > 0 && w.queue[0].Kind == session.WakePrompt {
		out = append(out, w.queue[0].Prompt)
		w.queue = w.queue[1:]
	}
	return out
}

func (w *wfExec) Ready() []*tacklr.Message {
	var inbox []*tacklr.Message
	ctx := w.ctx
	for {
		if len(*w.spawned) == 0 {
			return inbox
		}
		var gotID session.SessionID
		var gotResult string
		var gotErr error
		ready := false
		pending := 0
		sel := workflow.NewSelector(ctx)
		for _, c := range *w.spawned {
			if c.done {
				continue
			}
			pending++
			id := c.id
			sel.AddFuture(c.fut, func(f workflow.Future) {
				var result string
				err := f.Get(ctx, &result)
				gotID, gotResult, gotErr, ready = id, result, err, true
			})
		}
		if pending == 0 {
			return inbox
		}
		sel.AddDefault(func() {})
		sel.Select(ctx)
		if !ready {
			return inbox
		}
		if msg := markChildDone(w.spawned, gotID, gotResult, gotErr); msg != nil {
			inbox = adapter.AppendMessages(inbox, msg)
		}
	}
}

// Recv returns the oldest queued wake, blocking until one arrives.
func (w *wfExec) Recv() session.Wake {
	ctx := w.ctx
	for {
		w.fill()
		if len(w.queue) > 0 {
			wake := w.queue[0]
			w.queue = w.queue[1:]
			return wake
		}
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(w.wakeCh, func(c workflow.ReceiveChannel, more bool) { w.receive(c) })
		sel.AddReceive(w.childWaitCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
		})
		sel.Select(ctx)
	}
}

// Wait blocks a turn that waits on children until a child finishes, a cancel
// is queued, or a prompt is at the front of the queue. Other wakes stay
// queued for Recv.
func (w *wfExec) Wait() session.Wake {
	ctx := w.ctx
	for {
		w.fill()
		if w.takeCancel() {
			return session.Wake{Kind: session.WakeCancel}
		}
		if len(w.queue) > 0 && w.queue[0].Kind == session.WakePrompt {
			wake := w.queue[0]
			w.queue = w.queue[1:]
			return wake
		}
		var wake session.Wake
		sel := workflow.NewSelector(ctx)
		for _, c := range *w.spawned {
			if c.done {
				continue
			}
			id := c.id
			sel.AddFuture(c.fut, func(f workflow.Future) {
				var result string
				err := f.Get(ctx, &result)
				if msg := markChildDone(w.spawned, id, result, err); msg != nil {
					wake = session.Wake{Kind: session.WakeChild, Child: msg}
				}
			})
		}
		sel.AddReceive(w.wakeCh, func(c workflow.ReceiveChannel, more bool) { w.receive(c) })
		sel.Select(ctx)
		if wake.Kind != "" {
			return wake
		}
	}
}

func (w *wfExec) IDs() []session.SessionID {
	out := make([]session.SessionID, len(*w.spawned))
	for i, c := range *w.spawned {
		out[i] = c.id
	}
	return out
}

func (w *wfExec) Start(tout session.ToolOutput) error {
	if tout.CancelID != "" {
		cancelOne(w.ctx, w.spawned, tout.CancelID)
	}
	if tout.JobID == "" || tout.JobName == "" || findChild(*w.spawned, tout.JobID) >= 0 {
		return nil
	}
	spec, worker := "", ""
	if tout.Child {
		spec = tout.JobName
	} else {
		worker = tout.JobName
	}
	c, err := startChild(w.ctx, w.sessionCtx, w.in.SessionID, spec, worker, tout.JobTask, tout.JobID, w.turn.Mounts, w.in)
	if err != nil {
		return err
	}
	*w.spawned = append(*w.spawned, c)
	return nil
}

func (w *wfExec) Await(id session.SessionID) (string, error) {
	ctx := w.ctx
	for {
		i := findChild(*w.spawned, id)
		if i < 0 {
			return "", session.ErrSessionNotFound
		}
		if (*w.spawned)[i].done {
			break
		}
		w.fill()
		cancelled := w.takeCancel()
		if cancelled {
			w.CancelAll()
			return "", context.Canceled
		}
		sel := workflow.NewSelector(ctx)
		sel.AddFuture((*w.spawned)[i].fut, func(f workflow.Future) {
			var result string
			err := f.Get(ctx, &result)
			j := findChild(*w.spawned, id)
			if j < 0 {
				return
			}
			(*w.spawned)[j].done = true
			(*w.spawned)[j].result = result
			if err != nil {
				(*w.spawned)[j].err = err.Error()
			}
		})
		sel.AddReceive(w.wakeCh, func(c workflow.ReceiveChannel, more bool) {
			w.receive(c)
			cancelled = w.takeCancel()
		})
		sel.Select(ctx)
		if cancelled {
			w.CancelAll()
			return "", context.Canceled
		}
	}
	i := findChild(*w.spawned, id)
	if i < 0 {
		return "", session.ErrSessionNotFound
	}
	c := (*w.spawned)[i]
	dropChild(w.spawned, id)
	if c.err != "" {
		return c.err, nil
	}
	return c.result, nil
}

func (w *wfExec) CancelAll() {
	for _, c := range *w.spawned {
		if c.child != nil {
			var exec workflow.Execution
			if err := c.child.GetChildWorkflowExecution().Get(w.ctx, &exec); err == nil {
				_ = workflow.RequestCancelExternalWorkflow(w.ctx, exec.ID, exec.RunID).Get(w.ctx, nil)
			}
		}
		if c.cancel != nil {
			c.cancel()
		}
	}
	*w.spawned = nil
}

// fill moves every wake already delivered on wakeCh into the queue, in
// arrival order.
func (w *wfExec) fill() {
	var wake session.Wake
	for w.wakeCh.ReceiveAsync(&wake) {
		w.queue = append(w.queue, wake)
		wake = session.Wake{}
	}
}

// receive queues one wake from c. Selectors call it when wakeCh is ready.
func (w *wfExec) receive(c workflow.ReceiveChannel) {
	var wake session.Wake
	c.Receive(w.ctx, &wake)
	w.queue = append(w.queue, wake)
}

// takeCancel reports whether a cancel is queued and removes the first one,
// together with the prompts queued before it: those were steering for the
// turn the cancel ends. Wakes queued after the cancel stay, in order.
func (w *wfExec) takeCancel() bool {
	i := slices.IndexFunc(w.queue, func(wake session.Wake) bool { return wake.Kind == session.WakeCancel })
	if i < 0 {
		return false
	}
	kept := make([]session.Wake, 0, len(w.queue)-1)
	for j, wake := range w.queue {
		if j > i || (j < i && wake.Kind != session.WakePrompt) {
			kept = append(kept, wake)
		}
	}
	w.queue = kept
	return true
}

// failureText reads ApplicationError.Message. Error() appends the cause,
// which repeats the same text.
func failureText(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		if msg := app.Message(); msg != "" {
			return msg
		}
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

func turnCanceled(ctx workflow.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	return temporal.IsCanceledError(err) || errors.Is(err, workflow.ErrCanceled) || errors.Is(err, workflow.ErrSessionFailed)
}

// openTurnLocality pins activities to one worker when the host set a timeout.
// Timeout <= 0 skips CreateSession (no hidden default).
func openTurnLocality(ctx workflow.Context, timeout, creation time.Duration) (workflow.Context, bool) {
	if timeout == 0 {
		return ctx, false
	}
	creation = cmp.Or(creation, 2*time.Second)
	sctx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
		CreationTimeout:  creation,
		ExecutionTimeout: timeout,
	})
	if err != nil {
		workflow.GetLogger(ctx).Error("worker session", "error", err)
		return ctx, false
	}
	return sctx, true
}
