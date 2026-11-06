package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/orchestrator"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// sessionWorkflow is the session wait loop, registered as
// protocol.Workflow. New registers it; hosts use a session.Client.
func sessionWorkflow(agent string) func(workflow.Context, protocol.Input) (string, error) {
	return func(ctx workflow.Context, in protocol.Input) (string, error) { return runSession(ctx, in, agent) }
}

// runSession runs one session's workflow; agent is the name of the
// worker's agent, which a specialist's or a job's name replaces.
func runSession(ctx workflow.Context, in protocol.Input, agent string) (string, error) {
	var carry carried
	if len(in.Carry) > 0 {
		if err := json.Unmarshal(in.Carry, &carry); err != nil {
			return "", err
		}
	}
	stream, err := workflowstreams.NewWorkflowStream(ctx, carry.Stream)
	if err != nil {
		// Only a carried stream state that does not decode fails here.
		return "", err
	}

	wakeCh := workflow.GetSignalChannel(ctx, protocol.SignalWake)

	ident := orchestrator.Identity{Specialist: in.Specialist, Worker: in.Worker, Parent: in.Parent, Mounts: in.Mounts, Task: in.Task != ""}
	var prev *orchestrator.Carry
	if len(in.Carry) > 0 {
		prev = &carry.Carry
	}
	state := orchestrator.NewState(in.SessionID, ident, in.MCPServers, in.State, prev)
	rt := &workflowRuntime{
		ctx:         ctx,
		agent:       cmp.Or(in.Specialist, in.Worker, agent),
		in:          in,
		state:       state,
		opts:        activityOptions(in),
		stream:      stream,
		taskQueue:   carry.TaskQueue,
		liveURL:     carry.LiveURL,
		routed:      carry.TaskQueue != "",
		wakeCh:      wakeCh,
		childDoneCh: workflow.GetSignalChannel(ctx, signalChildDone),
	}
	if in.Task != "" && prev == nil {
		// A task session's first run: its task is its first prompt.
		rt.queue = []session.Wake{{Kind: session.WakePrompt, Prompt: session.Prompt{Text: in.Task}, Trace: in.Trace}}
	}
	// Queries answer from the first moment, before the route's local
	// activity returns: a client may ask for the status at once.
	_ = workflow.SetQueryHandler(ctx, protocol.QueryLive, func() (string, error) {
		return rt.liveURL, nil
	})
	_ = workflow.SetQueryHandler(ctx, protocol.QueryStatus, func() (session.Status, error) {
		return state.Status(), nil
	})
	_ = workflow.SetQueryHandler(ctx, protocol.QueryJobs, func() ([]session.ID, error) {
		out := make([]session.ID, len(state.Jobs))
		for i, j := range state.Jobs {
			out[i] = j.ID
		}
		return out, nil
	})
	// Route now, so a client that subscribes before the first step finds
	// the worker whose live endpoint streams the session's deltas.
	rt.route()
	result, err := orchestrator.Run(rt, state)
	if errors.Is(err, orchestrator.ErrSuspended) {
		// Continue as new from the main loop, not a signal handler, once
		// every handler finished; the stream's log and offsets carry over.
		next := in
		c := carried{Carry: state.Carry(), TaskQueue: rt.taskQueue, LiveURL: rt.liveURL}
		return "", stream.NewContinueAsNewError(ctx, protocol.Workflow, func(state *workflowstreams.WorkflowStreamState) []any {
			c.Stream = state
			// The carry is plain data: it encodes.
			next.Carry, _ = json.Marshal(c)
			return []any{next}
		})
	}
	// Children are abandoned on close, and Run stopped the session's own; a
	// child tells its parent its result, which reaches a parent that
	// continued as new. A cancelled session still does both.
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	rt.ctx = dctx
	if state.Closed {
		if forgot := rt.forget(); err == nil {
			err = forgot
		}
	}
	if in.Parent != "" {
		done := orchestrator.JobEnd(in.SessionID, result, err)
		_ = workflow.SignalExternalWorkflow(dctx, string(in.Parent), "", signalChildDone, done).Get(dctx, nil)
	}
	rt.closeHarness()
	return result, err
}

// activityOptions are a step's options: Infer, a Tool call and Commit
// heartbeat while they run. Retries back off from one second to thirty.
func activityOptions(in protocol.Input) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: cmp.Or(in.ActivityTimeout, defaultActivityTimeout),
		HeartbeatTimeout:    cmp.Or(in.HeartbeatTimeout, defaultHeartbeatTimeout),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    cmp.Or(in.ActivityAttempts, defaultActivityAttempts),
		},
	}
}

// shortTimeout bounds the steps that only touch the worker's own state
// (asking for its queue, closing a harness).
const shortTimeout = 30 * time.Second

// workflowRuntime is the Temporal orchestrator.Runtime for one session.
//
// Every wake arrives on wakeCh and moves into queue in arrival order. Readers
// take from the front of queue, so a cancel sent before a prompt or resume is
// always handled before it.
type workflowRuntime struct {
	ctx workflow.Context
	// taskQueue is the task queue of the worker that holds the session's
	// harness, and liveURL its live endpoint; routed says the workflow
	// asked for them since that worker was last lost.
	taskQueue string
	liveURL   string
	routed    bool
	stream    *workflowstreams.WorkflowStream
	// newTurn says the next Commit is a new turn's first.
	newTurn bool
	// agent names the session's agent in its turns' spans; span is the
	// turn's span when this run started it (a replay did not).
	agent string
	span  *telemetry.TurnSpan
	in    protocol.Input
	state *orchestrator.State
	opts  workflow.ActivityOptions
	// wakeCh carries host wakes, childDoneCh each job's end; both move into
	// queue in arrival order.
	wakeCh      workflow.ReceiveChannel
	childDoneCh workflow.ReceiveChannel
	queue       []session.Wake
}

// Begin starts the turn's span in a side effect: Temporal records the
// span's context in the history, so a replay of the workflow gets the same
// context and starts no span.
func (w *workflowRuntime) Begin(ts orchestrator.TurnStart) telemetry.Carrier {
	workflow.GetLogger(w.ctx).Info("turn start",
		"kind", ts.Kind, "session_id", w.in.SessionID,
		"prompt_len", ts.PromptLen, "resume_count", ts.Resumes,
	)
	turn := telemetry.Turn{Agent: w.agent, Session: string(w.in.SessionID), Parent: string(w.in.Parent), Kind: ts.Kind, Runtime: telemetry.RuntimeTemporal}
	var span telemetry.Carrier
	_ = workflow.SideEffect(w.ctx, func(workflow.Context) any {
		ctx, s := telemetry.StartTurn(ts.Parent.Context(context.Background()), turn, ts.Link)
		telemetry.TurnStarted(ctx, ts.PromptLen, ts.Resumes)
		w.span = s
		return telemetry.Inject(ctx)
	}).Get(&span)
	if ts.Kind == session.WakePrompt {
		// A new turn: the stream drops the turns before it now, and the
		// turn's first Commit drops their outcomes and fallback events once
		// it committed what they left open.
		w.newTurn = true
		if st, err := w.stream.GetState(time.Hour); err == nil {
			// The stream has no offset getter; its state has the offset.
			_ = w.stream.Truncate(st.BaseOffset + int64(len(st.Log)))
		}
	}
	return span
}

func (w *workflowRuntime) End() {
	if w.span != nil {
		w.span.End(w.state.Outcome(), w.state.Err)
		w.span = nil
	}
}

// Commit runs to completion: a cancel queued meanwhile stays queued for the
// turn. A Commit that a cancel abandoned could still save, and the turn
// would not learn the revision the next Commit's base must be.
func (w *workflowRuntime) Commit(base session.Revision, in tacklr.CommitIn) (session.Revision, error) {
	var act *activities
	var rev session.Revision
	err := w.harnessStep(func(opts func(workflow.Context) workflow.Context) error {
		return workflow.ExecuteActivity(opts(w.ctx), act.Commit, commitInput{stepSession: w.stepSession(), Base: base, In: in, NewTurn: w.newTurn}).Get(w.ctx, &rev)
	})
	if err == nil {
		w.newTurn = false
	}
	return rev, w.stepErr(err)
}

// harnessStep runs step on the worker that holds the session's harness, and
// again on the worker the shared queue picks when that one is gone. step
// applies opts to the context it runs its activity on.
func (w *workflowRuntime) harnessStep(step func(opts func(workflow.Context) workflow.Context) error) error {
	for {
		if err := step(w.harnessCtx); !w.lostWorker(err) {
			return err
		}
	}
}

// sharedCtx is ctx for a step that needs no harness: any worker on the shared
// queue runs it.
func (w *workflowRuntime) sharedCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, w.opts)
}

// harnessCtx is ctx for a step that uses the session's harness: it runs on the
// worker that holds it. The first such step asks a worker on the shared
// queue for its own queue; until a worker answers, steps use the shared
// queue.
func (w *workflowRuntime) harnessCtx(ctx workflow.Context) workflow.Context {
	w.route()
	opts := w.opts
	if w.taskQueue != "" {
		opts.TaskQueue = w.taskQueue
		opts.ScheduleToStartTimeout = cmp.Or(w.in.WorkerQueueTimeout, defaultWorkerQueueTimeout)
	}
	return workflow.WithActivityOptions(ctx, opts)
}

// route asks a worker on the shared queue for its route, unless the
// session has one. A local activity: it runs on the worker running this
// workflow task, which polls the shared queue, and records only a marker.
func (w *workflowRuntime) route() {
	if w.routed {
		return
	}
	var r protocol.Route
	lctx := workflow.WithLocalActivityOptions(w.ctx, workflow.LocalActivityOptions{StartToCloseTimeout: shortTimeout, RetryPolicy: w.opts.RetryPolicy})
	if err := workflow.ExecuteLocalActivity(lctx, "WorkerQueue").Get(w.ctx, &r); err == nil {
		w.taskQueue, w.liveURL, w.routed = r.Queue, r.Live, true
	}
}

// lostWorker reports whether err is a step that never started because the
// worker holding the session's harness is gone. The step runs again on the
// worker the shared queue picks, which builds the harness.
func (w *workflowRuntime) lostWorker(err error) bool {
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) && timeout.TimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		w.routed, w.liveURL = false, ""
		return true
	}
	return false
}

// forget deletes what the harnesses of this closed session and of its jobs
// saved. It runs on any worker: it needs no harness.
func (w *workflowRuntime) forget() error {
	ids := make([]session.ID, 0, len(w.state.Jobs)+1)
	for _, j := range w.state.Jobs {
		ids = append(ids, j.ID)
	}
	var act *activities
	ctx := workflow.WithActivityOptions(w.ctx, w.opts)
	return workflow.ExecuteActivity(ctx, act.Forget, append(ids, w.state.SessionID)).Get(w.ctx, nil)
}

// closeHarness closes the session's harness on its worker when the session
// ends. A worker that is gone closed it already.
func (w *workflowRuntime) closeHarness() {
	if !w.routed {
		return
	}
	var act *activities
	ctx := workflow.WithStartToCloseTimeout(workflow.WithHeartbeatTimeout(w.harnessCtx(w.ctx), 0), shortTimeout)
	_ = workflow.ExecuteActivity(ctx, act.CloseHarness, w.state.SessionID).Get(w.ctx, nil)
}

func (w *workflowRuntime) Infer() (tacklr.Reply, error) {
	var act *activities
	var out tacklr.Reply
	err := w.harnessStep(func(opts func(workflow.Context) workflow.Context) error {
		return w.waitActivity(opts, act.Inference, w.stepSession(), &out)
	})
	return out, w.stepErr(err)
}

// stepSession is what every activity needs to build the session's harness:
// the runtime holds it for the session, so steps do not carry it.
func (w *workflowRuntime) stepSession() stepSession {
	return stepSession{SessionID: w.state.SessionID, Identity: w.state.Identity, MCPServers: w.state.MCP, Agent: w.agent, Trace: w.state.Span}
}

// Tools runs one Tool activity per call, at most MaxToolCalls at once: the
// rest start as earlier ones finish. It returns each call as its run left
// it, in order. A queued cancel stops the ones still running.
func (w *workflowRuntime) Tools(calls []tacklr.Call) []tacklr.Call {
	var act *activities
	cctx, cancelAct := workflow.WithCancel(w.ctx)
	defer cancelAct()
	sess := w.stepSession()
	runs := calls
	futs := make([]workflow.Future, len(runs))
	start := func(i int) {
		runs[i].MaxJobs = cmp.Or(w.in.MaxChildren, defaultMaxChildren)
		ctx := workflow.WithStartToCloseTimeout(w.harnessCtx(cctx), max(w.opts.StartToCloseTimeout, runs[i].Timeout+orchestrator.ToolTimeoutMargin))
		futs[i] = workflow.ExecuteActivity(ctx, act.Tool, toolInput{stepSession: sess, Call: runs[i]})
	}
	limit := cmp.Or(w.in.MaxToolCalls, defaultMaxToolCalls)
	next := min(limit, len(runs))
	for i := range next {
		start(i)
	}
	results := slices.Clone(runs)
	done := make([]bool, len(runs))
	left := len(runs)
	w.fill()
	cancelled := w.takeCancel()
	for left > 0 && !cancelled {
		sel := workflow.NewSelector(w.ctx)
		for i, fut := range futs[:next] {
			if done[i] {
				continue
			}
			sel.AddFuture(fut, func(f workflow.Future) {
				// A zero Call: decoding into the input would keep what the
				// run cleared.
				var ran tacklr.Call
				err := f.Get(w.ctx, &ran)
				if w.lostWorker(err) {
					start(i)
					return
				}
				if err == nil {
					results[i] = ran
				}
				results[i].Err = w.stepErr(err)
				done[i] = true
				left--
				if next < len(runs) {
					start(next)
					next++
				}
			})
		}
		w.listen(sel, func() { cancelled = w.takeCancel() })
		sel.Select(w.ctx)
	}
	if cancelled {
		// The calls still running stop; each reports the cancel, so the turn
		// fails on it even when a call's tool returned on its own. A call
		// not started yet reports it too.
		cancelAct()
		for i := range runs {
			if !done[i] {
				if i < next {
					_ = futs[i].Get(w.ctx, nil)
				}
				results[i].Err = context.Canceled
			}
		}
	}
	return results
}

func (w *workflowRuntime) Emit(ev session.Event) {
	var act *activities
	// The turn already chose its outcome, so a cancel does not stop the
	// event that reports it; the cancel stays queued for the session. A
	// failed publish must not run the model again.
	_ = workflow.ExecuteActivity(w.sharedCtx(w.ctx), act.EmitEvent, emitEventInput{SessionID: w.in.SessionID, Event: ev}).Get(w.ctx, nil)
}

func (w *workflowRuntime) Job(name, task string) (string, error) {
	var act *activities
	var out string
	err := w.waitActivity(w.sharedCtx, act.RunJob, runJobInput{Name: name, Task: task}, &out)
	return out, w.stepErr(err)
}

// waitActivity runs one activity on opts and waits for it, or for a cancel,
// which stops it.
func (w *workflowRuntime) waitActivity(opts func(workflow.Context) workflow.Context, activity any, in, out any) error {
	cctx, cancelAct := workflow.WithCancel(w.ctx)
	defer cancelAct()
	fut := workflow.ExecuteActivity(opts(cctx), activity, in)
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
		w.listen(sel, func() { cancelled = w.takeCancel() })
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
	return err
}

// stepErr is a step's error as the turn sees it: a cancel is
// context.Canceled, and a failure its message.
func (w *workflowRuntime) stepErr(err error) error {
	if err == nil {
		return nil
	}
	if turnCanceled(w.ctx, err) {
		return context.Canceled
	}
	app, ok := errors.AsType[*temporal.ApplicationError](err)
	if !ok {
		return errors.New(err.Error())
	}
	// Error() appends the cause, which repeats the message.
	text := cmp.Or(app.Message(), err.Error())
	switch kind := session.FailKind(app.Type()); kind {
	case session.FailRefused, session.FailMaxTokens, session.FailContextLength:
		return kind.WithText(text)
	}
	return errors.New(text)
}

// continueAsNew reports whether the session should continue as new now:
// its history is long. Running children carry over by id. Every child end
// and child-waiting signal already delivered is taken first, so none is
// lost.
func (w *workflowRuntime) continueAsNew() bool {
	info := workflow.GetInfo(w.ctx)
	if n := w.in.ContinueAsNewEvents; n > 0 {
		if info.GetCurrentHistoryLength() < n {
			return false
		}
	} else if !info.GetContinueAsNewSuggested() {
		return false
	}
	return true
}

// Next is the oldest queued wake: a host wake or a job's end. With block it
// waits for one, or suspends a parent whose history is long (continue as
// new) once nothing is queued, so no wake is lost; without, it returns a
// zero Wake when none is queued.
func (w *workflowRuntime) Next(block bool) session.Wake {
	for {
		w.fill()
		if len(w.queue) > 0 {
			wake := w.queue[0]
			w.queue = w.queue[1:]
			return wake
		}
		if !block {
			return session.Wake{}
		}
		if w.continueAsNew() {
			return session.Wake{Kind: session.WakeSuspend}
		}
		sel := workflow.NewSelector(w.ctx)
		w.listen(sel, nil)
		sel.Select(w.ctx)
	}
}

// Start starts the job s names as a child session. A job already started
// under its id runs on: it is not started again.
func (w *workflowRuntime) Start(s tacklr.JobStart) error {
	spec, worker := "", s.Name
	if s.Specialist {
		spec, worker = s.Name, ""
	}
	return startChild(w.ctx, w.in.SessionID, spec, worker, s.Task, session.ID(s.ID), w.state.Mounts, w.in, w.state.Span)
}

// Cancel asks the job id names to stop.
func (w *workflowRuntime) Cancel(id session.ID) {
	// Disconnected: a cancel goes out even when the session's own run was
	// cancelled, as at its end.
	ctx, _ := workflow.NewDisconnectedContext(w.ctx)
	_ = workflow.RequestCancelExternalWorkflow(ctx, string(id), "").Get(ctx, nil)
}

// fill moves every wake already delivered on wakeCh into the queue, in
// arrival order.
func (w *workflowRuntime) fill() {
	var wake session.Wake
	for w.wakeCh.ReceiveAsync(&wake) {
		w.queue = append(w.queue, wake)
		wake = session.Wake{}
	}
	var done session.JobEnd
	for w.childDoneCh.ReceiveAsync(&done) {
		w.queue = append(w.queue, session.Wake{Kind: session.WakeJob, Job: done})
		done = session.JobEnd{}
	}
}

// listen adds to sel a receive for each mailbox channel that queues what
// arrives; then, when set, runs after.
func (w *workflowRuntime) listen(sel workflow.Selector, after func()) {
	sel.AddReceive(w.wakeCh, func(c workflow.ReceiveChannel, more bool) {
		w.receive(c)
		if after != nil {
			after()
		}
	})
	sel.AddReceive(w.childDoneCh, func(c workflow.ReceiveChannel, more bool) {
		var done session.JobEnd
		c.Receive(w.ctx, &done)
		w.queue = append(w.queue, session.Wake{Kind: session.WakeJob, Job: done})
	})
}

// receive queues one wake from c. Selectors call it when wakeCh is ready.
func (w *workflowRuntime) receive(c workflow.ReceiveChannel) {
	var wake session.Wake
	c.Receive(w.ctx, &wake)
	w.queue = append(w.queue, wake)
}

// takeCancel reports whether a cancel is queued and removes the first one,
// together with the prompts queued before it: those were steering for the
// turn the cancel ends. Wakes queued after the cancel stay, in order.
func (w *workflowRuntime) takeCancel() bool {
	kept, _, ok := orchestrator.TakeCancel(w.queue)
	w.queue = kept
	return ok
}

// turnCanceled reports whether a step's error is the turn's cancel.
func turnCanceled(ctx workflow.Context, err error) bool {
	return ctx.Err() != nil || temporal.IsCanceledError(err) || errors.Is(err, workflow.ErrCanceled) || errors.Is(err, workflow.ErrSessionFailed)
}

var _ orchestrator.Runtime = (*workflowRuntime)(nil)
