package loop

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	adapter "github.com/ryanaldo34/tacklr/session/internal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// ToolResult is one Tool step's result: the call's outcome, or the error
// that stopped the step (a tool's own failure is its outcome).
type ToolResult struct {
	Outcome tacklr.CallOutcome
	Err     error
}

// Executor is how one runtime runs a session: its steps, its mailbox and
// its children. Every runtime implements it; Run is the same for all of
// them.
//
// Steps. Each non-deterministic action is one step:
// Infer is one model request, Tool one tool call, Job one worker body. A
// runtime records each completed step and its result, and a replay returns
// the recorded result instead of running the step again; neither the
// harness nor the snapshot records which steps ran. A runtime may run a
// step more than once before it records it (a worker dies before it
// reports). Infer and Tool change nothing a later step reads, so running
// them again is safe; a tool with external side effects uses its call id
// (CallContext.CurrentToolCallID) as the idempotency key.
//
// Commit is the only step that writes the snapshot, and it is idempotent:
// base is the revision the previous Commit returned, and Commit applies in
// only when the snapshot is still at base, so a Commit that already saved
// does not apply it again. A cancel does not stop a Commit, so the turn
// always learns the revision it saved. Tools runs one Tool step per run,
// all at once (the runtime decides how: one activity each, or goroutines),
// and returns a result per run in run order.
//
// Steps take only what changes per step. The session's identity and MCP
// servers are the runtime's: it holds them for the session
// and builds each step's harness from them. Begin and End bracket a turn so
// the implementation can pin turn-scoped resources. A Begin of kind
// session.WakePrompt starts a new turn: the runtime then trims the event
// log and the outcome records of the turns before it, which committed. A
// resume or a continue goes on with the turn it parked or suspended.
//
// Mailbox. Next is the one read of the mailbox: prompts, resumes,
// cancels, closes, and job ends, in the order they arrived. With block it
// waits for one; without, it returns a zero Wake when none is queued. The
// session loop (Run) decides what each does. A step that runs while
// a cancel is queued stops early; the cancel keeps its place.
//
// Jobs. The session owns each job's lifecycle (Turn.Jobs): it starts a
// job by its deterministic id, records the call that waits on it, cancels
// it by id, and turns its end into that call's result or an inbox message.
// The runtime only runs it: Start starts the job (a job already started
// under that id is not started again) and Cancel stops it. A job's end
// reaches the session one way, as a WakeJob from Next.
//
// Suspend. Next with block may return WakeSuspend so the runtime can
// restart the session from the same state (on Temporal, continue-as-new),
// while idle or while a turn waits on jobs. The running jobs carry over
// with the turn.
type Executor interface {
	Begin(kind string, promptLen, resumes int)
	End()
	Commit(base session.Revision, in tacklr.CommitIn) (session.Revision, error)
	Infer() (tacklr.Reply, error)
	Tools(runs []tacklr.ToolRun) []ToolResult
	Emit(session.Event)
	Job(name, task string) (string, error)

	Next(block bool) session.Wake

	Start(tacklr.JobStart) error
	Cancel(id session.ID)
}

// Turn is one session's scheduler state. The snapshot holds what committed;
// Round is the reply whose calls do not all have results yet, rebuilt from
// recorded step results on replay. Run drives it: a turn is one prompt or
// resume, and ends when it parks, finishes, or fails.
type Turn struct {
	SessionID session.ID
	Identity

	MCP     []session.MCPServer
	Inbox   []*message.Message
	NextMCP []session.MCPServer
	// State is host values not yet committed. The next Commit writes them
	// into the session's user state.
	State map[string]any
	// Round is the open round: each call's last outcome.
	Round *tacklr.Round
	// Rev is the snapshot revision of the last Commit.
	Rev session.Revision

	Result   string
	Terminal session.State
	Err      error
	Yielded  bool
	// Finished says the model finished the turn, which waits on children.
	Finished bool
	// Suspended says the runtime stopped the turn mid-way to restart the
	// session, which goes on with it in the restarted session.
	Suspended bool
	// Jobs are the session's running jobs, in start order: the one record
	// of their lifecycle (started, running, ended).
	Jobs []Job
	// Closed says the session was closed: Run returned for good.
	Closed bool
	// cancelled says a cancel came in while the turn ran.
	cancelled bool
}

// Job is one job the session started, by a deterministic id. Call is the
// key of the call that waits on it; empty for a background job. A waited
// job's end is kept (Ended, Result) until its call takes it.
type Job struct {
	ID     session.ID
	Name   string
	Call   string `json:",omitempty"`
	Ended  bool   `json:",omitempty"`
	Result session.JobResult
}

// jobEnded records end: the waiting call's result, or for a background job
// an inbox message. A job the session no longer runs (cancelled) is gone.
func (t *Turn) jobEnded(end session.JobEnd) {
	i := slices.IndexFunc(t.Jobs, func(j Job) bool { return j.ID == end.ID })
	if i < 0 {
		return
	}
	if j := &t.Jobs[i]; j.Call != "" {
		j.Ended, j.Result = true, end.Result
		return
	}
	t.Inbox = append(t.Inbox, jobMessage(t.Jobs[i], end.Result))
	t.Jobs = slices.Delete(t.Jobs, i, i+1)
}

// jobMessage is a background job's result as the inbox message the turn
// gets: a new user message, not a second result for the call that started it.
func jobMessage(j Job, res session.JobResult) *message.Message {
	verb := "completed"
	if res.Failed {
		verb = "failed"
	}
	return &message.Message{Role: message.RoleUser, Content: fmt.Sprintf("Job %s (%s) %s:\n%s", j.ID, j.Name, verb, res.Output)}
}

// cancelJobs stops every job the session runs and forgets them.
func (t *Turn) cancelJobs(d Executor) {
	for _, j := range t.Jobs {
		if !j.Ended {
			d.Cancel(j.ID)
		}
	}
	t.Jobs = nil
}

// jobIDs are the running jobs' ids, in start order.
func (t *Turn) jobIDs() []string {
	ids := make([]string, len(t.Jobs))
	for i, j := range t.Jobs {
		ids[i] = string(j.ID)
	}
	return ids
}

// start opens one turn: user is the prompt; resume, the answers to a park.
func (t *Turn) start(user *message.Message, resume map[string][]byte, auth session.AuthContext, extra map[string]any) {
	t.queueState(extra)
	t.Mounts = adapter.ApplyAuth(t.Mounts, auth)
	t.Terminal = ""
	t.Err = nil
	t.Yielded = false
	t.Result = ""
	t.Finished = false
	t.cancelled = false
}

// drive runs the turn until it parks, finishes, fails, or is suspended.
// pump hands the session the wakes that come in meanwhile (blocking when
// the turn waits on jobs); a cancel or suspend among them stops the turn.
func (t *Turn) drive(d Executor, user *message.Message, resume map[string][]byte, pump func(block bool)) {
	fail := func(err error) {
		t.Err = err
		t.Terminal = session.Failed
		if errors.Is(err, context.Canceled) {
			t.Inbox = nil
			t.NextMCP = nil
			t.cancelJobs(d)
		}
		d.Emit(session.FailureEvent(err))
	}
	// stopped takes the wakes queued now (or waits for one) and reports
	// whether they stopped the turn.
	stopped := func(block bool) bool {
		pump(block)
		if t.cancelled {
			t.cancelled = false
			fail(context.Canceled)
			return true
		}
		return t.Suspended
	}
	commit := func(in tacklr.CommitIn) bool {
		in.State = t.State
		rev, err := d.Commit(t.Rev, in)
		if err != nil {
			fail(err)
			return false
		}
		t.State, t.Rev = nil, rev
		return true
	}

	// A prompt moves past an open round (a turn that failed): its calls
	// without a result are cancelled and it commits, then the prompt.
	var first tacklr.CommitIn
	if user != nil {
		first.Users = []*message.Message{user}
		first.Round, t.Round = t.Round, nil
	}
	if (first.Round != nil || first.Users != nil || t.State != nil) && !commit(first) {
		return
	}
	if t.Round == nil && len(resume) > 0 {
		fail(unknownAnswer(resume, ""))
		return
	}
	for {
		if r := t.Round; r != nil {
			if r.Complete() {
				if !commit(tacklr.CommitIn{Round: r}) {
					return
				}
				t.Round = nil
				continue
			}
			runs, err := t.runs(r, resume)
			resume = nil
			if err != nil {
				fail(err)
				return
			}
			if len(runs) == 0 && t.waits(r) {
				if runs = t.jobRuns(r); len(runs) == 0 {
					if stopped(true) {
						return
					}
					continue
				}
			}
			if len(runs) == 0 {
				// The round waits on its parked call.
				t.Yielded = true
				park := r.Outcomes[r.Parked()].Park
				d.Emit(session.Event{Type: session.EventInterrupt, MessageID: park.InterruptID, Data: park.Data})
				return
			}
			for i := range runs {
				runs[i].Children = t.jobIDs()
			}
			if err := t.record(r, d.Tools(runs), d); err != nil {
				fail(err)
				return
			}
			continue
		}
		if stopped(false) {
			return
		}
		if len(t.Inbox) > 0 {
			users := t.Inbox
			t.Inbox = nil
			if !commit(tacklr.CommitIn{Users: users}) {
				return
			}
			t.Finished = false
		}
		if t.Finished {
			if len(t.Jobs) == 0 {
				t.Terminal = session.Complete
				d.Emit(session.Event{Type: session.EventComplete})
				return
			}
			// The model is done; the turn waits for its jobs, whose results
			// (and any prompt) come back as user messages.
			if stopped(true) {
				return
			}
			continue
		}
		reply, err := d.Infer()
		if err != nil {
			fail(err)
			return
		}
		if len(reply.Calls) > 0 {
			t.Round = tacklr.NewRound(reply)
			continue
		}
		if !commit(tacklr.CommitIn{Round: &tacklr.Round{Reply: reply}}) {
			return
		}
		t.Result, t.Finished = reply.Result, true
	}
}

// runs is the calls of r to run next, in call order. While a call is
// parked nothing else runs; once resume answers it, it runs with every call
// that has no outcome yet (a call whose park another call's park
// deferred). An answer to no parked call is an error.
func (t *Turn) runs(r *tacklr.Round, resume map[string][]byte) ([]tacklr.ToolRun, error) {
	parked, id := r.Parked(), ""
	if parked >= 0 {
		id = r.Outcomes[parked].Park.InterruptID
	}
	if err := unknownAnswer(resume, id); err != nil {
		return nil, err
	}
	var runs []tacklr.ToolRun
	if parked >= 0 {
		answer, ok := resume[id]
		if !ok {
			// Still parked: nothing runs until the answer comes.
			return nil, nil
		}
		runs = append(runs, tacklr.ToolRun{Call: r.Reply.Calls[parked], Timeout: r.Reply.Timeout(parked), Prior: &r.Outcomes[parked], Answer: answer})
	}
	for i, o := range r.Outcomes {
		if o.Call.Key() == "" {
			runs = append(runs, tacklr.ToolRun{Call: r.Reply.Calls[i], Timeout: r.Reply.Timeout(i)})
		}
	}
	slices.SortStableFunc(runs, func(a, b tacklr.ToolRun) int {
		return slices.Index(r.Reply.Calls, a.Call) - slices.Index(r.Reply.Calls, b.Call)
	})
	return runs, nil
}

// unknownAnswer is an error for an answer in resume other than to id.
func unknownAnswer(resume map[string][]byte, id string) error {
	for key := range resume {
		if key != id {
			return fmt.Errorf("no tool call id found for interrupt %s: %w", key, tacklr.ErrInterruptNotFound)
		}
	}
	return nil
}

// waits reports whether a call of r waits on a job.
func (t *Turn) waits(r *tacklr.Round) bool {
	return slices.ContainsFunc(r.Outcomes, func(o tacklr.CallOutcome) bool { return !o.Done && o.AwaitID != "" })
}

// jobRuns gives each call of r whose job ended the job's result, in call
// order, and drops those jobs.
func (t *Turn) jobRuns(r *tacklr.Round) []tacklr.ToolRun {
	var runs []tacklr.ToolRun
	for i, call := range r.Reply.Calls {
		k := slices.IndexFunc(t.Jobs, func(j Job) bool { return j.Ended && j.Call == call.Key() })
		if k < 0 {
			continue
		}
		res := t.Jobs[k].Result
		runs = append(runs, tacklr.ToolRun{Call: call, Timeout: r.Reply.Timeout(i), Prior: &r.Outcomes[i], Child: &res})
		t.Jobs = slices.Delete(t.Jobs, k, k+1)
	}
	return runs
}

// record keeps each result's outcome on its call of r, and starts and
// cancels the children each kept outcome asks for. A turn parks on one
// interrupt: a call that parks after the first, in call order, keeps no
// outcome and runs again after resume. Every result is kept before a step
// error fails the turn.
func (t *Turn) record(r *tacklr.Round, results []ToolResult, d Executor) error {
	var errs []error
	for _, res := range results {
		if res.Err != nil {
			errs = append(errs, res.Err)
			continue
		}
		o := res.Outcome
		i := slices.IndexFunc(r.Reply.Calls, func(c message.ToolCall) bool { return c.Key() == o.Call.Key() })
		r.Outcomes[i] = tacklr.CallOutcome{}
		if o.Park != nil && !o.Done && r.Parked() >= 0 {
			continue
		}
		r.Outcomes[i] = o
		if err := t.startJobs(o, d); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// startJobs cancels and starts the jobs o asks for. A job id comes from its
// call, so a job the session already runs is not started again; the job o
// waits on is marked with o's call.
func (t *Turn) startJobs(o tacklr.CallOutcome, d Executor) error {
	for _, id := range o.Cancels {
		if i := slices.IndexFunc(t.Jobs, func(j Job) bool { return j.ID == session.ID(id) }); i >= 0 {
			d.Cancel(t.Jobs[i].ID)
			t.Jobs = slices.Delete(t.Jobs, i, i+1)
		}
	}
	for _, s := range o.Starts {
		call := ""
		if s.ID == o.AwaitID {
			call = o.Call.Key()
		}
		if i := slices.IndexFunc(t.Jobs, func(j Job) bool { return j.ID == session.ID(s.ID) }); i >= 0 {
			t.Jobs[i].Call = cmp.Or(t.Jobs[i].Call, call)
			continue
		}
		if err := d.Start(s); err != nil {
			return err
		}
		t.Jobs = append(t.Jobs, Job{ID: session.ID(s.ID), Name: s.Name, Call: call})
	}
	return nil
}

// Steer queues a prompt onto this turn with its auth and state.
func (t *Turn) Steer(p session.Prompt) {
	msg := p.UserMessage
	if msg == nil {
		msg = &message.Message{Role: message.RoleUser, Content: p.Text}
	}
	t.Inbox = append(t.Inbox, msg)
	t.Mounts = adapter.ApplyAuth(t.Mounts, p.Auth)
	t.queueState(p.State)
	if p.MCPServers != nil {
		t.NextMCP = p.MCPServers
	}
}

// queueState adds host values for the next step to send; a later value for
// a key wins.
func (t *Turn) queueState(state map[string]any) {
	if len(state) == 0 {
		return
	}
	if t.State == nil {
		t.State = make(map[string]any, len(state))
	}
	maps.Copy(t.State, state)
}
