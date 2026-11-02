package session

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
)

// PromptIn is one prompt delivered to a waiting session.
type PromptIn struct {
	Text        string
	UserMessage *tacklr.Message
	MCPServers  []mcp.MCPConfig
	Auth        AuthContext
	State       map[string]any
}

// ResumeIn is one resume delivered to a parked session.
type ResumeIn struct {
	Responses map[string][]byte
	Auth      AuthContext
	State     map[string]any
}

// ToolResult is one Tool step's result: the call's outcome, or the error
// that stopped the step (a tool's own failure is its outcome).
type ToolResult struct {
	Outcome tacklr.CallOutcome
	Err     error
}

// Step is the work of one turn. Each non-deterministic action is one step:
// Infer is one model request, Tool one tool call, Job one worker body. A
// runtime records each completed step and its result, and a replay returns
// the recorded result instead of running the step again; neither the
// harness nor the snapshot records which steps ran. A runtime may run a
// step more than once before it records it (a worker dies before it
// reports). Infer and Tool change nothing a later step reads, so running
// them again is safe; a tool with external side effects uses its call id
// (HarnessRuntime.CurrentToolCallID) as the idempotency key.
//
// Commit is the only step that writes the snapshot, and it is idempotent:
// base is the revision the previous Commit returned, and Commit applies in
// only when the snapshot is still at base, so a Commit that already saved
// does not apply it again. A cancel does not stop a Commit, so the turn
// always learns the revision it saved. Tools runs one Tool step per run,
// all at once (the runtime decides how: one activity each, or goroutines),
// and returns a result per run in run order.
//
// Steps take only what changes per step. The session's identity, MCP
// servers and children are the runtime's: it holds them for the session
// and builds each step's world from them. Begin and End bracket Run so the
// implementation can pin turn-scoped resources.
type Step interface {
	Begin(kind string, promptLen, resumes int)
	End()
	Commit(base Revision, in tacklr.CommitIn) (Revision, error)
	Infer() (tacklr.Reply, error)
	Tools(runs []tacklr.ToolRun) []ToolResult
	Emit(tacklr.StreamEvent)
	Job(name, task string) (string, error)
}

// Turn is one session's scheduler state. The snapshot holds what committed;
// Round is the reply whose calls do not all have results yet, rebuilt from
// recorded step results on replay. Run is one prompt or resume, and returns
// when the turn parks, finishes, or fails.
type Turn struct {
	SessionID SessionID
	Identity

	MCP     []mcp.MCPConfig
	Inbox   []*tacklr.Message
	NextMCP []mcp.MCPConfig
	// State is host values not yet committed. The next Commit writes them
	// into the session's user state.
	State map[string]any
	// Round is the open round: each call's last outcome.
	Round *tacklr.Round
	// Rev is the snapshot revision of the last Commit.
	Rev Revision

	Result   string
	Terminal SessionState
	Err      error
	Yielded  bool
}

// Run drives one turn. user is the prompt; resume, the answers to a park.
func (t *Turn) Run(step Step, sig Mailbox, ledger Ledger, user *tacklr.Message, resume map[string][]byte, auth AuthContext, extra map[string]any) {
	t.queueState(extra)
	t.Mounts = ApplyAuth(t.Mounts, auth)
	t.Terminal = ""
	t.Err = nil
	t.Yielded = false
	t.Result = ""

	fail := func(err error) {
		t.Err = err
		t.Terminal = SessionFailed
		msg := err.Error()
		if errors.Is(err, context.Canceled) {
			msg = context.Canceled.Error()
			t.Inbox = nil
			t.NextMCP = nil
			ledger.CancelAll()
		}
		step.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
	}
	commit := func(in tacklr.CommitIn) bool {
		in.State = t.State
		rev, err := step.Commit(t.Rev, in)
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
		first.Users = []*tacklr.Message{user}
		first.Round, t.Round = t.Round, nil
	}
	if (first.Round != nil || first.Users != nil || t.State != nil) && !commit(first) {
		return
	}
	if t.Round == nil && len(resume) > 0 {
		fail(unknownAnswer(resume, ""))
		return
	}
	inferComplete := false
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
			if len(runs) == 0 {
				runs, err = t.awaitChildren(r, ledger)
				if err != nil {
					fail(err)
					return
				}
			}
			if len(runs) == 0 {
				// The round waits on its parked call.
				t.Yielded = true
				park := r.Outcomes[r.Parked()].Park
				step.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventInterrupt, MessageID: park.InterruptID, Data: park.Data})
				return
			}
			if err := t.record(r, step.Tools(runs), ledger); err != nil {
				fail(err)
				return
			}
			continue
		}
		t.Inbox = append(t.Inbox, sig.Ready()...)
		if user == nil {
			for _, p := range sig.Prompts() {
				t.Steer(p)
			}
		}
		user = nil
		if len(t.Inbox) > 0 {
			users := t.Inbox
			t.Inbox = nil
			if !commit(tacklr.CommitIn{Users: users}) {
				return
			}
			inferComplete = false
		}
		if inferComplete {
			if len(ledger.IDs()) == 0 {
				t.Terminal = SessionComplete
				step.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventComplete})
				return
			}
			switch w := sig.Wait(); w.Kind {
			case WakePrompt:
				t.Steer(w.Prompt)
			case WakeChild:
				if w.Child != nil {
					t.Inbox = append(t.Inbox, w.Child)
				}
			case WakeCancel:
				fail(context.Canceled)
				return
			}
			continue
		}
		reply, err := step.Infer()
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
		t.Result, inferComplete = reply.Result, true
	}
}

// runs is the calls of r to run next, in call order. While a call is
// parked nothing else runs; once resume answers it, it runs with every call
// that has no outcome yet (a call whose park another call's park
// deferred). An answer to no parked call is an error.
func (t *Turn) runs(r *tacklr.Round, resume map[string][]byte) ([]tacklr.ToolRun, error) {
	var runs []tacklr.ToolRun
	if i := r.Parked(); i >= 0 {
		id := r.Outcomes[i].Park.InterruptID
		answer, ok := resume[id]
		if !ok {
			return nil, unknownAnswer(resume, "")
		}
		if err := unknownAnswer(resume, id); err != nil {
			return nil, err
		}
		runs = append(runs, tacklr.ToolRun{Call: r.Reply.Calls[i], Prior: &r.Outcomes[i], Answer: answer})
	} else if err := unknownAnswer(resume, ""); err != nil {
		return nil, err
	}
	for i, o := range r.Outcomes {
		if o.Call.Key() == "" {
			runs = append(runs, tacklr.ToolRun{Call: r.Reply.Calls[i]})
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

// awaitChildren waits for every child a call of r waits on and returns the
// runs that give each call its child's result.
func (t *Turn) awaitChildren(r *tacklr.Round, ledger Ledger) ([]tacklr.ToolRun, error) {
	var runs []tacklr.ToolRun
	for i, o := range r.Outcomes {
		if o.Done || o.AwaitID == "" {
			continue
		}
		output, failed, err := ledger.Await(SessionID(o.AwaitID))
		if err != nil {
			return nil, err
		}
		runs = append(runs, tacklr.ToolRun{Call: r.Reply.Calls[i], Prior: &r.Outcomes[i], Child: &tacklr.ChildResult{Output: output, Failed: failed}})
	}
	return runs, nil
}

// record keeps each result's outcome on its call of r, and starts and
// cancels the children each kept outcome asks for. A turn parks on one
// interrupt: a call that parks after the first, in call order, keeps no
// outcome and runs again after resume. Every result is kept before a step
// error fails the turn.
func (t *Turn) record(r *tacklr.Round, results []ToolResult, ledger Ledger) error {
	var errs []error
	for _, res := range results {
		if res.Err != nil {
			errs = append(errs, res.Err)
			continue
		}
		o := res.Outcome
		i := slices.IndexFunc(r.Reply.Calls, func(c tacklr.ToolCall) bool { return c.Key() == o.Call.Key() })
		r.Outcomes[i] = tacklr.CallOutcome{}
		if o.Park != nil && !o.Done && r.Parked() >= 0 {
			continue
		}
		r.Outcomes[i] = o
		if err := ledger.Start(o); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Steer queues a prompt onto this turn with its auth and state.
func (t *Turn) Steer(p PromptIn) {
	msg := p.UserMessage
	if msg == nil {
		msg = &tacklr.Message{Role: tacklr.RoleUser, Content: p.Text}
	}
	t.Inbox = append(t.Inbox, msg)
	t.Mounts = ApplyAuth(t.Mounts, p.Auth)
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
