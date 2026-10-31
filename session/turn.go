package session

import (
	"context"
	"errors"
	"maps"

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

// InferenceInput is one model step. State, on every step input, is host
// values the step writes into the session's user state before it runs; the
// turn sends each value once.
type InferenceInput struct {
	SessionID  SessionID
	Identity   Identity
	MCPServers []mcp.MCPConfig
	State      map[string]any
	User       *tacklr.Message
	Extra      []*tacklr.Message
}

// InferenceOutput is the model step result. Complete is a reply with no
// tool calls, and Result is its text. Otherwise the reply opened a round
// whose calls the next Tools step runs.
type InferenceOutput struct {
	Complete bool
	Result   string
}

// ToolsInput runs the open round's runnable calls. Seq numbers the Tools
// step, so a retried step reports what its first attempt recorded. Resume,
// when set, is the answers to a park: they apply first, so the parked call
// runs again.
type ToolsInput struct {
	SessionID  SessionID
	Identity   Identity
	MCPServers []mcp.MCPConfig
	State      map[string]any
	// Children are the session's child ids in start order.
	Children []SessionID
	Seq      int
	Resume   map[string][]byte
}

// ToolOutput is one call the Tools step ran. Starts and Cancels are the
// children the runtime starts and cancels. AwaitID blocks this call until
// that child finishes.
type ToolOutput struct {
	Call          tacklr.ToolCall
	Interrupted   bool
	InterruptID   string
	InterruptData []byte
	AwaitID       SessionID
	Starts        []tacklr.JobStart
	Cancels       []string
}

// CommitInput writes a tool output the runtime already has (a child result)
// without running the tool again.
type CommitInput struct {
	SessionID  SessionID
	Identity   Identity
	MCPServers []mcp.MCPConfig
	State      map[string]any
	Call       tacklr.ToolCall
	Output     string
	// Failed is a child that failed: Output is its error.
	Failed bool
}

// Step is the non-deterministic work inside one turn: model, tool calls,
// worker body, and the terminal event. The implementation retries a wrapped
// network error, a model refusal, and a stale checkpoint. Any other error
// stops on the first attempt. Begin and End bracket Run so the
// implementation can pin turn-scoped resources.
//
// Tools runs the open round's calls as one step, through the harness
// Engine's RunTools, and returns one output per call it ran, in call order.
// The runtime decides where a step runs (a durable activity, or inline).
type Step interface {
	Begin(kind string, promptLen, resumes int)
	End()
	Infer(InferenceInput) (InferenceOutput, error)
	Tools(ToolsInput) ([]ToolOutput, error)
	Commit(CommitInput) error
	Emit(tacklr.StreamEvent)
	Job(name, task string) (string, error)
}

// Turn is one session's scheduler state. The snapshot holds the
// conversation. Run is one prompt or resume, and returns when the turn
// parks, finishes, or fails.
type Turn struct {
	SessionID SessionID
	Identity

	MCP     []mcp.MCPConfig
	Inbox   []*tacklr.Message
	NextMCP []mcp.MCPConfig
	// State is host values not yet sent to a step. The next step writes
	// them into the session's user state, which the snapshot holds.
	State map[string]any

	// ToolSteps counts Tools steps over the session's life: the next step's
	// Seq.
	ToolSteps int

	Result   string
	Terminal SessionState
	Err      error
	Yielded  bool
}

// Run drives one turn. user is the prompt. resume is set when continuing a park.
func (t *Turn) Run(step Step, sig Mailbox, ledger Ledger, user *tacklr.Message, resume map[string][]byte, auth AuthContext, extra map[string]any) {
	t.queueState(extra)
	t.Mounts = ApplyAuth(t.Mounts, auth)
	t.Terminal = ""
	t.Err = nil
	t.Yielded = false
	t.Result = ""

	fail := func(err error) {
		if err == nil {
			return
		}
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

	var extraUsers []*tacklr.Message
	// runnable is an open round with calls to run: after a reply that called
	// tools, or a resume.
	runnable := len(resume) > 0
	parked := false
	inferComplete := false
	var parkedOn ToolOutput

	for {
		if !runnable && !parked {
			t.Inbox = append(t.Inbox, sig.Ready()...)
			if user == nil {
				for _, p := range sig.Prompts() {
					t.Steer(p)
				}
			}
			if len(t.Inbox) > 0 {
				extraUsers = append(extraUsers, t.Inbox...)
				t.Inbox = nil
				inferComplete = false
			}
		}
		switch tacklr.Next(runnable, parked, inferComplete, len(ledger.IDs()) > 0) {
		case tacklr.ActionInfer:
			out, err := step.Infer(InferenceInput{
				SessionID:  t.SessionID,
				Identity:   t.Identity,
				MCPServers: t.MCP,
				User:       user,
				Extra:      extraUsers,
				State:      t.State,
			})
			user = nil
			extraUsers = nil
			if err != nil {
				fail(err)
				return
			}
			t.State = nil
			inferComplete, runnable = out.Complete, !out.Complete
			if out.Complete {
				t.Result = out.Result
			}
		case tacklr.ActionRunTools:
			t.ToolSteps++
			outs, err := step.Tools(ToolsInput{
				SessionID:  t.SessionID,
				Identity:   t.Identity,
				MCPServers: t.MCP,
				State:      t.State,
				Children:   ledger.IDs(),
				Seq:        t.ToolSteps,
				Resume:     resume,
			})
			resume, runnable = nil, false
			if err != nil {
				fail(err)
				return
			}
			t.State = nil
			// Start every child before awaiting any, so blocking
			// specialists from one reply run at the same time.
			for _, tout := range outs {
				if err := ledger.Start(tout); err != nil {
					fail(err)
					return
				}
			}
			for _, tout := range outs {
				switch {
				case tout.AwaitID != "":
					output, failed, err := ledger.Await(tout.AwaitID)
					if err != nil {
						fail(err)
						return
					}
					if err := step.Commit(CommitInput{
						SessionID:  t.SessionID,
						Identity:   t.Identity,
						MCPServers: t.MCP,
						Call:       tout.Call,
						Output:     output,
						Failed:     failed,
						State:      t.State,
					}); err != nil {
						fail(err)
						return
					}
					t.State = nil
				case tout.Interrupted:
					parkedOn, parked = tout, true
				}
			}
		case tacklr.ActionYield:
			t.Yielded = true
			step.Emit(tacklr.StreamEvent{
				Type:      tacklr.StreamEventInterrupt,
				MessageID: parkedOn.InterruptID,
				Data:      parkedOn.InterruptData,
			})
			return
		case tacklr.ActionComplete:
			t.Terminal = SessionComplete
			step.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventComplete})
			return
		case tacklr.ActionWait:
			t.Inbox = append(t.Inbox, sig.Ready()...)
			if len(t.Inbox) > 0 || len(ledger.IDs()) == 0 {
				break
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
		}
	}
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
