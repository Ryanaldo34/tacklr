package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// TestSessionWorkflow_aReplysCallsRunAtMostMaxToolCallsAtOnce: a reply's
// calls run at most MaxToolCalls at once. Each result comes back in call
// order, even a result that finished after a later call's, and a call to an
// unknown tool gets a correction in its place.
func TestSessionWorkflow_aReplysCallsRunAtMostMaxToolCallsAtOnce(t *testing.T) {
	t.Parallel()
	// Arrange
	env := newTestWorkflow(t)
	var running, peak atomic.Int32
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []string, ch chan<- tacklr.LLMResponseChunk) {
		if got := testkit.ToolResults(msgs); len(got) > 0 {
			results = got
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		var calls []message.ToolCall
		for i := range 4 {
			id := fmt.Sprintf("c%d", i)
			calls = append(calls, message.ToolCall{ID: id, CallID: id, Name: "slow", Arguments: fmt.Sprintf(`{"n":%d}`, i)})
		}
		calls = slices.Insert(calls, 2, message.ToolCall{ID: "nope", CallID: "nope", Name: "nope", Arguments: `{}`})
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: calls, IsComplete: true}
	})
	// The calls run in pairs: the first of a pair waits for the second to
	// end, so it finishes after the call after it, and the second waits for
	// the first to start, so both run at once.
	var started, ended [4]chan struct{}
	for i := range 4 {
		started[i], ended[i] = make(chan struct{}), make(chan struct{})
	}
	slow := tacklr.NewTool(tacklr.ToolConfig{Name: "slow", Handler: func(ctx context.Context, args struct {
		N int `json:"n"`
	}) (string, error) {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		close(started[args.N])
		var wait chan struct{}
		if args.N%2 == 0 {
			wait = ended[args.N+1]
		} else {
			wait = started[args.N-1]
		}
		select {
		case <-wait:
		case <-ctx.Done():
		}
		running.Add(-1)
		close(ended[args.N])
		return fmt.Sprintf("r%d", args.N), nil
	}})
	registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{slow}})
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

	// Act
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-fanout", MaxToolCalls: 2})

	// Assert
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 2 || len(results) != 5 || !slices.Equal(slices.Delete(slices.Clone(results), 2, 3), []string{"r0", "r1", "r2", "r3"}) || !strings.Contains(results[2], "not a registered tool") {
		t.Fatalf("peak %d calls at once with results %q, want 2 at once and every result in call order, the unknown tool's correction third", peak.Load(), results)
	}
}

// TestSessionWorkflow_aToolLongerThanTheStepTimeoutRunsToItsOwnTimeout: a
// tool with its own timeout runs past the step timeout and its result
// reaches the model.
func TestSessionWorkflow_aToolLongerThanTheStepTimeoutRunsToItsOwnTimeout(t *testing.T) {
	t.Parallel()
	// Arrange
	env := newTestWorkflow(t)
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []string, ch chan<- tacklr.LLMResponseChunk) {
		if got := testkit.ToolResults(msgs); len(got) > 0 {
			results = got
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
			ToolCalls: []message.ToolCall{{ID: "l1", CallID: "l1", Name: "long", Arguments: `{}`}}}
	})
	long := tacklr.NewTool(tacklr.ToolConfig{Name: "long", Timeout: time.Hour, Handler: func(context.Context) (string, error) {
		time.Sleep(300 * time.Millisecond)
		return "finished", nil
	}})
	registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{long}})
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

	// Act
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-long-tool", ActivityTimeout: 100 * time.Millisecond, ActivityAttempts: 1})

	// Assert
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(results, []string{"finished"}) {
		t.Fatalf("results = %v, want the tool to finish within its own timeout", results)
	}
}

// TestLive_aLongSessionContinuesAsNewAndGoesOnWithItsState: a session
// whose history passes its limit continues as new while it is idle, and
// while its finished turn waits on a job. Each next run goes on with the
// conversation, and the job's result lands in the run that waits for it.
func TestLive_aLongSessionContinuesAsNewAndGoesOnWithItsState(t *testing.T) {
	t.Parallel()
	// Arrange
	ctx := t.Context()
	release, let := releaseLater(t)
	start := tacklr.NewTool(tacklr.ToolConfig{Name: "start", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "build"})
		return job.ID, err
	}})
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		var users []string
		for _, m := range msgs {
			if m.Role == message.RoleUser {
				users = append(users, m.Content)
			}
		}
		switch last := testkit.LastMessage(msgs); {
		case last.Content == "start":
			return testkit.Calls(testkit.Call("s1", "start", `{}`))
		case last.Role == message.RoleTool:
			return testkit.Text("waiting")
		}
		return testkit.Text("re: " + strings.Join(users, ", "))
	})
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{Tools: []*tacklr.Tool{start}}), func(c *Config) {
		c.Jobs = map[string]session.JobHandler{"ci": func(context.Context, string) (string, error) { <-release; return "built", nil }}
	})
	c := temporal.Open(stack.Temporal, temporal.Config{TaskQueue: stack.TaskQueue, Store: stack.Store, Fallback: stack.Log, Secrets: stack.Secrets, ContinueAsNewEvents: continueAfter})
	id, err := c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	runs := []string{run(t, stack, id)}
	// moved waits until the session runs as new, and records the run.
	moved := func() {
		t.Helper()
		for deadline := time.Now().Add(time.Minute); run(t, stack, id) == runs[len(runs)-1]; time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("the session still runs as %s; want it continued as new", runs[len(runs)-1])
			}
		}
		runs = append(runs, run(t, stack, id))
	}

	// Act
	if err := c.Prompt(ctx, id, session.Prompt{Text: "first"}); err != nil {
		t.Fatal(err)
	}
	waitTurn(t, c, id, sub, time.Minute)
	moved()
	if err := c.Prompt(ctx, id, session.Prompt{Text: "start"}); err != nil {
		t.Fatal(err)
	}
	moved()
	let()
	evs := waitTurn(t, c, id, sub, time.Minute)

	// Assert
	job := tacklr.ChildID(string(id), "ci", "s1", false)
	want := fmt.Sprintf("re: first, start, Job %s (ci) completed:\nbuilt", job)
	if got := evs[len(evs)-1]; got.Type != session.EventComplete || !slices.ContainsFunc(evs, func(ev session.Event) bool { return ev.Type == session.EventMessage && !ev.Delta && ev.Content == want }) {
		t.Fatalf("runs %v, the last turn %+v; want it to end with the conversation and the job's result: %q", runs, evs, want)
	}
}

// continueAfter is the history length past which the continue-as-new test
// continues as new: past a run that only started, short of one that ran a
// turn.
const continueAfter = 20

// run is the id of the run the session's workflow runs now.
func run(t *testing.T, stack *liveStack, id session.ID) string {
	t.Helper()
	d, err := stack.Temporal.DescribeWorkflowExecution(t.Context(), string(id), "")
	if err != nil {
		t.Fatal(err)
	}
	return d.GetWorkflowExecutionInfo().GetExecution().GetRunId()
}
