package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

func newTestWorkflow(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(sessionWorkflow("agent"), workflow.RegisterOptions{Name: protocol.Workflow})
	// A job tells its parent its result by signal, the one way a result
	// arrives; this delivers it as Temporal would. The root workflow runs
	// under the environment's own id, not its session id, so a signal to
	// an id the environment does not run is the root's. The environment
	// runs this on a goroutine of its own, so the signal goes through its
	// main loop, which owns the workflows it runs.
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(_, workflowID, _, signal string, arg any) error {
			env.RegisterDelayedCallback(func() {
				if env.SignalWorkflowByID(workflowID, signal, arg) != nil {
					env.SignalWorkflow(signal, arg)
				}
			}, 0)
			return nil
		})
	return env
}

func registerSession(env *testsuite.TestWorkflowEnvironment, agent tacklr.AgentOptions) (*activities, session.EventLog) {
	log := session.NewMemoryEventLog()
	acts := newActs(agent, log, true)
	env.RegisterActivity(acts)
	return acts, log
}

func signalAt(env *testsuite.TestWorkflowEnvironment, at time.Duration, wake session.Wake) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, wake)
	}, at)
}

// when runs then once ready holds, checking on the test clock. A wake whose
// meaning depends on the session's state (a prompt that must start a new
// turn, a cancel that must find a running tool) goes after that state, not
// after a fixed delay that a loaded machine can outrun. Wakes sent after it
// keep their order: the session queues them.
func when(env *testsuite.TestWorkflowEnvironment, ready func() bool, then func()) {
	var poll func()
	poll = func() {
		if !ready() {
			env.RegisterDelayedCallback(poll, 5*time.Millisecond)
			return
		}
		then()
	}
	env.RegisterDelayedCallback(poll, time.Millisecond)
}

func finishSession(t *testing.T, env *testsuite.TestWorkflowEnvironment, log session.EventLog, in protocol.Input) []session.Event {
	t.Helper()
	id := in.SessionID
	env.ExecuteWorkflow(protocol.Workflow, in)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	return drainLog(t, log, id)
}

func visibleText(evs []session.Event) string {
	var b strings.Builder
	for _, ev := range evs {
		b.WriteString(ev.Content)
		b.WriteByte('\n')
		b.WriteString(ev.Fail)
		b.WriteByte('\n')
	}
	return b.String()
}

func releaseLater(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	let := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(let)
	return ch, let
}

// waitAll blocks until every caller has arrived, or fails after a deadline:
// a group whose calls did not run at once never gets past it.
type waitAll struct {
	arrived sync.WaitGroup
	all     chan struct{}
}

func newWaitAll(n int) *waitAll {
	w := &waitAll{all: make(chan struct{})}
	w.arrived.Add(n)
	go func() { w.arrived.Wait(); close(w.all) }()
	return w
}

func (w *waitAll) arrive() error {
	w.arrived.Done()
	select {
	case <-w.all:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("the other calls of the group never ran alongside this one")
	}
}

// TestSessionWorkflow_blockingSpecialistsFromOneReplyRunAtOnce: two
// blocking specialists of one reply run at the same time, and their results
// come back in call order.
func TestSessionWorkflow_blockingSpecialistsFromOneReplyRunAtOnce(t *testing.T) {
	t.Parallel()
	env := newTestWorkflow(t)
	both := newWaitAll(2)
	var results []string
	spawn := func(id, task string) message.ToolCall {
		return testkit.Call(id, "spawn_specialist", fmt.Sprintf(`{"specialist":"researcher","task_description_and_context":%q,"block":true}`, task))
	}
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if last := testkit.LastMessage(msgs); last.Role == message.RoleUser && strings.HasPrefix(last.Content, "child-") {
			if err := both.arrive(); err != nil {
				return testkit.Text(err.Error())
			}
			return testkit.Text(last.Content + "-done")
		}
		if results = testkit.ToolResults(msgs); len(results) > 0 {
			return testkit.Text("done")
		}
		return testkit.Calls(spawn("sp_a", "child-a"), spawn("sp_b", "child-b"))
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
	registerSession(env, agent)
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, 400*time.Millisecond, session.Wake{Kind: session.WakeClose})
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-two-blocking"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !strings.Contains(results[0], "child-a-done") || !strings.Contains(results[1], "child-b-done") {
		t.Fatalf("tool results = %q, want both specialists' results in call order", results)
	}
}

// TestSessionWorkflow_aRetriedCommitRunsEachToolOnce: a Commit whose
// summary request fails once on the network runs again and the turn
// completes. Each tool runs once.
func TestSessionWorkflow_aRetriedCommitRunsEachToolOnce(t *testing.T) {
	t.Parallel()
	env := newTestWorkflow(t)
	var aRuns, bRuns, summaries atomic.Int32
	big := strings.Repeat("b", 600)
	model := testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		summary := slices.ContainsFunc(msgs, func(m *message.Message) bool {
			return strings.Contains(m.Content, "Summarize the entire message history")
		})
		switch last := testkit.LastMessage(msgs); {
		case summary && summaries.Add(1) == 1:
			ch <- testkit.Fail(fmt.Errorf("%w: blip", tacklr.ErrNetwork))
		case summary:
			ch <- testkit.Text("S")
		case last.Role == message.RoleTool && last.Content == "ok":
			ch <- testkit.Calls(testkit.Call("b", "b", `{}`))
		case last.Role == message.RoleTool:
			ch <- testkit.Text("done")
		default:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: strings.Repeat("w", 600), IsComplete: true}
			ch <- testkit.Calls(testkit.Call("a", "a", `{}`))
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 6000, Tools: []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "a", Handler: func(context.Context) (string, error) { aRuns.Add(1); return "ok", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "b", Handler: func(context.Context) (string, error) { bRuns.Add(1); return big, nil }}),
	}}
	registerSession(env, agent)
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-retried-commit"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != session.Complete || aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Fatalf("state %v, a ran %d and b ran %d times; want complete after the retried summary, each tool once", st.State, aRuns.Load(), bRuns.Load())
	}
}

// TestSessionWorkflow_aStepThatDiesRunsAgainWithoutTheCallsThatCompleted:
// one call of a reply dies before its step reports (its first run panics,
// as a worker that dies does), and the runtime runs that step again. The
// call that completed is not run again, and the round commits both results
// in call order.
func TestSessionWorkflow_aStepThatDiesRunsAgainWithoutTheCallsThatCompleted(t *testing.T) {
	t.Parallel()
	env := newTestWorkflow(t)
	var aRuns, bRuns atomic.Int32
	var results []string
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if results = testkit.ToolResults(msgs); len(results) > 0 {
			return testkit.Text("done")
		}
		return testkit.Calls(testkit.Call("a", "a", `{}`), testkit.Call("b", "b", `{}`))
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "a", Handler: func(context.Context) (string, error) { aRuns.Add(1); return "a-ok", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "b", Handler: func(context.Context) (string, error) {
			if bRuns.Add(1) == 1 {
				panic("worker died")
			}
			return "b-ok", nil
		}}),
	}}
	registerSession(env, agent)
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-step-dies"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != session.Complete || aRuns.Load() != 1 || bRuns.Load() != 2 || !slices.Equal(results, []string{"a-ok", "b-ok"}) {
		t.Fatalf("state %v, a ran %d and b %d times, results %q; want a once, b again after it died, both results in call order",
			st.State, aRuns.Load(), bRuns.Load(), results)
	}
}

// TestSessionWorkflow_aSpecialistThatParksIsResumedAndItsResultReachesItsCall:
// a blocking specialist's tool asks for permission; the child parks and
// tells its parent; resumed by its own id, it finishes and its result is
// the parent call's result.
func TestSessionWorkflow_aSpecialistThatParksIsResumedAndItsResultReachesItsCall(t *testing.T) {
	t.Parallel()
	env := newTestWorkflow(t)
	const parent = session.ID("sess-child-hitl")
	child := tacklr.ChildID(string(parent), "researcher", "sp1", true)
	var parentSaw atomic.Pointer[string]
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch last := testkit.LastMessage(msgs); {
		case last.Content == "child-task":
			return testkit.Calls(testkit.Call("g1", "gated", `{}`))
		case last.Role == message.RoleTool && last.Content == "gated-ok":
			return testkit.Text("child-found")
		case last.Role == message.RoleTool:
			parentSaw.Store(&last.Content)
			return testkit.Text("done")
		}
		return testkit.Calls(testkit.Call("sp1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"child-task"}`))
	})
	gated := tacklr.NewTool(tacklr.ToolConfig{Name: "gated", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) { return "gated-ok", nil }})
	registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model, Tools: []*tacklr.Tool{gated}}}})
	allow, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	// The test environment lists a child before it can answer a query: ask
	// only once it started.
	var childStarted atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) { childStarted.Store(true) })
	childParked := func() bool {
		if !childStarted.Load() {
			return false
		}
		val, err := env.QueryWorkflowByID(child, protocol.QueryStatus)
		var st session.Status
		return err == nil && val.Get(&st) == nil && st.Waiting
	}
	when(env, childParked, func() {
		_ = env.SignalWorkflowByID(child, protocol.SignalWake, session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: map[string][]byte{"g1": allow}}})
	})
	when(env, func() bool { return parentSaw.Load() != nil }, func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
	})

	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: parent})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if saw := parentSaw.Load(); saw == nil || !strings.Contains(*saw, "child-found") {
		t.Fatalf("parent call result = %v, want the resumed specialist's", saw)
	}
}

// TestSessionWorkflow_aToolStepThatKeepsFailingFailsTheTurn: a Tool step
// whose worker dies on it every time fails the turn once its attempts ran
// out.
func TestSessionWorkflow_aToolStepThatKeepsFailingFailsTheTurn(t *testing.T) {
	t.Parallel()
	// Arrange
	env := newTestWorkflow(t)
	dies := tacklr.NewTool(tacklr.ToolConfig{Name: "dies", Handler: func(context.Context) (string, error) { panic("worker died") }})
	_, log := registerSession(env, tacklr.AgentOptions{Model: testkit.ScriptedModel(t, testkit.CallThenAnswer("dies", `{}`)), MaxWindowSize: 8192, Tools: []*tacklr.Tool{dies}})
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

	// Act
	got := visibleText(finishSession(t, env, log, protocol.Input{SessionID: "sess-step-fails", ActivityAttempts: 1}))

	// Assert
	if !strings.Contains(got, "worker died") {
		t.Fatalf("events = %q, want the turn failed with the step's error", got)
	}
}

// TestSessionWorkflow_aCancelledWorkflowStopsItsJobs: the host cancelling
// the session's workflow while its turn waits on a background specialist
// ends the session and stops the specialist.
func TestSessionWorkflow_aCancelledWorkflowStopsItsJobs(t *testing.T) {
	t.Parallel()
	// Arrange
	env := newTestWorkflow(t)
	release, let := releaseLater(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		switch testkit.LastMessage(msgs).Content {
		case "child-task":
			select {
			case <-release:
				ch <- testkit.Text("child-done")
			case <-ctx.Done():
			}
		case "go":
			ch <- testkit.Calls(testkit.Call("sp1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`))
		default:
			ch <- testkit.Text("waiting")
		}
	})
	registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}})
	var childErr error
	var jobRunning atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) { jobRunning.Store(true) })
	env.SetOnChildWorkflowCompletedListener(func(_ *workflow.Info, _ converter.EncodedValue, err error) { childErr = err })
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	when(env, jobRunning.Load, func() {
		env.CancelWorkflow()
		// The test environment does not stop a cancelled child's running
		// activity; its model returns once released.
		env.RegisterDelayedCallback(let, 10*time.Millisecond)
	})
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

	// Act
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-cancelled-workflow"})

	// Assert
	if err := env.GetWorkflowError(); err != nil || childErr == nil {
		t.Fatalf("workflow %v, child %v; want the session ended and its job cancelled", err, childErr)
	}
}
