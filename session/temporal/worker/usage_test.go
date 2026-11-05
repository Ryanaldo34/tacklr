package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// toolResults is the results of the tool messages in msgs, in order.
func toolResults(msgs []*message.Message) []string {
	var out []string
	for _, m := range msgs {
		if m != nil && m.Role == message.RoleTool {
			out = append(out, m.Content)
		}
	}
	return out
}

// TestSessionWorkflow_aReplysCallsRunAtMostMaxToolCallsAtOnce: a reply's
// calls run at most MaxToolCalls at once. Each result comes back in call
// order, even a result that finished after a later call's, and a call to an
// unknown tool gets a correction in its place.
func TestSessionWorkflow_aReplysCallsRunAtMostMaxToolCallsAtOnce(t *testing.T) {
	// Arrange
	env := newTestWorkflow(t)
	var running, peak atomic.Int32
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if got := toolResults(msgs); len(got) > 0 {
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
	// An earlier call runs longer, so it finishes after the call after it.
	slow := tacklr.NewTool(tacklr.ToolConfig{Name: "slow", Handler: func(_ context.Context, args struct {
		N int `json:"n"`
	}) (string, error) {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(time.Duration(4-args.N) * 15 * time.Millisecond)
		running.Add(-1)
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
	// Arrange
	env := newTestWorkflow(t)
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if got := toolResults(msgs); len(got) > 0 {
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

// TestSessionWorkflow_aLongSessionContinuesAsNewWithItsState: a session
// whose history is long continues as new at an idle point, or while its
// turn waits on a job, and the next run goes on from its state: committed
// prompts, a close that waits for the turn, and the result or cancel of a
// running job or blocking specialist.
func TestSessionWorkflow_aLongSessionContinuesAsNewWithItsState(t *testing.T) {
	// continued is the input the run that ended in env continues as new with.
	continued := func(t *testing.T, env *testsuite.TestWorkflowEnvironment) protocol.Input {
		t.Helper()
		can, ok := errors.AsType[*workflow.ContinueAsNewError](env.GetWorkflowError())
		if !ok {
			t.Fatalf("run ended with %v, want it to continue as new", env.GetWorkflowError())
		}
		var next protocol.Input
		if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
			t.Fatal(err)
		}
		next.ContinueAsNewEvents = 0
		return next
	}
	for _, tc := range []struct {
		name   string
		events int
		long   func(*testsuite.TestWorkflowEnvironment)
	}{
		{"when idle and Temporal suggests it", 0, func(env *testsuite.TestWorkflowEnvironment) { env.SetContinueAsNewSuggested(true) }},
		{"when idle at the configured history length", 5, func(env *testsuite.TestWorkflowEnvironment) { env.SetCurrentHistoryLength(5) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []string
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				var users []string
				for _, m := range msgs {
					if m != nil && m.Role == message.RoleUser {
						users = append(users, m.Content)
					}
				}
				seen = users
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok", IsComplete: true}
			})
			first := newTestWorkflow(t)
			acts, _ := registerSession(first, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})
			// The history grows long while the first prompt is on its way:
			// the run continues once that turn is done and it is idle.
			first.RegisterDelayedCallback(func() {
				tc.long(first)
				first.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "first"}})
			}, time.Millisecond)

			first.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-can", ContinueAsNewEvents: tc.events})

			next := continued(t, first)
			second := newTestWorkflow(t)
			second.RegisterActivity(acts)
			signalAt(second, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "second"}})
			signalAt(second, time.Minute, session.Wake{Kind: session.WakeClose})
			second.ExecuteWorkflow(protocol.Workflow, next)

			if err := second.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if strings.Join(seen, ",") != "first,second" {
				t.Fatalf("the continued run saw %v, want the first prompt kept and the second committed on it", seen)
			}
		})
	}

	t.Run("while a task session's finished turn waits on a background job, the job's result lands in the next run, which ends with the task", func(t *testing.T) {
		release, let := releaseLater(t)
		var saw string
		model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			last := lastMsg(msgs)
			switch {
			case last != nil && last.Role == message.RoleUser && last.Content == "go":
				ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: "s1", CallID: "s1", Name: "start", Arguments: `{}`}}}
			case last != nil && last.Role == message.RoleUser && strings.Contains(last.Content, "bg-done"):
				saw = last.Content
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			default:
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "waiting", IsComplete: true}
			}
		})
		start := tacklr.NewTool(tacklr.ToolConfig{Name: "start", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
			job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "build"})
			return job.ID, err
		}})
		first := newTestWorkflow(t)
		acts, _ := registerSession(first, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{start}})
		acts.Jobs = map[string]session.JobHandler{"ci": func(context.Context, string) (string, error) { <-release; return "first-run", nil }}
		started, commits := false, 0
		first.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) {
			started = true
			first.SetContinueAsNewSuggested(true)
		})
		// The job finishes only once the turn has committed its last reply
		// (the round's commit, then the reply's): the turn then waits on
		// it and continues as new before the job's result comes.
		first.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, _ error) {
			if started && info.ActivityType.Name == "Commit" {
				if commits++; commits == 2 {
					let()
				}
			}
		})
		first.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-can-job", Task: "go"})

		next := continued(t, first)
		second := newTestWorkflow(t)
		second.RegisterActivity(acts)
		job := session.ID(tacklr.ChildID("sess-can-job", "ci", "s1", false))
		second.RegisterDelayedCallback(func() {
			second.SignalWorkflow(signalChildDone, session.JobEnd{ID: job, Result: session.JobResult{Output: "bg-done"}})
		}, time.Millisecond)
		second.ExecuteWorkflow(protocol.Workflow, next)

		var result string
		if err := second.GetWorkflowResult(&result); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(saw, "bg-done") || result != "done" {
			t.Fatalf("model saw %q, result %q; want the job's result in the next run, and the task's answer", saw, result)
		}
	})

	t.Run("a close that waits for the running turn carries over, and closes the session once the turn ends in the next run", func(t *testing.T) {
		model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == message.RoleUser && last.Content == "go" {
				ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: "s1", CallID: "s1", Name: "start", Arguments: `{}`}}}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
		})
		start := tacklr.NewTool(tacklr.ToolConfig{Name: "start", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
			job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "build"})
			return job.ID, err
		}})
		first := newTestWorkflow(t)
		acts, _ := registerSession(first, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{start}})
		release, let := releaseLater(t)
		acts.Jobs = map[string]session.JobHandler{"ci": func(context.Context, string) (string, error) { <-release; return "built", nil }}
		started, commits := false, 0
		first.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) {
			started = true
			first.SetContinueAsNewSuggested(true)
		})
		// A close comes while the turn runs, so it waits for the turn, which
		// then waits on its job and continues as new; the job's result comes
		// in the next run.
		first.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, _ error) {
			if started && info.ActivityType.Name == "Commit" {
				switch commits++; commits {
				case 1:
					first.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
				case 2:
					let()
				}
			}
		})
		signalAt(first, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})

		first.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-can-held"})

		next := continued(t, first)
		second := newTestWorkflow(t)
		second.RegisterActivity(acts)
		job := session.ID(tacklr.ChildID("sess-can-held", "ci", "s1", false))
		second.RegisterDelayedCallback(func() {
			second.SignalWorkflow(signalChildDone, session.JobEnd{ID: job, Result: session.JobResult{Output: "built"}})
		}, time.Millisecond)
		second.ExecuteWorkflow(protocol.Workflow, next)

		if !second.IsWorkflowCompleted() || second.GetWorkflowError() != nil {
			t.Fatalf("next run completed %v with %v; want the session closed once its turn ended", second.IsWorkflowCompleted(), second.GetWorkflowError())
		}
	})

	// A turn that waits on a blocking specialist continues as new too: the
	// child carries over by id, and its result reaches the next run by
	// signal, or a cancel there stops it.
	const parent = session.ID("sess-can-child")
	child := tacklr.ChildID(string(parent), "researcher", "sp1", true)
	for _, tc := range []struct {
		name string
		then session.Wake
		done bool
	}{
		{"while a blocking specialist runs, its result lands in the next run", session.Wake{}, true},
		{"while a blocking specialist runs, a cancel in the next run stops it", session.Wake{Kind: session.WakeCancel}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result string
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				last := lastMsg(msgs)
				switch {
				case last != nil && last.Content == "child-task":
					// The parent continues as new as soon as the child starts;
					// this run's child ends after that.
					ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "first-run", IsComplete: true}
				case last != nil && last.Role == message.RoleTool:
					result = last.Content
					ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
				default:
					ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{
						ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
						Arguments: `{"specialist":"researcher","task_description_and_context":"child-task"}`,
					}}}
				}
			})
			agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
			first := newTestWorkflow(t)
			acts, _ := registerSession(first, agent)
			first.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) {
				first.SetContinueAsNewSuggested(true)
			})
			signalAt(first, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})

			first.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: parent})

			next := continued(t, first)
			second := newTestWorkflow(t)
			second.RegisterActivity(acts)
			var cancelled []string
			second.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, mock.Anything).Return(func(_, id, _ string) error {
				cancelled = append(cancelled, id)
				return nil
			})
			second.RegisterDelayedCallback(func() {
				if tc.done {
					second.SignalWorkflow(signalChildDone, session.JobEnd{ID: session.ID(child), Result: session.JobResult{Output: "found"}})
					return
				}
				second.SignalWorkflow(protocol.SignalWake, tc.then)
			}, time.Millisecond)
			signalAt(second, time.Minute, session.Wake{Kind: session.WakeClose})
			second.ExecuteWorkflow(protocol.Workflow, next)

			if err := second.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if tc.done && result != "found" {
				t.Fatalf("the call's result in the next run = %q, want the specialist's", result)
			}
			if !tc.done && !slices.Equal(cancelled, []string{child}) {
				t.Fatalf("cancelled %v, want the carried specialist", cancelled)
			}
		})
	}
}
