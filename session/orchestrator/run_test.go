package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/orchestrator"
	"github.com/ryanaldo34/tacklr/telemetry"
)

const (
	askArgs = `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`
	allow   = `{"optionId":"allow-once"}`
)

// requests records the conversation of each model request: each user
// message's text and each tool result as "result <call id>".
type requests struct {
	mu   sync.Mutex
	seen [][]string
}

func (r *requests) add(msgs []*message.Message) {
	var seq []string
	for _, m := range msgs {
		switch m.Role {
		case message.RoleTool:
			seq = append(seq, "result "+m.ToolCallID)
		case message.RoleUser:
			seq = append(seq, m.Content)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, seq)
}

func (r *requests) all() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

func types(evs []session.Event) []session.EventType {
	out := make([]session.EventType, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

func last(evs []session.Event) session.Event {
	if len(evs) == 0 {
		return session.Event{}
	}
	return evs[len(evs)-1]
}

// results are the contents of the tool results in evs, in order.
func results(evs []session.Event) []string {
	var out []string
	for _, ev := range evs {
		if ev.Type == session.EventToolResult {
			out = append(out, ev.Content)
		}
	}
	return out
}

// gate is a tool that asks for permission, then answers name-ok.
func gate(name string, runs *atomic.Int32) *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{Name: name, OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) {
		runs.Add(1)
		return name + "-ok", nil
	}})
}

// TestRun_aToolWhoseAuthExpiredParksAndRunsAgainOnTheResume: a tool whose
// credentials expired parks the turn; the resume runs it again, and its
// result reaches the model.
func TestRun_aToolWhoseAuthExpiredParksAndRunsAgainOnTheResume(t *testing.T) {
	t.Parallel()
	// Arrange
	var calls atomic.Int32
	cloud := tacklr.NewTool(tacklr.ToolConfig{Name: "cloud_read", Handler: func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			return "", fmt.Errorf("gdrive: %w", tacklr.ErrAuthExpired)
		}
		return "from-cloud", nil
	}})
	rt := newRef(t, tacklr.AgentOptions{Model: testkit.ScriptedModel(t, testkit.CallThenAnswer("cloud_read", `{}`)), Tools: []*tacklr.Tool{cloud}}, nil)

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("read")), answer(`{}`), wakes(wake(session.WakeClose)))

	// Assert
	evs := rt.durable()
	if err != nil || !slices.Contains(types(evs), session.EventInterrupt) || last(evs).Type != session.EventComplete || rt.state.Result != "done: from-cloud" {
		t.Fatalf("run %v, events %+v, result %q; want the turn parked, then the tool's second result answered", err, evs, rt.state.Result)
	}
}

// TestRun_aResumeSentWhileTheTurnRunsWaitsForItsPark: a resume that comes
// while the turn runs, before it parks, answers the park once it parks.
func TestRun_aResumeSentWhileTheTurnRunsWaitsForItsPark(t *testing.T) {
	t.Parallel()
	// Arrange
	var rt *refRuntime
	early := tacklr.NewTool(tacklr.ToolConfig{Name: "early", Handler: func(context.Context) (string, error) {
		rt.send(resume(map[string][]byte{"ask1": []byte(`{"selectionIdx":1}`)}))
		return "sent", nil
	}})
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch results := testkit.ToolResults(msgs); len(results) {
		case 0:
			return testkit.Calls(testkit.Call("early", "early", `{}`))
		case 1:
			return testkit.Calls(testkit.Call("ask1", "ask_user_choice", askArgs))
		}
		return testkit.Text("chose")
	})
	rt = newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{early}}, nil)

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("go")), wakes(wake(session.WakeClose)))

	// Assert
	if err != nil || rt.state.Result != "chose" || !slices.Contains(types(rt.durable()), session.EventInterrupt) {
		t.Fatalf("run %v, result %q; want the turn parked, then answered by the early resume", err, rt.state.Result)
	}
}

// TestRun_aCancelWhileParkedFailsTheTurn: a cancel while the turn is
// parked on a question fails it as cancelled.
func TestRun_aCancelWhileParkedFailsTheTurn(t *testing.T) {
	t.Parallel()
	// Arrange
	model := testkit.ScriptedModel(t, func([]*message.Message) tacklr.LLMResponseChunk {
		return testkit.Calls(testkit.Call("ask1", "ask_user_choice", askArgs))
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model}, nil)

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("ask")), wakes(wake(session.WakeCancel), wake(session.WakeClose)))

	// Assert
	evs := rt.durable()
	want := []session.EventType{session.EventFunctionCall, session.EventInterrupt, session.EventError}
	if err != nil || !slices.Equal(types(evs), want) || last(evs).FailKind != session.FailCancelled || rt.state.Status().State != session.Failed {
		t.Fatalf("run %v, events %v, status %+v; want the turn parked, then failed as cancelled", err, types(evs), rt.state.Status())
	}
}

// TestRun_aPromptWhileParkedKeepsTheParkAndSteersTheNextRequest: a prompt
// sent while a call is parked keeps the park. The model's next request
// comes after the resume, and holds the parked call's result, then the
// prompt.
func TestRun_aPromptWhileParkedKeepsTheParkAndSteersTheNextRequest(t *testing.T) {
	t.Parallel()
	// Arrange
	var reqs requests
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		reqs.add(msgs)
		if len(reqs.all()) == 1 {
			return testkit.Calls(testkit.Call("ask1", "ask_user_choice", askArgs))
		}
		return testkit.Text("chose")
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model}, nil)

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("ask")),
		wakes(prompt("steer"), resume(map[string][]byte{"ask1": []byte(`{"selectionIdx":0}`)}), wake(session.WakeClose)))

	// Assert
	want := [][]string{{"ask"}, {"ask", "result ask1", "steer"}}
	if err != nil || !slices.EqualFunc(reqs.all(), want, slices.Equal) || rt.state.Status().State != session.Complete {
		t.Fatalf("run %v, model requests %q, status %+v; want the steer after the parked call's result in the request after the resume", err, reqs.all(), rt.state.Status())
	}
}

// TestRun_twoCallsThatAskForPermissionParkOneAtATime: two calls of one
// reply that ask for permission park one at a time. Each runs once on its
// answer, and every result comes back in call order.
func TestRun_twoCallsThatAskForPermissionParkOneAtATime(t *testing.T) {
	t.Parallel()
	// Arrange
	var runs atomic.Int32
	free := tacklr.NewTool(tacklr.ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { return "free-ok", nil }})
	var got []string
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if got = testkit.ToolResults(msgs); len(got) == 3 {
			return testkit.Text("done")
		}
		return testkit.Calls(testkit.Call("g1", "gate1", `{}`), testkit.Call("free", "free", `{}`), testkit.Call("g2", "gate2", `{}`))
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{gate("gate1", &runs), free, gate("gate2", &runs)}}, nil)

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("go")), answer(allow), answer(allow), wakes(wake(session.WakeClose)))

	// Assert
	var parks []string
	for _, ev := range rt.durable() {
		if ev.Type == session.EventInterrupt {
			parks = append(parks, ev.MessageID)
		}
	}
	if err != nil || !slices.Equal(parks, []string{"g1", "g2"}) || !slices.Equal(got, []string{"gate1-ok", "free-ok", "gate2-ok"}) || runs.Load() != 2 {
		t.Fatalf("run %v, parks %v, results %q after %d gated runs; want gate1 then gate2 parked, each run once, results in call order", err, parks, got, runs.Load())
	}
}

// TestRun_aPromptsStateReachesAToolAcrossAParkAndAToolsOwnChangeStays: a
// prompt's state reaches a tool that parks and resumes, and the tool's own
// change to that key is not overwritten by a later step.
func TestRun_aPromptsStateReachesAToolAcrossAParkAndAToolsOwnChangeStays(t *testing.T) {
	t.Parallel()
	// Arrange
	gated := tacklr.NewTool(tacklr.ToolConfig{Name: "gated", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		v, _ := cc.StateGet("k")
		return fmt.Sprintf("gated saw %v", v), cc.StateSet("k", "tool")
	}})
	read := tacklr.NewTool(tacklr.ToolConfig{Name: "read_state", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		v, _ := cc.StateGet("k")
		return fmt.Sprintf("read saw %v", v), nil
	}})
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch len(testkit.ToolResults(msgs)) {
		case 0:
			return testkit.Calls(testkit.Call("gate", "gated", `{}`))
		case 1:
			return testkit.Calls(testkit.Call("read", "read_state", `{}`))
		}
		return testkit.Text("done")
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{gated, read}}, nil)
	first := session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go", State: map[string]any{"k": "host"}}}

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(first), answer(allow), wakes(wake(session.WakeClose)))

	// Assert
	if got := results(rt.durable()); err != nil || !slices.Equal(got, []string{"gated saw host", "read saw tool"}) {
		t.Fatalf("run %v, results %q; want the host value across the park, then the tool's own change kept", err, got)
	}
}

// TestRun_anAnswerToNoParkedCallFailsTheTurn: a resume with an answer to
// no parked call fails the turn: one beside the parked call's answer, one
// in its place, and one to a session that is not parked.
func TestRun_anAnswerToNoParkedCallFailsTheTurn(t *testing.T) {
	t.Parallel()
	nope := []byte(allow)
	for name, steps := range map[string][]step{
		"in place of the parked call's":   {wakes(prompt("go")), wakes(resume(map[string][]byte{"nope": nope}))},
		"beside the parked call's":        {wakes(prompt("go")), wakes(resume(map[string][]byte{"c1": nope, "nope": nope}))},
		"to a session that is not parked": {wakes(resume(map[string][]byte{"nope": nope}))},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			var runs atomic.Int32
			model := testkit.ScriptedModel(t, func([]*message.Message) tacklr.LLMResponseChunk {
				return testkit.Calls(testkit.Call("c1", "gate", `{}`))
			})
			rt := newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{gate("gate", &runs)}}, nil)

			// Act
			_, err := rt.run(orchestrator.Identity{}, append(steps, wakes(wake(session.WakeClose)))...)

			// Assert
			end := last(rt.durable())
			if err != nil || end.Type != session.EventError || !strings.Contains(end.Fail, "no tool call id found for interrupt nope") || runs.Load() != 0 {
				t.Fatalf("run %v, end %+v, gate runs %d; want the turn failed on the unknown answer", err, end, runs.Load())
			}
		})
	}
}

// TestRun_aCancelStopsARoundsToolsAndTheNextPromptCancelsTheirCalls: a
// cancel while a round's calls run fails the turn as cancelled; the call
// that finished keeps its result. The next prompt gives the stopped call a
// cancelled result, commits the round, then the prompt.
func TestRun_aCancelStopsARoundsToolsAndTheNextPromptCancelsTheirCalls(t *testing.T) {
	t.Parallel()
	// Arrange
	var rt *refRuntime
	fastDone := make(chan struct{})
	slow := tacklr.NewTool(tacklr.ToolConfig{Name: "slow", Handler: func(ctx context.Context) (string, error) {
		<-fastDone
		rt.send(wake(session.WakeCancel))
		<-ctx.Done()
		return "slow-ok", nil
	}})
	fast := tacklr.NewTool(tacklr.ToolConfig{Name: "fast", Handler: func(context.Context) (string, error) {
		defer close(fastDone)
		return "fast-ok", nil
	}})
	var seen []string
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if testkit.LastMessage(msgs).Content == "next" {
			seen = testkit.ToolResults(msgs)
			return testkit.Text("done")
		}
		return testkit.Calls(testkit.Call("slow", "slow", `{}`), testkit.Call("fast", "fast", `{}`))
	})
	rt = newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{slow, fast}}, nil)
	var cancelled session.Event

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("go")), func(rt *refRuntime) []session.Wake {
		cancelled = last(rt.durable())
		return []session.Wake{prompt("next"), wake(session.WakeClose)}
	})

	// Assert
	if err != nil || cancelled.FailKind != session.FailCancelled || !slices.Equal(seen, []string{tacklr.CancelledToolResultContent, "fast-ok"}) || rt.state.Status().State != session.Complete {
		t.Fatalf("run %v, cancel %+v, next request's results %q, status %+v; want the slow call cancelled, the fast result kept, and the next turn complete", err, cancelled, seen, rt.state.Status())
	}
}

// TestRun_aCancelFailsTheTurnAndStopsTheSessionsJobs: a cancel fails the
// turn as cancelled while its model answers, while a tool runs, while a
// call waits on its job, and while the finished turn waits on its
// background job, and it stops those jobs. The session runs the next
// prompt; a cancel while it is idle makes no event.
func TestRun_aCancelFailsTheTurnAndStopsTheSessionsJobs(t *testing.T) {
	t.Parallel()
	// Arrange
	var rt *refRuntime
	block := tacklr.NewTool(tacklr.ToolConfig{Name: "block", Handler: func(ctx context.Context) (string, error) {
		rt.send(wake(session.WakeCancel))
		<-ctx.Done()
		return "", ctx.Err()
	}})
	schedule := func(name string, wait bool) *tacklr.Tool {
		return tacklr.NewTool(tacklr.ToolConfig{Name: name, Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
			job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "build", Wait: wait})
			return job.ID, err
		}})
	}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		switch last := testkit.LastMessage(msgs); {
		case last.Role == message.RoleTool:
			ch <- testkit.Text("waiting")
		case last.Content == "offline":
			rt.send(wake(session.WakeCancel))
			<-ctx.Done()
		case last.Content == "hello":
			ch <- testkit.Text("hi")
		default:
			ch <- testkit.Calls(testkit.Call(last.Content, last.Content, `{}`))
		}
	})
	agent := tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{block, schedule("wait", true), schedule("background", false)}}
	rt = newRef(t, agent, map[string]session.JobHandler{"ci": nil})
	var ends []session.Event
	end := func(then ...session.Wake) step {
		return func(rt *refRuntime) []session.Wake {
			ends = append(ends, last(rt.durable()))
			return then
		}
	}
	var idle int

	// Act
	_, err := rt.run(orchestrator.Identity{},
		wakes(prompt("offline")), end(prompt("block")), end(prompt("wait")), wakes(wake(session.WakeCancel)),
		end(prompt("background")), wakes(wake(session.WakeCancel)), end(prompt("hello")),
		func(rt *refRuntime) []session.Wake {
			idle = len(rt.durable())
			return []session.Wake{wake(session.WakeCancel)}
		}, end(wake(session.WakeClose)))

	// Assert
	if err != nil || len(ends) != 5 {
		t.Fatalf("run %v, turn ends %+v; want five turns", err, ends)
	}
	for i, ev := range ends[:4] {
		if ev.Type != session.EventError || ev.FailKind != session.FailCancelled {
			t.Fatalf("turn %d ended %+v; want it failed as cancelled", i, ev)
		}
	}
	jobs := []session.ID{session.ID(tacklr.ChildID("sess", "ci", "wait", false)), session.ID(tacklr.ChildID("sess", "ci", "background", false))}
	if ends[4].Type != session.EventComplete || len(rt.durable()) != idle || !slices.Equal(rt.cancelled, jobs) {
		t.Fatalf("next turn %+v, events after an idle cancel %d of %d, jobs stopped %v; want the turn complete, no event, and both jobs stopped", ends[4], len(rt.durable()), idle, rt.cancelled)
	}
}

// TestRun_aJobsEndIsItsCallsResultOrAnInboxMessage: the end of a job a
// call waits on is the call's result. While the finished turn waits on a
// background job, a prompt steers into its next reply, and the job's end
// (here, its failure) comes as a user message. The end of a job the
// session does not run is dropped.
func TestRun_aJobsEndIsItsCallsResultOrAnInboxMessage(t *testing.T) {
	t.Parallel()
	// Arrange
	schedule := func(name string, wait bool) *tacklr.Tool {
		return tacklr.NewTool(tacklr.ToolConfig{Name: name, Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
			job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: name, Task: "q3", Wait: wait})
			return "scheduled " + job.ID, err
		}})
	}
	var reqs requests
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		reqs.add(msgs)
		switch last := testkit.LastMessage(msgs); {
		case last.Role == message.RoleTool:
			return testkit.Text("done: " + last.Content)
		case last.Content == "export", last.Content == "ci":
			return testkit.Calls(testkit.Call(last.Content, last.Content, `{}`))
		}
		return testkit.Text("saw: " + testkit.LastMessage(msgs).Content)
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{schedule("export", true), schedule("ci", false)}},
		map[string]session.JobHandler{"export": nil, "ci": nil})
	export, ci := tacklr.ChildID("sess", "export", "export", false), tacklr.ChildID("sess", "ci", "ci", false)

	// Act
	_, err := rt.run(orchestrator.Identity{},
		wakes(prompt("export")), wakes(jobEnd(export, "exported q3", false)),
		wakes(prompt("ci")), wakes(prompt("meanwhile")), wakes(jobEnd(ci, "ci broke", true)),
		wakes(jobEnd("gone", "late", false), wake(session.WakeClose)))

	// Assert
	failure := fmt.Sprintf("Job %s (ci) failed:\nci broke", ci)
	var lasts []string
	for _, req := range reqs.all() {
		lasts = append(lasts, req[len(req)-1])
	}
	if err != nil || results(rt.durable())[0] != "exported q3" || rt.state.Status().State != session.Complete {
		t.Fatalf("run %v, results %q, status %+v; want the job's output as its call's result and the turns complete", err, results(rt.durable()), rt.state.Status())
	}
	if n := len(lasts); n < 2 || lasts[n-2] != "meanwhile" || lasts[n-1] != failure {
		t.Fatalf("each request's last message %q; want the steer in a reply, then the job's failure as a user message", lasts)
	}
}

// TestRun_aParkedTurnLeavesItsBackgroundJobRunning: a turn parked on a
// question keeps its background job. Resumed, the finished turn waits for
// the job and ends with its result; closed while parked, the session stops
// the job.
func TestRun_aParkedTurnLeavesItsBackgroundJobRunning(t *testing.T) {
	t.Parallel()
	child := tacklr.ChildID("sess", "researcher", "sp1", true)
	for name, tc := range map[string]struct {
		then      []step
		cancelled []session.ID
	}{
		"resumed, the turn ends with the job's result": {
			then: []step{answer(`{"selectionIdx":0}`), wakes(jobEnd(child, "child-done", false)), wakes(wake(session.WakeClose))},
		},
		"closed while parked, the job is stopped": {
			then:      []step{wakes(wake(session.WakeClose))},
			cancelled: []session.ID{session.ID(child)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
				switch results := testkit.ToolResults(msgs); {
				case strings.Contains(testkit.LastMessage(msgs).Content, "child-done"):
					return testkit.Text("saw the child")
				case len(results) == 0:
					return testkit.Calls(testkit.Call("sp1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`))
				case len(results) == 1:
					return testkit.Calls(testkit.Call("ask1", "ask_user_choice", askArgs))
				}
				return testkit.Text("resumed")
			})
			rt := newRef(t, tacklr.AgentOptions{Model: model, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}, nil)
			var parkedJobs int
			parked := func(rt *refRuntime) []session.Wake {
				parkedJobs = len(rt.state.Jobs)
				return nil
			}

			// Act
			_, err := rt.run(orchestrator.Identity{}, append([]step{wakes(prompt("go")), parked}, tc.then...)...)

			// Assert
			if err != nil || parkedJobs != 1 || !slices.Equal(rt.cancelled, tc.cancelled) {
				t.Fatalf("run %v, jobs while parked %d, stopped %v; want the job kept while parked, and stopped %v", err, parkedJobs, rt.cancelled, tc.cancelled)
			}
			if tc.cancelled == nil && rt.state.Result != "saw the child" {
				t.Fatalf("result %q; want the turn to end with the job's result", rt.state.Result)
			}
		})
	}
}

// TestRun_aWorkerSessionRunsItsTaskAsItsJob: a task session of a worker
// runs the worker on its task and ends with its output. A worker no one
// registered fails the session, and its result says not found.
func TestRun_aWorkerSessionRunsItsTaskAsItsJob(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		worker, result string
		state          session.State
	}{
		{"ci", "built main", session.Complete},
		{"nightly", "not found: job nightly", session.Failed},
	} {
		t.Run(tc.worker, func(t *testing.T) {
			t.Parallel()
			// Arrange
			rt := newRef(t, tacklr.AgentOptions{Model: testkit.ScriptedModel(t, nil)}, map[string]session.JobHandler{
				"ci": func(_ context.Context, task string) (string, error) { return "built " + task, nil },
			})

			// Act
			out, err := rt.run(orchestrator.Identity{Worker: tc.worker, Task: true}, wakes(prompt("main")))

			// Assert
			st := rt.state.Status()
			if got := cmpOr(err, out); got != tc.result || st.State != tc.state || st.Kind != session.KindWorker || st.Specialist != tc.worker {
				t.Fatalf("run %q %v, status %+v; want %q and %v", out, err, st, tc.result, tc.state)
			}
		})
	}
}

// cmpOr is err's text, or out without one.
func cmpOr(err error, out string) string {
	if err != nil {
		return err.Error()
	}
	return out
}

// TestRun_aRequestOverTheWindowCollapsesItAndAsksAgainOnce: a request the
// provider refuses as over the context window collapses the window into a
// summary, and the model is asked again: the turn ends with its answer.
func TestRun_aRequestOverTheWindowCollapsesItAndAsksAgainOnce(t *testing.T) {
	t.Parallel()
	// Arrange
	var refusals atomic.Int32
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch last := testkit.LastMessage(msgs).Content; {
		case strings.Contains(last, "Summarize the entire message history"):
			return testkit.Text("summary")
		case last == "too long" && refusals.Add(1) == 1:
			return testkit.Fail(session.ErrContextLength)
		default:
			return testkit.Text("re: " + last)
		}
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model}, nil)

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("a")), wakes(prompt("b")), wakes(prompt("too long")), wakes(wake(session.WakeClose)))

	// Assert
	if err != nil || last(rt.durable()).Type != session.EventComplete || rt.state.Result != "re: too long" {
		t.Fatalf("run %v, end %+v, result %q; want the answer after the collapse", err, last(rt.durable()), rt.state.Result)
	}
}

// TestRun_aModelThatFailsFailsTheTurnWithItsKind: a refusal, the token
// limit, and a request over the context window that the provider refuses
// again after the collapse each fail the turn with their kind.
func TestRun_aModelThatFailsFailsTheTurnWithItsKind(t *testing.T) {
	t.Parallel()
	// Arrange
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch last := testkit.LastMessage(msgs).Content; {
		case last == "refuse":
			return testkit.Fail(session.ErrModelRefused)
		case last == "long":
			return testkit.Fail(session.ErrMaxTokens)
		case strings.Contains(last, "Summarize the entire message history"):
			return testkit.Text("summary")
		}
		return testkit.Fail(session.ErrContextLength)
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model}, nil)
	var kinds []session.FailKind
	end := func(then session.Wake) step {
		return func(rt *refRuntime) []session.Wake {
			kinds = append(kinds, last(rt.durable()).FailKind)
			return []session.Wake{then}
		}
	}

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("refuse")), end(prompt("long")), end(prompt("too long")), end(wake(session.WakeClose)))

	// Assert
	want := []session.FailKind{session.FailRefused, session.FailMaxTokens, session.FailContextLength}
	if err != nil || !slices.Equal(kinds, want) || !errors.Is(last(rt.durable()).Error, session.ErrMaxTokens) {
		t.Fatalf("run %v, failure kinds %v; want %v", err, kinds, want)
	}
}

// TestRun_aTurnFailsOnAPromptOrAReplyItCannotCommit: a prompt the model
// cannot read, sent while a tool runs, fails the turn; so does a reply too
// large for the window.
func TestRun_aTurnFailsOnAPromptOrAReplyItCannotCommit(t *testing.T) {
	t.Parallel()
	image := &message.Message{Role: message.RoleUser, ContentParts: []message.ContentPart{{Type: message.ContentTypeInputImage, FileData: &message.FileData{MIMEType: "image/png"}}}}
	for name, tc := range map[string]struct {
		window uint
		reply  tacklr.LLMResponseChunk
		want   string
	}{
		"an unreadable steer": {8192, testkit.Calls(testkit.Call("h1", "hold", `{}`)), "unsupported content type(s): image/png"},
		"a reply too large":   {6000, testkit.Text(strings.Repeat("w", 3000)), "the round needs"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			var rt *refRuntime
			hold := tacklr.NewTool(tacklr.ToolConfig{Name: "hold", Handler: func(context.Context) (string, error) {
				rt.send(session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{UserMessage: image}})
				return "held", nil
			}})
			model := testkit.TextHTTPModel(t, func(_ context.Context, _ []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tc.reply
			})
			rt = newRef(t, tacklr.AgentOptions{Model: model, MaxWindowSize: tc.window, Tools: []*tacklr.Tool{hold}}, nil)

			// Act
			_, err := rt.run(orchestrator.Identity{}, wakes(prompt("go")), wakes(wake(session.WakeClose)))

			// Assert
			if end := last(rt.durable()); err != nil || end.Type != session.EventError || !strings.Contains(end.Fail, tc.want) || rt.state.Status().State != session.Failed {
				t.Fatalf("run %v, end %+v; want the turn failed with %q", err, end, tc.want)
			}
		})
	}
}

// TestRun_aSuspendedSessionGoesOnFromItsCarry: a session suspended while
// idle goes on from its carry with its conversation. One suspended while
// its finished turn waits on a job, with a close that waits for the turn,
// goes on with that turn: the job's end, queued after the suspend, is the
// next run's, it comes as a user message, the turn ends, and the close
// closes the session.
func TestRun_aSuspendedSessionGoesOnFromItsCarry(t *testing.T) {
	t.Parallel()
	ci := tacklr.ChildID("sess", "ci", "start", false)
	for name, tc := range map[string]struct {
		before, after []step
		result        string
	}{
		"idle": {
			before: []step{wakes(prompt("first")), wakes(wake(session.WakeSuspend))},
			after:  []step{wakes(prompt("second")), wakes(wake(session.WakeClose))},
			result: "re: first, second",
		},
		"while its turn waits on a job, with the job's end queued after the suspend": {
			before: []step{wakes(prompt("start")), wakes(wake(session.WakeClose), wake(session.WakeSuspend), jobEnd(ci, "built", false))},
			result: fmt.Sprintf("re: start, Job %s (ci) completed:\nbuilt", ci),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
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
				if last := testkit.LastMessage(msgs); last.Content == "start" {
					return testkit.Calls(testkit.Call("start", "start", `{}`))
				}
				return testkit.Text("re: " + strings.Join(users, ", "))
			})
			rt := newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{start}}, map[string]session.JobHandler{"ci": nil})
			_, suspended := rt.run(orchestrator.Identity{}, tc.before...)
			carry := rt.state.Carry()

			// Act
			_, err := rt.runFrom(orchestrator.NewState(rt.id, orchestrator.Identity{}, nil, nil, &carry), tc.after...)

			// Assert
			if !errors.Is(suspended, orchestrator.ErrSuspended) || err != nil || rt.state.Result != tc.result || !rt.state.Closed {
				t.Fatalf("first run %v, next run %v, result %q, closed %v; want a suspend, then %q and the session closed", suspended, err, rt.state.Result, rt.state.Closed, tc.result)
			}
		})
	}
}

// TestRun_aToolStopsAJobByItsIDAndAJobThatCannotStartFailsTheTurn: a
// call that cancels a job the session runs stops it, and the turn ends
// without waiting on it. A job whose start fails fails the turn.
func TestRun_aToolStopsAJobByItsIDAndAJobThatCannotStartFailsTheTurn(t *testing.T) {
	t.Parallel()
	// Arrange
	job := tacklr.ChildID("sess", "ci", "start", false)
	start := tacklr.NewTool(tacklr.ToolConfig{Name: "start", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		started, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "build"})
		return started.ID, err
	}})
	stop := tacklr.NewTool(tacklr.ToolConfig{Name: "stop", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		return "stopped", cc.CancelJob(ctx, job)
	}})
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch results := testkit.ToolResults(msgs); {
		case testkit.LastMessage(msgs).Content == "start":
			return testkit.Calls(testkit.Call("start", "start", `{}`))
		case len(results) == 1:
			return testkit.Calls(testkit.Call("stop", "stop", `{}`))
		}
		return testkit.Text("done")
	})
	rt := newRef(t, tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{start, stop}}, map[string]session.JobHandler{"ci": nil})
	var stopped session.Event

	// Act
	_, err := rt.run(orchestrator.Identity{}, wakes(prompt("start")), func(rt *refRuntime) []session.Wake {
		stopped = last(rt.durable())
		rt.startErr = errors.New("platform down")
		return []session.Wake{prompt("start")}
	}, wakes(wake(session.WakeClose)))

	// Assert
	if err != nil || stopped.Type != session.EventComplete || !slices.Equal(rt.cancelled, []session.ID{session.ID(job)}) {
		t.Fatalf("run %v, first turn %+v, stopped %v; want the job stopped and the turn ended", err, stopped, rt.cancelled)
	}
	if end := last(rt.durable()); end.Type != session.EventError || !strings.Contains(end.Fail, "platform down") {
		t.Fatalf("turn whose job cannot start ended %+v; want it failed with the start's error", end)
	}
}

// TestRun_aPromptsMCPServersAreItsTurnsAndOnesSentWhileParkedTheNextTurns:
// a prompt with MCP servers runs its turn with them; one sent while the
// turn is parked, with its own message, steers it and its servers wait for
// the next turn. A resume sent while the turn runs waits for it.
func TestRun_aPromptsMCPServersAreItsTurnsAndOnesSentWhileParkedTheNextTurns(t *testing.T) {
	t.Parallel()
	// Arrange
	var rt *refRuntime
	var reqs requests
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		reqs.add(msgs)
		if testkit.LastMessage(msgs).Content == "ask" {
			return testkit.Calls(testkit.Call("ask1", "ask_user_choice", askArgs))
		}
		return testkit.Text("ok")
	})
	rt = newRef(t, tacklr.AgentOptions{Model: model}, nil)
	withMCP := func(text, server string) session.Wake {
		return session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{
			UserMessage: &message.Message{Role: message.RoleUser, Content: text},
			MCPServers:  []session.MCPServer{{Name: server}},
		}}
	}
	choose := resume(map[string][]byte{"ask1": []byte(`{"selectionIdx":0}`)})

	// Act
	_, err := rt.run(orchestrator.Identity{},
		wakes(withMCP("ask", "first")), wakes(withMCP("steer", "second")), wakes(choose), wakes(prompt("next")), wakes(wake(session.WakeClose)))

	// Assert
	want := [][]string{{"first"}, {"first"}, {"second"}}
	if err != nil || !slices.EqualFunc(rt.mcp, want, slices.Equal) || !slices.Contains(reqs.all()[1], "steer") {
		t.Fatalf("run %v, each turn's MCP servers %q, requests %q; want the prompt's for its turn, the steer's for the next", err, rt.mcp, reqs.all())
	}
}

// TestJobEnd_isTheResultOrTheFailure: a task session's end is its result,
// or its error's text as a failure.
func TestJobEnd_isTheResultOrTheFailure(t *testing.T) {
	t.Parallel()
	// Act
	ok, failed := orchestrator.JobEnd("j", "done", nil), orchestrator.JobEnd("j", "", errors.New("broke"))

	// Assert
	if ok.ID != "j" || ok.Result != (session.JobResult{Output: "done"}) || failed.Result != (session.JobResult{Output: "broke", Failed: true}) {
		t.Fatalf("ends %+v and %+v; want the result, then the failure", ok, failed)
	}
}

// TestState_reportsItsStatusAndTheOutcomeOfItsLastTurn: a session's status
// names its kind and its turn's state, and its outcome is how that turn
// ended.
func TestState_reportsItsStatusAndTheOutcomeOfItsLastTurn(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		state   orchestrator.State
		status  session.Status
		outcome string
	}{
		"a parked specialist": {
			orchestrator.State{SessionID: "s", Identity: orchestrator.Identity{Specialist: "r", Parent: "p"}, Yielded: true},
			session.Status{ID: "s", Parent: "p", Specialist: "r", Kind: session.KindSpecialist, State: session.Running, Waiting: true},
			telemetry.OutcomeYield,
		},
		"a cancelled turn": {
			orchestrator.State{SessionID: "s", Terminal: session.Failed, Err: context.Canceled},
			session.Status{ID: "s", State: session.Failed},
			telemetry.OutcomeCancelled,
		},
		"a failed turn": {
			orchestrator.State{SessionID: "s", Terminal: session.Failed, Err: errors.New("broke")},
			session.Status{ID: "s", State: session.Failed},
			telemetry.OutcomeError,
		},
		"a closed session": {
			orchestrator.State{SessionID: "s", Closed: true, Result: "r"},
			session.Status{ID: "s", State: session.Complete, Result: "r"},
			telemetry.OutcomeOK,
		},
	} {
		// Act
		status, outcome := tc.state.Status(), tc.state.Outcome()

		// Assert
		if status != tc.status || outcome != tc.outcome {
			t.Errorf("%s: status %+v, outcome %q; want %+v, %q", name, status, outcome, tc.status, tc.outcome)
		}
	}
}
