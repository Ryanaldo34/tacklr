package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/workspace"
)

// TestSessionWorkflow_authExpiredYieldThenResume: a tool whose auth expired
// parks the turn. A resume runs the tool again, and its result reaches the
// model.
func TestSessionWorkflow_authExpiredYieldThenResume(t *testing.T) {
	env := newTestWorkflow(t)
	var calls atomic.Int32
	cloud := tacklr.NewTool(tacklr.ToolConfig{
		Name: "cloud_read",
		Handler: func(context.Context) (string, error) {
			if calls.Add(1) == 1 {
				return "", fmt.Errorf("gdrive: %w", tacklr.ErrAuthExpired)
			}
			return "from-cloud", nil
		},
	})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: last.Content, IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{{
				ID: "c1", CallID: "c1", Name: "cloud_read", Arguments: `{}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{cloud}}
	_, fallback := registerSession(env, agent)

	id := session.ID("sess-auth")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "read"}})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: map[string][]byte{"c1": []byte(`{}`)}}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded, saw bool
	for _, ev := range got {
		if ev.Type == session.EventInterrupt {
			yielded = true
		}
		if ev.Type == session.EventMessage && strings.Contains(ev.Content, "from-cloud") {
			saw = true
		}
	}
	if !yielded || !saw {
		t.Fatalf("want yield + retried read, got %+v", got)
	}
	if st := querySession(t, env); st.State != session.Complete {
		t.Fatalf("Status after auth resume: %+v", st)
	}
}

// TestSessionWorkflow_hitlCancel: a cancel while the turn is parked on a
// question fails the turn.
func TestSessionWorkflow_hitlCancel(t *testing.T) {
	env := newTestWorkflow(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{{
				ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
				Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	_, fallback := registerSession(env, agent)

	id := session.ID("sess-hitl-cancel")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "ask"}})
	}, time.Millisecond)
	when(env, func() bool { return querySession(t, env).Waiting }, func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeCancel})
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
	})

	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var types []session.EventType
	for _, ev := range drainLog(t, fallback, id) {
		types = append(types, ev.Type)
	}
	if st := querySession(t, env); st.State != session.Failed || !slices.Equal(types, []session.EventType{session.EventFunctionCall, session.EventInterrupt, session.EventError}) {
		t.Fatalf("state %v, events %q; want the turn parked, then failed on the cancel", st.State, types)
	}
}

// TestSessionWorkflow_steerDuringYieldKeepsPark: a prompt sent while a call
// is parked keeps the park. The model's next request comes after the
// resume, and it holds the parked call's result, then the steer.
func TestSessionWorkflow_steerDuringYieldKeepsPark(t *testing.T) {
	// Arrange
	env := newTestWorkflow(t)
	var requests [][]string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		var seq []string
		for _, m := range msgs {
			switch m.Role {
			case message.RoleTool:
				seq = append(seq, "result "+m.ToolCallID)
			case message.RoleUser:
				seq = append(seq, m.Content)
			}
		}
		requests = append(requests, seq)
		if len(requests) == 1 {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{
				ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
				Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
			}}}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "chose", IsComplete: true}
	})
	registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "ask"}})
	when(env, func() bool { return querySession(t, env).Waiting }, func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "steer"}})
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: map[string][]byte{"ask1": []byte(`{"selectionIdx":0}`)}}})
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
	})

	// Act
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-steer-yield"})

	// Assert
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"ask"}, {"ask", "result ask1", "steer"}}
	if !slices.EqualFunc(requests, want, slices.Equal) || querySession(t, env).State != session.Complete {
		t.Fatalf("model requests %q, state %v; want the steer after the parked call's result in the request after the resume", requests, querySession(t, env).State)
	}
}

// TestSessionWorkflow_badWorkspaceBindingFailsTurn: a workspace binding
// that cannot open fails the turn.
func TestSessionWorkflow_badWorkspaceBindingFailsTurn(t *testing.T) {
	env := newTestWorkflow(t)
	missing := filepath.Join(t.TempDir(), "missing")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{{
				ID: "r1", CallID: "r1", Name: "read",
				Arguments: `{"path":"/workspace/docs/hello.txt"}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	acts, fallback := registerSession(env, agent)
	acts.Workspace = workspace.Config{Open: vfs.Tree(vfs.At("docs", vfs.Local(missing)))}
	id := session.ID("sess-bad-binding")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{
			Text: "read",
			Auth: session.AuthContext{Bindings: []mount.Binding{{
				Provider: "local",
				Params:   map[string]string{mount.ParamName: "docs"},
				Auth:     mount.Credential{Token: "x"},
			}}},
		}})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose}) }, 80*time.Millisecond)
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != session.Failed {
		t.Fatalf("status %+v events %+v", st, drainLog(t, fallback, id))
	}
}

// TestSessionWorkflow_grandchildResultReachesParent: a blocking specialist
// of a blocking specialist answers, and its answer reaches the root
// session through its parent.
func TestSessionWorkflow_grandchildResultReachesParent(t *testing.T) {
	env := newTestWorkflow(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == message.RoleUser && last.Content == "leaf-task" {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "leaf-answer", IsComplete: true}
			return
		}
		if last != nil && last.Role == message.RoleUser && last.Content == "go-deeper" {
			ch <- tacklr.LLMResponseChunk{
				Type: session.EventFunctionCall,
				ToolCalls: []message.ToolCall{{
					ID: "leaf1", CallID: "leaf1", Name: "spawn_specialist",
					Arguments: `{"specialist":"leaf","task_description_and_context":"leaf-task","block":true}`,
				}},
				IsComplete: true,
			}
			return
		}
		if last != nil && last.Role == message.RoleTool && strings.Contains(last.Content, "leaf-answer") {
			var parent bool
			for _, m := range msgs {
				if m != nil && m.Role == message.RoleUser && m.Content == "go" {
					parent = true
				}
			}
			if parent {
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "got-grandchild", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "leaf-answer", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{{
				ID: "mid1", CallID: "mid1", Name: "spawn_specialist",
				Arguments: `{"specialist":"mid","task_description_and_context":"go-deeper","block":true}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "mid",
			Model: model,
			Specialists: []*tacklr.Specialist{{
				Name: "leaf", Model: model,
			}},
		}},
	}
	_, fallback := registerSession(env, agent)
	id := session.ID("sess-grandchild")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose}) }, 150*time.Millisecond)
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == session.EventMessage && ev.Content == "got-grandchild" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want the grandchild text to finish the parent, got %+v", drainLog(t, fallback, id))
	}
}

// TestSessionWorkflow_parkedParentLeavesAsyncChildRunning: a parent parked
// on a question keeps its background job running. Resumed, it finishes;
// closed while parked, it cancels the job.
func TestSessionWorkflow_parkedParentLeavesAsyncChildRunning(t *testing.T) {
	for _, resume := range []bool{true, false} {
		t.Run(map[bool]string{true: "resumed, it finishes", false: "closed while parked, its job is cancelled"}[resume], func(t *testing.T) {
			env := newTestWorkflow(t)
			release := make(chan struct{})
			var once sync.Once
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				last := lastMsg(msgs)
				if last != nil && last.Role == message.RoleUser && last.Content == "child-task" {
					select {
					case <-ctx.Done():
						return
					case <-release:
					}
					ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "child-alive", IsComplete: true}
					return
				}
				for _, m := range msgs {
					if m != nil && m.Role == message.RoleTool && strings.Contains(m.Content, "User selected") {
						ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "parent-resumed", IsComplete: true}
						return
					}
				}
				if last != nil && last.Role == message.RoleTool && strings.Contains(last.Content, "scheduled") {
					ch <- tacklr.LLMResponseChunk{
						Type: session.EventFunctionCall,
						ToolCalls: []message.ToolCall{{
							ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
							Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
						}},
						IsComplete: true,
					}
					return
				}
				ch <- tacklr.LLMResponseChunk{
					Type: session.EventFunctionCall,
					ToolCalls: []message.ToolCall{{
						ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
						Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
					}},
					IsComplete: true,
				}
			})
			agent := tacklr.AgentOptions{
				Model: model, MaxWindowSize: 8192,
				Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}},
			}
			_, fallback := registerSession(env, agent)
			id := session.ID("sess-park-child")
			childID := session.ID(tacklr.ChildID(string(id), "researcher", "sp1", true))
			var sawChild bool
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
			}, time.Millisecond)
			when(env, func() bool { return querySession(t, env).Waiting }, func() {
				val, err := env.QueryWorkflow(protocol.QueryJobs)
				if err != nil {
					t.Fatal(err)
				}
				var ids []session.ID
				if err := val.Get(&ids); err != nil {
					t.Fatal(err)
				}
				for _, got := range ids {
					if got == childID {
						sawChild = true
					}
				}
				if resume {
					once.Do(func() { close(release) })
					env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeResume, Resume: session.Resume{
						Responses: map[string][]byte{"ask1": []byte(`{"selectionIdx":0}`)},
					}})
				}
				env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
			})
			var childErr error
			env.SetOnChildWorkflowCompletedListener(func(_ *workflow.Info, _ converter.EncodedValue, err error) { childErr = err })
			env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if !sawChild {
				t.Fatal("async child was gone while the parent was parked")
			}
			if !resume {
				if !temporal.IsCanceledError(childErr) {
					t.Fatalf("child = %v, want the job a closed session ran cancelled", childErr)
				}
				return
			}
			var resumed bool
			for _, ev := range drainLog(t, fallback, id) {
				if ev.Type == session.EventMessage && ev.Content == "parent-resumed" {
					resumed = true
				}
			}
			if !resumed {
				t.Fatalf("want the parent to resume, got %+v", drainLog(t, fallback, id))
			}
		})
	}
}

// turnStatus is the status description of the last turn span id ended.
func turnStatus(t *testing.T, id session.ID) string {
	t.Helper()
	status := ""
	for _, sp := range spans.Ended() {
		if sp.Name() != telemetry.SpanTurn {
			continue
		}
		for _, kv := range sp.Attributes() {
			if kv.Key == telemetry.AttrSessionID && kv.Value.AsString() == string(id) {
				status = sp.Status().Description
			}
		}
	}
	return status
}

func newTestWorkflow(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(sessionWorkflow, workflow.RegisterOptions{Name: protocol.Workflow})
	// A job tells its parent its result by signal, the one way a result
	// arrives; this delivers it as Temporal would. The root workflow runs
	// under the environment's own id, not its session id, so a signal to
	// an id the environment does not run is the root's.
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(_, workflowID, _, signal string, arg any) error {
			if env.SignalWorkflowByID(workflowID, signal, arg) != nil {
				env.SignalWorkflow(signal, arg)
			}
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

func finishSession(t *testing.T, env *testsuite.TestWorkflowEnvironment, log session.EventLog, id session.ID) []session.Event {
	t.Helper()
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})
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

// TestSessionWorkflow_aSteerTheModelCannotReadFailsTheTurn: a prompt sent
// while a tool runs, with content the model cannot read, fails the turn.
func TestSessionWorkflow_aSteerTheModelCannotReadFailsTheTurn(t *testing.T) {
	// Arrange
	release, let := releaseLater(t)
	model := testkit.TextHTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: "h1", CallID: "h1", Name: "hold", Arguments: `{}`}}}
	})
	hold := tacklr.NewTool(tacklr.ToolConfig{Name: "hold", Handler: func(context.Context) (string, error) { <-release; return "held", nil }})
	env := newTestWorkflow(t)
	_, log := registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{hold}})
	image := &message.Message{Role: message.RoleUser, ContentParts: []message.ContentPart{{Type: message.ContentTypeInputImage, FileData: &message.FileData{MIMEType: "image/png"}}}}
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, 20*time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{UserMessage: image}})
	env.RegisterDelayedCallback(let, 30*time.Millisecond)
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

	// Act
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-steer-image"})

	// Assert
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st, got := querySession(t, env), visibleText(drainLog(t, log, "sess-steer-image")); st.State != session.Failed || !strings.Contains(got, "unsupported content type(s): image/png") {
		t.Fatalf("status %+v, events %q; want the turn failed on the unreadable steer", st, got)
	}
}

// TestSessionWorkflow_failedWorkerEndsTheChild: a job whose worker fails
// ends, and its failure reaches the parent's next reply.
func TestSessionWorkflow_failedWorkerEndsTheChild(t *testing.T) {
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		for _, m := range msgs {
			if m != nil && m.Role == message.RoleUser && strings.Contains(m.Content, "failed:") && strings.Contains(m.Content, "ci broke") {
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "parent-saw-failure", IsComplete: true}
				return
			}
		}
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleTool && strings.Contains(last.Content, "scheduled") {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "waiting", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{{
				ID: "ci1", CallID: "ci1", Name: "watch_ci", Arguments: `{}`,
			}},
			IsComplete: true,
		}
	})
	watch := tacklr.NewTool(tacklr.ToolConfig{
		Name: "watch_ci",
		Handler: func(ctx context.Context, _ struct{}, cc *tacklr.CallContext) (string, error) {
			job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "pipe"})
			if err != nil {
				return "", err
			}
			return "Job " + job.ID + " scheduled (name=ci).", nil
		},
	})
	env := newTestWorkflow(t)
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{watch}}
	acts, log := registerSession(env, agent)
	acts.Jobs = map[string]session.JobHandler{
		"ci": func(context.Context, string) (string, error) { return "", errors.New("ci broke") },
	}
	id := session.ID("sess-worker-fail")
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, 80*time.Millisecond, session.Wake{Kind: session.WakeClose})
	got := finishSession(t, env, log, id)
	if !strings.Contains(visibleText(got), "parent-saw-failure") {
		t.Fatalf("a failed worker must reach the parent as a failed job, got %s", visibleText(got))
	}
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

// TestSessionWorkflow_twoPermissionPromptsInOneGroupParkOneAtATime: two
// calls of one reply that ask for permission park one at a time. Each runs
// once on its answer, and every result comes back in call order.
func TestSessionWorkflow_twoPermissionPromptsInOneGroupParkOneAtATime(t *testing.T) {
	env := newTestWorkflow(t)
	var runs atomic.Int32
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		var seen []string
		for _, m := range msgs {
			if m != nil && m.Role == message.RoleTool {
				seen = append(seen, m.Content)
			}
		}
		if len(seen) == 3 {
			results = seen
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{
				{ID: "fc_g1", CallID: "call_g1", Name: "gate1", Arguments: `{}`},
				{ID: "fc_free", CallID: "call_free", Name: "free", Arguments: `{}`},
				{ID: "fc_g2", CallID: "call_g2", Name: "gate2", Arguments: `{}`},
			},
			IsComplete: true,
		}
	})
	gate := func(name string) *tacklr.Tool {
		return tacklr.NewTool(tacklr.ToolConfig{
			Name:   name,
			OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
			Handler: func(context.Context) (string, error) {
				runs.Add(1)
				return name + "-ok", nil
			},
		})
	}
	agent := tacklr.AgentOptions{MaxWindowSize: 8192, Model: model, Tools: []*tacklr.Tool{
		gate("gate1"), gate("gate2"),
		tacklr.NewTool(tacklr.ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { return "free-ok", nil }}),
	}}
	_, log := registerSession(env, agent)

	id := session.ID("sess-two-prompts")
	allow, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
	parked := func() []string {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		ch, err := log.Subscribe(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for ev := range ch {
			if ev.Type == session.EventInterrupt {
				ids = append(ids, ev.MessageID)
			}
		}
		return ids
	}
	var parks []string
	var resumeWhenParked func()
	resumeWhenParked = func() {
		if ids := parked(); len(ids) > len(parks) {
			next := ids[len(parks)]
			parks = append(parks, next)
			env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: map[string][]byte{next: allow}}})
		}
		if len(parks) < 2 {
			env.RegisterDelayedCallback(resumeWhenParked, 5*time.Millisecond)
		}
	}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	}, time.Millisecond)
	env.RegisterDelayedCallback(resumeWhenParked, 5*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose}) }, 400*time.Millisecond)
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(parks, []string{"fc_g1", "fc_g2"}) {
		t.Errorf("parks = %v, want gate1 then gate2, one at a time", parks)
	}
	if !slices.Equal(results, []string{"gate1-ok", "free-ok", "gate2-ok"}) || runs.Load() != 2 {
		t.Errorf("results = %v after %d gated runs, want every result in call order, each gate run once", results, runs.Load())
	}
}

// TestSessionWorkflow_aToolSchedulingAJobTwiceStartsItOnceAndListsIt: a
// tool that schedules a job twice, parks, and runs again after the resume
// while the job runs, gets the same job each time. After the resume it
// starts a second job. Each job starts once, and the tool lists the job
// that runs and the job it started.
func TestSessionWorkflow_aToolSchedulingAJobTwiceStartsItOnceAndListsIt(t *testing.T) {
	// Arrange
	env := newTestWorkflow(t)
	release, let := releaseLater(t)
	var listed string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if got := toolResults(msgs); len(got) > 0 {
			listed = got[0]
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type:       session.EventFunctionCall,
			ToolCalls:  []message.ToolCall{{ID: "fc_twice", CallID: "call_twice", Name: "twice", Arguments: `{}`}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{MaxWindowSize: 8192, Model: model,
		Tools: []*tacklr.Tool{tacklr.NewTool(tacklr.ToolConfig{Name: "twice", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
			first, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "job-task"})
			if err != nil {
				return "", err
			}
			again, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "job-task"})
			if err != nil {
				return "", err
			}
			if _, err := cc.Park(&tacklr.UserSelectionInterrupt{Options: []tacklr.UserChoice{{Title: "A"}, {Title: "B"}}}); err != nil {
				return "", err
			}
			let()
			if _, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "lint", Task: "job-task"}); err != nil {
				return "", err
			}
			var ids []string
			for _, j := range cc.Jobs() {
				ids = append(ids, j.ID)
			}
			return fmt.Sprintf("same=%v jobs=%v", first.ID == again.ID, ids), nil
		}})}}
	acts, _ := registerSession(env, agent)
	acts.Jobs = map[string]session.JobHandler{
		"ci":   func(context.Context, string) (string, error) { <-release; return "job-done", nil },
		"lint": func(context.Context, string) (string, error) { return "linted", nil },
	}
	var started atomic.Int32
	env.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) { started.Add(1) })
	id := session.ID("sess-schedule-twice")
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	when(env, func() bool { return querySession(t, env).Waiting }, func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: map[string][]byte{"fc_twice": []byte(`{"selectionIdx":1}`)}}})
	})
	signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

	// Act
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})

	// Assert
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("same=true jobs=[%s %s]", tacklr.ChildID(string(id), "ci", "fc_twice", false), tacklr.ChildID(string(id), "lint", "fc_twice", false))
	if listed != want || started.Load() != 2 {
		t.Errorf("tool saw %q with %d children started, want %q and two children", listed, started.Load(), want)
	}
}

// TestSessionWorkflow_blockingSpecialistsFromOneReplyRunAtOnce: two
// blocking specialists of one reply run at the same time, and their results
// come back in call order.
func TestSessionWorkflow_blockingSpecialistsFromOneReplyRunAtOnce(t *testing.T) {
	env := newTestWorkflow(t)
	both := newWaitAll(2)
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleUser && strings.HasPrefix(last.Content, "child-") {
			if err := both.arrive(); err != nil {
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: err.Error(), IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: last.Content + "-done", IsComplete: true}
			return
		}
		var seen []string
		for _, m := range msgs {
			if m != nil && m.Role == message.RoleTool {
				seen = append(seen, m.Content)
			}
		}
		if len(seen) > 0 {
			results = seen
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		spawn := func(id, task string) message.ToolCall {
			return message.ToolCall{ID: id, CallID: id, Name: "spawn_specialist",
				Arguments: fmt.Sprintf(`{"specialist":"researcher","task_description_and_context":%q,"block":true}`, task)}
		}
		ch <- tacklr.LLMResponseChunk{
			Type:       session.EventFunctionCall,
			ToolCalls:  []message.ToolCall{spawn("sp_a", "child-a"), spawn("sp_b", "child-b")},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
	registerSession(env, agent)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose}) }, 400*time.Millisecond)
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-two-blocking"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !strings.Contains(results[0], "child-a-done") || !strings.Contains(results[1], "child-b-done") {
		t.Fatalf("tool results = %q, want both specialists' results in call order", results)
	}
}

// TestSessionWorkflow_aBlockingSpecialistsOutcomeIsItsCallsResult: a
// specialist's output is its call's result and is kept as an Episode. A
// specialist that fails gives its call an error result: its own failure.
func TestSessionWorkflow_aBlockingSpecialistsOutcomeIsItsCallsResult(t *testing.T) {
	const token = "unique-retain-specialist-token"
	for _, tc := range []struct {
		name       string
		child      tacklr.LLMResponseChunk
		wantStatus string
		wantResult string
	}{
		{"its output is kept", tacklr.LLMResponseChunk{Type: session.EventMessage, Content: token, IsComplete: true}, "success", token},
		{"its failure is an error result", tacklr.LLMResponseChunk{Error: errors.New("child model broke")}, "error", "provider HTTP 400 (invalid_request): child model broke"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestWorkflow(t)
			eng, err := brain.NewEngine(brain.NewMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			if err := eng.ApplyKinds(t.Context(), tacklr.EpisodeKinds()...); err != nil {
				t.Fatal(err)
			}
			ns, err := brain.ParseNamespace("org", "acme")
			if err != nil {
				t.Fatal(err)
			}
			var parentSaw string
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				switch last := lastMsg(msgs); {
				case last != nil && last.Role == message.RoleUser && last.Content == "look it up":
					ch <- tc.child
				case last != nil && last.Role == message.RoleTool:
					parentSaw = last.Content
					ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
				default:
					ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
						Arguments: `{"specialist":"researcher","task_description_and_context":"look it up"}`}}}
				}
			})
			_, log := registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Brain: eng, SearchNamespace: ns,
				Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}})
			const id = session.ID("sess-specialist-outcome")
			signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
			signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

			got := finishSession(t, env, log, id)

			status := ""
			for _, ev := range got {
				if ev.Type == session.EventToolResult && ev.MessageID == "sp1" {
					status = ev.ToolCalls[0].Status
				}
			}
			if status != tc.wantStatus || parentSaw != tc.wantResult {
				t.Fatalf("result status %q, parent saw %q; want %q with %q", status, parentSaw, tc.wantStatus, tc.wantResult)
			}
			if tc.wantStatus != "success" {
				return
			}
			page, err := eng.Search(t.Context(), brain.Scope{Namespace: ns}, brain.SearchRequest{Query: token, Kind: tacklr.KindEpisode})
			if err != nil {
				t.Fatal(err)
			}
			hits := slices.Collect(page.Results.Objects())
			if len(hits) == 0 {
				t.Fatal("no episode, want the specialist's result kept")
			}
			if kept, err := eng.Get(t.Context(), brain.Scope{Namespace: ns}, hits[0].ID); err != nil || kept.Content != token {
				t.Fatalf("episode = %+v %v, want the specialist's result", kept, err)
			}
		})
	}
}

// TestSessionWorkflow_aRoundThatCannotFitFailsTheStepThatCommitsIt: a reply
// too large for the window, with no result to cut, fails the commit, and
// the turn fails with it, for a tool's round, a blocking specialist's
// round, and a reply without calls.
func TestSessionWorkflow_aRoundThatCannotFitFailsTheStepThatCommitsIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		call message.ToolCall
	}{
		{"a tool's round", message.ToolCall{ID: "f1", CallID: "f1", Name: "free", Arguments: `{}`}},
		{"a blocking specialist's round", message.ToolCall{ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
			Arguments: `{"specialist":"researcher","task_description_and_context":"child-task"}`}},
		{"a reply without calls", message.ToolCall{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestWorkflow(t)
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				if last := lastMsg(msgs); last != nil && last.Content == "child-task" {
					ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "found", IsComplete: true}
					return
				}
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: strings.Repeat("w", 3000), IsComplete: true}
				if tc.call.Name != "" {
					ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: []message.ToolCall{tc.call}, IsComplete: true}
				}
			})
			free := tacklr.NewTool(tacklr.ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { return "ok", nil }})
			agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 6000, Tools: []*tacklr.Tool{free},
				Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
			_, log := registerSession(env, agent)
			id := session.ID("sess-too-large")
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
			}, time.Millisecond)
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose}) }, 400*time.Millisecond)
			env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})

			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			events, err := log.Subscribe(ctx, id, 0)
			if err != nil {
				t.Fatal(err)
			}
			failure := ""
			for ev := range events {
				if ev.Type == session.EventError {
					failure = ev.Fail
				}
			}
			if !strings.Contains(failure, "the round needs") {
				t.Fatalf("turn failure = %q, want the round that could not fit", failure)
			}
		})
	}
}

// TestSessionWorkflow_aRetriedCommitRunsEachToolOnce: a Commit whose
// summary request fails on the network runs again and the turn completes.
// Each tool runs once.
func TestSessionWorkflow_aRetriedCommitRunsEachToolOnce(t *testing.T) {
	env := newTestWorkflow(t)
	var aRuns, bRuns, summaries atomic.Int32
	big := strings.Repeat("b", 600)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		for _, m := range msgs {
			if m != nil && strings.Contains(m.Content, "Summarize the entire message history") {
				if summaries.Add(1) == 1 {
					ch <- tacklr.LLMResponseChunk{Type: session.EventError, Error: fmt.Errorf("%w: blip", tacklr.ErrNetwork)}
					return
				}
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "S", IsComplete: true}
				return
			}
		}
		call := func(name string) {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
				ToolCalls: []message.ToolCall{{ID: "fc_" + name, CallID: "call_" + name, Name: name, Arguments: `{}`}}}
		}
		switch last := lastMsg(msgs); {
		case last != nil && last.Role == message.RoleTool && last.Content == "ok":
			call("b")
		case last != nil && last.Role == message.RoleTool:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: strings.Repeat("w", 600), IsComplete: true}
			call("a")
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 6000, Tools: []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "a", Handler: func(context.Context) (string, error) { aRuns.Add(1); return "ok", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "b", Handler: func(context.Context) (string, error) { bRuns.Add(1); return big, nil }}),
	}}
	registerSession(env, agent)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose}) }, time.Minute)
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-retried-commit"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != session.Complete || summaries.Load() != 2 || aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Fatalf("state %v after %d summary requests, a ran %d and b ran %d times; want complete after a retried summary, each tool once",
			st.State, summaries.Load(), aRuns.Load(), bRuns.Load())
	}
}

// TestSessionWorkflow_jobRequestsTheRuntimeCannotServeFail: a tool's job
// request for a specialist, an unknown job, an empty task or an empty name,
// and a cancel of an unknown job, each fail with the reason.
func TestSessionWorkflow_jobRequestsTheRuntimeCannotServeFail(t *testing.T) {
	env := newTestWorkflow(t)
	var seen string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleTool {
			seen = last.Content
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
			ToolCalls: []message.ToolCall{{ID: "j1", CallID: "j1", Name: "jobs", Arguments: `{}`}}}
	})
	jobs := tacklr.NewTool(tacklr.ToolConfig{Name: "jobs", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		_, specialist := cc.Schedule(ctx, tacklr.JobRequest{Name: "researcher", Task: "x"})
		_, unknown := cc.Schedule(ctx, tacklr.JobRequest{Name: "nightly", Task: "x"})
		_, emptyTask := cc.Schedule(ctx, tacklr.JobRequest{Name: "indexer", Task: " "})
		_, emptyName := cc.Schedule(ctx, tacklr.JobRequest{Name: " ", Task: "x"})
		return fmt.Sprintf("specialist=%v | unknown=%v | cancel=%v | task=%v | name=%v", specialist, unknown, cc.CancelJob(ctx, "nope"), emptyTask, emptyName), nil
	}})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{jobs},
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
	acts, _ := registerSession(env, agent)
	acts.Jobs = map[string]session.JobHandler{"indexer": func(context.Context, string) (string, error) { return "", nil }}
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, 100*time.Millisecond, session.Wake{Kind: session.WakeClose})
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-bad-jobs"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"specialist=invalid: researcher is a specialist", "unknown=not found: nightly is not a registered job", `cancel=job "nope" is unknown`, "task=task is required", "name=job name is required"} {
		if !strings.Contains(seen, want) {
			t.Errorf("tool saw %q, want %q", seen, want)
		}
	}
}

// TestSessionWorkflow_jobsFromEarlierRepliesAreListedAndCancelled: jobs a
// session started in earlier replies stay listed while they run, and
// cancel_child stops one by id. Closed, the session deletes its saved
// messages from the brain.
func TestSessionWorkflow_jobsFromEarlierRepliesAreListedAndCancelled(t *testing.T) {
	env := newTestWorkflow(t)
	const id = session.ID("sess-listed-jobs")
	var listed, cancelled string
	release := make(chan struct{})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleUser && last.Content == "child-task" {
			<-release // the jobs run until the parent has listed them
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "child-done", IsComplete: true}
			return
		}
		var results []string
		for _, m := range msgs {
			if m != nil && m.Role == message.RoleTool {
				results = append(results, m.Content)
			}
		}
		spawn := func(id string) message.ToolCall {
			return message.ToolCall{ID: id, CallID: id, Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`}
		}
		call := func(tc message.ToolCall) {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{tc}}
		}
		switch len(results) {
		case 0:
			call(spawn("sp1"))
		case 1:
			call(spawn("sp2"))
		case 2:
			call(message.ToolCall{ID: "l1", CallID: "l1", Name: "list_children", Arguments: `{}`})
		case 3:
			listed = results[2]
			call(message.ToolCall{ID: "c1", CallID: "c1", Name: "cancel_child", Arguments: `{"child_id":"` + tacklr.ChildID(string(id), "researcher", "sp1", true) + `"}`})
		default:
			if cancelled == "" {
				cancelled = results[3]
				close(release)
			}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
		}
	})
	var forgot []string
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(func(_ context.Context, ev brain.AuditEvent) error {
		if ev.Op == brain.AuditSessionDelete {
			forgot = append(forgot, ev.SessionID)
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	ns, err := brain.ParseNamespace("org", "acme")
	if err != nil {
		t.Fatal(err)
	}
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Brain: eng, SearchNamespace: ns,
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
	registerSession(env, agent)
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	signalAt(env, 200*time.Millisecond, session.Wake{Kind: session.WakeClose})
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	for _, call := range []string{"sp1", "sp2"} {
		if child := string(session.ID(tacklr.ChildID(string(id), "researcher", call, true))); !strings.Contains(listed, child) {
			t.Errorf("list_children = %q, want the running job %s", listed, child)
		}
	}
	if !strings.Contains(cancelled, "cancelled and removed") {
		t.Errorf("cancel_child = %q, want the job stopped", cancelled)
	}
	if !slices.Equal(forgot, []string{string(id)}) {
		t.Errorf("brain deletes = %v, want the session's messages deleted", forgot)
	}
}

// TestSessionWorkflow_hostStateIsAppliedOnceAndSurvivesAPark: a prompt's
// state reaches a tool that parks and resumes, and a tool's own change to
// that key is not overwritten by a later step.
func TestSessionWorkflow_hostStateIsAppliedOnceAndSurvivesAPark(t *testing.T) {
	env := newTestWorkflow(t)
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		var seen []string
		for _, m := range msgs {
			if m != nil && m.Role == message.RoleTool {
				seen = append(seen, m.Content)
			}
		}
		switch len(seen) {
		case 0:
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
				ToolCalls: []message.ToolCall{{ID: "fc_gate", CallID: "call_gate", Name: "gated", Arguments: `{}`}}}
		case 1:
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
				ToolCalls: []message.ToolCall{{ID: "fc_read", CallID: "call_read", Name: "read_state", Arguments: `{}`}}}
		default:
			results = seen
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
		}
	})
	agent := tacklr.AgentOptions{MaxWindowSize: 8192, Model: model, Tools: []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "gated", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
			Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
				v, _ := cc.StateGet("k")
				return fmt.Sprintf("gated saw %v", v), cc.StateSet("k", "tool")
			}}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "read_state", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
			v, _ := cc.StateGet("k")
			return fmt.Sprintf("read saw %v", v), nil
		}}),
	}}
	registerSession(env, agent)
	allow, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go", State: map[string]any{"k": "host"}}})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: map[string][]byte{"fc_gate": allow}}})
	}, 50*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose}) }, 200*time.Millisecond)
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-host-state"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(results, []string{"gated saw host", "read saw tool"}) {
		t.Fatalf("tool results = %q, want the host value across the park, then the tool's own change kept", results)
	}
}

// TestSessionWorkflow_aStepThatDiesRunsAgainWithoutTheCallsThatCompleted:
// one call of a reply dies before its step reports (its first run panics,
// as a worker that dies does), and the runtime runs that step again. The
// call that completed is not run again, and the round commits both results
// in call order.
func TestSessionWorkflow_aStepThatDiesRunsAgainWithoutTheCallsThatCompleted(t *testing.T) {
	env := newTestWorkflow(t)
	var aRuns, bRuns atomic.Int32
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleTool {
			for _, m := range msgs {
				if m.Role == message.RoleTool {
					results = append(results, m.Content)
				}
			}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{
			{ID: "a", CallID: "a", Name: "a", Arguments: `{}`}, {ID: "b", CallID: "b", Name: "b", Arguments: `{}`}}}
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

// TestSessionWorkflow_aCancelStopsARoundsToolsAndTheNextPromptCancelsTheirCalls:
// a cancel while a round's calls run fails the turn on the cancel, even
// though the call still running returns on its own; the call that finished
// keeps its result. The next prompt gives
// the stopped call a cancelled result, commits the round, then the prompt.
func TestSessionWorkflow_aCancelStopsARoundsToolsAndTheNextPromptCancelsTheirCalls(t *testing.T) {
	env := newTestWorkflow(t)
	var slowRunning atomic.Bool
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var results []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Content == "next" {
			for _, m := range msgs {
				if m.Role == message.RoleTool {
					results = append(results, m.Content)
				}
			}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{
			{ID: "slow", CallID: "slow", Name: "slow", Arguments: `{}`}, {ID: "fast", CallID: "fast", Name: "fast", Arguments: `{}`}}}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "slow", Handler: func(ctx context.Context) (string, error) {
			slowRunning.Store(true)
			select {
			case <-ctx.Done():
			case <-release:
			}
			return "slow-ok", nil
		}}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "fast", Handler: func(context.Context) (string, error) { return "fast-ok", nil }}),
	}}
	registerSession(env, agent)
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	// Cancel once the fast call finished and the slow one runs.
	var toolsDone atomic.Int32
	env.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, _ error) {
		if info.ActivityType.Name == "Tool" {
			toolsDone.Add(1)
		}
	})
	when(env, func() bool { return slowRunning.Load() && toolsDone.Load() == 1 }, func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeCancel})
	})
	// The next prompt must start a turn of its own, not steer the cancelled one.
	when(env, func() bool { return querySession(t, env).State == session.Failed }, func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "next"}})
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
	})
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-cancel-tools"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(results, []string{tacklr.CancelledToolResultContent, "fast-ok"}) {
		t.Fatalf("results %q; want the slow call cancelled and the fast result kept", results)
	}
	if st := querySession(t, env); st.State != session.Complete {
		t.Fatalf("state = %v, want the next prompt's turn complete", st.State)
	}
}

// TestSessionWorkflow_aResumeOrAToolStepThatFailsFailsTheTurn: an answer to
// no parked call, or a Tool step that keeps failing (a worker that dies on
// it every time), fails the turn.
func TestSessionWorkflow_aResumeOrAToolStepThatFailsFailsTheTurn(t *testing.T) {
	allow := []byte(`{"optionId":"allow-once"}`)
	for _, tc := range []struct {
		name, tool string
		answers    map[string][]byte
		want       string
	}{
		{"an answer to no parked call", "gate", map[string][]byte{"nope": allow}, "no tool call id found for interrupt nope"},
		{"an answer beside the parked call's", "gate", map[string][]byte{"c1": allow, "nope": allow}, "no tool call id found for interrupt nope"},
		{"a Tool step that keeps failing", "dies", nil, "worker died"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestWorkflow(t)
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
					ToolCalls: []message.ToolCall{{ID: "c1", CallID: "c1", Name: tc.tool, Arguments: `{}`}}}
			})
			agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{
				tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "ok", nil }}),
				tacklr.NewTool(tacklr.ToolConfig{Name: "dies", Handler: func(context.Context) (string, error) { panic("worker died") }}),
			}}
			_, log := registerSession(env, agent)
			id := session.ID("sess-step-fails")
			signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
			signalAt(env, 50*time.Millisecond, session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: tc.answers}})
			signalAt(env, 200*time.Millisecond, session.Wake{Kind: session.WakeClose})
			env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: id, ActivityAttempts: 1})

			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if got := visibleText(drainLog(t, log, id)); !strings.Contains(got, tc.want) {
				t.Fatalf("events = %q, want the turn failed with %q", got, tc.want)
			}
		})
	}
}

// TestSessionWorkflow_aTurnWaitingOnABackgroundJob: a turn whose model is
// done waits on its background specialist. A prompt then steers into its
// next reply; a cancel stops the job and fails the turn; the host
// cancelling the session's workflow stops the job too.
func TestSessionWorkflow_aTurnWaitingOnABackgroundJob(t *testing.T) {
	for _, tc := range []struct {
		name string
		wake session.Wake
	}{
		{"a prompt steers into its next reply", session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "meanwhile"}}},
		{"a cancel stops the job and fails the turn", session.Wake{Kind: session.WakeCancel}},
		{"a cancelled workflow stops the job", session.Wake{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestWorkflow(t)
			release, let := releaseLater(t)
			var steered atomic.Bool
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				last := lastMsg(msgs)
				switch {
				case last != nil && last.Content == "child-task":
					select {
					case <-release:
						ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "child-done", IsComplete: true}
					case <-ctx.Done():
					}
				case last != nil && last.Content == "meanwhile":
					steered.Store(true)
					let()
					ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "saw-meanwhile", IsComplete: true}
				case last != nil && last.Role == message.RoleUser && last.Content == "go":
					ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{
						ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
						Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
					}}}
				default:
					ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "waiting", IsComplete: true}
				}
			})
			registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}})
			var childErr error
			var jobRunning atomic.Bool
			env.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) { jobRunning.Store(true) })
			env.SetOnChildWorkflowCompletedListener(func(_ *workflow.Info, _ converter.EncodedValue, err error) { childErr = err })
			signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
			when(env, jobRunning.Load, func() {
				if tc.wake.Kind == "" {
					env.CancelWorkflow()
				} else {
					env.SignalWorkflow(protocol.SignalWake, tc.wake)
				}
				// The test environment does not stop a cancelled child's
				// running activity; its model returns once released.
				env.RegisterDelayedCallback(let, 10*time.Millisecond)
			})
			signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

			env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-waiting"})

			if tc.wake.Kind == "" {
				if err := env.GetWorkflowError(); err != nil || childErr == nil {
					t.Fatalf("workflow %v, child %v; want the session ended and its job cancelled", err, childErr)
				}
				return
			}
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			st := querySession(t, env)
			if tc.wake.Kind == session.WakePrompt && (!steered.Load() || st.State != session.Complete) {
				t.Fatalf("steered %v, status %+v; want the prompt in the next reply and the turn complete", steered.Load(), st)
			}
			if tc.wake.Kind == session.WakeCancel && (st.State != session.Failed || childErr == nil || turnStatus(t, "sess-waiting") != telemetry.OutcomeCancelled) {
				t.Fatalf("status %+v, child %v, span %q; want the turn failed and its span cancelled, and the job stopped", st, childErr, turnStatus(t, "sess-waiting"))
			}
		})
	}
}

// TestSessionWorkflow_aSpecialistThatParksIsResumedAndItsResultReachesItsCall:
// a blocking specialist's tool asks for permission; the child parks and
// tells its parent; resumed by its own id, it finishes and its result is
// the parent call's result.
func TestSessionWorkflow_aSpecialistThatParksIsResumedAndItsResultReachesItsCall(t *testing.T) {
	env := newTestWorkflow(t)
	const parent = session.ID("sess-child-hitl")
	child := tacklr.ChildID(string(parent), "researcher", "sp1", true)
	var parentSaw atomic.Pointer[string]
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		switch {
		case last != nil && last.Content == "child-task":
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: "g1", CallID: "g1", Name: "gated", Arguments: `{}`}}}
		case last != nil && last.Role == message.RoleTool && last.Content == "gated-ok":
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "child-found", IsComplete: true}
		case last != nil && last.Role == message.RoleTool:
			parentSaw.Store(&last.Content)
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task"}`,
			}}}
		}
	})
	gated := tacklr.NewTool(tacklr.ToolConfig{Name: "gated", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) { return "gated-ok", nil }})
	registerSession(env, tacklr.AgentOptions{Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model, Tools: []*tacklr.Tool{gated}}}})
	allow, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go"}})
	childParked := func() bool {
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

// TestSessionWorkflow_aWorkerSessionRunsEachPromptAsItsJob: a session made
// for a job worker runs the worker on each prompt's text. A session for a
// worker that no one registered fails, and its result says not found.
func TestSessionWorkflow_aWorkerSessionRunsEachPromptAsItsJob(t *testing.T) {
	for _, tc := range []struct {
		worker, result string
		state          session.State
	}{
		{"ci", "built main", session.Complete},
		{"nightly", "not found: nightly", session.Failed},
	} {
		t.Run(tc.worker, func(t *testing.T) {
			// Arrange
			env := newTestWorkflow(t)
			acts, _ := registerSession(env, tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192})
			acts.Jobs = map[string]session.JobHandler{"ci": func(_ context.Context, task string) (string, error) { return "built " + task, nil }}
			signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "main"}})
			signalAt(env, time.Minute, session.Wake{Kind: session.WakeClose})

			// Act
			env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-worker", Worker: tc.worker})

			// Assert
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if st := querySession(t, env); st.State != tc.state || st.Result != tc.result || st.Kind != session.KindWorker {
				t.Fatalf("status %+v, want %v with result %q", st, tc.state, tc.result)
			}
		})
	}
}
