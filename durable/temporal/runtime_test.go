package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestActivities_unknownAgentAndDirectCall(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	acts := &activities{Catalog: cat, Snapshots: snaps, Fallback: log, DisableStreams: true, Secrets: durable.NewMemorySecretStorage()}
	_, err := acts.Inference(t.Context(), inferenceInput{SessionID: "s", AgentID: "nope"})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("missing agent: %v", err)
	}
	_, err = acts.Tool(t.Context(), toolInput{SessionID: "s", AgentID: "nope", Call: tacklr.ToolCall{ID: "c", Name: "x"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("tool missing agent: %v", err)
	}
	out, err := acts.Inference(t.Context(), inferenceInput{
		SessionID: "s", AgentID: "default",
		User:  &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
		State: map[string]any{"user": "Ryan"},
	})
	if err != nil || !out.Complete {
		t.Fatalf("direct inference: %+v %v", out, err)
	}
	snap, _, err := snaps.Load(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.Checkpoint.UserState()["user"]) != `"Ryan"` {
		t.Fatalf("userState=%s", snap.Checkpoint.UserState()["user"])
	}
}

func TestNew_panicsWithoutClientOrCatalog(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	mustPanic(t, func() { New(nil, Config{Catalog: cat}) })
	mustPanic(t, func() { New(&struct{ client.Client }{}, Config{}) })
	mustPanic(t, func() { New(&struct{ client.Client }{}, Config{Catalog: cat}) })
	mustPanic(t, func() { NewWorker(&struct{ client.Client }{}, Config{Catalog: cat}) })
	log := inprocess.NewMemoryEventLog()
	stub := &struct{ client.Client }{}
	secrets := durable.NewMemorySecretStorage()
	rt := New(stub, Config{Catalog: cat, Secrets: secrets, DisableStreams: true, TurnLocality: time.Minute, Fallback: log})
	if rt.taskQueue != "tacklr" || !rt.disableStreams {
		t.Fatalf("defaults tq=%q streams=%v", rt.taskQueue, rt.disableStreams)
	}
	if rt.activityTimeout != 10*time.Minute || rt.heartbeatTimeout != 30*time.Second || rt.activityAttempts != 3 {
		t.Fatalf("activity defaults timeout=%v heartbeat=%v attempts=%d", rt.activityTimeout, rt.heartbeatTimeout, rt.activityAttempts)
	}
	hour := New(stub, Config{Catalog: cat, Secrets: secrets, TaskQueue: "q", ActivityTimeout: time.Hour, HeartbeatTimeout: time.Minute, ActivityAttempts: 1})
	if hour.activityTimeout != time.Hour || hour.heartbeatTimeout != time.Minute || hour.activityAttempts != 1 {
		t.Fatalf("cfg timeout=%v heartbeat=%v attempts=%d", hour.activityTimeout, hour.heartbeatTimeout, hour.activityAttempts)
	}
	ctx := t.Context()
	if _, err := rt.Head(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, "gone", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = sub.Close()
	rt.markClosed("gone")
	if err := rt.Prompt(ctx, "gone", durable.Prompt{Text: "x"}); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("closed session: %v", err)
	}
	if _, err := rt.Status(ctx, "gone"); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("closed status: %v", err)
	}
	if _, err := rt.Children(ctx, "gone"); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("closed children: %v", err)
	}
	if _, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "missing"}); !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("unknown agent: %v", err)
	}
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	fn()
}

func lastMsg(msgs []*tacklr.Message) *tacklr.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil {
			return msgs[i]
		}
	}
	return nil
}

func drainLog(t *testing.T, log durable.EventLog, id durable.SessionID) []tacklr.StreamEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ch, err := log.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []tacklr.StreamEvent
	for ev := range ch {
		got = append(got, ev)
	}
	return got
}

func querySession(t *testing.T, env *testsuite.TestWorkflowEnvironment) durable.SessionStatus {
	t.Helper()
	val, err := env.QueryWorkflow(queryStatus)
	if err != nil {
		t.Fatal(err)
	}
	var st durable.SessionStatus
	if err := val.Get(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

type retryLog struct {
	durable.EventLog
	retry []tacklr.StreamEvent
}

func (l *retryLog) Append(ctx context.Context, id durable.SessionID, topic string, ev tacklr.StreamEvent) error {
	if topic == durable.TopicRetry {
		l.retry = append(l.retry, ev)
	}
	return l.EventLog.Append(ctx, id, topic, ev)
}

func newActs(cat *durable.MemoryCatalog, log durable.EventLog, disableStreams bool) *activities {
	return &activities{
		Catalog:        cat,
		Snapshots:      inprocess.NewMemorySnapshot(),
		Projection:     vfs.DirectProjection{},
		Fallback:       log,
		DisableStreams: disableStreams,
		Secrets:        durable.NewMemorySecretStorage(),
	}
}

func TestSessionWorkflow_promptCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello-temporal", IsComplete: true}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-complete")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawMsg bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "hello-temporal") {
			sawMsg = true
		}
	}
	if !sawMsg {
		t.Fatalf("want hello-temporal, got %+v", got)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("Status after prompt: %+v", st)
	}
}

func TestSessionWorkflow_workerSessionFailedEndsTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{
					{ID: "fc_alpha", CallID: "call_alpha", Name: "alpha", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192},
			Model:  model,
			Tools: []*tacklr.Tool{
				tacklr.NewTool(tacklr.ToolConfig{Name: "alpha", Handler: func(context.Context) (string, error) { return "from-alpha", nil }}),
			},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	acts := newActs(cat, fallback, true)
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(acts)
	env.OnActivity(acts.Tool, mock.Anything, mock.Anything).Return(toolOutput{}, workflow.ErrSessionFailed)

	id := durable.SessionID("sess-session-failed")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	st := querySession(t, env)
	if st.State != durable.SessionFailed || st.Waiting {
		t.Fatalf("status %+v", st)
	}
}

func TestSessionWorkflow_inferenceRefusedFailsTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Model:  &testkit.ScriptedModel{InvokeErr: tacklr.ErrModelRefused},
			Config: tacklr.Config{MaxWindowSize: 8192},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-model-refused")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	st := querySession(t, env)
	if st.State != durable.SessionFailed || st.Waiting {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventError && (errors.Is(ev.Error, tacklr.ErrModelRefused) ||
			strings.Contains(ev.Content, tacklr.ErrModelRefused.Error()) ||
			strings.Contains(ev.Fail, tacklr.ErrModelRefused.Error())) {
			saw = true
		}
	}
	if !saw {
		t.Fatal("want StreamEventError with model refused")
	}
}

func TestSessionWorkflow_activityRetryThenCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var attempts atomic.Int32
	model := &testkit.ScriptedModel{
		InvokeErrFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) error {
			if attempts.Add(1) == 1 {
				return errors.New("transient")
			}
			return nil
		},
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-retry", IsComplete: true}
		},
	}
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := &retryLog{EventLog: inprocess.NewMemoryEventLog()}
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-retry")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fallback.retry) == 0 || fallback.retry[0].Content != "retry" {
		t.Fatalf("want retry event, got %+v", fallback.retry)
	}
	got := drainLog(t, fallback, id)
	var sawMsg bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-retry") {
			sawMsg = true
		}
	}
	if !sawMsg {
		t.Fatalf("want after-retry, got %+v", got)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("Status after retry: %+v", st)
	}
}

func TestSessionWorkflow_askUserYieldThenResume(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "chose", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
					Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
				}},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-yield")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "ask"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
		env.SignalWorkflow(signalResume, resumeSignal{Responses: map[string][]byte{"ask1": payload}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded, chose bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "chose" {
			chose = true
		}
	}
	if !yielded || !chose {
		t.Fatalf("want yield+chose, got %+v", got)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("Status after resume: %+v", st)
	}
}

// TestSessionWorkflow_parallelBatchHitlRunsRemainder: Temporal ran Tool
// activities sequentially and broke the batch on HITL, so leftover function
// calls never got outputs. Resume must still execute the rest of the batch.
func TestSessionWorkflow_parallelBatchHitlRunsRemainder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var (
		invokes int
		results []string
	)
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			invokes++
			if invokes == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{
						{ID: "fc_alpha", CallID: "call_alpha", Name: "alpha", Arguments: `{}`},
						{ID: "fc_gate", CallID: "call_gate", Name: "gate", Arguments: `{}`},
						{ID: "fc_beta", CallID: "call_beta", Name: "beta", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleTool {
					results = append(results, m.Content)
				}
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "all-three", IsComplete: true}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192},
			Model:  model,
			Tools: []*tacklr.Tool{
				tacklr.NewTool(tacklr.ToolConfig{Name: "alpha", Handler: func(context.Context) (string, error) { return "from-alpha", nil }}),
				tacklr.NewTool(tacklr.ToolConfig{
					Name:    "gate",
					OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
					Handler: func(context.Context) (string, error) { return "gate-ok", nil },
				}),
				tacklr.NewTool(tacklr.ToolConfig{Name: "beta", Handler: func(context.Context) (string, error) { return "from-beta", nil }}),
			},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-parallel-hitl")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "batch"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
		env.SignalWorkflow(signalResume, resumeSignal{Responses: map[string][]byte{"fc_gate": payload}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	saw := map[string]bool{}
	for _, c := range results {
		saw[c] = true
	}
	if !saw["from-alpha"] || !saw["gate-ok"] || !saw["from-beta"] {
		t.Fatalf("next model turn missing leftover tool results: %v", results)
	}
}

func TestSessionWorkflow_hitlCancel(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
					Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
				}},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-hitl-cancel")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "ask"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalCancel, nil)
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield before cancel, got %+v", got)
	}
}

func TestSessionWorkflow_cancelThenNextPromptCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	var n atomic.Int64
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if n.Add(1) == 1 {
				<-ctx.Done()
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-cancel", IsComplete: true}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-cancel-next")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "slow"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalCancel, nil)
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "again"})
	}, 40*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawAfter bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-cancel") {
			sawAfter = true
		}
	}
	if !sawAfter {
		t.Fatalf("want after-cancel, got %+v", got)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("Status after cancel+prompt: %+v", st)
	}
}

func TestSessionWorkflow_spawnWorker(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			last := lastMsg(msgs)
			if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "child-hello", IsComplete: true}
				return
			}
			if last != nil && last.Role == tacklr.RoleTool && last.Content == "child-hello" {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-after-spawn", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"child-task"}`,
				}},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Model:  model,
			Config: tacklr.Config{MaxWindowSize: 8192},
			Specialists: []*tacklr.Specialist{{
				Name:  "researcher",
				Model: model,
			}},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	acts := newActs(cat, fallback, true)
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(acts)
	var childStarted atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})

	id := durable.SessionID("sess-spawn")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want child SessionWorkflow started")
	}
	got := drainLog(t, fallback, id)
	var sawParent, sawChild, sawSpawnResult bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "parent-after-spawn") {
			sawParent = true
		}
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "child-hello") {
			sawChild = true
		}
		if ev.Type == tacklr.StreamEventToolResult && ev.Content == "child-hello" {
			sawSpawnResult = true
		}
	}
	childGot := drainLog(t, fallback, durable.ChildSessionID(id, "researcher", "sp1"))
	for _, ev := range childGot {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "child-hello") {
			sawChild = true
		}
	}
	if !sawParent {
		t.Fatalf("want parent complete after spawn, got %+v", got)
	}
	if !sawChild {
		t.Fatalf("want child complete event, parent=%+v child=%+v", got, childGot)
	}
	if !sawSpawnResult {
		t.Fatalf("want spawn_specialist tool result paired with child output, got %+v", got)
	}
	if snap, _, err := acts.Snapshots.Load(t.Context(), id); err != nil {
		t.Fatal(err)
	} else if snap.AgentID != "default" {
		t.Fatalf("parent snapshot agent=%q", snap.AgentID)
	}
	wantChild := durable.ChildSessionID(id, "researcher", "sp1")
	csnap, _, err := acts.Snapshots.Load(t.Context(), wantChild)
	if err != nil {
		t.Fatal(err)
	}
	if csnap.AgentID != "default" || csnap.Parent != id || csnap.Specialist != "researcher" {
		t.Fatalf("child snapshot identity=%+v", csnap)
	}
}

func TestSessionWorkflow_mixedBatchPairsBeforeNextRound(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			last := lastMsg(msgs)
			if last != nil && last.Role == tacklr.RoleUser && last.Content == "block-task" {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "block-result", IsComplete: true}
				return
			}
			var sawBlock, sawList bool
			for _, m := range msgs {
				if m == nil || m.Role != tacklr.RoleTool {
					continue
				}
				if m.Content == "block-result" {
					sawBlock = true
				}
				if strings.Contains(m.Content, "Child sessions:") || m.Content == "No child sessions." {
					sawList = true
				}
			}
			if sawBlock && sawList {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{
					{ID: "b1", CallID: "b1", Name: "spawn_specialist", Arguments: `{"specialist":"blocker","task_description_and_context":"block-task","block":true}`},
					{ID: "l1", CallID: "l1", Name: "list_children", Arguments: `{}`},
				},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Model:  model,
			Config: tacklr.Config{MaxWindowSize: 8192},
			Specialists: []*tacklr.Specialist{
				{Name: "blocker", Model: model},
			},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))
	id := durable.SessionID("sess-mixed-spawn")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 120*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawBlock, sawList, sawSecond bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && ev.Content == "block-result" {
			sawBlock = true
		}
		if ev.Type == tacklr.StreamEventToolResult && (strings.Contains(ev.Content, "Child sessions:") || ev.Content == "No child sessions.") {
			sawList = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "second-round" {
			sawSecond = true
		}
	}
	if !sawBlock || !sawList || !sawSecond {
		t.Fatalf("pairing block=%v list=%v second=%v events=%+v", sawBlock, sawList, sawSecond, got)
	}
}

func TestSessionWorkflow_asyncSpawnDoesNotWaitForChild(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	id := durable.SessionID("sess-async-spawn")
	childID := durable.ChildSessionID(id, "researcher", "sp1")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			last := lastMsg(msgs)
			if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "async-child", IsComplete: true}
				return
			}
			var scheduled, collected bool
			for _, m := range msgs {
				if m == nil || m.Role != tacklr.RoleTool {
					continue
				}
				if strings.Contains(m.Content, "scheduled") {
					scheduled = true
				}
				if strings.Contains(m.Content, "async-child") {
					collected = true
				}
			}
			switch {
			case collected:
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
			case scheduled:
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "gc1", CallID: "gc1", Name: "get_child",
						Arguments: `{"child_id":"` + string(childID) + `","block":true}`,
					}},
					IsComplete: true,
				}
			default:
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
						Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
					}},
					IsComplete: true,
				}
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Model:  model,
			Config: tacklr.Config{MaxWindowSize: 8192},
			Specialists: []*tacklr.Specialist{{
				Name:  "researcher",
				Model: model,
			}},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))
	var childStarted atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child SessionWorkflow started")
	}
	got := drainLog(t, fallback, id)
	var sawParent, sawScheduled bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "parent-continued") {
			sawParent = true
		}
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "scheduled") {
			sawScheduled = true
		}
	}
	childGot := drainLog(t, fallback, durable.ChildSessionID(id, "researcher", "sp1"))
	var sawChild bool
	for _, ev := range childGot {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "async-child") {
			sawChild = true
		}
	}
	if !sawScheduled || !sawParent || !sawChild {
		t.Fatalf("scheduled=%v parent=%v child=%v parentEv=%+v childEv=%+v", sawScheduled, sawParent, sawChild, got, childGot)
	}
}

func TestSessionWorkflow_listChildren(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			last := lastMsg(msgs)
			if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "from-child", IsComplete: true}
				return
			}
			if last != nil && last.Role == tacklr.RoleTool {
				if strings.Contains(last.Content, "cancelled and removed") {
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "listed", IsComplete: true}
					return
				}
				if strings.Contains(last.Content, "Child sessions:") {
					childID := string(durable.ChildSessionID("sess-list-children", "researcher", "sp1"))
					ch <- tacklr.LLMResponseChunk{
						Type: tacklr.StreamEventFunctionCall,
						ToolCalls: []tacklr.ToolCall{{
							ID: "cc1", CallID: "cc1", Name: "cancel_child",
							Arguments: `{"child_id":"` + childID + `"}`,
						}},
						IsComplete: true,
					}
					return
				}
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "ls1", CallID: "ls1", Name: "list_children", Arguments: `{}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
				}},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Model:  model,
			Config: tacklr.Config{MaxWindowSize: 8192},
			Specialists: []*tacklr.Specialist{{
				Name: "researcher", Model: model,
			}},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	id := durable.SessionID("sess-list-children")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 120*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var listed, cancelled bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "listed" {
			listed = true
		}
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "cancelled and removed") {
			cancelled = true
		}
	}
	if !listed || !cancelled {
		t.Fatalf("want listed after cancel_child, listed=%v cancelled=%v got %+v", listed, cancelled, drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_cancelStopsAsyncChild(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			last := lastMsg(msgs)
			if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
				select {
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "should-not-finish", IsComplete: true}
				}
				return
			}
			for _, m := range msgs {
				if m == nil {
					continue
				}
				for _, tc := range m.ToolCalls {
					if tc.Name == "spawn_specialist" {
						ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
						return
					}
				}
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
				}},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Model:  model,
			Config: tacklr.Config{MaxWindowSize: 8192},
			Specialists: []*tacklr.Specialist{{
				Name:  "researcher",
				Model: model,
			}},
		},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))
	var childStarted atomic.Bool
	var childErr error
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
		env.SignalWorkflow(signalCancel, nil)
	})
	env.SetOnChildWorkflowCompletedListener(func(info *workflow.Info, result converter.EncodedValue, err error) {
		childErr = err
	})
	id := durable.SessionID("sess-cancel-child")
	childID := durable.ChildSessionID(id, "researcher", "sp1")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child started before cancel")
	}
	st := querySession(t, env)
	if st.State != durable.SessionFailed || st.Waiting {
		t.Fatalf("parent status %+v", st)
	}
	if childErr == nil {
		if val, qerr := env.QueryWorkflowByID(string(childID), queryStatus); qerr == nil {
			var cst durable.SessionStatus
			if err := val.Get(&cst); err == nil && cst.State == durable.SessionFailed {
				return
			}
		}
		t.Fatalf("want child canceled/failed, parent=%+v", st)
	}
}

func TestSessionWorkflow_resumeRemountsWorkspaceFromCachedRecipe(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				if strings.Contains(last.Content, "from-workspace") {
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
					return
				}
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "read1", CallID: "read1", Name: "read",
						Arguments: `{"path":"/workspace/docs/hello.txt"}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
					Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
				}},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
		OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir))),
	})
	fallback := inprocess.NewMemoryEventLog()
	secrets := durable.NewMemorySecretStorage()
	acts := newActs(cat, fallback, true)
	acts.Secrets = secrets
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(acts)

	id := durable.SessionID("sess-remount")
	auth1 := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "tok-1"},
	}}}
	auth2 := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Auth:     vfs.Credential{Token: "tok-2"},
	}}}
	env.RegisterDelayedCallback(func() {
		_ = secrets.Put(context.Background(), id, durable.Secrets{Auth: auth1})
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "ask then read", Auth: auth1.WithoutSecrets()})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
		_ = secrets.Put(context.Background(), id, durable.Secrets{Auth: auth2})
		env.SignalWorkflow(signalResume, resumeSignal{
			Responses: map[string][]byte{"ask1": payload},
			Auth:      auth2.WithoutSecrets(),
		})
	}, 30*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 120*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded, sawFile bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "from-workspace") {
			sawFile = true
		}
	}
	if !yielded || !sawFile {
		t.Fatalf("want yield + remounted workspace, got %+v", got)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("Status after remount: %+v", st)
	}
}

type failPutSecrets struct{}

func (failPutSecrets) Put(context.Context, durable.SessionID, durable.Secrets) error {
	return errors.New("vault sealed")
}
func (failPutSecrets) Get(context.Context, durable.SessionID) (durable.Secrets, error) {
	return durable.Secrets{}, nil
}
func (failPutSecrets) Delete(context.Context, durable.SessionID) error { return nil }

type nopWorkflowClient struct{ client.Client }

func (nopWorkflowClient) SignalWorkflow(context.Context, string, string, string, any) error {
	return nil
}
func (nopWorkflowClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	return nil, errors.New("no query")
}

func TestRuntime_promptPutFailureDoesNotSignal(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	rt := New(nopWorkflowClient{}, Config{
		Catalog: cat, Secrets: failPutSecrets{}, DisableStreams: true,
		Fallback: inprocess.NewMemoryEventLog(),
	})
	err := rt.Prompt(t.Context(), "s", durable.Prompt{
		Text: "x",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "gdrive", Auth: vfs.Credential{Token: "tok"},
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "vault sealed") {
		t.Fatalf("put fail: %v", err)
	}
}

func TestRuntime_closeDeletesSecrets(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	store := durable.NewMemorySecretStorage()
	if err := store.Put(t.Context(), "s", durable.Secrets{Auth: durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive", Auth: vfs.Credential{Token: "tok"},
	}}}}); err != nil {
		t.Fatal(err)
	}
	rt := New(nopWorkflowClient{}, Config{
		Catalog: cat, Secrets: store, DisableStreams: true,
		Fallback: inprocess.NewMemoryEventLog(),
	})
	if err := rt.Close(t.Context(), "s"); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(t.Context(), "s")
	if err != nil || len(got.Auth.Bindings) != 0 {
		t.Fatalf("close left secrets: %+v %v", got, err)
	}
}

func TestActivities_harnessFallsBackToParentSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-parent"), 0o644); err != nil {
		t.Fatal(err)
	}
	var gotToken string
	open := vfs.Tree(vfs.At("docs", builtins.Local(dir)))
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
		OpenVFS: func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
			if len(req.Bindings) > 0 {
				gotToken = req.Bindings[0].Auth.Token
			}
			return open(ctx, sessionID, req)
		},
	})
	store := durable.NewMemorySecretStorage()
	parentAuth := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "parent-tok"},
	}}}
	if err := store.Put(t.Context(), "parent", durable.Secrets{Auth: parentAuth}); err != nil {
		t.Fatal(err)
	}
	acts := newActs(cat, inprocess.NewMemoryEventLog(), true)
	acts.Secrets = store
	mounts := durable.ApplyAuth(nil, parentAuth)
	if _, err := acts.Inference(t.Context(), inferenceInput{
		SessionID: "child", Parent: "parent", AgentID: "default",
		User:   &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
		Mounts: mounts,
	}); err != nil {
		t.Fatal(err)
	}
	if gotToken != "parent-tok" {
		t.Fatalf("OpenVFS token=%q", gotToken)
	}
}
