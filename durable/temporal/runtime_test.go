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
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestFailFromWire(t *testing.T) {
	cases := []struct {
		in   string
		want error
	}{
		{"model refused: nope", tacklr.ErrModelRefused},
		{"max tokens reached", tacklr.ErrMaxTokens},
		{"max turn model requests exceeded", tacklr.ErrMaxTurnRequests},
		{"context canceled", context.Canceled},
		{"run: context cancelled: context canceled", context.Canceled},
		{"boom", errors.New("boom")},
	}
	for _, tc := range cases {
		got := failFromWire(tc.in)
		if tc.want.Error() == "boom" {
			if got.Error() != "boom" {
				t.Fatalf("%q: %v", tc.in, got)
			}
			continue
		}
		if !errors.Is(got, tc.want) {
			t.Fatalf("%q: got %v want %v", tc.in, got, tc.want)
		}
	}
}

func TestActivities_unknownAgentAndDirectCall(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	log := inprocess.NewMemoryEventLog()
	acts := &Activities{Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Fallback: log, DisableStreams: true}
	_, err := acts.Inference(t.Context(), InferenceInput{SessionID: "s", AgentID: "nope"})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("missing agent: %v", err)
	}
	_, err = acts.Tool(t.Context(), ToolInput{SessionID: "s", AgentID: "nope", Call: streaming.ToolCall{ID: "c", Name: "x"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("tool missing agent: %v", err)
	}
	out, err := acts.Inference(t.Context(), InferenceInput{
		SessionID: "s", AgentID: "default",
		User: &streaming.Message{Role: streaming.RoleUser, Content: "hi"},
	})
	if err != nil || !out.Complete {
		t.Fatalf("direct inference: %+v %v", out, err)
	}
}

func TestNew_panicsWithoutClientOrCatalog(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	mustPanic(t, func() { New(nil, "q", cat) })
	mustPanic(t, func() { New(&struct{ client.Client }{}, "q", nil) })
	log := inprocess.NewMemoryEventLog()
	rt := New(&struct{ client.Client }{}, "", cat, nil, WithDisableStreams(), WithSnapshotStore(nil), WithEventLog(nil), WithEventLog(log))
	if rt.taskQueue != "tacklr" || !rt.disableStreams {
		t.Fatalf("defaults tq=%q streams=%v", rt.taskQueue, rt.disableStreams)
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

func drainLog(t *testing.T, log durable.EventLog, id durable.SessionID) []streaming.StreamEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ch, err := log.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []streaming.StreamEvent
	for ev := range ch {
		got = append(got, ev)
	}
	return got
}

func newActs(cat *durable.MemoryCatalog, log durable.EventLog, disableStreams bool) *Activities {
	return &Activities{
		Catalog:        cat,
		Snapshots:      inprocess.NewMemorySnapshot(),
		Projection:     vfs.DirectProjection{},
		Fallback:       log,
		DisableStreams: disableStreams,
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

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawMsg, sawComplete bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "hello-temporal") {
			sawMsg = true
		}
		if ev.Type == streaming.StreamEventComplete {
			sawComplete = true
		}
	}
	if !sawMsg || !sawComplete {
		t.Fatalf("want hello-temporal + complete, got %+v", got)
	}
}

func TestSessionWorkflow_turnLocalityTimeoutCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "session-on", IsComplete: true}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-worker-session")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{
		SessionID:           id,
		AgentID:             "default",
		TurnLocalityTimeout: time.Minute,
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawMsg, sawComplete bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "session-on") {
			sawMsg = true
		}
		if ev.Type == streaming.StreamEventComplete {
			sawComplete = true
		}
	}
	if !sawMsg || !sawComplete {
		t.Fatalf("want session-on + complete, got %+v", got)
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
	env.OnActivity(acts.Tool, mock.Anything, mock.Anything).Return(ToolOutput{}, workflow.ErrSessionFailed)

	id := durable.SessionID("sess-session-failed")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
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

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded, chose, complete bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == streaming.StreamEventMessage && ev.Content == "chose" {
			chose = true
		}
		if ev.Type == streaming.StreamEventComplete {
			complete = true
		}
	}
	if !yielded || !chose || !complete {
		t.Fatalf("want yield+chose+complete, got %+v", got)
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

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
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

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
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

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawAfter, complete bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "after-cancel") {
			sawAfter = true
		}
		if ev.Type == streaming.StreamEventComplete {
			complete = true
		}
	}
	if !sawAfter || !complete {
		t.Fatalf("want after-cancel + complete, got %+v", got)
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
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))
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

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want child SessionWorkflow started")
	}
	got := drainLog(t, fallback, id)
	var sawParent, sawChild, sawSpawnResult bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "parent-after-spawn") {
			sawParent = true
		}
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "child-hello") {
			sawChild = true
		}
		if ev.Type == streaming.StreamEventToolResult && ev.Content == "child-hello" {
			sawSpawnResult = true
		}
	}
	childGot := drainLog(t, fallback, durable.ChildSessionID(id, "researcher", "sp1"))
	for _, ev := range childGot {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "child-hello") {
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
	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawBlock, sawList, sawSecond bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventToolResult && ev.Content == "block-result" {
			sawBlock = true
		}
		if ev.Type == streaming.StreamEventToolResult && (strings.Contains(ev.Content, "Child sessions:") || ev.Content == "No child sessions.") {
			sawList = true
		}
		if ev.Type == streaming.StreamEventMessage && ev.Content == "second-round" {
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
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			last := lastMsg(msgs)
			if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "async-child", IsComplete: true}
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
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})
	id := durable.SessionID("sess-async-spawn")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child SessionWorkflow started")
	}
	got := drainLog(t, fallback, id)
	var sawParent, sawScheduled bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "parent-continued") {
			sawParent = true
		}
		if ev.Type == streaming.StreamEventToolResult && strings.Contains(ev.Content, "scheduled") {
			sawScheduled = true
		}
	}
	childGot := drainLog(t, fallback, durable.ChildSessionID(id, "researcher", "sp1"))
	var sawChild bool
	for _, ev := range childGot {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "async-child") {
			sawChild = true
		}
	}
	if !sawParent || !sawChild {
		t.Fatalf("parent=%v child=%v scheduled=%v parentEv=%+v childEv=%+v", sawParent, sawChild, sawScheduled, got, childGot)
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
	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var listed, cancelled bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == streaming.StreamEventMessage && ev.Content == "listed" {
			listed = true
		}
		if ev.Type == streaming.StreamEventToolResult && strings.Contains(ev.Content, "cancelled and removed") {
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
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})
	id := durable.SessionID("sess-cancel-child")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalCancel, nil)
	}, 40*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child started before cancel")
	}
	for _, ev := range drainLog(t, fallback, durable.ChildSessionID(id, "researcher", "sp1")) {
		if ev.Type == streaming.StreamEventMessage && ev.Content == "should-not-finish" {
			t.Fatal("cancelled child completed")
		}
	}
}

func TestSessionWorkflow_resumeRemountsWorkspaceFromCachedRecipe(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
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
		Options:    tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
		FSRegistry: fsReg,
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-remount")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{
			Text: "ask then read",
			Auth: durable.AuthContext{Bindings: []vfs.Binding{{
				Provider: "local",
				Params:   map[string]string{vfs.ParamName: "docs"},
				Auth:     vfs.Credential{Token: "tok-1"},
			}}},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
		env.SignalWorkflow(signalResume, resumeSignal{
			Responses: map[string][]byte{"ask1": payload},
			Auth: durable.AuthContext{Bindings: []vfs.Binding{{
				Provider: "local",
				Auth:     vfs.Credential{Token: "tok-2"},
			}}},
		})
	}, 30*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 120*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded, sawFile, complete bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "from-workspace") {
			sawFile = true
		}
		if ev.Type == streaming.StreamEventComplete {
			complete = true
		}
	}
	if !yielded || !sawFile || !complete {
		t.Fatalf("want yield + remounted workspace + complete, got %+v", got)
	}
}
