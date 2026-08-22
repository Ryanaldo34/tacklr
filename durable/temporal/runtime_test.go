package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
			for _, m := range msgs {
				if m == nil {
					continue
				}
				for _, tc := range m.ToolCalls {
					if tc.Name == "spawn_worker" {
						ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-after-spawn", IsComplete: true}
						return
					}
				}
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_worker",
					Arguments: `{"worker_name":"researcher","task_description_and_context":"child-task"}`,
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
	var sawParent, sawChild bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "parent-after-spawn") {
			sawParent = true
		}
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "child-hello") {
			sawChild = true
		}
	}
	childGot := drainLog(t, fallback, durable.SessionID(string(id)+"/worker/sp1"))
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
}

func TestRuntime_createPromptSubscribeClose(t *testing.T) {
	if testing.Short() {
		t.Skip("temporal dev server")
	}
	ctx := t.Context()
	ds, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		LogLevel: "error",
		Stdout:   io.Discard,
		Stderr:   io.Discard,
	})
	if err != nil {
		t.Fatalf("start temporal dev server: %v", err)
	}
	t.Cleanup(func() { _ = ds.Stop() })
	c := ds.Client()
	tq := "tacklr-runtime-test"
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello-runtime", IsComplete: true}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	snaps := inprocess.NewMemorySnapshot()
	rt := New(c, tq, cat,
		WithEventLog(fallback),
		WithSnapshotStore(snaps),
	)
	w := NewWorker(c, tq, WorkerOptions{
		Catalog:    cat,
		Snapshots:  snaps,
		Fallback:   fallback,
		Projection: vfs.DirectProjection{},
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default", SessionID: "rt1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	deadline := time.After(20 * time.Second)
	var sawMsg, sawComplete bool
	for !sawMsg || !sawComplete {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscribe closed msg=%v complete=%v", sawMsg, sawComplete)
			}
			if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "hello-runtime") {
				sawMsg = true
			}
			if ev.Type == streaming.StreamEventComplete {
				sawComplete = true
			}
		case <-deadline:
			t.Fatalf("timeout msg=%v complete=%v", sawMsg, sawComplete)
		}
	}

	if err := rt.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	_, _, err = snaps.Load(ctx, id)
	if !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("want snapshot gone, got %v", err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "again"}); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("want session-not-found, got %v", err)
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
