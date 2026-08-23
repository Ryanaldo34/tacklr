package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/temporallive"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

var liveSeq atomic.Int64

type liveStack struct {
	Runtime   durable.Runtime
	Snapshots durable.SnapshotStore
	Catalog   durable.Catalog
	Client    client.Client
	TaskQueue string
	Worker    worker.Worker
}

func newLiveStack(t *testing.T, cat durable.Catalog) *liveStack {
	t.Helper()
	c := temporallive.Client(t)
	n := liveSeq.Add(1)
	tq := fmt.Sprintf("tacklr-live-%d", n)
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	rt := New(c, tq, cat, WithSnapshotStore(snaps), WithEventLog(log))
	w := NewWorker(c, tq, WorkerOptions{
		Catalog:    cat,
		Snapshots:  snaps,
		Fallback:   log,
		Projection: vfs.DirectProjection{},
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return &liveStack{Runtime: rt, Snapshots: snaps, Catalog: cat, Client: c, TaskQueue: tq, Worker: w}
}

func (s *liveStack) RestartWorker(t *testing.T) {
	t.Helper()
	s.Worker.Stop()
	w := NewWorker(s.Client, s.TaskQueue, WorkerOptions{
		Catalog:    s.Catalog,
		Snapshots:  s.Snapshots,
		Fallback:   s.Runtime.(*Runtime).FallbackLog(),
		Projection: vfs.DirectProjection{},
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	s.Worker = w
	t.Cleanup(w.Stop)
}

func TestMain(m *testing.M) {
	code := m.Run()
	temporallive.Stop()
	os.Exit(code)
}

func liveCat(t *testing.T, model tacklr.InferenceStrategy, extra durable.AgentSpec) *durable.MemoryCatalog {
	t.Helper()
	cat := durable.NewCatalog("default")
	spec := extra
	if spec.Options.Model == nil {
		spec.Options.Model = model
	}
	if spec.Options.Config.MaxWindowSize == 0 {
		spec.Options.Config.MaxWindowSize = 8192
	}
	cat.Register("default", spec)
	return cat
}

func waitTurn(t *testing.T, sub durable.Subscription, timeout time.Duration) []streaming.StreamEvent {
	t.Helper()
	deadline := time.After(timeout)
	var got []streaming.StreamEvent
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
			if ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError || ev.Type == streaming.StreamEventInterrupt {
				return got
			}
		case <-deadline:
			t.Fatalf("timeout waiting for turn events, got %d %+v", len(got), got)
		}
	}
}

func waitContains(t *testing.T, sub durable.Subscription, timeout time.Duration, want func(streaming.StreamEvent) bool) []streaming.StreamEvent {
	t.Helper()
	deadline := time.After(timeout)
	var got []streaming.StreamEvent
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed, got %+v", got)
			}
			got = append(got, ev)
			if want(ev) {
				return got
			}
		case <-deadline:
			t.Fatalf("timeout, got %+v", got)
		}
	}
}

func TestLive_promptSubscribeComplete(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello-live", IsComplete: true}
		},
	}
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stack.Runtime.Head(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitTurn(t, sub, 30*time.Second)
	var sawMsg, sawComplete bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "hello-live") {
			sawMsg = true
		}
		if ev.Type == streaming.StreamEventComplete {
			sawComplete = true
		}
	}
	if !sawMsg || !sawComplete {
		t.Fatalf("want hello-live + complete via workflow streams, got %+v", got)
	}
	if err := stack.Runtime.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	_, _, err = stack.Snapshots.Load(ctx, id)
	if !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("want snapshot gone, got %v", err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "again"}); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("want session-not-found, got %v", err)
	}
}

func TestLive_hitlYieldThenResume(t *testing.T) {
	ctx := t.Context()
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "ask"}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitTurn(t, sub, 30*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield, got %+v", got)
	}
	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := stack.Runtime.Resume(ctx, id, durable.Resume{Responses: map[string][]byte{"ask1": payload}}); err != nil {
		t.Fatal(err)
	}
	waitContains(t, sub, 30*time.Second, func(ev streaming.StreamEvent) bool {
		return ev.Type == streaming.StreamEventMessage && ev.Content == "chose" || ev.Type == streaming.StreamEventComplete
	})
}

func TestLive_permissionAllowRunsTool(t *testing.T) {
	ctx := t.Context()
	var ran atomic.Bool
	sensitive := tacklr.NewTool(tacklr.ToolConfig{
		Name:   "sensitive",
		OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			ran.Store(true)
			return "secret-ok", nil
		},
	})
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "all clear", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "call_sens", CallID: "call_sens", Name: "sensitive", Arguments: `{}`,
				}},
				IsComplete: true,
			}
		},
	}
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{
		Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{sensitive}},
	}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "run sensitive"}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitTurn(t, sub, 30*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want permission yield, got %+v", got)
	}
	payload, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
	if err := stack.Runtime.Resume(ctx, id, durable.Resume{Responses: map[string][]byte{"call_sens": payload}}); err != nil {
		t.Fatal(err)
	}
	waitContains(t, sub, 30*time.Second, func(ev streaming.StreamEvent) bool {
		return ev.Type == streaming.StreamEventToolResult && strings.Contains(ev.Content, "secret-ok")
	})
	if !ran.Load() {
		t.Fatal("expected tool to run after allow")
	}
}

func TestLive_workspaceAuthRemountsAfterResume(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{FSRegistry: fsReg}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	bind := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "tok-1"},
	}}}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "ask then read", Auth: bind}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitTurn(t, sub, 30*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield, got %+v", got)
	}
	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := stack.Runtime.Resume(ctx, id, durable.Resume{
		Responses: map[string][]byte{"ask1": payload},
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Auth:     vfs.Credential{Token: "tok-2"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	waitContains(t, sub, 30*time.Second, func(ev streaming.StreamEvent) bool {
		return ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "from-workspace")
	})
}

func TestLive_workerRestartWhileParkedThenResumeRemounts(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{FSRegistry: fsReg}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{
		Text: "park",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Params:   map[string]string{vfs.ParamName: "docs"},
			Auth:     vfs.Credential{Token: "tok-1"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitTurn(t, sub, 30*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == streaming.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield before worker restart, got %+v", got)
	}
	stack.RestartWorker(t)
	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := stack.Runtime.Resume(ctx, id, durable.Resume{
		Responses: map[string][]byte{"ask1": payload},
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Auth:     vfs.Credential{Token: "tok-2"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	waitContains(t, sub, 30*time.Second, func(ev streaming.StreamEvent) bool {
		return ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "from-workspace")
	})
}

func TestLive_cancelThenNextPrompt(t *testing.T) {
	ctx := t.Context()
	started := make(chan struct{}, 1)
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
		},
	}
	cat := liveCat(t, model, durable.AgentSpec{})
	stack := newLiveStack(t, cat)
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "slow"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("model did not start")
	}
	subCancel, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subCancel.Close() })
	if err := stack.Runtime.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	waitContains(t, subCancel, 15*time.Second, func(ev streaming.StreamEvent) bool {
		return ev.Type == streaming.StreamEventError && errors.Is(ev.Error, context.Canceled)
	})
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-cancel", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "again"}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	waitContains(t, sub, 30*time.Second, func(ev streaming.StreamEvent) bool {
		return ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "after-cancel")
	})
}

func TestLive_twoPromptsShareSnapshot(t *testing.T) {
	ctx := t.Context()
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "first"}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitTurn(t, sub, 30*time.Second)
	_ = sub.Close()
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "second"}); err != nil {
		t.Fatal(err)
	}
	sub2, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	_ = waitTurn(t, sub2, 30*time.Second)
	var sawFirst bool
	for _, m := range model.LastInvokeMsgs {
		if m != nil && m.Role == tacklr.RoleUser && m.Content == "first" {
			sawFirst = true
		}
	}
	if !sawFirst {
		t.Fatalf("second prompt must see first snapshot messages, last=%+v", model.LastInvokeMsgs)
	}
}

func TestLive_cachedRecipePlusTokenOnlyPrompt(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
		t.Fatal(err)
	}
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "read-1", CallID: "read-1", Name: "read",
					Arguments: `{"path":"/workspace/docs/hello.txt"}`,
				}},
				IsComplete: true,
			}
		},
	}
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{FSRegistry: fsReg}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{
		AgentID: "default",
		Mounts: []durable.MountRecipe{{
			Provider: "local",
			Alias:    "docs",
			Params:   map[string]string{vfs.ParamName: "docs"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{
		Text: "read",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Auth:     vfs.Credential{Token: "x"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitTurn(t, sub, 30*time.Second)
	var body string
	for _, ev := range got {
		if ev.Type == streaming.StreamEventMessage {
			body = ev.Content
		}
	}
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace from cached recipe + prompt token, got %q %+v", body, got)
	}
}

func TestLive_spawnWorker(t *testing.T) {
	ctx := t.Context()
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	waitContains(t, sub, 45*time.Second, func(ev streaming.StreamEvent) bool {
		return ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "parent-after-spawn")
	})
}
