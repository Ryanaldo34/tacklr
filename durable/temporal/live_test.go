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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/durtest"
	"github.com/ryanaldo34/tacklr/internal/temporallive"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/telemetry"
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
	Secrets   durable.SecretStorage
}

func newLiveStack(t *testing.T, cat durable.Catalog) *liveStack {
	t.Helper()
	_ = temporallive.Client(t)
	c, err := Dial(client.Options{HostPort: temporallive.HostPort(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	n := liveSeq.Add(1)
	tq := fmt.Sprintf("tacklr-live-%d", n)
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	secrets := durable.NewMemorySecretStorage()
	cfg := Config{Catalog: cat, TaskQueue: tq, Snapshots: snaps, Fallback: log, Projection: vfs.DirectProjection{}, Secrets: secrets}
	rt := New(c, cfg)
	w := NewWorker(c, cfg)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return &liveStack{Runtime: rt, Snapshots: snaps, Catalog: cat, Client: c, TaskQueue: tq, Worker: w, Secrets: secrets}
}

func (s *liveStack) RestartWorker(t *testing.T) {
	t.Helper()
	s.Worker.Stop()
	w := NewWorker(s.Client, Config{
		Catalog:    s.Catalog,
		TaskQueue:  s.TaskQueue,
		Snapshots:  s.Snapshots,
		Fallback:   s.Runtime.(*Runtime).fallback,
		Projection: vfs.DirectProjection{},
		Secrets:    s.Secrets,
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	s.Worker = w
	t.Cleanup(w.Stop)
}

func TestMain(m *testing.M) {
	shutdown, err := telemetry.Init(context.Background(), telemetry.Config{})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
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

func waitTurn(t *testing.T, rt durable.Runtime, id durable.SessionID, sub durable.Subscription, timeout time.Duration) []tacklr.StreamEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var got []tacklr.StreamEvent
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
			if ev.Type == tacklr.StreamEventComplete || ev.Type == tacklr.StreamEventError || ev.Type == tacklr.StreamEventInterrupt {
				durtest.AssertStatusMatchesEvent(t, rt, id, ev)
				return got
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for turn events, got %d %+v", len(got), got)
		}
	}
}

func waitContains(t *testing.T, sub durable.Subscription, timeout time.Duration, want func(tacklr.StreamEvent) bool) []tacklr.StreamEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var got []tacklr.StreamEvent
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
		case <-ctx.Done():
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
	got := waitTurn(t, stack.Runtime, id, sub, 30*time.Second)
	var sawMsg, sawComplete bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "hello-live") {
			sawMsg = true
		}
		if ev.Type == tacklr.StreamEventComplete {
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
	got := waitTurn(t, stack.Runtime, id, sub, 30*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
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
	waitContains(t, sub, 30*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventMessage && ev.Content == "chose" || ev.Type == tacklr.StreamEventComplete
	})
}

func TestLive_workspaceAuthRemountsAfterResume(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir)))}))
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
	got := waitTurn(t, stack.Runtime, id, sub, 30*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
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
	waitContains(t, sub, 30*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "from-workspace")
	})
}

func TestLive_workerRestartWhileParkedThenResumeRemounts(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir)))}))
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
	got := waitTurn(t, stack.Runtime, id, sub, 30*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
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
	waitContains(t, sub, 30*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "from-workspace")
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
	waitStart, stopWait := context.WithTimeout(t.Context(), 20*time.Second)
	defer stopWait()
	select {
	case <-started:
	case <-waitStart.Done():
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
	waitContains(t, subCancel, 15*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventError && errors.Is(ev.Error, context.Canceled)
	})
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-cancel", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	head, err := stack.Runtime.Head(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "again"}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	waitContains(t, sub, 30*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-cancel")
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
	_ = waitTurn(t, stack.Runtime, id, sub, 30*time.Second)
	_ = sub.Close()
	head, err := stack.Runtime.Head(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{Text: "second"}); err != nil {
		t.Fatal(err)
	}
	sub2, err := stack.Runtime.Subscribe(ctx, id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub2.Close() })
	_ = waitTurn(t, stack.Runtime, id, sub2, 30*time.Second)
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir)))}))
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
	got := waitTurn(t, stack.Runtime, id, sub, 30*time.Second)
	var body string
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage {
			body = ev.Content
		}
	}
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace from cached recipe + prompt token, got %q %+v", body, got)
	}
}

func TestLive_secretsNotInHistory(t *testing.T) {
	ctx := t.Context()
	token := "tok-history-secret-7f3a"
	header := "Bearer mcp-secret-9c1e"
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{}))
	id, err := stack.Runtime.CreateSession(ctx, durable.CreateSession{
		AgentID: "default",
		MCPServers: []mcp.MCPConfig{{
			Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test/mcp",
			Headers:       []mcp.HTTPHeader{{Name: "Authorization", Value: header}},
			CredentialRef: "vault://mcp",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, durable.Prompt{
		Text: "hi",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "local",
			Params:   map[string]string{vfs.ParamName: "docs"},
			Auth:     vfs.Credential{Token: token},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Runtime.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	waitTurn(t, stack.Runtime, id, sub, 30*time.Second)

	iter := stack.Client.GetWorkflowHistory(ctx, string(id), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		blob := ev.String()
		if strings.Contains(blob, token) || strings.Contains(blob, header) {
			t.Fatalf("secret in history event %s", ev.GetEventType())
		}
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
					if tc.Name == "spawn_specialist" {
						ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-after-spawn", IsComplete: true}
						return
					}
				}
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
	stack := newLiveStack(t, liveCat(t, model, durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Specialists: []*tacklr.Specialist{{
				Name:  "researcher",
				Model: model,
			}},
		},
	}))
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
	waitContains(t, sub, 45*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "parent-after-spawn")
	})
}
