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

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/durtest"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

var liveSeq atomic.Int64

func liveHostPort(t *testing.T) string {
	t.Helper()
	return temporaldocker.HostPort(t)
}

type liveStack struct {
	Runtime   session.Runtime
	Snapshots session.SnapshotStore
	Agent     tacklr.AgentOptions
	Client    client.Client
	TaskQueue string
	Worker    worker.Worker
	Secrets   session.SecretStorage
}

func newLiveStack(t *testing.T, agent tacklr.AgentOptions) *liveStack {
	t.Helper()
	addr := liveHostPort(t)
	c, err := Dial(client.Options{HostPort: addr})
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(c.Close)
	n := liveSeq.Add(1)
	tq := fmt.Sprintf("tacklr-live-%d", n)
	snaps := session.NewMemorySnapshot()
	log := session.NewMemoryEventLog()
	secrets := session.NewMemorySecretStorage()
	cfg := Config{Agent: agent, TaskQueue: tq, Snapshots: snaps, Fallback: log, Secrets: secrets}
	rt := Open(c, cfg)
	w := rt.StartWorker()
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return &liveStack{Runtime: rt, Snapshots: snaps, Agent: agent, Client: c, TaskQueue: tq, Worker: w, Secrets: secrets}
}

func (s *liveStack) RestartWorker(t *testing.T) {
	t.Helper()
	s.Worker.Stop()
	w := s.Runtime.(*Runtime).StartWorker()
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
	// Workflow tests do not dial. Dial is what installs the replay-safe tracer.
	if err := telemetry.ReinstallTracer(context.Background(), func(opts ...sdktrace.TracerProviderOption) (trace.TracerProvider, func(context.Context) error) {
		tp := temporalotel.NewReplaySafeTracerProvider(opts...)
		return tp, tp.Shutdown
	}); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
	temporaldocker.Stop()
	os.Exit(code)
}

func liveAgent(t *testing.T, model tacklr.InferenceStrategy, extra tacklr.AgentOptions) tacklr.AgentOptions {
	t.Helper()
	spec := extra
	if spec.Model == nil {
		spec.Model = model
	}
	if spec.MaxWindowSize == 0 {
		spec.MaxWindowSize = 8192
	}
	return spec
}

func waitTurn(t *testing.T, rt session.Runtime, id session.SessionID, sub session.Subscription, timeout time.Duration) []tacklr.StreamEvent {
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

func waitContains(t *testing.T, sub session.Subscription, timeout time.Duration, want func(tacklr.StreamEvent) bool) []tacklr.StreamEvent {
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

func TestLive_workerRestartWhileParkedThenResumeRemounts(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
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
	})
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(dir)))}))
	id, err := stack.Runtime.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, session.Prompt{
		Text: "park",
		Auth: session.AuthContext{Bindings: []vfs.Binding{{
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
	if err := stack.Runtime.Resume(ctx, id, session.Resume{
		Responses: map[string][]byte{"ask1": payload},
		Auth: session.AuthContext{Bindings: []vfs.Binding{{
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

func TestLive_cachedRecipePlusTokenOnlyPrompt(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
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
	})
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(dir)))}))
	id, err := stack.Runtime.CreateSession(ctx, session.CreateSession{
		Mounts: []session.MountRecipe{{
			Provider: "local",
			Alias:    "docs",
			Params:   map[string]string{vfs.ParamName: "docs"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, session.Prompt{
		Text: "read",
		Auth: session.AuthContext{Bindings: []vfs.Binding{{
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
	var body strings.Builder
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage {
			body.WriteString(ev.Content)
		}
	}
	if !strings.Contains(body.String(), "from-workspace") {
		t.Fatalf("want workspace from cached recipe + prompt token, got %q %+v", body.String(), got)
	}
}

func TestLive_secretsNotInHistory(t *testing.T) {
	ctx := t.Context()
	token := "tok-history-secret-7f3a"
	header := "Bearer mcp-secret-9c1e"
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
	})
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{}))
	id, err := stack.Runtime.CreateSession(ctx, session.CreateSession{
		MCPServers: []mcp.MCPConfig{{
			Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test/mcp",
			Headers:       []mcp.HTTPHeader{{Name: "Authorization", Value: header}},
			CredentialRef: "vault://mcp",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Runtime.Prompt(ctx, id, session.Prompt{
		Text: "hi",
		Auth: session.AuthContext{Bindings: []vfs.Binding{{
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

func TestOpen_panicsWithoutClient(t *testing.T) {
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}
	stub := &struct{ client.Client }{}
	mustPanic(t, func() { Open(nil, Config{Agent: agent}) })
	rt := Open(stub, Config{Agent: agent, DisableStreams: true, TurnLocality: time.Minute})
	if rt.snapshots == nil || rt.secrets == nil {
		t.Fatal("zero Snapshots and Secrets must use the memory stores")
	}
	if rt.taskQueue != "tacklr" || !rt.disableStreams {
		t.Fatalf("defaults tq=%q streams=%v", rt.taskQueue, rt.disableStreams)
	}
	if rt.activityTimeout != 10*time.Minute || rt.heartbeatTimeout != 30*time.Second || rt.activityAttempts != 3 {
		t.Fatalf("activity defaults timeout=%v heartbeat=%v attempts=%d", rt.activityTimeout, rt.heartbeatTimeout, rt.activityAttempts)
	}
	hour := Open(stub, Config{Agent: agent, TaskQueue: "q", ActivityTimeout: time.Hour, HeartbeatTimeout: time.Minute, ActivityAttempts: 1})
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
	if err := rt.Prompt(ctx, "gone", session.Prompt{Text: "x"}); !errors.Is(err, session.ErrSessionNotFound) {
		t.Fatalf("closed session: %v", err)
	}
	if _, err := rt.Status(ctx, "gone"); !errors.Is(err, session.ErrSessionNotFound) {
		t.Fatalf("closed status: %v", err)
	}
	if _, err := rt.Children(ctx, "gone"); !errors.Is(err, session.ErrSessionNotFound) {
		t.Fatalf("closed children: %v", err)
	}
}

type failPutSecrets struct{}

func (failPutSecrets) Put(context.Context, session.SessionID, session.Secrets) error {
	return errors.New("vault sealed")
}
func (failPutSecrets) Get(context.Context, session.SessionID) (session.Secrets, error) {
	return session.Secrets{}, nil
}
func (failPutSecrets) Delete(context.Context, session.SessionID) error { return nil }

type nopWorkflowClient struct{ client.Client }

func (nopWorkflowClient) SignalWorkflow(context.Context, string, string, string, any) error {
	return nil
}
func (nopWorkflowClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	return nil, errors.New("no query")
}

func TestRuntime_promptFailsWhenVaultSealed(t *testing.T) {
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}
	rt := Open(nopWorkflowClient{}, Config{
		Agent: agent, Snapshots: session.NewMemorySnapshot(), Secrets: failPutSecrets{}, DisableStreams: true,
		Fallback: session.NewMemoryEventLog(),
	})
	err := rt.Prompt(t.Context(), "s", session.Prompt{
		Text: "x",
		Auth: session.AuthContext{Bindings: []vfs.Binding{{
			Provider: "gdrive", Auth: vfs.Credential{Token: "tok"},
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "vault sealed") {
		t.Fatalf("put fail: %v", err)
	}
}

func TestRuntime_closeDeletesSecrets(t *testing.T) {
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}
	store := session.NewMemorySecretStorage()
	if err := store.Put(t.Context(), "s", session.Secrets{Auth: session.AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive", Auth: vfs.Credential{Token: "tok"},
	}}}}); err != nil {
		t.Fatal(err)
	}
	snaps := session.NewMemorySnapshot()
	if _, err := snaps.Save(t.Context(), "s", session.Snapshot{}, ""); err != nil {
		t.Fatal(err)
	}
	rt := Open(nopWorkflowClient{}, Config{
		Agent: agent, Snapshots: snaps, Secrets: store, DisableStreams: true,
		Fallback: session.NewMemoryEventLog(),
	})
	if err := rt.Close(t.Context(), "s"); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(t.Context(), "s")
	if err != nil || len(got.Auth.Bindings) != 0 {
		t.Fatalf("close left secrets: %+v %v", got, err)
	}
	if _, _, err := snaps.Load(t.Context(), "s"); !errors.Is(err, session.ErrSessionNotFound) {
		t.Fatalf("close left the snapshot: %v", err)
	}
	if err := rt.Prompt(t.Context(), "s", session.Prompt{Text: "again"}); !errors.Is(err, session.ErrSessionNotFound) {
		t.Fatalf("prompt after close: %v", err)
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

func drainLog(t *testing.T, log session.EventLog, id session.SessionID) []tacklr.StreamEvent {
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

func querySession(t *testing.T, env *testsuite.TestWorkflowEnvironment) session.SessionStatus {
	t.Helper()
	val, err := env.QueryWorkflow(queryStatus)
	if err != nil {
		t.Fatal(err)
	}
	var st session.SessionStatus
	if err := val.Get(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

type retryLog struct {
	session.EventLog
	retry []tacklr.StreamEvent
}

func (l *retryLog) Append(ctx context.Context, id session.SessionID, topic string, ev tacklr.StreamEvent) error {
	if topic == session.TopicRetry {
		l.retry = append(l.retry, ev)
	}
	return l.EventLog.Append(ctx, id, topic, ev)
}

func newActs(agent tacklr.AgentOptions, log session.EventLog, disableStreams bool) *activities {
	return &activities{
		Agent:          agent,
		Snapshots:      session.NewMemorySnapshot(),
		Fallback:       log,
		DisableStreams: disableStreams,
		Secrets:        session.NewMemorySecretStorage(),
	}
}
