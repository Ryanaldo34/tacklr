package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/durtest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

const (
	temporalImage   = "temporalio/server:latest"
	adminToolsImage = "temporalio/admin-tools:latest"
	temporalPGImage = "tacklr-pg-brain:test"
)

var (
	liveSeq     atomic.Int64
	liveMu      sync.Mutex
	liveCtr     testcontainers.Container
	livePG      testcontainers.Container
	liveNet     *testcontainers.DockerNetwork
	liveAddr    string
	liveStart   error
	liveStarted bool
)

func liveHostPort(t *testing.T) string {
	t.Helper()
	liveMu.Lock()
	defer liveMu.Unlock()
	if !liveStarted {
		liveStarted = true
		liveStart = startTemporal()
	}
	if liveStart != nil {
		t.Skipf("Temporal unavailable: %v", liveStart)
	}
	return liveAddr
}

func stopLive() {
	if liveCtr != nil {
		_ = liveCtr.Terminate(context.Background())
		liveCtr = nil
	}
	if livePG != nil {
		_ = livePG.Terminate(context.Background())
		livePG = nil
	}
	if liveNet != nil {
		_ = liveNet.Remove(context.Background())
		liveNet = nil
	}
}

func startTemporal() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nw, err := network.New(ctx)
	if err != nil {
		return err
	}
	liveNet = nw
	pg, err := postgres.Run(ctx, temporalPGImage,
		postgres.WithDatabase("temporal"),
		postgres.WithUsername("temporal"),
		postgres.WithPassword("temporal"),
		postgres.BasicWaitStrategies(),
		network.WithNetwork([]string{"postgresql"}, nw),
	)
	if err != nil {
		stopLive()
		return fmt.Errorf("%w (build image: make brain-pg-image)", err)
	}
	livePG = pg
	if code, _, err := pg.Exec(ctx, []string{"psql", "-U", "temporal", "-d", "temporal", "-c", "CREATE DATABASE temporal_visibility;"}); err != nil || code != 0 {
		stopLive()
		if err != nil {
			return err
		}
		return fmt.Errorf("create temporal_visibility: exit %d", code)
	}
	admin, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      adminToolsImage,
			Env:        map[string]string{"SQL_PASSWORD": "temporal"},
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd: []string{`set -eu
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal setup-schema -v 0.0
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal update-schema -d /etc/temporal/schema/postgresql/v12/temporal/versioned
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal_visibility setup-schema -v 0.0
temporal-sql-tool --plugin postgres12 --ep postgresql -u temporal -p 5432 --db temporal_visibility update-schema -d /etc/temporal/schema/postgresql/v12/visibility/versioned
`},
			Networks:   []string{nw.Name},
			WaitingFor: wait.ForExit().WithExitTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		stopLive()
		return err
	}
	st, stErr := admin.State(ctx)
	_ = admin.Terminate(context.Background())
	if stErr != nil || st.ExitCode != 0 {
		stopLive()
		if stErr != nil {
			return stErr
		}
		return fmt.Errorf("temporal schema setup exit %d", st.ExitCode)
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        temporalImage,
			ExposedPorts: []string{"7233/tcp"},
			Networks:     []string{nw.Name},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader("{}\n"),
				ContainerFilePath: "/etc/temporal/config/dynamicconfig/docker.yaml",
				FileMode:          0o644,
			}},
			Env: map[string]string{
				"DB":                   "postgres12",
				"DB_PORT":              "5432",
				"DBNAME":               "temporal",
				"VISIBILITY_DBNAME":    "temporal_visibility",
				"POSTGRES_USER":        "temporal",
				"POSTGRES_PWD":         "temporal",
				"POSTGRES_SEEDS":       "postgresql",
				"BIND_ON_IP":           "0.0.0.0",
				"TEMPORAL_ADDRESS":     "127.0.0.1:7233",
				"TEMPORAL_CLI_ADDRESS": "127.0.0.1:7233",
			},
			WaitingFor: wait.ForListeningPort("7233/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		stopLive()
		return err
	}
	addr, err := ctr.PortEndpoint(ctx, "7233/tcp", "")
	if err != nil {
		_ = ctr.Terminate(context.Background())
		stopLive()
		return err
	}
	deadline := time.Now().Add(time.Minute)
	var last error
	for {
		c, dialErr := client.Dial(client.Options{HostPort: addr})
		if dialErr == nil {
			hctx, hcancel := context.WithTimeout(ctx, 2*time.Second)
			_, last = c.CheckHealth(hctx, &client.CheckHealthRequest{})
			hcancel()
			c.Close()
			if last == nil {
				break
			}
		} else {
			last = dialErr
		}
		if time.Now().After(deadline) {
			_ = ctr.Terminate(context.Background())
			stopLive()
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
	ns, err := client.NewNamespaceClient(client.Options{HostPort: addr})
	if err != nil {
		_ = ctr.Terminate(context.Background())
		stopLive()
		return err
	}
	err = ns.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "default",
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	ns.Close()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		_ = ctr.Terminate(context.Background())
		stopLive()
		return err
	}
	liveCtr = ctr
	liveAddr = addr
	return nil
}

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
	addr := liveHostPort(t)
	c, err := Dial(client.Options{HostPort: addr})
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
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
	liveMu.Lock()
	stopLive()
	liveMu.Unlock()
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

func TestNew_panicsWithoutClientOrCatalog(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	stub := &struct{ client.Client }{}
	snaps := inprocess.NewMemorySnapshot()
	secrets := durable.NewMemorySecretStorage()
	mustPanic(t, func() { New(nil, Config{Catalog: cat, Snapshots: snaps, Secrets: secrets}) })
	mustPanic(t, func() { New(stub, Config{}) })
	mustPanic(t, func() { New(stub, Config{Catalog: cat}) })
	mustPanic(t, func() { New(stub, Config{Catalog: cat, Snapshots: snaps}) })
	mustPanic(t, func() { NewWorker(stub, Config{}) })
	log := inprocess.NewMemoryEventLog()
	rt := New(stub, Config{Catalog: cat, Snapshots: snaps, Secrets: secrets, DisableStreams: true, TurnLocality: time.Minute, Fallback: log})
	if rt.taskQueue != "tacklr" || !rt.disableStreams {
		t.Fatalf("defaults tq=%q streams=%v", rt.taskQueue, rt.disableStreams)
	}
	if rt.activityTimeout != 10*time.Minute || rt.heartbeatTimeout != 30*time.Second || rt.activityAttempts != 3 {
		t.Fatalf("activity defaults timeout=%v heartbeat=%v attempts=%d", rt.activityTimeout, rt.heartbeatTimeout, rt.activityAttempts)
	}
	hour := New(stub, Config{Catalog: cat, Snapshots: snaps, Secrets: secrets, TaskQueue: "q", ActivityTimeout: time.Hour, HeartbeatTimeout: time.Minute, ActivityAttempts: 1})
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

func TestRuntime_promptFailsWhenVaultSealed(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	rt := New(nopWorkflowClient{}, Config{
		Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Secrets: failPutSecrets{}, DisableStreams: true,
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
		Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Secrets: store, DisableStreams: true,
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
