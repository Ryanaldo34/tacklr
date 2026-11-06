package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/telemetry/export"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/workspace"
)

var liveSeq atomic.Int64

type liveStack struct {
	Client    session.Client
	Store     session.Store
	Temporal  client.Client
	TaskQueue string
	Worker    sdkworker.Worker
	Secrets   session.SecretStorage
	// cfg is the worker's Config, so a restart builds the same worker.
	cfg Config
	// live serves the worker's live endpoint when cfg.Live has a key: a
	// restart serves the new worker at a new URL, as a new machine is.
	live *httptest.Server
	// listening closes once a client listens on the running worker's live
	// endpoint, for a model that waits for it.
	listening atomic.Pointer[chan struct{}]
}

// flushSignal is a live response that closes its worker's listening once
// the endpoint flushes: the client listens from then on.
type flushSignal struct {
	http.ResponseWriter
	once      *sync.Once
	listening chan struct{}
}

// FlushError flushes the response (http.ResponseController uses it) and
// reports why it could not.
func (f flushSignal) FlushError() error {
	err := http.NewResponseController(f.ResponseWriter).Flush()
	f.once.Do(func() { close(f.listening) })
	return err
}

func newLiveStack(t *testing.T, agent tacklr.AgentOptions, change ...func(*Config)) *liveStack {
	t.Helper()
	addr := temporaldocker.HostPort(t)
	c, err := temporal.Dial(client.Options{HostPort: addr})
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(c.Close)
	n := liveSeq.Add(1)
	tq := fmt.Sprintf("tacklr-live-%d", n)
	store := session.NewMemoryStore()
	log := session.NewMemoryEventLog()
	secrets := session.NewMemorySecretStorage()
	cfg := Config{Agent: agent, TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets}
	for _, f := range change {
		f(&cfg)
	}
	// A lost worker's steps go elsewhere after two seconds, not ten.
	rt := temporal.Open(c, temporal.Config{TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets, WorkerQueueTimeout: 2 * time.Second, Live: temporal.Live{Key: cfg.Live.Key}})
	s := &liveStack{Client: rt, Store: store, Temporal: c, TaskQueue: tq, Secrets: secrets, cfg: cfg}
	s.startWorker(t)
	return s
}

// startWorker starts a worker on the stack's config and, with a live key,
// serves its live endpoint at a URL of its own.
func (s *liveStack) startWorker(t *testing.T) {
	t.Helper()
	cfg := s.cfg
	var handler atomic.Pointer[http.Handler]
	if len(cfg.Live.Key) > 0 {
		listening, once := make(chan struct{}), new(sync.Once)
		s.listening.Store(&listening)
		s.live = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			(*handler.Load()).ServeHTTP(flushSignal{ResponseWriter: w, once: once, listening: listening}, r)
		}))
		t.Cleanup(s.live.Close)
		cfg.Live.URL = s.live.URL
	}
	w := New(s.Temporal, cfg)
	h := w.LiveHandler()
	handler.Store(&h)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	s.Worker = w
	t.Cleanup(w.Stop)
}

// RestartWorker stops the worker, as a machine that goes away does, and
// starts another.
func (s *liveStack) RestartWorker(t *testing.T) {
	t.Helper()
	s.Worker.Stop()
	if s.live != nil {
		s.live.CloseClientConnections()
		s.live.Close()
	}
	s.startWorker(t)
}

// spans records every span the package's sessions end.
var spans = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	shutdown, err := export.Init(context.Background(), export.Config{})
	if err != nil {
		panic(err)
	}
	// The standard SDK provider: a turn's span starts once, in a side
	// effect, so it needs no replay-safe provider.
	telemetry.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	code := m.Run()
	_ = shutdown(context.Background())
	temporaldocker.Stop()
	os.Exit(code)
}

func liveAgent(t *testing.T, model tacklr.Model, extra tacklr.AgentOptions) tacklr.AgentOptions {
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

func waitTurn(t *testing.T, rt session.Client, id session.ID, sub session.Subscription, timeout time.Duration) []session.Event {
	t.Helper()
	got := waitContains(t, sub, timeout, func(ev session.Event) bool {
		return ev.Type == session.EventComplete || ev.Type == session.EventError || ev.Type == session.EventInterrupt
	})
	testkit.AssertStatusMatchesEvent(t, rt, id, got[len(got)-1])
	return got
}

func waitContains(t *testing.T, sub session.Subscription, timeout time.Duration, want func(session.Event) bool) []session.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var got []session.Event
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

// TestLive_aClientGetsDeltasFromTheWorkerAndTheLogKeepsWholeMessages: a
// client dials the live endpoint of the worker that runs the model call and
// gets the reply's deltas there; the session's stream keeps only the whole
// message. After the worker goes away, the next turn's deltas come from the
// worker that took the session. A request without the session's token is
// refused, and one without a session is not found.
func TestLive_aClientGetsDeltasFromTheWorkerAndTheLogKeepsWholeMessages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var stacks atomic.Pointer[liveStack]
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		// The reply streams once the client listens on this worker.
		select {
		case <-*stacks.Load().listening.Load():
		case <-ctx.Done():
		}
		for _, piece := range []string{"Hel", "lo ", "world"} {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: piece}
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, IsComplete: true}
	})
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{}), func(c *Config) { c.Live.Key = []byte("live-key") })
	stacks.Store(stack)
	id, err := stack.Client.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	turn := func() []session.Event {
		head, err := stack.Client.Head(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		sub, err := stack.Client.Subscribe(ctx, id, head)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Close() }()
		if err := stack.Client.Prompt(ctx, id, session.Prompt{Text: "hi"}); err != nil {
			t.Fatal(err)
		}
		return waitTurn(t, stack.Client, id, sub, 60*time.Second)
	}
	deltas := func(evs []session.Event) string {
		var b strings.Builder
		for _, ev := range evs {
			if ev.Delta {
				b.WriteString(ev.Content)
			}
		}
		return b.String()
	}
	whole := func(evs []session.Event) []string {
		var out []string
		for _, ev := range evs {
			if ev.Type == session.EventMessage && !ev.Delta {
				out = append(out, ev.Content)
			}
		}
		return out
	}

	first := turn()
	stored, err := stack.Client.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	logged := waitTurn(t, stack.Client, id, stored, 10*time.Second)
	_ = stored.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, stack.live.URL+protocol.LivePath+string(id), nil)
	req.Header.Set("Authorization", "Bearer "+protocol.LiveToken([]byte("other-key"), id))
	refused, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = refused.Body.Close()
	unknown, err := http.Get(stack.live.URL + protocol.LivePath)
	if err != nil {
		t.Fatal(err)
	}
	_ = unknown.Body.Close()
	stack.RestartWorker(t)
	second := turn()

	if deltas(first) != "Hello world" || !slices.Equal(whole(first), []string{"Hello world"}) {
		t.Fatalf("first turn %+v; want the deltas live, then the whole message", first)
	}
	if !slices.EqualFunc(logged, slices.DeleteFunc(slices.Clone(first), func(ev session.Event) bool { return ev.Delta }), sameEvent) {
		t.Fatalf("the stream from its start %+v; want the first turn's events without its deltas", logged)
	}
	if refused.StatusCode != http.StatusUnauthorized || unknown.StatusCode != http.StatusNotFound {
		t.Fatalf("a request with another key's token got %d, one with no session %d; want 401 and 404", refused.StatusCode, unknown.StatusCode)
	}
	if deltas(second) != "Hello world" {
		t.Fatalf("turn after the worker went away %+v; want its deltas from the worker that took the session", second)
	}
}

// TestLive_workerRestartWhileParkedThenResumeRemounts: a session parks, its
// worker goes away, and a resume with a new token runs on another worker,
// which mounts the workspace again and reads it. The next prompt goes on
// after one more restart.
func TestLive_workerRestartWhileParkedThenResumeRemounts(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleTool {
			if strings.Contains(last.Content, "from-workspace") {
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: last.Content, IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: session.EventFunctionCall,
				ToolCalls: []message.ToolCall{{
					ID: "read1", CallID: "read1", Name: "read",
					Arguments: `{"path":"/workspace/docs/hello.txt"}`,
				}},
				IsComplete: true,
			}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{{
				ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
				Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
			}},
			IsComplete: true,
		}
	})
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{}), func(cfg *Config) {
		cfg.Workspace = workspace.Config{Open: vfs.Tree(vfs.At("docs", vfs.Local(dir)))}
	})
	id, err := stack.Client.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Client.Prompt(ctx, id, session.Prompt{
		Text: "park",
		Auth: session.AuthContext{Bindings: []mount.Binding{{
			Provider: "local",
			Params:   map[string]string{mount.ParamName: "docs"},
			Auth:     mount.Credential{Token: "tok-1"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Client.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitTurn(t, stack.Client, id, sub, 60*time.Second)
	var yielded bool
	for _, ev := range got {
		if ev.Type == session.EventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield before worker restart, got %+v", got)
	}
	stack.RestartWorker(t)
	payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
	if err := stack.Client.Resume(ctx, id, session.Resume{
		Responses: map[string][]byte{"ask1": payload},
		Auth: session.AuthContext{Bindings: []mount.Binding{{
			Provider: "local",
			Auth:     mount.Credential{Token: "tok-2"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	// The restart waits for the resumed turn to end: the reply's text
	// streams before the turn's last Commit, and a worker stopped during
	// a step leaves it to the heartbeat timeout.
	resumed := waitTurn(t, stack.Client, id, sub, 60*time.Second)
	if !slices.ContainsFunc(resumed, func(ev session.Event) bool {
		return ev.Type == session.EventMessage && strings.Contains(ev.Content, "from-workspace")
	}) {
		t.Fatalf("want the remounted workspace read after the restart, got %+v", resumed)
	}
	// The next prompt's Commit finds the worker holding the harness gone too,
	// and the session goes on on the new one.
	stack.RestartWorker(t)
	if err := stack.Client.Prompt(ctx, id, session.Prompt{Text: "again"}); err != nil {
		t.Fatal(err)
	}
	waitContains(t, sub, 60*time.Second, func(ev session.Event) bool {
		return ev.Type == session.EventInterrupt
	})
}

// TestLive_fuseRootGivesTheAgentAShellOnItsWorkspace: with a FuseRoot the
// worker mounts the session's workspace on the host and the agent gets
// run_command there. An empty command gets a correction; a command reads
// the workspace.
func TestLive_fuseRootGivesTheAgentAShellOnItsWorkspace(t *testing.T) {
	t.Parallel()
	// Arrange
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-shell"), 0o644); err != nil {
		t.Fatal(err)
	}
	runCommand := func(id, command string) tacklr.LLMResponseChunk {
		return tacklr.LLMResponseChunk{
			Type:       session.EventFunctionCall,
			ToolCalls:  []message.ToolCall{{ID: id, CallID: id, Name: "run_command", Arguments: fmt.Sprintf(`{"command":%q}`, command)}},
			IsComplete: true,
		}
	}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		switch {
		case last.Role != message.RoleTool:
			ch <- runCommand("empty", "  ")
		case strings.Contains(last.Content, "command is required"):
			ch <- runCommand("cat", "cat workspace/docs/hello.txt")
		default:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: last.Content, IsComplete: true}
		}
	})
	agent := liveAgent(t, model, tacklr.AgentOptions{AllowTools: []string{"run_command"}})
	stack := newLiveStack(t, agent, func(cfg *Config) {
		cfg.Workspace = workspace.Config{Open: vfs.Tree(vfs.At("docs", vfs.Local(dir)))}
		cfg.FuseRoot = t.TempDir()
	})
	id, err := stack.Client.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Client.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	// Act
	if err := stack.Client.Prompt(ctx, id, session.Prompt{Text: "look"}); err != nil {
		t.Fatal(err)
	}
	got := waitTurn(t, stack.Client, id, sub, 60*time.Second)

	// Assert
	if !slices.ContainsFunc(got, func(ev session.Event) bool {
		return ev.Type == session.EventMessage && strings.Contains(ev.Content, "exit=0") && strings.Contains(ev.Content, "from-shell")
	}) {
		t.Fatalf("want the shell's read of the workspace, got %+v", got)
	}
}

// TestLive_historyHoldsNoSecretsOrToolResults: a session whose prompt binds a
// VFS token and an MCP header, and whose reply makes four calls with 100 KB
// results, keeps neither the secrets nor the results in history, which
// stays smaller than one result: each result is in the session.Store once. Subscribe still delivers every
// result whole, and the model's next request has them all. Once the
// session's records are deleted, a replay still delivers each result
// event, without the content that went with them.
func TestLive_historyHoldsNoSecretsOrToolResults(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	token := "tok-history-secret-7f3a"
	header := "Bearer mcp-secret-9c1e"
	result := func(n int) string { return fmt.Sprintf("result-%d-", n) + strings.Repeat("x", 100<<10) }
	var seen []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if got := toolResults(msgs); len(got) > 0 {
			seen = got
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok", IsComplete: true}
			return
		}
		var calls []message.ToolCall
		for n := range 4 {
			id := fmt.Sprintf("big%d", n)
			calls = append(calls, message.ToolCall{ID: id, CallID: id, Name: "big", Arguments: fmt.Sprintf(`{"n":%d}`, n)})
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: calls, IsComplete: true}
	})
	big := tacklr.NewTool(tacklr.ToolConfig{Name: "big", Handler: func(_ context.Context, args struct {
		N int `json:"n"`
	}) (string, error) {
		return result(args.N), nil
	}})
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{Tools: []*tacklr.Tool{big}, MaxWindowSize: 1 << 20, MCP: mcp.Client{}}))
	id, err := stack.Client.CreateSession(ctx, session.CreateSession{
		MCPServers: []session.MCPServer{{
			Name: "remote", Type: session.MCPHTTP, URL: "https://example.test/mcp",
			Headers: []session.MCPHeader{{Name: "Authorization", Value: header}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Client.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	if err := stack.Client.Prompt(ctx, id, session.Prompt{
		Text: "hi",
		Auth: session.AuthContext{Bindings: []mount.Binding{{
			Provider: "local",
			Params:   map[string]string{mount.ParamName: "docs"},
			Auth:     mount.Credential{Token: token},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	events := waitTurn(t, stack.Client, id, sub, 60*time.Second)

	var streamed []string
	for _, ev := range events {
		if ev.Type == session.EventToolResult {
			streamed = append(streamed, ev.Content)
		}
	}
	size, count := 0, 0
	iter := stack.Temporal.GetWorkflowHistory(ctx, string(id), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		blob := ev.String()
		if strings.Contains(blob, token) || strings.Contains(blob, header) || strings.Contains(blob, "result-0-") {
			t.Fatalf("a secret or a tool result in history event %s", ev.GetEventType())
		}
		size, count = size+proto.Size(ev), count+1
	}
	t.Logf("history: %d events, %d bytes, for a reply of 4 calls with %d bytes of results", count, size, 4*len(result(0)))
	slices.Sort(streamed)
	want := []string{result(0), result(1), result(2), result(3)}
	if !slices.Equal(streamed, want) || !slices.Equal(seen, want) {
		t.Fatalf("streamed %d results and the model saw %d, want all four whole", len(streamed), len(seen))
	}
	if size > len(result(0)) {
		t.Fatalf("history is %d bytes, want it under one result (%d): results are not in it", size, len(result(0)))
	}

	if err := stack.Store.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	replay, err := stack.Client.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replay.Close() })
	var gone []string
	for ev := range replay.Events() {
		if ev.Type == session.EventToolResult {
			gone = append(gone, ev.Content)
		}
		if ev.Type == session.EventComplete {
			break
		}
	}
	if !slices.Equal(gone, []string{"", "", "", ""}) {
		t.Fatalf("replayed results %q, want four events without their deleted content", gone)
	}

	// A store whose database is gone ends the subscription with that error.
	pool, err := pgxpool.New(ctx, "postgres://tacklr@127.0.0.1:1/tacklr")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	db, err := postgres.New(pool, postgres.Options{})
	if err != nil {
		t.Fatal(err)
	}
	down := temporal.Open(stack.Temporal, temporal.Config{TaskQueue: stack.TaskQueue, Store: db.Store(), Secrets: stack.Secrets})
	broken, err := down.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broken.Close() })
	for range broken.Events() {
	}
	if broken.Err() == nil || !strings.Contains(broken.Err().Error(), "closed pool") {
		t.Fatalf("subscription error = %v, want the closed pool's", broken.Err())
	}
}

// TestLive_theHostAPIServesSessionsAndTheirJobs: CreateSession rejects a
// specialist and a worker together, an empty state key, and state JSON
// cannot hold; Prompt and Resume reject such state too. A session's head
// follows its stream, and Jobs lists its running specialist and worker
// with their kinds. An unknown session has no status or jobs. Close
// deletes the session's secrets and record, and after it the session has
// no status or jobs and takes no prompt.
func TestLive_theHostAPIServesSessionsAndTheirJobs(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	release, let := releaseLater(t)
	// running has a value for each job that runs and waits for release.
	running := make(chan struct{}, 2)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		switch {
		case last != nil && last.Content == "child-task":
			running <- struct{}{}
			<-release
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "child-done", IsComplete: true}
		case last != nil && last.Role == message.RoleUser && last.Content == "go":
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{
				{ID: "sp1", CallID: "sp1", Name: "spawn_specialist", Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`},
				{ID: "ci1", CallID: "ci1", Name: "start", Arguments: `{}`},
			}}
		default:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "waiting", IsComplete: true}
		}
	})
	start := tacklr.NewTool(tacklr.ToolConfig{Name: "start", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "build"})
		return job.ID, err
	}})
	agent := liveAgent(t, model, tacklr.AgentOptions{Tools: []*tacklr.Tool{start}, Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}})
	stack := newLiveStack(t, agent, func(c *Config) {
		c.Jobs = map[string]session.JobHandler{"ci": func(context.Context, string) (string, error) {
			running <- struct{}{}
			<-release
			return "built", nil
		}}
	})
	t.Cleanup(let) // before the stack stops its worker
	rt := stack.Client
	bad := map[string]any{"c": make(chan int)}
	_, both := rt.CreateSession(ctx, session.CreateSession{Specialist: "researcher", Worker: "ci"})
	_, emptyKey := rt.CreateSession(ctx, session.CreateSession{State: map[string]any{"": 1}})
	_, unencodable := rt.CreateSession(ctx, session.CreateSession{State: bad})
	id, err := rt.CreateSession(ctx, session.CreateSession{State: map[string]any{"mode": "fast"}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	if err := rt.Prompt(ctx, id, session.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	// The turn waits on both jobs; it ends only once they finish.
	for range 2 {
		select {
		case <-running:
		case <-time.After(60 * time.Second):
			t.Fatal("the session's jobs did not start")
		}
	}
	jobs, jobsErr := rt.Jobs(ctx, id)
	head, headErr := rt.Head(ctx, id)
	badPrompt := rt.Prompt(ctx, id, session.Prompt{Text: "x", State: bad})
	badResume := rt.Resume(ctx, id, session.Resume{State: bad})
	_, noJobs := rt.Jobs(ctx, "no-such-session")
	_, noStatus := rt.Status(ctx, "no-such-session")
	let()
	if err := rt.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	_, closed := rt.Status(ctx, id)
	_, closedJobs := rt.Jobs(ctx, id)
	closedPrompt := rt.Prompt(ctx, id, session.Prompt{Text: "again"})
	kept, keptErr := stack.Secrets.Get(ctx, id)
	_, _, record := stack.Store.Load(ctx, id)

	for name, err := range map[string]error{"specialist and worker": both, "an empty state key": emptyKey, "unencodable state": unencodable, "a prompt's unencodable state": badPrompt, "a resume's unencodable state": badResume} {
		if !errors.Is(err, session.ErrInvalid) {
			t.Errorf("%s = %v, want ErrInvalid", name, err)
		}
	}
	kinds := map[string]bool{}
	for _, j := range jobs {
		kinds[j.Kind] = true
	}
	if headErr != nil || head == 0 || jobsErr != nil || !kinds[session.KindSpecialist] || !kinds[session.KindWorker] {
		t.Fatalf("head %d %v, jobs %+v %v; want the stream's offset and both running jobs with their kinds", head, headErr, jobs, jobsErr)
	}
	for name, err := range map[string]error{"jobs of an unknown session": noJobs, "status of an unknown session": noStatus,
		"status of a closed session": closed, "jobs of a closed session": closedJobs, "a prompt to a closed session": closedPrompt, "the record of a closed session": record} {
		if !errors.Is(err, session.ErrNotFound) {
			t.Errorf("%s = %v, want ErrNotFound", name, err)
		}
	}
	if keptErr != nil || len(kept.Auth.Bindings) != 0 {
		t.Errorf("secrets of a closed session = %+v %v, want an empty bag", kept, keptErr)
	}
}

// unreachable is a real Temporal client for a server that is not there.
func unreachable(t *testing.T) client.Client {
	t.Helper()
	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestNew_needsAClientAStoreAndSecrets: New panics without any of them, and
// with a live URL but no key to sign its tokens.
func TestNew_needsAClientAStoreAndSecrets(t *testing.T) {
	t.Parallel()
	// Arrange
	c, store, secrets := unreachable(t), session.NewMemoryStore(), session.NewMemorySecretStorage()
	for name, tc := range map[string]struct {
		c   client.Client
		cfg Config
	}{
		"no client":   {nil, Config{Store: store, Secrets: secrets}},
		"no store":    {c, Config{Secrets: secrets}},
		"no secrets":  {c, Config{Store: store}},
		"no live key": {c, Config{Store: store, Secrets: secrets, Live: Live{URL: "http://worker.test"}}},
	} {
		// Act and assert
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mustPanic(t, func() { New(tc.c, tc.cfg) })
		})
	}
}

// TestWorker_aWorkerForAnUnreachableServerFailsToStart: Start and Run
// report that the server is not there, also for a worker with streams off
// and its own fallback log.
func TestWorker_aWorkerForAnUnreachableServerFailsToStart(t *testing.T) {
	t.Parallel()
	// Arrange
	w := New(unreachable(t), Config{
		Agent: tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}, TaskQueue: "unreachable",
		Store: session.NewMemoryStore(), Secrets: session.NewMemorySecretStorage(), DisableStreams: true,
	})

	// Act
	startErr := w.Start()
	runErr := w.Run(nil)

	// Assert
	if startErr == nil || runErr == nil {
		t.Fatalf("Start = %v, Run = %v; want both to fail", startErr, runErr)
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

func lastMsg(msgs []*message.Message) *message.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil {
			return msgs[i]
		}
	}
	return nil
}

func drainLog(t *testing.T, log session.EventLog, id session.ID) []session.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ch, err := log.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []session.Event
	for ev := range ch {
		got = append(got, ev)
	}
	return got
}

func querySession(t *testing.T, env *testsuite.TestWorkflowEnvironment) session.Status {
	t.Helper()
	val, err := env.QueryWorkflow(protocol.QueryStatus)
	if err != nil {
		t.Fatal(err)
	}
	var st session.Status
	if err := val.Get(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

// sameEvent reports whether two events have the same type, content and
// delta flag.
func sameEvent(a, b session.Event) bool {
	return a.Type == b.Type && a.Content == b.Content && a.Delta == b.Delta
}

func newActs(agent tacklr.AgentOptions, log session.EventLog, disableStreams bool) *activities {
	return &activities{
		Agent:          agent,
		Store:          session.NewMemoryStore(),
		Fallback:       log,
		DisableStreams: disableStreams,
		Secrets:        session.NewMemorySecretStorage(),
		harnesses:      newHarnessCache(time.Minute, 0),
	}
}
