package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"

	enumspb "go.temporal.io/api/enums/v1"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
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
}

func newLiveStack(t *testing.T, agent tacklr.AgentOptions, change ...func(*Config)) *liveStack {
	t.Helper()
	addr := temporaldocker.HostPort(t)
	// As a host does: Temporal's tracing plugin on the client; the worker
	// built from it gets it too.
	plugin, err := temporalotel.NewPlugin(temporalotel.PluginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := temporal.Dial(client.Options{HostPort: addr, Plugins: []client.Plugin{plugin}})
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(c.Close)
	n := liveSeq.Add(1)
	tq := fmt.Sprintf("tacklr-live-%d", n)
	store := session.NewMemoryStore()
	log := session.NewMemoryEventLog()
	secrets := session.NewMemorySecretStorage()
	// A lost worker's steps go elsewhere after two seconds, not ten.
	rt := temporal.Open(c, temporal.Config{TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets, WorkerQueueTimeout: 2 * time.Second})
	cfg := Config{Agent: agent, TaskQueue: tq, Store: store, Fallback: log, Secrets: secrets}
	for _, f := range change {
		f(&cfg)
	}
	w := New(c, cfg)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return &liveStack{Client: rt, Store: store, Temporal: c, TaskQueue: tq, Worker: w, Secrets: secrets, cfg: cfg}
}

func (s *liveStack) RestartWorker(t *testing.T) {
	t.Helper()
	s.Worker.Stop()
	w := New(s.Temporal, s.cfg)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	s.Worker = w
	t.Cleanup(w.Stop)
}

// spans records every span the package's sessions end.
var spans = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	shutdown, err := telemetry.Init(context.Background(), telemetry.Config{})
	if err != nil {
		panic(err)
	}
	// As a host does: Temporal tracing needs its replay-safe tracer provider.
	if err := telemetry.ReinstallTracer(context.Background(), func(opts ...sdktrace.TracerProviderOption) (trace.TracerProvider, func(context.Context) error) {
		tp := temporalotel.NewReplaySafeTracerProvider(append(opts, sdktrace.WithSpanProcessor(spans))...)
		return tp, tp.Shutdown
	}); err != nil {
		panic(err)
	}
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

func TestLive_workerRestartWhileParkedThenResumeRemounts(t *testing.T) {
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
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(dir)))}))
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

// TestLive_historyHoldsNoSecretsOrToolResults: a session whose prompt binds a
// VFS token and an MCP header, and whose reply makes four calls with 100 KB
// results, keeps neither the secrets nor the results in history, which
// stays smaller than one result: each result is in the session.Store once. Subscribe still delivers every
// result whole, and the model's next request has them all. Once the
// session's records are deleted, a replay still delivers each result
// event, without the content that went with them.
func TestLive_historyHoldsNoSecretsOrToolResults(t *testing.T) {
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
	stack := newLiveStack(t, liveAgent(t, model, tacklr.AgentOptions{Tools: []*tacklr.Tool{big}, MaxWindowSize: 1 << 20}))
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

	// A store that cannot read the records ends the subscription with
	// that error.
	down := temporal.Open(stack.Temporal, temporal.Config{TaskQueue: stack.TaskQueue, Store: downStore{stack.Store}, Secrets: stack.Secrets})
	broken, err := down.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broken.Close() })
	for range broken.Events() {
	}
	if !errors.Is(broken.Err(), errStoreDown) {
		t.Fatalf("subscription error = %v, want %v", broken.Err(), errStoreDown)
	}
}

// downStore is a session.Store that cannot read call outcomes.
type downStore struct{ session.Store }

var errStoreDown = errors.New("session store down")

func (downStore) Outcomes(context.Context, session.ID, []string) (map[string]session.Outcome, error) {
	return nil, errStoreDown
}

// TestLive_theHostAPIServesSessionsAndTheirJobs: CreateSession rejects a
// specialist and a worker together, an empty state key, and state JSON
// cannot hold; Prompt and Resume reject such state too. A
// session's head follows its stream, and Jobs lists its running specialist
// and worker with their kinds. An unknown session, or one this runtime
// closed, has no status.
func TestLive_theHostAPIServesSessionsAndTheirJobs(t *testing.T) {
	ctx := t.Context()
	release, let := releaseLater(t)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		switch {
		case last != nil && last.Content == "child-task":
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
		c.Jobs = map[string]JobHandler{"ci": func(context.Context, string) (string, error) { <-release; return "built", nil }}
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
	var jobs []session.Status
	var jobsErr error
	for deadline := time.Now().Add(60 * time.Second); len(jobs) < 2 && jobsErr == nil && time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		jobs, jobsErr = rt.Jobs(ctx, id)
	}
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
	for name, err := range map[string]error{"jobs of an unknown session": noJobs, "status of an unknown session": noStatus, "status of a closed session": closed} {
		if !errors.Is(err, session.ErrNotFound) {
			t.Errorf("%s = %v, want ErrNotFound", name, err)
		}
	}
}

// TestNew_needsAClientAStoreAndSecrets: New panics without any of them.
func TestNew_needsAClientAStoreAndSecrets(t *testing.T) {
	store, secrets := session.NewMemoryStore(), session.NewMemorySecretStorage()
	stub := &struct{ client.Client }{}
	for name, tc := range map[string]struct {
		c   client.Client
		cfg Config
	}{
		"no client":  {nil, Config{Store: store, Secrets: secrets}},
		"no store":   {stub, Config{Secrets: secrets}},
		"no secrets": {stub, Config{Store: store}},
	} {
		t.Run(name, func(t *testing.T) { mustPanic(t, func() { New(tc.c, tc.cfg) }) })
	}
}

// TestWorker_aWorkerForAnUnreachableServerFailsToStart: Start and Run
// report that the server is not there.
func TestWorker_aWorkerForAnUnreachableServerFailsToStart(t *testing.T) {
	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	w := New(c, Config{
		Agent: tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}, TaskQueue: "unreachable",
		Store: session.NewMemoryStore(), Secrets: session.NewMemorySecretStorage(),
	})
	startErr := w.Start()
	runErr := w.Run(nil)
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

type retryLog struct {
	session.EventLog
	retry []session.Event
}

func (l *retryLog) Append(ctx context.Context, id session.ID, topic string, ev session.Event) error {
	if topic == session.TopicRetry {
		l.retry = append(l.retry, ev)
	}
	return l.EventLog.Append(ctx, id, topic, ev)
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
