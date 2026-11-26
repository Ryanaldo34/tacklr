package durableobject_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// fixture is a platform and a client over it.
type fixture struct {
	p *dotest.Platform
	c *durableobject.Client
}

func newFixture(t *testing.T, answer testkit.Reply, tools []*tacklr.Tool, configure ...func(*durableobject.Config)) fixture {
	t.Helper()
	return newFixtureOn(t, dotest.New, answer, tools, configure...)
}

// newWireFixture is newFixture on a platform where every call to an object
// crosses the object protocol, as on Cloudflare.
func newWireFixture(t *testing.T, answer testkit.Reply, tools []*tacklr.Tool, configure ...func(*durableobject.Config)) fixture {
	t.Helper()
	return newFixtureOn(t, dotest.NewWire, answer, tools, configure...)
}

func newFixtureOn(t *testing.T, platform func(durableobject.Config) *dotest.Platform, answer testkit.Reply, tools []*tacklr.Tool, configure ...func(*durableobject.Config)) fixture {
	t.Helper()
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	cfg := durableobject.Config{
		Agent:        tacklr.AgentOptions{Model: testkit.ScriptedModel(t, answer), MaxWindowSize: 8192, Tools: tools},
		Sealer:       sealer,
		RetryBackoff: time.Millisecond,
	}
	for _, f := range configure {
		f(&cfg)
	}
	p := platform(cfg)
	return fixture{p: p, c: durableobject.NewClient(p)}
}

// always fails every write.
func always([]durableobject.Op) bool { return true }

// subscribe tails the session from its head.
func (f fixture) subscribe(t *testing.T, id session.ID) session.Subscription {
	t.Helper()
	head, err := f.c.Head(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := f.c.Subscribe(t.Context(), id, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

// untilEnd reads sub until the turn completes, parks or fails, and returns
// every event it read.
func untilEnd(t *testing.T, sub session.Subscription) []session.Event {
	t.Helper()
	var got []session.Event
	timeout := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed (%v) after %+v", sub.Err(), got)
			}
			got = append(got, ev)
			switch ev.Type {
			case session.EventComplete, session.EventInterrupt, session.EventError:
				return got
			}
		case <-timeout:
			t.Fatalf("no end of turn; events so far %+v", got)
		}
	}
}

func last(evs []session.Event) session.Event { return evs[len(evs)-1] }

func toolResult(evs []session.Event) string {
	for _, ev := range evs {
		if ev.Type == session.EventToolResult {
			return ev.Content
		}
	}
	return ""
}

// types are the types of the durable events of evs, in order.
func types(evs []session.Event) []session.EventType {
	var out []session.EventType
	for _, ev := range evs {
		if !ev.Delta {
			out = append(out, ev.Type)
		}
	}
	return out
}

// TestSession_anObjectStoppedMidToolReplaysAndFinishesTheTurn: the object
// stops while a tool runs. Its watchdog alarm restarts it; the new object
// replays the journal, so the reply the model already gave is not asked
// for again, the unfinished tool call runs again, and the turn ends with
// each event once.
func TestSession_anObjectStoppedMidToolReplaysAndFinishesTheTurn(t *testing.T) {
	t.Parallel()
	// Arrange
	var runs atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	slow := tacklr.NewTool(tacklr.ToolConfig{Name: "slow", Handler: func(ctx context.Context) (string, error) {
		if runs.Add(1) == 1 {
			close(started)
			<-release
		}
		return "finished", nil
	}})
	f := newFixture(t, testkit.CallThenAnswer("slow", `{}`), []*tacklr.Tool{slow}, func(cfg *durableobject.Config) { cfg.Watchdog = 20 * time.Millisecond })
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	<-started
	first := f.p.Alarm(id)
	armed, err := f.p.WaitAlarm(ctx, id, first)
	if err != nil {
		t.Fatal(err)
	}

	// Act
	f.p.Evict(id)
	close(release) // the stopped object's tool returns into storage it can no longer write
	if err := f.p.FireAlarm(ctx, id); err != nil {
		t.Fatal(err)
	}
	sub, err := f.c.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	evs := untilEnd(t, sub)

	// Assert
	if first.IsZero() || armed.IsZero() {
		t.Fatalf("watchdog alarm %v, then %v; want it set while the tool ran, and set again while it kept running", first, armed)
	}
	want := []session.EventType{session.EventFunctionCall, session.EventToolResult, session.EventMessage, session.EventComplete}
	if !slices.Equal(types(evs), want) || toolResult(evs) != "finished" || runs.Load() != 2 {
		t.Fatalf("events %v, tool runs %d; want each event once, the tool run again and the turn ended", types(evs), runs.Load())
	}
}

// TestSession_aParkedTurnResumesAfterItsObjectStopped: a tool asks the user
// and the turn parks. The object stops; the next one reports the turn
// waiting from storage. The answer restarts it, the parked call returns
// the answer, and the turn ends with it.
func TestSession_aParkedTurnResumesAfterItsObjectStopped(t *testing.T) {
	t.Parallel()
	// Arrange
	confirm := tacklr.NewTool(tacklr.ToolConfig{Name: "confirm", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		answer, err := cc.Park(&tacklr.UserSelectionInterrupt{Question: "Delete?", Options: []tacklr.UserChoice{{Title: "Delete"}, {Title: "Keep"}}})
		if err != nil {
			return "", err
		}
		return "chose " + answer.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title, nil
	}})
	f := newWireFixture(t, testkit.CallThenAnswer("confirm", `{}`), []*tacklr.Tool{confirm})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	sub := f.subscribe(t, id)
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "clean up"}); err != nil {
		t.Fatal(err)
	}
	parked := last(untilEnd(t, sub))

	// Act
	lost := sub
	f.p.Evict(id)
	for range lost.Events() {
	}
	waiting, _ := f.c.Status(ctx, id)
	idle := f.p.Idle(ctx, id)
	sub = f.subscribe(t, id)
	err = f.c.Resume(ctx, id, session.Resume{Responses: map[string][]byte{parked.MessageID: []byte(`{"selectionIdx":0}`)}, Auth: driveAuth("refreshed")})
	evs := untilEnd(t, sub)

	// Assert
	if parked.Type != session.EventInterrupt || !waiting.Waiting || idle != nil {
		t.Fatalf("parked %+v, status %+v %v; want the turn waiting on the question, answered from storage", parked, waiting, idle)
	}
	if lost.Err() == nil {
		t.Fatal("the stream of the stopped object ended without an error; want it broken")
	}
	if err != nil || last(evs).Type != session.EventComplete || toolResult(evs) != "chose Delete" {
		t.Fatalf("resume %v, events %+v; want the answer as the call's result and the turn ended", err, evs)
	}
}

// TestSession_aJobRunsInItsOwnObjectWhichTheSessionCloses: a job runs as a
// child session in its own object, and once the session has the job's
// result it closes the job's object. A job that still runs when the
// session is cancelled and closed is closed with it.
func TestSession_aJobRunsInItsOwnObjectWhichTheSessionCloses(t *testing.T) {
	t.Parallel()
	// Arrange
	schedule := func(name string) *tacklr.Tool {
		return tacklr.NewTool(tacklr.ToolConfig{Name: name, Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
			_, err := cc.Schedule(ctx, tacklr.JobRequest{Name: name, Task: "q3", Wait: true})
			return "", err
		}})
	}
	reply := func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if last := testkit.LastMessage(msgs); last.Role == message.RoleTool {
			return testkit.Text("done: " + last.Content)
		}
		name := testkit.LastMessage(msgs).Content
		return testkit.Calls(testkit.Call(name, name, `{}`))
	}
	started := make(chan struct{})
	f := newFixture(t, reply, []*tacklr.Tool{schedule("export"), schedule("hold")}, func(cfg *durableobject.Config) {
		cfg.Jobs = map[string]session.JobHandler{
			"export": func(_ context.Context, task string) (string, error) { return "exported " + task, nil },
			"hold": func(ctx context.Context, _ string) (string, error) {
				close(started)
				<-ctx.Done()
				return "", ctx.Err()
			},
		}
	})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	export, hold := session.ID(tacklr.ChildID(string(id), "export", "export", false)), session.ID(tacklr.ChildID(string(id), "hold", "hold", false))
	idle := func(ids ...session.ID) {
		t.Helper()
		for _, obj := range ids {
			if err := f.p.Idle(ctx, obj); err != nil {
				t.Fatal(err)
			}
		}
	}
	sub := f.subscribe(t, id)

	// Act
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "export"}); err != nil {
		t.Fatal(err)
	}
	evs := untilEnd(t, sub)
	idle(id, export, id)
	_, done := f.c.Status(ctx, export)
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "hold"}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := f.c.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	idle(id, hold)
	_, closed := f.c.Status(ctx, hold)

	// Assert
	if last(evs).Type != session.EventComplete || toolResult(evs) != "exported q3" {
		t.Fatalf("events %+v; want the job's output as the call's result", evs)
	}
	if !errors.Is(done, session.ErrNotFound) || !errors.Is(closed, session.ErrNotFound) {
		t.Fatalf("the job once its result was taken: %v; the job of the closed session: %v; want both objects closed", done, closed)
	}
}

// driveAuth is a prompt's credentials for one Drive folder.
func driveAuth(token string) session.AuthContext {
	return session.AuthContext{Bindings: []mount.Binding{{
		Provider: mount.ProviderGoogleDrive,
		Params:   map[string]string{mount.ParamName: "docs"},
		Auth:     mount.Credential{Token: token},
	}}}
}

// TestSession_aModelRequestThatFailsOnTheNetworkRunsAgain: a model request
// that fails on the network runs again, and the turn ends with the answer.
func TestSession_aModelRequestThatFailsOnTheNetworkRunsAgain(t *testing.T) {
	t.Parallel()
	// Arrange
	var flaky atomic.Int32
	f := newFixture(t, func([]*message.Message) tacklr.LLMResponseChunk {
		if flaky.Add(1) == 1 {
			return testkit.Fail(tacklr.Network(errors.New("connection reset")))
		}
		return testkit.Text("back")
	}, nil)
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	sub := f.subscribe(t, id)

	// Act
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "flaky"}); err != nil {
		t.Fatal(err)
	}
	end := last(untilEnd(t, sub))
	st, stErr := f.c.Status(ctx, id)

	// Assert
	if end.Type != session.EventComplete || stErr != nil || st.Result != "back" {
		t.Fatalf("after a network failure %+v, status %+v %v; want the answer of the request run again", end, st, stErr)
	}
}

// TestSession_aPromptWithMCPServersOnADeploymentWithoutAConnectorFailsItsTurn:
// a session's harness is built again for a prompt that brings MCP servers,
// and on a deployment without an MCP connector that fails the prompt's
// turn.
func TestSession_aPromptWithMCPServersOnADeploymentWithoutAConnectorFailsItsTurn(t *testing.T) {
	t.Parallel()
	// Arrange
	f := newWireFixture(t, func([]*message.Message) tacklr.LLMResponseChunk { return testkit.Text("hi") }, nil)
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	end := func(p session.Prompt) session.Event {
		t.Helper()
		sub := f.subscribe(t, id)
		if err := f.c.Prompt(ctx, id, p); err != nil {
			t.Fatal(err)
		}
		return last(untilEnd(t, sub))
	}

	// Act
	first := end(session.Prompt{Text: "hello"})
	withMCP := end(session.Prompt{Text: "check", MCPServers: []session.MCPServer{{Name: "tools", Type: session.MCPHTTP, URL: "http://127.0.0.1:1/mcp"}}})

	// Assert
	if first.Type != session.EventComplete || withMCP.Type != session.EventError || !strings.Contains(withMCP.Fail, "MCP") {
		t.Fatalf("first turn %+v, the turn with MCP servers %+v; want the first answered and the second failed naming MCP", first, withMCP)
	}
}

// TestObject_refusesAnInitWithoutAnIDAndNeedsItsParts: an object refuses a
// session without an id, and the constructors refuse what they cannot run
// without.
func TestObject_refusesAnInitWithoutAnIDAndNeedsItsParts(t *testing.T) {
	t.Parallel()
	// Arrange
	f := newFixture(t, testkit.CallThenAnswer("none", `{}`), nil)

	// Act
	noID := f.p.Object("blank").Create(t.Context(), durableobject.Init{})

	// Assert
	if !errors.Is(noID, session.ErrInvalid) {
		t.Fatalf("no id %v; want invalid", noID)
	}
	for name, build := range map[string]func(){
		"object without a sealer":   func() { durableobject.NewObject(nil, durableobject.Config{}) },
		"client without a platform": func() { durableobject.NewClient(nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s did not panic", name)
				}
			}()
			build()
		}()
	}
}

// TestSession_aStorageOutageAtAnyOperationOfATurnOrACloseHealsAtTheAlarm:
// a cold object's storage, or its job's, fails one of its operations once:
// any read or write of a turn whose reply calls a tool and a job (started
// with the prompt's credentials) and of the session's close. The request that met
// the outage fails with it and is sent again, and a run that stopped on it
// goes on at the alarm, in a new object over the same storage; a letter to
// the job's object that did not land is sent again. Each time, the turn ends with each event once in
// the log, the tool's effect applied once (it may run again, by its call
// id), the job's output as its call's result, and the close finishes with
// the session's messages forgotten. A subscriber that listened meanwhile
// got the turn or the outage's error.
func TestSession_aStorageOutageAtAnyOperationOfATurnOrACloseHealsAtTheAlarm(t *testing.T) {
	t.Parallel()
	ops := outageRun(t, storageOp{})
	if len(ops) < 50 {
		t.Fatalf("the turn and close made %d storage operations; want them all counted", len(ops))
	}
	for i, op := range ops {
		t.Run(fmt.Sprint(i+1), func(t *testing.T) {
			t.Parallel()
			outageRun(t, op)
		})
	}
}

// storageOp is the nth read or write of one key set, so a run fails the
// same operation however its objects' goroutines interleave.
type storageOp struct {
	what string
	nth  int
}

// outageRun runs the session of the outage test, failing the storage
// operation fail once (none when it is zero), checks its outcomes, and
// returns the operations it made.
func outageRun(t *testing.T, fail storageOp) []storageOp {
	t.Helper()
	// Arrange
	var mu sync.Mutex
	effects := map[string]int{}
	var forgot []string
	lookup := tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		effects[cc.CurrentToolCallID()] = 1
		return "42", nil
	}})
	export := tacklr.NewTool(tacklr.ToolConfig{Name: "export", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		_, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "export", Task: "q3", Wait: true})
		return "", err
	}})
	reply := func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if testkit.LastMessage(msgs).Content == "warm up" {
			return testkit.Text("ready")
		}
		if results := testkit.ToolResults(msgs); len(results) > 0 {
			return testkit.Text("done: " + strings.Join(results, ", "))
		}
		return testkit.Calls(testkit.Call("l1", "lookup", `{}`), testkit.Call("e1", "export", `{}`))
	}
	f := newWireFixture(t, reply, []*tacklr.Tool{lookup, export}, func(cfg *durableobject.Config) {
		eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(func(_ context.Context, ev brain.AuditEvent) error {
			if ev.Op == brain.AuditSessionDelete {
				mu.Lock()
				forgot = append(forgot, ev.SessionID)
				mu.Unlock()
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
		cfg.Agent.Brain, cfg.Agent.SearchNamespace = eng, ns
		cfg.Jobs = map[string]session.JobHandler{"export": func(_ context.Context, task string) (string, error) { return "exported " + task, nil }}
	})
	ctx := t.Context()
	const id = session.ID("s")
	job := session.ID(tacklr.ChildID(string(id), "export", "e1", false))
	if _, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: id}); err != nil {
		t.Fatal(err)
	}
	warm := f.subscribe(t, id)
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "warm up"}); err != nil {
		t.Fatal(err)
	}
	untilEnd(t, warm)
	if err := f.p.Idle(ctx, id); err != nil {
		t.Fatal(err)
	}
	f.p.Evict(id)
	seen := map[string]int{}
	var ops []storageOp
	op := func(w string) bool {
		mu.Lock()
		defer mu.Unlock()
		seen[w]++
		ops = append(ops, storageOp{w, seen[w]})
		return ops[len(ops)-1] == fail
	}
	for _, obj := range []session.ID{id, job} {
		f.p.FailReads(obj, func(k string) bool { return op("read " + k) })
		f.p.FailWrites(obj, func(ops []durableobject.Op) bool {
			var keys []string
			for _, o := range ops {
				keys = append(keys, o.Key)
			}
			return op(fmt.Sprint("write ", keys))
		})
	}
	// retry sends a request until it is not the outage's.
	retry := func(request func() error) {
		t.Helper()
		for range 3 {
			if err := request(); err == nil || !strings.Contains(err.Error(), "storage unavailable") {
				return
			}
		}
		t.Fatal("a request kept failing with the outage")
	}
	// heal fires the alarm until done holds.
	heal := func(done func() bool) {
		t.Helper()
		for range 5 {
			for _, obj := range []session.ID{id, job, id} {
				if err := f.p.Idle(ctx, obj); err != nil {
					t.Fatal(err)
				}
			}
			if done() {
				return
			}
			for _, obj := range []session.ID{id, job} {
				f.p.Evict(obj)
				_ = f.p.FireAlarm(ctx, obj)
			}
		}
		st, err := f.c.Status(ctx, id)
		t.Fatalf("the session did not heal at the alarm after failing %v: status %+v %v", fail, st, err)
	}
	var live session.Subscription
	retry(func() error {
		head, err := f.c.Head(ctx, id)
		if err != nil {
			return err
		}
		live, err = f.c.Subscribe(ctx, id, head)
		return err
	})
	t.Cleanup(func() { _ = live.Close() })

	// Act
	retry(func() error { return f.c.Prompt(ctx, id, session.Prompt{Text: "go", Auth: driveAuth("tok")}) })
	heal(func() bool {
		st, err := f.c.Status(ctx, id)
		return err == nil && st.State == session.Complete && strings.Contains(st.Result, "exported q3")
	})
	var evs []session.Event
	retry(func() error {
		logged, err := f.c.Subscribe(ctx, id, 0)
		if err != nil {
			return err
		}
		defer func() { _ = logged.Close() }()
		evs, err = heardTurn(t, logged)
		return err
	})
	heard, liveErr := heardTurn(t, live)
	retry(func() error { return f.c.Close(ctx, id) })
	heal(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(forgot, string(id))
	})
	_, gone := f.c.Status(ctx, id)
	for _, obj := range []session.ID{id, job} {
		f.p.FailReads(obj, nil)
		f.p.FailWrites(obj, nil)
	}

	// Assert
	want := []session.EventType{session.EventFunctionCall, session.EventFunctionCall, session.EventToolResult, session.EventToolResult, session.EventMessage, session.EventComplete}
	if !slices.Equal(types(evs), want) || last(evs).Type != session.EventComplete || !strings.Contains(evs[len(evs)-2].Content, "exported q3") {
		t.Fatalf("failing %v: the log %+v; want each event once and the job's output in the answer", fail, evs)
	}
	if (len(heard) == 0 || last(heard).Type != session.EventComplete) && liveErr == nil {
		t.Fatalf("failing %v: a live subscriber heard %v (%v); want the turn or the outage's error", fail, types(heard), liveErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(effects) != 1 || !errors.Is(gone, session.ErrNotFound) {
		t.Fatalf("failing %v: the tool's effects %v, the session after its close %v; want one effect and the session gone", fail, effects, gone)
	}
	return slices.Clone(ops)
}

// heardTurn is what sub heard until the turn completed or, with the error
// that ended it, until its events ended.
func heardTurn(t *testing.T, sub session.Subscription) ([]session.Event, error) {
	t.Helper()
	var heard []session.Event
	timeout := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return heard, cmp.Or(sub.Err(), errors.New("the events ended"))
			}
			if heard = append(heard, ev); ev.Type == session.EventComplete {
				return heard, nil
			}
		case <-timeout:
			t.Fatalf("a subscriber heard %v and nothing more", types(heard))
		}
	}
}

// TestObject_aRequestThatMeetsAnOutageFailsWithItAndThenGetsItsAnswer: a
// cold object's storage fails one operation of a request once, any read or
// write of it or of the run it starts: the request fails with the outage's
// error or, when the operation was not the request's own, gets its answer;
// sent again, it gets its answer.
func TestObject_aRequestThatMeetsAnOutageFailsWithItAndThenGetsItsAnswer(t *testing.T) {
	t.Parallel()
	const id = session.ID("s")
	for name, tc := range map[string]struct {
		request func(fixture, context.Context) error
		want    error
	}{
		"a status": {request: func(f fixture, ctx context.Context) error {
			_, err := f.c.Status(ctx, id)
			return err
		}},
		"its jobs": {request: func(f fixture, ctx context.Context) error {
			_, err := f.c.Jobs(ctx, id)
			return err
		}},
		"its head": {request: func(f fixture, ctx context.Context) error {
			_, err := f.c.Head(ctx, id)
			return err
		}},
		"a subscription": {request: func(f fixture, ctx context.Context) error {
			sub, err := f.c.Subscribe(ctx, id, 0)
			if err != nil {
				return err
			}
			defer func() { _ = sub.Close() }()
			_, err = heardTurn(t, sub)
			return err
		}},
		"a prompt": {request: func(f fixture, ctx context.Context) error {
			return f.c.Prompt(ctx, id, session.Prompt{Text: "hello", Auth: driveAuth("tok")})
		}},
		"a resume": {request: func(f fixture, ctx context.Context) error {
			return f.c.Resume(ctx, id, session.Resume{Auth: driveAuth("tok")})
		}},
		"a create of its id": {request: func(f fixture, ctx context.Context) error {
			_, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: id})
			return err
		}, want: session.ErrExists},
		"its alarm": {request: func(f fixture, ctx context.Context) error { return f.p.FireAlarm(ctx, id) }},
		"a close":   {request: func(f fixture, ctx context.Context) error { return f.c.Close(ctx, id) }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ops := requestRun(t, tc.request, tc.want, 0)
			for n := 1; n <= ops; n++ {
				requestRun(t, tc.request, tc.want, n)
			}
		})
	}
}

// requestRun sends request to a cold object of a session that ran a turn,
// failing its nth storage operation once (none when n is 0), and checks
// the request's outcomes. It returns the operations it counted.
func requestRun(t *testing.T, request func(fixture, context.Context) error, want error, n int) (ops int) {
	t.Helper()
	// Arrange
	f := newWireFixture(t, func([]*message.Message) tacklr.LLMResponseChunk { return testkit.Text("hi") }, nil)
	ctx := t.Context()
	const id = session.ID("s")
	if _, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: id}); err != nil {
		t.Fatal(err)
	}
	sub := f.subscribe(t, id)
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	untilEnd(t, sub)
	if err := f.p.Idle(ctx, id); err != nil {
		t.Fatal(err)
	}
	f.p.Evict(id)
	var count atomic.Int32
	op := func() bool { return int(count.Add(1)) == n }
	f.p.FailReads(id, func(string) bool { return op() })
	f.p.FailWrites(id, func([]durableobject.Op) bool { return op() })

	// Act
	first := request(f, ctx)
	if err := f.p.Idle(ctx, id); err != nil {
		t.Fatal(err)
	}
	ops = int(count.Load())
	f.p.FailReads(id, nil)
	f.p.FailWrites(id, nil)
	again := request(f, ctx)

	// Assert
	if first != nil && !errors.Is(first, want) && !strings.Contains(first.Error(), "storage unavailable") {
		t.Fatalf("failing operation %d: the request %v; want the outage's error or its answer", n, first)
	}
	if !errors.Is(again, want) || (want == nil && again != nil) {
		t.Fatalf("failing operation %d: the request sent again %v; want %v", n, again, want)
	}
	return ops
}
