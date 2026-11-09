package durableobject_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
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

// fixture is a platform, a client over it, and a model that counts its
// requests.
type fixture struct {
	p      *dotest.Platform
	c      *durableobject.Client
	infers *atomic.Int32
}

// reply is what the scripted model answers to a conversation.
type reply func(msgs []*message.Message) tacklr.LLMResponseChunk

func newFixture(t *testing.T, answer reply, tools []*tacklr.Tool, configure ...func(*durableobject.Config)) fixture {
	t.Helper()
	return newFixtureOn(t, dotest.New, answer, tools, configure...)
}

// newWireFixture is newFixture on a platform where every call to an object
// crosses the object protocol, as on Cloudflare.
func newWireFixture(t *testing.T, answer reply, tools []*tacklr.Tool, configure ...func(*durableobject.Config)) fixture {
	t.Helper()
	return newFixtureOn(t, dotest.NewWire, answer, tools, configure...)
}

func newFixtureOn(t *testing.T, platform func(durableobject.Config) *dotest.Platform, answer reply, tools []*tacklr.Tool, configure ...func(*durableobject.Config)) fixture {
	t.Helper()
	var infers atomic.Int32
	model := testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		infers.Add(1)
		c := answer(msgs)
		if c.Type == session.EventMessage && len(c.Content) > 1 {
			// The text streams as two deltas, then the message completes.
			n := len(c.Content) / 2
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: c.Content[:n]}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: c.Content[n:]}
			c.Content = ""
		}
		ch <- c
	})
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	cfg := durableobject.Config{
		Agent:        tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: tools},
		Sealer:       sealer,
		RetryBackoff: time.Millisecond,
	}
	for _, f := range configure {
		f(&cfg)
	}
	p := platform(cfg)
	return fixture{p: p, c: durableobject.NewClient(p), infers: &infers}
}

// always fails every write.
func always([]durableobject.Op) bool { return true }

// callThenAnswer calls tool once, then answers with the tool's result.
func callThenAnswer(tool, args string) reply {
	return func(msgs []*message.Message) tacklr.LLMResponseChunk {
		last := msgs[len(msgs)-1]
		if last.Role == message.RoleTool {
			return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done: " + last.Content, IsComplete: true}
		}
		id := fmt.Sprintf("call-%d", len(msgs))
		return tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: id, CallID: id, Name: tool, Arguments: args}}}
	}
}

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
				t.Fatalf("subscription closed after %+v", got)
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

// TestSession_aPromptRunsATurnWithItsToolAndStreamsItsEvents: the model
// calls a tool, the tool runs, and the model streams its answer from the
// result. A subscriber sees the call, its result, the answer's deltas live,
// the whole answer and the end of the turn. The session reports the turn
// complete with its result, and the log holds the durable events only. The
// next turn sees the conversation, and then the log and the stored call
// results hold that turn's alone.
func TestSession_aPromptRunsATurnWithItsToolAndStreamsItsEvents(t *testing.T) {
	t.Parallel()
	// Arrange
	lookup := tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Timeout: time.Minute, Handler: func(context.Context) (string, error) { return "42", nil }})
	var sawFirstTurn atomic.Bool
	answer := func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if slices.ContainsFunc(msgs, func(m *message.Message) bool { return m.Role == message.RoleAssistant && m.Content == "done: 42" }) {
			sawFirstTurn.Store(true)
		}
		return callThenAnswer("lookup", `{}`)(msgs)
	}
	f := newWireFixture(t, answer, []*tacklr.Tool{lookup})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{State: map[string]any{"tier": "pro"}})
	if err != nil {
		t.Fatal(err)
	}
	turn := func() []session.Event {
		sub := f.subscribe(t, id)
		if err := f.c.Prompt(ctx, id, session.Prompt{Text: "what is it?"}); err != nil {
			t.Fatal(err)
		}
		return untilEnd(t, sub)
	}
	fromStart := func() []session.Event {
		sub, err := f.c.Subscribe(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Close() }()
		return untilEnd(t, sub)
	}

	// Act
	evs := turn()
	st, stErr := f.c.Status(ctx, id)
	logged := fromStart()
	second := turn()
	if err := f.p.Idle(ctx, id); err != nil {
		t.Fatal(err)
	}
	replayed := fromStart()
	bodies := slices.DeleteFunc(f.p.Keys(id), func(k string) bool { return !strings.HasPrefix(k, "body/") })

	// Assert
	if last(evs).Type != session.EventComplete || toolResult(evs) != "42" {
		t.Fatalf("events %+v; want the tool's result, then the end of the turn", evs)
	}
	if deltas(evs) != "done: 42" || !slices.Equal(answers(evs), []string{"done: 42"}) {
		t.Fatalf("events %+v; want the answer's deltas live, then the whole answer", evs)
	}
	if stErr != nil || st.State != session.Complete || st.Result != "done: 42" || st.Waiting || st.ID != id {
		t.Fatalf("status %+v %v; want the turn complete with its answer", st, stErr)
	}
	want := []session.EventType{session.EventFunctionCall, session.EventToolResult, session.EventMessage, session.EventComplete}
	if !slices.Equal(types(logged), want) || !slices.Equal(answers(logged), []string{"done: 42"}) || toolResult(logged) != "42" {
		t.Fatalf("the log %+v; want the call, its result, the whole answer and the end", logged)
	}
	call := second[1].MessageID
	if !sawFirstTurn.Load() || !slices.Equal(summary(replayed), summary(second)) || !slices.Equal(bodies, []string{"body/" + url.PathEscape(string(id)) + "/" + call}) {
		t.Fatalf("next turn saw the first %v; log %v and call results %v after it; want the conversation kept and the next turn's events %v and result of %s alone", sawFirstTurn.Load(), summary(replayed), bodies, summary(second), call)
	}
}

// types are the types of evs, in order.
func types(evs []session.Event) []session.EventType {
	out := make([]session.EventType, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

// summary is each durable event of evs as its type, message id and content.
func summary(evs []session.Event) []string {
	var out []string
	for _, ev := range evs {
		if !ev.Delta {
			out = append(out, fmt.Sprintf("%s %s %s", ev.Type, ev.MessageID, ev.Content))
		}
	}
	return out
}

// deltas is the text evs streamed as deltas.
func deltas(evs []session.Event) string {
	var b strings.Builder
	for _, ev := range evs {
		if ev.Delta {
			b.WriteString(ev.Content)
		}
	}
	return b.String()
}

// answers are the whole messages in evs.
func answers(evs []session.Event) []string {
	var out []string
	for _, ev := range evs {
		if ev.Type == session.EventMessage && !ev.Delta {
			out = append(out, ev.Content)
		}
	}
	return out
}

// TestSession_anObjectStoppedMidToolReplaysAndFinishesTheTurn: the object
// stops while a tool runs. Its watchdog alarm restarts it; the new object
// replays the journal, so the model is not asked again for the reply it
// already gave, the unfinished tool call runs again, and the turn ends.
func TestSession_anObjectStoppedMidToolReplaysAndFinishesTheTurn(t *testing.T) {
	t.Parallel()
	var runs atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	slow := tacklr.NewTool(tacklr.ToolConfig{Name: "slow", Handler: func(ctx context.Context) (string, error) {
		if runs.Add(1) == 1 {
			close(started)
			<-release
		}
		return "finished", nil
	}})
	f := newFixture(t, callThenAnswer("slow", `{}`), []*tacklr.Tool{slow}, func(cfg *durableobject.Config) { cfg.Watchdog = 20 * time.Millisecond })
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

	f.p.Evict(id)
	close(release) // the stopped object's tool returns into storage it can no longer write
	sub := f.subscribe(t, id)
	if err := f.p.FireAlarm(ctx, id); err != nil {
		t.Fatal(err)
	}
	evs := untilEnd(t, sub)

	if first.IsZero() || armed.IsZero() {
		t.Fatalf("watchdog alarm %v, then %v; want it set while the tool ran, and set again while it kept running", first, armed)
	}
	if last(evs).Type != session.EventComplete || runs.Load() != 2 || f.infers.Load() != 2 {
		t.Fatalf("events %+v, tool runs %d, model requests %d; want the turn to end with the tool run again and the first reply replayed", evs, runs.Load(), f.infers.Load())
	}
}

// TestSession_aParkedTurnResumesAfterItsObjectStopped: a tool asks the user
// and the turn parks. The object stops; the next one reports the turn
// waiting from storage. The answer restarts it, the parked call returns
// the answer, and the turn ends with it.
func TestSession_aParkedTurnResumesAfterItsObjectStopped(t *testing.T) {
	t.Parallel()
	confirm := tacklr.NewTool(tacklr.ToolConfig{Name: "confirm", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		answer, err := cc.Park(&tacklr.UserSelectionInterrupt{Question: "Delete?", Options: []tacklr.UserChoice{{Title: "Delete"}, {Title: "Keep"}}})
		if err != nil {
			return "", err
		}
		return "chose " + answer.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title, nil
	}})
	f := newWireFixture(t, callThenAnswer("confirm", `{}`), []*tacklr.Tool{confirm})
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

	lost := sub
	f.p.Evict(id)
	for range lost.Events() {
	}
	// A new object answers from storage, without running the session.
	waiting, _ := f.c.Status(ctx, id)
	idle := f.p.Idle(ctx, id)
	sub = f.subscribe(t, id)
	err = f.c.Resume(ctx, id, session.Resume{Responses: map[string][]byte{parked.MessageID: []byte(`{"selectionIdx":0}`)}, Auth: driveAuth("refreshed")})
	evs := untilEnd(t, sub)

	if parked.Type != session.EventInterrupt || !waiting.Waiting || idle != nil {
		t.Fatalf("parked %+v, status %+v %v; want the turn waiting on the question", parked, waiting, idle)
	}
	if lost.Err() == nil {
		t.Fatal("the stream of the stopped object ended without an error; want it broken")
	}
	if err != nil || last(evs).Type != session.EventComplete || toolResult(evs) != "chose Delete" {
		t.Fatalf("resume %v, events %+v; want the answer as the call's result and the turn ended", err, evs)
	}
}

// TestSession_aCancelStopsWhatRunsAndTheSessionGoesOn: a cancel stops the
// tool that is running, the job a call waits on in its own object, and a
// model request that failed on the network, and fails each turn as
// cancelled; the session runs the next prompt. A close after a cancel also
// closes the job's object.
func TestSession_aCancelStopsWhatRunsAndTheSessionGoesOn(t *testing.T) {
	t.Parallel()
	// Arrange
	started := make(chan struct{}, 2)
	block := tacklr.NewTool(tacklr.ToolConfig{Name: "block", Handler: func(ctx context.Context) (string, error) {
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	}})
	wait := tacklr.NewTool(tacklr.ToolConfig{Name: "wait", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		_, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "long", Task: "forever", Wait: true})
		return "", err
	}})
	answer := func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch msgs[len(msgs)-1].Content {
		case "hello":
			return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hi", IsComplete: true}
		case "job":
			return callThenAnswer("wait", `{}`)(msgs)
		case "offline":
			started <- struct{}{}
			return tacklr.LLMResponseChunk{Type: session.EventError, Error: tacklr.Network(errors.New("connection reset"))}
		}
		return callThenAnswer("block", `{}`)(msgs)
	}
	f := newFixture(t, answer, []*tacklr.Tool{block, wait}, func(cfg *durableobject.Config) {
		cfg.RetryBackoff = time.Hour // the model request waits until the cancel
		cfg.Jobs = map[string]session.JobHandler{"long": func(ctx context.Context, _ string) (string, error) {
			started <- struct{}{}
			<-ctx.Done()
			return "", ctx.Err()
		}}
	})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	sub := f.subscribe(t, id)
	running := func(text string) {
		t.Helper()
		if err := f.c.Prompt(ctx, id, session.Prompt{Text: text}); err != nil {
			t.Fatal(err)
		}
		<-started
	}
	// jobs is the session's jobs: a job that runs is listed, since the
	// write that starts it holds it.
	jobs := func() []session.Status {
		t.Helper()
		jobs, err := f.c.Jobs(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return jobs
	}
	cancelled := func() session.Event {
		t.Helper()
		if err := f.c.Cancel(ctx, id); err != nil {
			t.Fatal(err)
		}
		return last(untilEnd(t, sub))
	}

	// Act
	running("wait")
	tool := cancelled()
	running("job")
	waited := jobs()
	job := cancelled()
	running("offline")
	model := cancelled()
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	next := last(untilEnd(t, sub))
	running("job")
	child := jobs()[0].ID
	if err := f.c.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []session.ID{id, child} {
		if err := f.p.Idle(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	_, childGone := f.c.Status(ctx, child)

	// Assert
	for name, ev := range map[string]session.Event{"tool": tool, "job": job, "model request": model} {
		if ev.Type != session.EventError || ev.FailKind != session.FailCancelled {
			t.Fatalf("after cancelling the %s: %+v; want the turn failed as cancelled", name, ev)
		}
	}
	if len(waited) != 1 || waited[0].State != session.Running || waited[0].Kind != session.KindWorker {
		t.Fatalf("jobs while the job ran: %+v; want the one running job", waited)
	}
	if next.Type != session.EventComplete {
		t.Fatalf("next turn %+v; want it to end", next)
	}
	if !errors.Is(childGone, session.ErrNotFound) {
		t.Fatalf("the job after its session closed: %v; want it gone", childGone)
	}
}

// TestSession_aJobRunsInItsOwnObjectAndCloseDeletesBoth: a tool schedules a
// named job and waits for it. The job runs as a child session in its own
// object, started with the session's credentials, and its output is the
// call's result. Once the session has that result it closes the job's
// object, and closing the session deletes what it saved.
func TestSession_aJobRunsInItsOwnObjectAndCloseDeletesBoth(t *testing.T) {
	t.Parallel()
	var jobID atomic.Value
	export := tacklr.NewTool(tacklr.ToolConfig{Name: "export", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "export", Task: "q3", Wait: true})
		jobID.Store(session.ID(job.ID))
		return "", err
	}})
	f := newWireFixture(t, callThenAnswer("export", `{}`), []*tacklr.Tool{export}, func(cfg *durableobject.Config) {
		eng, err := brain.NewEngine(brain.NewMemoryStore())
		if err != nil {
			t.Fatal(err)
		}
		ns, err := brain.ParseNamespace("org", "acme")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Agent.Brain, cfg.Agent.SearchNamespace = eng, ns
		cfg.Jobs = map[string]session.JobHandler{"export": func(_ context.Context, task string) (string, error) {
			return "exported " + task, nil
		}}
	})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	sub := f.subscribe(t, id)
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "export", Auth: driveAuth("tok")}); err != nil {
		t.Fatal(err)
	}
	evs := untilEnd(t, sub)
	child := jobID.Load().(session.ID)
	for _, obj := range []session.ID{id, child, id} {
		if err := f.p.Idle(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	_, jobGone := f.c.Status(ctx, child)
	if err := f.c.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []session.ID{id, child} {
		if err := f.p.Idle(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	_, parentGone := f.c.Status(ctx, id)
	promptGone := f.c.Prompt(ctx, id, session.Prompt{Text: "more"})
	again := f.c.Close(ctx, id)
	parentKeys, childKeys := f.p.Keys(id), f.p.Keys(child)

	// Assert
	if last(evs).Type != session.EventComplete || toolResult(evs) != "exported q3" {
		t.Fatalf("events %+v; want the job's output as the call's result", evs)
	}
	if !errors.Is(jobGone, session.ErrNotFound) || !errors.Is(parentGone, session.ErrNotFound) || !errors.Is(promptGone, session.ErrNotFound) || again != nil {
		t.Fatalf("job once its result was taken %v, session after close %v, a prompt after close %v, second close %v; want both gone and the close safe to repeat", jobGone, parentGone, promptGone, again)
	}
	if kept := []string{"meta", "status"}; !slices.Equal(parentKeys, kept) || !slices.Equal(childKeys, kept) {
		t.Fatalf("storage after close: session %v, job %v; want only who each object is and its status (every letter delivered), %v", parentKeys, childKeys, kept)
	}
}

// TestSession_eachSessionSearchesInTheScopeItsHostGivesIt: the agent's
// SessionScope replaces its SearchNamespace for each session, so a session
// finds its own tenant's note, and a session the scope refuses fails its
// turn with the scope's error.
func TestSession_eachSessionSearchesInTheScopeItsHostGivesIt(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	tenants := map[string]brain.Namespace{}
	for _, org := range []string{"acme", "globex"} {
		if tenants[org], err = brain.ParseNamespace("org", org); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Save(t.Context(), brain.Scope{Namespace: tenants[org]}, brain.Object{Kind: "Note", Title: org + " renewal"}); err != nil {
			t.Fatal(err)
		}
	}
	f := newFixture(t, callThenAnswer("search", `{"query":"renewal"}`), nil, func(cfg *durableobject.Config) {
		cfg.Agent.Brain, cfg.Agent.SearchNamespace = eng, tenants["globex"]
		cfg.Agent.SessionScope = func(_ context.Context, id string) (brain.Scope, error) {
			if id != "sess-acme" {
				return brain.Scope{}, fmt.Errorf("no tenant for %s", id)
			}
			return brain.Scope{Namespace: tenants["acme"]}, nil
		}
	})
	ctx := t.Context()
	turns := map[session.ID][]session.Event{}
	for _, id := range []session.ID{"sess-acme", "sess-unknown"} {
		if _, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: id}); err != nil {
			t.Fatal(err)
		}
		sub := f.subscribe(t, id)
		if err := f.c.Prompt(ctx, id, session.Prompt{Text: "find renewals"}); err != nil {
			t.Fatal(err)
		}
		turns[id] = untilEnd(t, sub)
	}

	if got := toolResult(turns["sess-acme"]); last(turns["sess-acme"]).Type != session.EventComplete || !strings.Contains(got, "acme renewal") {
		t.Errorf("acme session's search = %q (end %+v), want the acme note its scope reaches", got, last(turns["sess-acme"]))
	}
	if end := last(turns["sess-unknown"]); end.Type != session.EventError || !strings.Contains(end.Fail, "no tenant for sess-unknown") {
		t.Errorf("unknown session's end = %+v, want the turn to fail on the scope", end)
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

// TestClient_refusesBadAndUnknownSessions: a session that is both a
// specialist and a worker, an id that is taken, and state JSON cannot hold
// are refused; every call on an unknown session is not found.
func TestClient_refusesBadAndUnknownSessions(t *testing.T) {
	t.Parallel()
	f := newWireFixture(t, callThenAnswer("none", `{}`), nil)
	ctx := t.Context()
	_, both := f.c.CreateSession(ctx, session.CreateSession{Specialist: "a", Worker: "b"})
	_, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: "taken"})
	if err != nil {
		t.Fatal(err)
	}
	_, taken := f.c.CreateSession(ctx, session.CreateSession{SessionID: "taken"})
	_, badState := f.c.CreateSession(ctx, session.CreateSession{State: map[string]any{"f": func() {}}})
	badPrompt := f.c.Prompt(ctx, "taken", session.Prompt{State: map[string]any{"f": func() {}}})
	badResume := f.c.Resume(ctx, "taken", session.Resume{State: map[string]any{"f": func() {}}})

	unknown := []error{
		f.c.Prompt(ctx, "nope", session.Prompt{Text: "hi"}),
		f.c.Cancel(ctx, "nope"),
	}
	_, err = f.c.Status(ctx, "nope")
	unknown = append(unknown, err)
	_, err = f.c.Head(ctx, "nope")
	unknown = append(unknown, err)
	_, err = f.c.Subscribe(ctx, "nope", 0)
	unknown = append(unknown, err)
	_, err = f.c.Jobs(ctx, "nope")
	unknown = append(unknown, err)

	if !errors.Is(both, session.ErrInvalid) || !errors.Is(taken, session.ErrExists) || !errors.Is(badState, session.ErrInvalid) {
		t.Fatalf("both kinds %v, taken id %v, bad state %v; want invalid, exists, invalid", both, taken, badState)
	}
	if !errors.Is(badPrompt, session.ErrInvalid) || !errors.Is(badResume, session.ErrInvalid) {
		t.Fatalf("bad prompt state %v, bad resume state %v; want both invalid", badPrompt, badResume)
	}
	for i, err := range unknown {
		if !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("call %d on an unknown session: %v; want not found", i, err)
		}
	}
}

// TestSession_aModelFailureRunsAgainOrFailsTheTurnWithItsKind: a model
// request that fails on the network runs again, and the turn ends with the
// second answer. A model that keeps refusing fails the turn as refused once
// its retries ran out, and one that hits the token limit fails it as max
// tokens.
func TestSession_aModelFailureRunsAgainOrFailsTheTurnWithItsKind(t *testing.T) {
	t.Parallel()
	// Arrange
	var flaky atomic.Int32
	answer := func(msgs []*message.Message) tacklr.LLMResponseChunk {
		switch msgs[len(msgs)-1].Content {
		case "flaky":
			if flaky.Add(1) == 1 {
				return tacklr.LLMResponseChunk{Type: session.EventError, Error: tacklr.Network(errors.New("connection reset"))}
			}
			return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "back", IsComplete: true}
		case "long":
			return tacklr.LLMResponseChunk{Type: session.EventError, Error: session.ErrMaxTokens}
		}
		return tacklr.LLMResponseChunk{Type: session.EventError, Error: session.ErrModelRefused}
	}
	f := newFixture(t, answer, nil)
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	sub := f.subscribe(t, id)
	end := func(text string) session.Event {
		t.Helper()
		if err := f.c.Prompt(ctx, id, session.Prompt{Text: text}); err != nil {
			t.Fatal(err)
		}
		return last(untilEnd(t, sub))
	}

	// Act
	recovered := end("flaky")
	st, stErr := f.c.Status(ctx, id)
	refused, long := end("say it"), end("long")

	// Assert
	if recovered.Type != session.EventComplete || stErr != nil || st.Result != "back" || flaky.Load() != 2 {
		t.Fatalf("after a network failure %+v, status %+v %v, %d requests; want the second answer", recovered, st, stErr, flaky.Load())
	}
	if refused.FailKind != session.FailRefused || long.FailKind != session.FailMaxTokens || !errors.Is(refused.Error, session.ErrModelRefused) {
		t.Fatalf("refused %+v, long %+v; want the refusal and the token limit", refused, long)
	}
}

// TestSession_aRequestOverTheContextWindowCollapsesItAndAsksAgainOnce: a
// request the provider refuses as over the context window collapses the
// window into a summary, and the model is asked again: the turn ends with
// its answer. The messages after the last request the provider took stay.
// A request it refuses again, or a first prompt it refuses (there is
// nothing to collapse), fails the turn as over the context window.
func TestSession_aRequestOverTheContextWindowCollapsesItAndAsksAgainOnce(t *testing.T) {
	t.Parallel()
	// Arrange
	var refusals atomic.Int32
	var mu sync.Mutex
	var retried []string
	answer := func(msgs []*message.Message) tacklr.LLMResponseChunk {
		last := msgs[len(msgs)-1].Content
		switch {
		case strings.Contains(last, "Summarize the entire message history"):
			return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "summary", IsComplete: true}
		case last == "always too long", last == "too long" && refusals.Add(1) == 1:
			return tacklr.LLMResponseChunk{Type: session.EventError, Error: session.ErrContextLength}
		case last == "too long":
			mu.Lock()
			for _, m := range msgs[1:] {
				retried = append(retried, m.Content)
			}
			mu.Unlock()
		}
		return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "re: " + last, IsComplete: true}
	}
	f := newFixture(t, answer, nil)
	ctx := t.Context()
	open := func() func(text string) session.Event {
		id, err := f.c.CreateSession(ctx, session.CreateSession{})
		if err != nil {
			t.Fatal(err)
		}
		sub := f.subscribe(t, id)
		return func(text string) session.Event {
			t.Helper()
			if err := f.c.Prompt(ctx, id, session.Prompt{Text: text}); err != nil {
				t.Fatal(err)
			}
			return last(untilEnd(t, sub))
		}
	}
	end, first := open(), open()
	end("a")
	end("b")

	// Act
	answered := end("too long")
	failed := end("always too long")
	refusedFirst := first("always too long")

	// Assert
	mu.Lock()
	defer mu.Unlock()
	want := []string{"a", "CONVERSATION SUMMARY\n────────────\nsummary", "re: b", "too long"}
	if answered.Type != session.EventComplete || !slices.Equal(retried, want) {
		t.Fatalf("after a refusal %+v, the request asked again %q; want the answer to %q", answered, retried, want)
	}
	for _, ev := range []session.Event{failed, refusedFirst} {
		if ev.FailKind != session.FailContextLength || !errors.Is(ev.Error, session.ErrMaxTokens) {
			t.Fatalf("refused twice or first %+v, want a turn over the context window", ev)
		}
	}
}

// TestSession_aSpecialistRunsInItsOwnObjectAndAnUnknownKindFails: the model
// starts a specialist, which runs in its own object with its own
// instructions, and its answer is the call's result. A session for a
// specialist or a job the deployment does not have fails its turn, and so
// does a prompt with MCP servers on a deployment without an MCP connector.
func TestSession_aSpecialistRunsInItsOwnObjectAndAnUnknownKindFails(t *testing.T) {
	t.Parallel()
	var specialistRan atomic.Bool
	answer := func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if strings.Contains(msgs[0].Content, "Review strictly.") {
			specialistRan.Store(true)
			return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "reviewed", IsComplete: true}
		}
		return callThenAnswer("spawn_specialist", `{"specialist":"reviewer","task_description_and_context":"check the change","block":true}`)(msgs)
	}
	f := newWireFixture(t, answer, nil, func(cfg *durableobject.Config) {
		cfg.Agent.Specialists = []*tacklr.Specialist{{Name: "reviewer", Description: "Reviews.", Instructions: "Review strictly.", Model: cfg.Agent.Model}}
	})
	ctx := t.Context()
	prompt := func(id session.ID, p session.Prompt) []session.Event {
		t.Helper()
		sub := f.subscribe(t, id)
		if err := f.c.Prompt(ctx, id, p); err != nil {
			t.Fatal(err)
		}
		return untilEnd(t, sub)
	}
	run := func(req session.CreateSession) (session.ID, []session.Event) {
		t.Helper()
		id, err := f.c.CreateSession(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return id, prompt(id, session.Prompt{Text: "check"})
	}

	id, spawned := run(session.CreateSession{})
	noMCP := last(prompt(id, session.Prompt{Text: "check", MCPServers: []session.MCPServer{{Name: "tools", Type: session.MCPHTTP, URL: "http://127.0.0.1:1/mcp"}}}))
	_, evs := run(session.CreateSession{Specialist: "nobody"})
	noSpecialist := last(evs)
	_, evs = run(session.CreateSession{Worker: "nojob"})
	noJob := last(evs)

	if last(spawned).Type != session.EventComplete || toolResult(spawned) != "reviewed" || !specialistRan.Load() {
		t.Fatalf("events %+v, specialist ran %v; want its answer as the call's result", spawned, specialistRan.Load())
	}
	for name, ev := range map[string]session.Event{"nobody": noSpecialist, "nojob": noJob, "MCP": noMCP} {
		if ev.Type != session.EventError || !strings.Contains(ev.Fail, name) {
			t.Fatalf("session for %s: %+v; want the turn failed naming it", name, ev)
		}
	}
}

// TestSession_aStorageOutageStopsTheObjectAndTheAlarmFinishesTheTurn: while
// an object's storage is down, a prompt and a resume with credentials and
// the create of its session fail with the outage's error, and a turn whose
// tool returns into the outage stops. Other objects work. Once storage is
// back, the watchdog alarm restarts the object and the turn ends.
func TestSession_aStorageOutageStopsTheObjectAndTheAlarmFinishesTheTurn(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	var runs atomic.Int32
	slow := tacklr.NewTool(tacklr.ToolConfig{Name: "slow", Handler: func(context.Context) (string, error) {
		if runs.Add(1) == 1 {
			close(started)
			<-release
		}
		return "done", nil
	}})
	f := newFixture(t, callThenAnswer("slow", `{}`), []*tacklr.Tool{slow})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	f.p.FailWrites(id, always)
	f.p.FailWrites("down", always)
	refused := []error{
		f.c.Prompt(ctx, id, session.Prompt{Text: "go", Auth: driveAuth("tok")}),
		f.c.Resume(ctx, id, session.Resume{Auth: driveAuth("tok")}),
	}
	_, err = f.c.CreateSession(ctx, session.CreateSession{SessionID: "down"})
	refused = append(refused, err)
	_, other := f.c.CreateSession(ctx, session.CreateSession{SessionID: id + "-x"})
	f.p.FailWrites(id, nil)
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	<-started

	f.p.FailWrites(id, always)
	close(release)
	if err := f.p.Idle(ctx, id); err != nil {
		t.Fatal(err)
	}
	f.p.FailWrites(id, nil)
	sub := f.subscribe(t, id)
	if err := f.p.FireAlarm(ctx, id); err != nil {
		t.Fatal(err)
	}
	ended := last(untilEnd(t, sub))

	for i, err := range refused {
		if err == nil || !strings.Contains(err.Error(), "storage unavailable") {
			t.Fatalf("request %d while storage was down: %v; want the outage's error", i, err)
		}
	}
	if other != nil {
		t.Fatalf("another object's create: %v; want the session made", other)
	}
	if ended.Type != session.EventComplete || runs.Load() < 2 {
		t.Fatalf("after the outage %+v, tool runs %d; want the turn ended", ended, runs.Load())
	}
}

// TestObject_refusesAnInitWithoutAnIDAndNeedsItsParts: an object refuses a
// session without an id, and the constructors refuse what they cannot run
// without.
func TestObject_refusesAnInitWithoutAnIDAndNeedsItsParts(t *testing.T) {
	t.Parallel()
	// Arrange
	f := newFixture(t, callThenAnswer("none", `{}`), nil)

	// Act
	noID := f.p.Object("blank").Create(t.Context(), durableobject.Init{})
	both := f.p.Object("both").Create(t.Context(), durableobject.Init{SessionID: "both", Specialist: "a", Worker: "b"})

	// Assert
	if !errors.Is(noID, session.ErrInvalid) || !errors.Is(both, session.ErrInvalid) {
		t.Fatalf("no id %v, specialist and worker %v; want both invalid", noID, both)
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

// TestSession_anObjectStoppedAtAnyStepGoesOnToTheSameTurn: a cold object
// stops at the write of each kind of step (taking the prompt, a commit, the
// model's reply, the tool calls' results, the turn's end, the suspend that
// keeps the session's carry), or because it cannot read what a step needs:
// the session's carry, its journal, its event head, its events to trim, the
// call results to drop, or the journal to drop at the suspend. A new object
// over the same storage ends the same turn, with each event once and the
// tool run once.
func TestSession_anObjectStoppedAtAnyStepGoesOnToTheSameTurn(t *testing.T) {
	t.Parallel()
	// write fails, once, the write of the step of kind.
	write := func(kind string) func(fixture, session.ID, *atomic.Bool) {
		return func(f fixture, id session.ID, failed *atomic.Bool) {
			f.p.FailWrites(id, func(ops []durableobject.Op) bool {
				return slices.ContainsFunc(ops, func(op durableobject.Op) bool {
					return strings.HasPrefix(op.Key, "journal/") && strings.Contains(string(op.Value), `"kind":"`+kind+`"`)
				}) && failed.CompareAndSwap(false, true)
			})
		}
	}
	// suspend fails, once, the write that keeps the session's carry.
	suspend := func(f fixture, id session.ID, failed *atomic.Bool) {
		f.p.FailWrites(id, func(ops []durableobject.Op) bool {
			return slices.ContainsFunc(ops, func(op durableobject.Op) bool {
				return op.Key == "carry/"+url.PathEscape(string(id)) && !op.Delete
			}) && failed.CompareAndSwap(false, true)
		})
	}
	// read fails, once, the nth read of key (a format of the session id).
	read := func(key string, nth int32) func(fixture, session.ID, *atomic.Bool) {
		return func(f fixture, id session.ID, failed *atomic.Bool) {
			var reads atomic.Int32
			f.p.FailReads(id, func(k string) bool {
				return k == fmt.Sprintf(key, url.PathEscape(string(id))) && reads.Add(1) == nth && failed.CompareAndSwap(false, true)
			})
		}
	}
	for name, fail := range map[string]func(fixture, session.ID, *atomic.Bool){
		"next":            write("next"),
		"commit":          write("commit"),
		"infer":           write("infer"),
		"tools":           write("tools"),
		"emit":            write("emit"),
		"suspend":         suspend,
		"carry":           read("carry/%s", 1),
		"journal":         read("journal/%s/", 1),
		"event head":      read("eventhead/%s", 1),
		"events":          read("event/%s/", 1),
		"call results":    read("body/%s/", 1),
		"journal to drop": read("journal/%s/", 2),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			var runs atomic.Int32
			lookup := tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Handler: func(context.Context) (string, error) {
				runs.Add(1)
				return "42", nil
			}})
			f := newFixture(t, callThenAnswer("lookup", `{}`), []*tacklr.Tool{lookup})
			ctx := t.Context()
			id, err := f.c.CreateSession(ctx, session.CreateSession{})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.p.Idle(ctx, id); err != nil {
				t.Fatal(err)
			}
			f.p.Evict(id)
			var failed atomic.Bool
			fail(f, id, &failed)

			// Act
			if err := f.c.Prompt(ctx, id, session.Prompt{Text: "what is it?"}); err != nil {
				t.Fatal(err)
			}
			if err := f.p.Idle(ctx, id); err != nil {
				t.Fatal(err)
			}
			f.p.FailWrites(id, nil)
			f.p.Evict(id)
			if err := f.p.FireAlarm(ctx, id); err != nil {
				t.Fatal(err)
			}
			if err := f.p.Idle(ctx, id); err != nil {
				t.Fatal(err)
			}
			sub, err := f.c.Subscribe(ctx, id, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			evs := untilEnd(t, sub)
			st, _ := f.c.Status(ctx, id)

			// Assert
			want := []session.EventType{session.EventFunctionCall, session.EventToolResult, session.EventMessage, session.EventComplete}
			if !failed.Load() || !slices.Equal(types(evs), want) || toolResult(evs) != "42" || runs.Load() != 1 || st.State != session.Complete {
				t.Fatalf("stopped %v; events %v, tool runs %d, status %+v; want each event once, the tool run once, and the turn complete", failed.Load(), types(evs), runs.Load(), st)
			}
		})
	}
}

// TestSession_aTurnThatCannotReadWhatItCommitsFailsAndTheNextTurnRuns: a
// turn reads the session's checkpoint revision and rows when its object
// starts again (also to resume a parked call), the results of its tool
// calls when it commits them, and the session's credentials when it starts
// a job. A turn whose read of one of them fails ends with the outage's
// error. Once reads work, the next turn runs.
func TestSession_aTurnThatCannotReadWhatItCommitsFailsAndTheNextTurnRuns(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		tool string
		// fails reports whether the read of key fails, for session id.
		fails func(key string, id session.ID) bool
	}{
		"checkpoint revision":       {"lookup", func(k string, id session.ID) bool { return k == "snapshot/"+string(id) }},
		"checkpoint rows":           {"lookup", func(k string, id session.ID) bool { return k == "checkpoint/"+string(id)+"/" }},
		"checkpoint rows to resume": {"confirm", func(k string, id session.ID) bool { return k == "checkpoint/"+string(id)+"/" }},
		"call results": {"lookup", func(k string, id session.ID) bool {
			return strings.HasPrefix(k, "body/"+string(id)+"/") && k != "body/"+string(id)+"/"
		}},
		"credentials": {"export", func(k string, id session.ID) bool { return k == "secrets/"+string(id) }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			lookup := tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Handler: func(context.Context) (string, error) { return "42", nil }})
			export := tacklr.NewTool(tacklr.ToolConfig{Name: "export", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
				_, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "export", Task: "q3", Wait: true})
				return "", err
			}})
			confirm := tacklr.NewTool(tacklr.ToolConfig{Name: "confirm", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
				_, err := cc.Park(&tacklr.UserSelectionInterrupt{Question: "Go?", Options: []tacklr.UserChoice{{Title: "Go"}}})
				return "went", err
			}})
			answer := func(msgs []*message.Message) tacklr.LLMResponseChunk {
				if msgs[len(msgs)-1].Content == "again" {
					return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ready", IsComplete: true}
				}
				return callThenAnswer(tc.tool, `{}`)(msgs)
			}
			f := newFixture(t, answer, []*tacklr.Tool{lookup, export, confirm}, func(cfg *durableobject.Config) {
				cfg.Jobs = map[string]session.JobHandler{"export": func(_ context.Context, task string) (string, error) { return "exported " + task, nil }}
			})
			ctx := t.Context()
			id, err := f.c.CreateSession(ctx, session.CreateSession{})
			if err != nil {
				t.Fatal(err)
			}
			// turn sends wake (a prompt, or a resume when the turn parked) and
			// returns the turn's end.
			turn := func(wake func() error) session.Event {
				t.Helper()
				sub := f.subscribe(t, id)
				if err := wake(); err != nil {
					t.Fatal(err)
				}
				return last(untilEnd(t, sub))
			}
			prompt := func(text string) func() error {
				return func() error { return f.c.Prompt(ctx, id, session.Prompt{Text: text}) }
			}
			warm := turn(prompt("warm up"))
			if err := f.p.Idle(ctx, id); err != nil {
				t.Fatal(err)
			}
			f.p.Evict(id)
			f.p.FailReads(id, func(k string) bool { return tc.fails(k, id) })
			next := prompt("go")
			if warm.Type == session.EventInterrupt {
				next = func() error {
					return f.c.Resume(ctx, id, session.Resume{Responses: map[string][]byte{warm.MessageID: []byte(`{"selectionIdx":0}`)}})
				}
			}

			// Act
			failed := turn(next)
			f.p.FailReads(id, nil)
			again := turn(prompt("again"))

			// Assert
			if failed.Type != session.EventError || !strings.Contains(failed.Fail, "storage unavailable") {
				t.Fatalf("a turn that cannot read its %s: %+v; want it failed with the outage's error", name, failed)
			}
			if again.Type != session.EventComplete {
				t.Fatalf("the next turn: %+v; want it to end", again)
			}
		})
	}
}

// TestSession_aCloseThatCannotReadOrWriteItsStorageEndsOnceStorageWorks: a
// close deletes what the session saved. While storage cannot give the
// session's jobs, its tool calls or its journal, or cannot take the close's
// last write, the close stops: the close of an ended task fails its caller,
// and the close of a live session goes on at the alarm. Once storage works,
// the close ends, and the object keeps only who it was and its status.
func TestSession_aCloseThatCannotReadOrWriteItsStorageEndsOnceStorageWorks(t *testing.T) {
	t.Parallel()
	// read fails each read of key (a format of the session id).
	read := func(key string) func(fixture, session.ID, *atomic.Bool) {
		return func(f fixture, id session.ID, failed *atomic.Bool) {
			f.p.FailReads(id, func(k string) bool {
				fails := k == fmt.Sprintf(key, url.PathEscape(string(id)))
				if fails {
					failed.Store(true)
				}
				return fails
			})
		}
	}
	// final fails each write that closes the session.
	final := func(f fixture, id session.ID, failed *atomic.Bool) {
		f.p.FailWrites(id, func(ops []durableobject.Op) bool {
			closes := slices.ContainsFunc(ops, func(op durableobject.Op) bool {
				return op.Key == "meta" && strings.Contains(string(op.Value), `"closed":true`)
			})
			if closes {
				failed.Store(true)
			}
			return closes
		})
	}
	for name, tc := range map[string]struct {
		task bool
		fail func(fixture, session.ID, *atomic.Bool)
	}{
		"a session's jobs":           {false, read("child/%s/")},
		"a session's tool calls":     {false, read("call/%s/")},
		"a session's last write":     {false, final},
		"an ended task's journal":    {true, read("journal/%s/")},
		"an ended task's last write": {true, final},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			f := newFixture(t, func([]*message.Message) tacklr.LLMResponseChunk {
				return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hi", IsComplete: true}
			}, nil)
			ctx := t.Context()
			const id = "s"
			init := durableobject.Init{SessionID: id}
			if tc.task {
				init.Task = "say hi"
			}
			if err := f.p.Object(id).Create(ctx, init); err != nil {
				t.Fatal(err)
			}
			if err := f.p.Idle(ctx, id); err != nil {
				t.Fatal(err)
			}
			var failed atomic.Bool
			tc.fail(f, id, &failed)

			// Act
			closeErr := f.c.Close(ctx, id)
			if err := f.p.Idle(ctx, id); err != nil {
				t.Fatal(err)
			}
			f.p.FailReads(id, nil)
			f.p.FailWrites(id, nil)
			if err := f.p.FireAlarm(ctx, id); err != nil {
				t.Fatal(err)
			}
			if err := f.p.Idle(ctx, id); err != nil {
				t.Fatal(err)
			}
			again := f.c.Close(ctx, id)
			keys := f.p.Keys(id)

			// Assert
			if !failed.Load() || (closeErr != nil) != tc.task {
				t.Fatalf("storage failed %v, close %v; want the close stopped, and its caller told only for an ended task", failed.Load(), closeErr)
			}
			if again != nil || !slices.Equal(keys, []string{"meta", "status"}) {
				t.Fatalf("close once storage worked %v, storage %v; want the close done and only who the object was and its status kept", again, keys)
			}
		})
	}
}

// TestObject_aColdObjectThatCannotReadItsStateFailsItsRequestUntilReadsWork:
// an object starts again after its platform stopped it, and reads the
// session's state on its first request. While a part of that state cannot
// be read, the request fails with the outage's error. Once reads work, the
// same request gets its answer.
func TestObject_aColdObjectThatCannotReadItsStateFailsItsRequestUntilReadsWork(t *testing.T) {
	t.Parallel()
	status := func(f fixture, ctx context.Context) error {
		_, err := f.c.Status(ctx, "s")
		return err
	}
	head := func(f fixture, ctx context.Context) error {
		_, err := f.c.Head(ctx, "s")
		return err
	}
	for name, tc := range map[string]struct {
		key     string
		request func(fixture, context.Context) error
		want    error
	}{
		"who the object is": {"meta", status, nil},
		"the mailbox's head": {"mailboxhead/s", func(f fixture, ctx context.Context) error {
			return f.c.Prompt(ctx, "s", session.Prompt{Text: "hi"})
		}, nil},
		"the mailbox": {"mailbox/s/", func(f fixture, ctx context.Context) error { return f.p.FireAlarm(ctx, "s") }, nil},
		"the status": {"status", func(f fixture, ctx context.Context) error {
			_, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: "s"})
			return err
		}, session.ErrExists},
		"the outbox": {"outbox/", func(f fixture, ctx context.Context) error {
			_, err := f.c.Jobs(ctx, "s")
			return err
		}, nil},
		"the event head": {"eventhead/s", head, nil},
		"the event head of a subscription": {"eventhead/s", func(f fixture, ctx context.Context) error {
			sub, err := f.c.Subscribe(ctx, "s", 0)
			if err == nil {
				_ = sub.Close()
			}
			return err
		}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			f := newFixture(t, func([]*message.Message) tacklr.LLMResponseChunk {
				return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hi", IsComplete: true}
			}, nil)
			ctx := t.Context()
			if _, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: "s"}); err != nil {
				t.Fatal(err)
			}
			if err := f.p.Idle(ctx, "s"); err != nil {
				t.Fatal(err)
			}
			f.p.Evict("s")
			f.p.FailReads("s", func(k string) bool { return k == tc.key })

			// Act
			down := tc.request(f, ctx)
			f.p.FailReads("s", nil)
			up := tc.request(f, ctx)
			if err := f.p.Idle(ctx, "s"); err != nil {
				t.Fatal(err)
			}

			// Assert
			if down == nil || !strings.Contains(down.Error(), "storage unavailable") {
				t.Fatalf("a request while %s cannot be read: %v; want the outage's error", name, down)
			}
			if !errors.Is(up, tc.want) {
				t.Fatalf("the request once reads work: %v; want %v", up, tc.want)
			}
		})
	}
}

// TestSession_aJobThatCannotStartYetStartsOnARetry: while the job's object
// cannot write, the session's start of it, with the session's credentials,
// fails, and the session's jobs report it not found; the session sets its
// alarm to try again. The session's object stops; once the job's object
// works, the alarm wakes the next one, which starts the job from what the
// first one stored. That object cannot drop the delivered start once, so
// the next alarm delivers it again, and the job's object, which has the
// job, takes it as done. The job runs once, and its output is the call's
// result.
func TestSession_aJobThatCannotStartYetStartsOnARetry(t *testing.T) {
	t.Parallel()
	var parent atomic.Value
	var exports atomic.Int32
	var f fixture
	down := make(chan session.ID, 1)
	export := tacklr.NewTool(tacklr.ToolConfig{Name: "export", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		child := session.ID(tacklr.ChildID(string(parent.Load().(session.ID)), "export", cc.CurrentToolCallID(), false))
		f.p.FailWrites(child, always)
		down <- child
		_, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "export", Task: "q3", Wait: true})
		return "", err
	}})
	f = newFixture(t, callThenAnswer("export", `{}`), []*tacklr.Tool{export}, func(cfg *durableobject.Config) {
		cfg.RetryBackoff = time.Hour // only the alarm tries again
		cfg.Jobs = map[string]session.JobHandler{"export": func(_ context.Context, task string) (string, error) {
			exports.Add(1)
			return "exported " + task, nil
		}}
	})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	parent.Store(id)

	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "export", Auth: driveAuth("tok")}); err != nil {
		t.Fatal(err)
	}
	child := <-down
	if err := f.p.Idle(ctx, id); err != nil {
		t.Fatal(err)
	}
	retry := f.p.Alarm(id)
	_, pending := f.c.Jobs(ctx, id)
	// The letter that starts the job is in storage: an object that took
	// the session's place delivers it.
	f.p.Evict(id)
	f.p.FailWrites(child, nil)
	var kept atomic.Bool
	f.p.FailWrites(id, func(ops []durableobject.Op) bool {
		return len(ops) == 1 && ops[0].Delete && strings.HasPrefix(ops[0].Key, "outbox/") && kept.CompareAndSwap(false, true)
	})
	sub := f.subscribe(t, id)
	for range 2 {
		if err := f.p.FireAlarm(ctx, id); err != nil {
			t.Fatal(err)
		}
		if err := f.p.Idle(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	evs := untilEnd(t, sub)

	if !errors.Is(pending, session.ErrNotFound) {
		t.Fatalf("the session's jobs before the job started: %v; want the job not found", pending)
	}
	if retry.IsZero() || !kept.Load() || last(evs).Type != session.EventComplete || toolResult(evs) != "exported q3" || exports.Load() != 1 {
		t.Fatalf("retry alarm %v, start kept %v, events %+v, job runs %d; want an alarm set for the start, then the job run once and its output as the call's result", retry, kept.Load(), evs, exports.Load())
	}
}

// TestSubscribe_aSubscriberWhoseLogCannotBeReadGetsTheErrorAndCanReadAgain:
// when the object cannot read its log for a subscriber, the subscriber's
// events end with the outage's error. A new subscription, once reads work,
// reads the turn from the log.
func TestSubscribe_aSubscriberWhoseLogCannotBeReadGetsTheErrorAndCanReadAgain(t *testing.T) {
	t.Parallel()
	// Arrange
	f := newFixture(t, func([]*message.Message) tacklr.LLMResponseChunk {
		return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hi", IsComplete: true}
	}, nil)
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	live := f.subscribe(t, id)
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	untilEnd(t, live)
	events := "event/" + url.PathEscape(string(id)) + "/"
	read := func() session.Subscription {
		t.Helper()
		sub, err := f.c.Subscribe(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Close() })
		return sub
	}

	// Act
	f.p.FailReads(id, func(k string) bool { return k == events })
	broken := read()
	for range broken.Events() {
	}
	lost := broken.Err()
	f.p.FailReads(id, nil)
	evs := untilEnd(t, read())

	// Assert
	if lost == nil || !strings.Contains(lost.Error(), "storage unavailable") {
		t.Fatalf("the subscription whose log failed ended with %v; want the outage's error", lost)
	}
	if last(evs).Type != session.EventComplete || !slices.Equal(answers(evs), []string{"hi"}) {
		t.Fatalf("a new subscription read %+v; want the turn's answer and its end", evs)
	}
}

// TestSession_aToolReadsItsSessionAndThePromptsCredentials: a tool gets the
// session's id and the credentials the prompt brought.
func TestSession_aToolReadsItsSessionAndThePromptsCredentials(t *testing.T) {
	t.Parallel()
	creds := tacklr.NewTool(tacklr.ToolConfig{Name: "creds", Access: tacklr.ToolReadAccess, Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		var got []string
		for _, b := range cc.Bindings() {
			got = append(got, b.Alias()+":"+b.Auth.Token)
		}
		return cc.SessionID() + " " + strings.Join(got, ","), nil
	}})
	f := newFixture(t, callThenAnswer("creds", `{}`), []*tacklr.Tool{creds})
	ctx := t.Context()
	if _, err := f.c.CreateSession(ctx, session.CreateSession{SessionID: "sess-creds"}); err != nil {
		t.Fatal(err)
	}
	sub := f.subscribe(t, "sess-creds")
	if err := f.c.Prompt(ctx, "sess-creds", session.Prompt{Text: "read", Auth: driveAuth("tok")}); err != nil {
		t.Fatal(err)
	}
	evs := untilEnd(t, sub)

	if got := toolResult(evs); got != "sess-creds docs:tok" {
		t.Errorf("tool result = %q (end %+v), want the session and the prompt's Drive credential", got, last(evs))
	}
}
