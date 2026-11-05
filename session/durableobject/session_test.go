package durableobject_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
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
	time.Sleep(60 * time.Millisecond)
	armed := f.p.Alarm(id)

	f.p.Evict(id)
	close(release) // the stopped object's tool returns into storage it can no longer write
	sub := f.subscribe(t, id)
	if err := f.p.FireAlarm(ctx, id); err != nil {
		t.Fatal(err)
	}
	evs := untilEnd(t, sub)

	if first.IsZero() || !armed.After(first) {
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
// tool that is running, then the job a call waits on in its own object, and
// fails each turn as cancelled; the session runs the next prompt.
func TestSession_aCancelStopsWhatRunsAndTheSessionGoesOn(t *testing.T) {
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
		}
		return callThenAnswer("block", `{}`)(msgs)
	}
	f := newFixture(t, answer, []*tacklr.Tool{block, wait}, func(cfg *durableobject.Config) {
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
	cancelled := func(text string) (session.Event, []session.Status) {
		t.Helper()
		if err := f.c.Prompt(ctx, id, session.Prompt{Text: text}); err != nil {
			t.Fatal(err)
		}
		<-started
		jobs, err := f.c.Jobs(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.c.Cancel(ctx, id); err != nil {
			t.Fatal(err)
		}
		return last(untilEnd(t, sub)), jobs
	}

	tool, _ := cancelled("wait")
	job, running := cancelled("job")
	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	next := last(untilEnd(t, sub))

	for name, ev := range map[string]session.Event{"tool": tool, "job": job} {
		if ev.Type != session.EventError || ev.FailKind != session.FailCancelled {
			t.Fatalf("after cancelling the %s: %+v; want the turn failed as cancelled", name, ev)
		}
	}
	if len(running) != 1 || running[0].State != session.Running || running[0].Kind != session.KindWorker {
		t.Fatalf("jobs while the job ran: %+v; want the one running job", running)
	}
	if next.Type != session.EventComplete {
		t.Fatalf("next turn %+v; want it to end", next)
	}
}

// TestSession_aJobRunsInItsOwnObjectAndCloseDeletesBoth: a tool schedules a
// named job and waits for it. The job runs as a child session in its own
// object, started with the session's credentials, and its output is the
// call's result. Once the session has that result it closes the job's
// object, and closing the session deletes what it saved.
func TestSession_aJobRunsInItsOwnObjectAndCloseDeletesBoth(t *testing.T) {
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
	again := f.c.Close(ctx, id)
	parentKeys, childKeys := f.p.Keys(id), f.p.Keys(child)

	// Assert
	if last(evs).Type != session.EventComplete || toolResult(evs) != "exported q3" {
		t.Fatalf("events %+v; want the job's output as the call's result", evs)
	}
	if !errors.Is(jobGone, session.ErrNotFound) || !errors.Is(parentGone, session.ErrNotFound) || again != nil {
		t.Fatalf("job once its result was taken %v, session after close %v, second close %v; want both gone and the close safe to repeat", jobGone, parentGone, again)
	}
	if kept := []string{"meta", "status"}; !slices.Equal(parentKeys, kept) || !slices.Equal(childKeys, kept) {
		t.Fatalf("storage after close: session %v, job %v; want only who each object is and its status (every letter delivered), %v", parentKeys, childKeys, kept)
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
	if refused.FailKind != session.FailRefused || long.FailKind != session.FailMaxTokens || !errors.Is(refused.FailKind.Err(refused.Fail), session.ErrModelRefused) {
		t.Fatalf("refused %+v, long %+v; want the refusal and the token limit", refused, long)
	}
}

// TestSession_aSpecialistRunsInItsOwnObjectAndAnUnknownKindFails: the model
// starts a specialist, which runs in its own object with its own
// instructions, and its answer is the call's result. A session for a
// specialist or a job the deployment does not have fails its turn.
func TestSession_aSpecialistRunsInItsOwnObjectAndAnUnknownKindFails(t *testing.T) {
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
	run := func(req session.CreateSession) []session.Event {
		t.Helper()
		id, err := f.c.CreateSession(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		sub := f.subscribe(t, id)
		if err := f.c.Prompt(ctx, id, session.Prompt{Text: "check"}); err != nil {
			t.Fatal(err)
		}
		return untilEnd(t, sub)
	}

	spawned := run(session.CreateSession{})
	noSpecialist := last(run(session.CreateSession{Specialist: "nobody"}))
	noJob := last(run(session.CreateSession{Worker: "nojob"}))

	if last(spawned).Type != session.EventComplete || toolResult(spawned) != "reviewed" || !specialistRan.Load() {
		t.Fatalf("events %+v, specialist ran %v; want its answer as the call's result", spawned, specialistRan.Load())
	}
	for name, ev := range map[string]session.Event{"nobody": noSpecialist, "nojob": noJob} {
		if ev.Type != session.EventError || !strings.Contains(ev.Fail, name) {
			t.Fatalf("session for %s: %+v; want the turn failed naming it", name, ev)
		}
	}
}

// TestSession_aStorageOutageStopsTheObjectAndTheAlarmFinishesTheTurn: while
// storage is down a prompt fails, and a turn whose tool returns into the
// outage stops; once storage is back, the watchdog alarm restarts the object
// and the turn ends.
func TestSession_aStorageOutageStopsTheObjectAndTheAlarmFinishesTheTurn(t *testing.T) {
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
	refused := f.c.Prompt(ctx, id, session.Prompt{Text: "go"})
	_, noCreate := f.c.CreateSession(ctx, session.CreateSession{SessionID: id + "-x", State: nil})
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

	if refused == nil || !strings.Contains(refused.Error(), "storage unavailable") || noCreate != nil {
		t.Fatalf("a prompt while storage was down %v, another session's create %v; want the outage's error, and the other session made", refused, noCreate)
	}
	if ended.Type != session.EventComplete || runs.Load() < 2 {
		t.Fatalf("after the outage %+v, tool runs %d; want the turn ended", ended, runs.Load())
	}
}

// TestObject_refusesAnInitWithoutAnIDAndNeedsItsParts: an object refuses a
// session without an id, and the constructors refuse what they cannot run
// without.
func TestObject_refusesAnInitWithoutAnIDAndNeedsItsParts(t *testing.T) {
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

// TestSession_anObjectStoppedAtAnyWriteGoesOnToTheSameTurn: the object
// stops at the write of each kind of step: taking the prompt, a commit,
// the model's reply, the tool calls' results, the turn's end. A new object
// over the same storage ends the same turn, with each event once and the
// tool run once.
func TestSession_anObjectStoppedAtAnyWriteGoesOnToTheSameTurn(t *testing.T) {
	for _, kind := range []string{"next", "commit", "infer", "tools", "emit"} {
		t.Run(kind, func(t *testing.T) {
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
			var failed atomic.Bool
			f.p.FailWrites(id, func(ops []durableobject.Op) bool {
				return slices.ContainsFunc(ops, func(op durableobject.Op) bool {
					return strings.HasPrefix(op.Key, "journal/") && strings.Contains(string(op.Value), `"kind":"`+kind+`"`)
				}) && failed.CompareAndSwap(false, true)
			})

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

			want := []session.EventType{session.EventFunctionCall, session.EventToolResult, session.EventMessage, session.EventComplete}
			if !failed.Load() || !slices.Equal(types(evs), want) || toolResult(evs) != "42" || runs.Load() != 1 || st.State != session.Complete {
				t.Fatalf("stopped %v; events %v, tool runs %d, status %+v; want each event once, the tool run once, and the turn complete", failed.Load(), types(evs), runs.Load(), st)
			}
		})
	}
}

// TestSession_aJobThatCannotStartYetStartsOnARetry: while the job's object
// cannot write, the session's start of it fails; the session sets its
// alarm to try again. The session's object stops; once the job's object
// works, the alarm wakes the next one, which starts the job from what the
// first one stored, and the job's output is the call's result.
func TestSession_aJobThatCannotStartYetStartsOnARetry(t *testing.T) {
	var parent atomic.Value
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
		cfg.Jobs = map[string]session.JobHandler{"export": func(_ context.Context, task string) (string, error) { return "exported " + task, nil }}
	})
	ctx := t.Context()
	id, err := f.c.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	parent.Store(id)

	if err := f.c.Prompt(ctx, id, session.Prompt{Text: "export"}); err != nil {
		t.Fatal(err)
	}
	child := <-down
	if err := f.p.Idle(ctx, id); err != nil {
		t.Fatal(err)
	}
	retry := f.p.Alarm(id)
	// The letter that starts the job is in storage: an object that took
	// the session's place delivers it.
	f.p.Evict(id)
	f.p.FailWrites(child, nil)
	sub := f.subscribe(t, id)
	if err := f.p.FireAlarm(ctx, id); err != nil {
		t.Fatal(err)
	}
	evs := untilEnd(t, sub)

	if retry.IsZero() || last(evs).Type != session.EventComplete || toolResult(evs) != "exported q3" {
		t.Fatalf("retry alarm %v, events %+v; want an alarm set for the start, then the job's output as the call's result", retry, evs)
	}
}
