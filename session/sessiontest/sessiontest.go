// Package sessiontest is the contract every session runtime's
// session.Client must meet. Run it from a runtime's own tests (the Durable
// Object runtime on its test platform, the Temporal worker on its live
// stack), so both run a session the same way.
package sessiontest

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// Deploy starts a runtime for one test that runs agent, with jobs as its
// named jobs, and returns its client.
type Deploy func(t *testing.T, agent tacklr.AgentOptions, jobs map[string]session.JobHandler) session.Client

// Suite is the contract for one runtime.
type Suite struct {
	Deploy Deploy
	// LiveDeltas says a subscriber that listens before the prompt gets
	// each of the reply's deltas; a runtime that streams them from
	// elsewhere tests that on its own.
	LiveDeltas bool
}

// turnTimeout is how long a test waits for a turn to end.
const turnTimeout = time.Minute

// Run runs the contract.
func (s Suite) Run(t *testing.T) {
	t.Run("a prompt runs a turn with its tool, streams its events and keeps the whole ones in its log", s.aPromptRunsATurn)
	t.Run("a parked turn waits and goes on with the answer", s.aParkedTurnResumes)
	t.Run("a cancel stops the running tool or model request and the session runs the next prompt", s.aCancelStopsTheStep)
	t.Run("a session the deployment cannot run, or a model that fails, fails its turn", s.aTurnFails)
	t.Run("a call that waits on a specialist or a job gets its answer as its result", s.aWaitedJobIsTheCallsResult)
	t.Run("the session's jobs are listed with their kinds while they run", s.jobsAreListed)
	t.Run("a tool sees its session and the prompt's credentials", s.aToolSeesItsSession)
	t.Run("a close forgets the session's messages and ends it for good", s.aCloseEndsTheSession)
	t.Run("a bad request is invalid and an unknown session is not found", s.badAndUnknown)
}

func (s Suite) aPromptRunsATurn(t *testing.T) {
	t.Parallel()
	// Arrange
	lookup := tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Handler: func(context.Context) (string, error) { return "42", nil }})
	var mu sync.Mutex
	sawFirst := false
	reply := testkit.CallThenAnswer("lookup", `{}`)
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if slices.ContainsFunc(msgs, func(m *message.Message) bool { return m.Role == message.RoleAssistant && m.Content == "done: 42" }) {
			mu.Lock()
			sawFirst = true
			mu.Unlock()
		}
		return reply(msgs)
	})
	c := s.Deploy(t, agent(model, lookup), nil)
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})

	// Act
	first := turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "what is it?"}) })
	st, stErr := c.Status(ctx, id)
	logged := untilEnd(t, subscribe(t, c, id, 0))
	second := turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "again"}) })

	// Assert
	want := []session.EventType{session.EventFunctionCall, session.EventToolResult, session.EventMessage, session.EventComplete}
	if !slices.Equal(types(durable(first)), want) || toolResult(first) != "42" || !slices.Equal(answers(first), []string{"done: 42"}) {
		t.Fatalf("events %+v; want the call, its result, the whole answer and the end", first)
	}
	if s.LiveDeltas && deltas(first) != "done: 42" {
		t.Fatalf("events %+v; want the answer's deltas live", first)
	}
	if stErr != nil || st.State != session.Complete || st.Result != "done: 42" || st.Waiting || st.ID != id {
		t.Fatalf("status %+v %v; want the turn complete with its answer", st, stErr)
	}
	if !slices.Equal(types(logged), want) || toolResult(logged) != "42" || deltas(logged) != "" {
		t.Fatalf("the log %+v; want the turn's whole events and no delta", logged)
	}
	mu.Lock()
	defer mu.Unlock()
	if !sawFirst || !slices.Equal(types(durable(second)), want) {
		t.Fatalf("the next turn saw the first %v, events %v; want the conversation kept and its own events from the head", sawFirst, types(durable(second)))
	}
}

func (s Suite) aParkedTurnResumes(t *testing.T) {
	t.Parallel()
	// Arrange
	confirm := tacklr.NewTool(tacklr.ToolConfig{Name: "confirm", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		answer, err := cc.Park(&tacklr.UserSelectionInterrupt{Question: "Delete?", Options: []tacklr.UserChoice{{Title: "Delete"}, {Title: "Keep"}}})
		if err != nil {
			return "", err
		}
		chosen, _ := answer.(*tacklr.UserSelectionInterrupt)
		return "chose " + chosen.ConfirmedChoice.Title, nil
	}})
	c := s.Deploy(t, agent(testkit.ScriptedModel(t, testkit.CallThenAnswer("confirm", `{}`)), confirm), nil)
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})
	parked := last(turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "clean up"}) }))
	waiting, waitErr := c.Status(ctx, id)

	// Act
	evs := turn(t, c, id, func() error {
		return c.Resume(ctx, id, session.Resume{Responses: map[string][]byte{parked.MessageID: []byte(`{"selectionIdx":0}`)}})
	})

	// Assert
	if parked.Type != session.EventInterrupt || waitErr != nil || !waiting.Waiting || waiting.State != session.Running {
		t.Fatalf("parked %+v, status %+v %v; want the turn waiting on the question", parked, waiting, waitErr)
	}
	if last(evs).Type != session.EventComplete || toolResult(evs) != "chose Delete" {
		t.Fatalf("after the answer %+v; want it as the call's result and the turn ended", evs)
	}
}

func (s Suite) aCancelStopsTheStep(t *testing.T) {
	t.Parallel()
	// Arrange
	started := make(chan struct{}, 1)
	block := tacklr.NewTool(tacklr.ToolConfig{Name: "block", Handler: func(ctx context.Context) (string, error) {
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	}})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		switch testkit.LastMessage(msgs).Content {
		case "hello":
			ch <- testkit.Text("hi")
		case "think":
			started <- struct{}{}
			<-ctx.Done()
		default:
			ch <- testkit.Calls(testkit.Call("b1", "block", `{}`))
		}
	})
	c := s.Deploy(t, agent(model, block), nil)
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})
	cancelled := func(text string) session.Event {
		t.Helper()
		return last(turn(t, c, id, func() error {
			if err := c.Prompt(ctx, id, session.Prompt{Text: text}); err != nil {
				return err
			}
			<-started
			return c.Cancel(ctx, id)
		}))
	}

	// Act
	tool, request := cancelled("wait"), cancelled("think")
	next := turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "hello"}) })

	// Assert
	for name, ev := range map[string]session.Event{"tool": tool, "model request": request} {
		if ev.Type != session.EventError || ev.FailKind != session.FailCancelled {
			t.Fatalf("after cancelling the %s: %+v; want the turn failed as cancelled", name, ev)
		}
	}
	if last(next).Type != session.EventComplete || !slices.Equal(answers(next), []string{"hi"}) {
		t.Fatalf("next turn %+v; want it answered", next)
	}
}

func (s Suite) aTurnFails(t *testing.T) {
	t.Parallel()
	// Arrange
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		return testkit.Fail(session.ErrMaxTokens)
	})
	a := agent(model)
	a.SessionScope = func(_ context.Context, id string) (brain.Scope, error) {
		if strings.HasPrefix(id, "unscoped-") {
			return brain.Scope{}, errors.New("no tenant for " + id)
		}
		return brain.Scope{}, nil
	}
	c := s.Deploy(t, a, nil)
	ctx := t.Context()
	end := func(req session.CreateSession) session.Event {
		t.Helper()
		id := create(t, c, req)
		return last(turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "go"}) }))
	}

	// Act
	ends := map[string]session.Event{
		"nobody":      end(session.CreateSession{Specialist: "nobody"}),
		"nightly":     end(session.CreateSession{Worker: "nightly"}),
		"no tenant":   end(session.CreateSession{SessionID: session.ID("unscoped-" + uuid.NewString())}),
		"token limit": end(session.CreateSession{}),
	}

	// Assert
	for name, ev := range ends {
		if ev.Type != session.EventError || (name != "token limit" && !strings.Contains(ev.Fail, name)) {
			t.Errorf("the turn of %s: %+v; want it failed naming why", name, ev)
		}
	}
	if kind := ends["token limit"].FailKind; kind != session.FailMaxTokens {
		t.Errorf("a turn whose model hit its token limit failed as %q; want %q", kind, session.FailMaxTokens)
	}
}

func (s Suite) aWaitedJobIsTheCallsResult(t *testing.T) {
	t.Parallel()
	// Arrange
	export := tacklr.NewTool(tacklr.ToolConfig{Name: "export", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		_, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "export", Task: "q3", Wait: true})
		return "", err
	}})
	var mu sync.Mutex
	var got []string
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if strings.Contains(msgs[0].Content, "Review strictly.") {
			return testkit.Text("reviewed")
		}
		if results := testkit.ToolResults(msgs); len(results) > 0 {
			mu.Lock()
			got = results
			mu.Unlock()
			return testkit.Text("done")
		}
		return testkit.Calls(
			testkit.Call("review", "spawn_specialist", `{"specialist":"reviewer","task_description_and_context":"check the change","block":true}`),
			testkit.Call("export", "export", `{}`),
		)
	})
	a := agent(model, export)
	a.Specialists = []*tacklr.Specialist{{Name: "reviewer", Description: "Reviews.", Instructions: "Review strictly.", Model: model}}
	c := s.Deploy(t, a, map[string]session.JobHandler{"export": func(_ context.Context, task string) (string, error) { return "exported " + task, nil }})
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})

	// Act
	evs := turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "check and export"}) })

	// Assert
	mu.Lock()
	defer mu.Unlock()
	if last(evs).Type != session.EventComplete || len(got) != 2 || !strings.Contains(got[0], "reviewed") || got[1] != "exported q3" {
		t.Fatalf("end %+v, the model saw %q; want the specialist's answer and the job's output as their calls' results", last(evs), got)
	}
}

func (s Suite) jobsAreListed(t *testing.T) {
	t.Parallel()
	// Arrange
	release := make(chan struct{})
	let := sync.OnceFunc(func() { close(release) })
	t.Cleanup(let)
	running := make(chan struct{}, 2)
	wait := func() {
		running <- struct{}{}
		<-release
	}
	start := tacklr.NewTool(tacklr.ToolConfig{Name: "start", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "build"})
		return job.ID, err
	}})
	model := testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		switch testkit.LastMessage(msgs).Content {
		case "child-task":
			wait()
			ch <- testkit.Text("child-done")
		case "go":
			ch <- testkit.Calls(
				testkit.Call("sp1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`),
				testkit.Call("ci1", "start", `{}`),
			)
		default:
			ch <- testkit.Text("waiting")
		}
	})
	a := agent(model, start)
	a.Specialists = []*tacklr.Specialist{{Name: "researcher", Model: model}}
	c := s.Deploy(t, a, map[string]session.JobHandler{"ci": func(context.Context, string) (string, error) {
		wait()
		return "built", nil
	}})
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})
	sub := subscribe(t, c, id, head(t, c, id))
	if err := c.Prompt(ctx, id, session.Prompt{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-running:
		case <-time.After(turnTimeout):
			t.Fatal("the session's jobs did not start")
		}
	}

	// Act
	jobs, err := c.Jobs(ctx, id)
	let()
	evs := untilEnd(t, sub)

	// Assert
	kinds := map[string]session.State{}
	for _, j := range jobs {
		kinds[j.Kind] = j.State
	}
	if err != nil || len(jobs) != 2 || kinds[session.KindSpecialist] != session.Running || kinds[session.KindWorker] != session.Running {
		t.Fatalf("jobs %+v %v; want the running specialist and worker", jobs, err)
	}
	if last(evs).Type != session.EventComplete {
		t.Fatalf("end %+v; want the turn to end once its jobs did", last(evs))
	}
}

func (s Suite) aToolSeesItsSession(t *testing.T) {
	t.Parallel()
	// Arrange
	creds := tacklr.NewTool(tacklr.ToolConfig{Name: "creds", Access: tacklr.ToolReadAccess, Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		var got []string
		for _, b := range cc.Bindings() {
			got = append(got, b.Alias()+":"+b.Auth.Token)
		}
		return cc.SessionID() + " " + strings.Join(got, ","), nil
	}})
	c := s.Deploy(t, agent(testkit.ScriptedModel(t, testkit.CallThenAnswer("creds", `{}`)), creds), nil)
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})
	auth := session.AuthContext{Bindings: []mount.Binding{{Provider: mount.ProviderGoogleDrive, Params: map[string]string{mount.ParamName: "docs"}, Auth: mount.Credential{Token: "tok"}}}}

	// Act
	evs := turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "read", Auth: auth}) })

	// Assert
	if got := toolResult(evs); got != string(id)+" docs:tok" {
		t.Fatalf("tool result %q (end %+v); want the session and the prompt's Drive credential", got, last(evs))
	}
}

func (s Suite) aCloseEndsTheSession(t *testing.T) {
	t.Parallel()
	// Arrange
	var mu sync.Mutex
	var forgot []string
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
	a := agent(testkit.ScriptedModel(t, func([]*message.Message) tacklr.LLMResponseChunk { return testkit.Text("hi") }))
	a.Brain, a.SearchNamespace = eng, ns
	c := s.Deploy(t, a, nil)
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})
	turn(t, c, id, func() error { return c.Prompt(ctx, id, session.Prompt{Text: "hello"}) })

	// Act
	closeErr := c.Close(ctx, id)
	gone := eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, err := c.Status(ctx, id)
		return len(forgot) > 0 && errors.Is(err, session.ErrNotFound)
	})
	prompt := c.Prompt(ctx, id, session.Prompt{Text: "more"})
	_, jobs := c.Jobs(ctx, id)
	again := c.Close(ctx, id)

	// Assert
	mu.Lock()
	defer mu.Unlock()
	if closeErr != nil || !gone || !slices.Equal(forgot, []string{string(id)}) {
		t.Fatalf("close %v, gone %v, brain deletes %v; want the session gone and its messages forgotten", closeErr, gone, forgot)
	}
	if !errors.Is(prompt, session.ErrNotFound) || !errors.Is(jobs, session.ErrNotFound) || again != nil {
		t.Fatalf("a prompt after close %v, its jobs %v, a second close %v; want not found, not found, and the close safe to repeat", prompt, jobs, again)
	}
}

func (s Suite) badAndUnknown(t *testing.T) {
	t.Parallel()
	// Arrange
	c := s.Deploy(t, agent(testkit.ScriptedModel(t, func([]*message.Message) tacklr.LLMResponseChunk { return testkit.Text("hi") })), nil)
	ctx := t.Context()
	id := create(t, c, session.CreateSession{})
	bad := map[string]any{"f": func() {}}
	unknown := session.ID("no-such-session-" + uuid.NewString())

	// Act
	_, both := c.CreateSession(ctx, session.CreateSession{Specialist: "a", Worker: "b"})
	_, badState := c.CreateSession(ctx, session.CreateSession{State: bad})
	invalid := map[string]error{
		"a specialist that is also a worker": both,
		"a create's bad state":               badState,
		"a prompt's bad state":               c.Prompt(ctx, id, session.Prompt{Text: "x", State: bad}),
		"a resume's bad state":               c.Resume(ctx, id, session.Resume{State: bad}),
	}
	_, status := c.Status(ctx, unknown)
	_, jobs := c.Jobs(ctx, unknown)
	notFound := map[string]error{
		"a prompt": c.Prompt(ctx, unknown, session.Prompt{Text: "hi"}),
		"a cancel": c.Cancel(ctx, unknown),
		"a status": status,
		"its jobs": jobs,
	}

	// Assert
	for name, err := range invalid {
		if !errors.Is(err, session.ErrInvalid) {
			t.Errorf("%s: %v; want invalid", name, err)
		}
	}
	for name, err := range notFound {
		if !errors.Is(err, session.ErrNotFound) {
			t.Errorf("%s of an unknown session: %v; want not found", name, err)
		}
	}
}

// agent is an agent of model with tools.
func agent(model tacklr.Model, tools ...*tacklr.Tool) tacklr.AgentOptions {
	return tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: tools}
}

func create(t *testing.T, c session.Client, req session.CreateSession) session.ID {
	t.Helper()
	id, err := c.CreateSession(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func head(t *testing.T, c session.Client, id session.ID) session.Seq {
	t.Helper()
	h, err := c.Head(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func subscribe(t *testing.T, c session.Client, id session.ID, after session.Seq) session.Subscription {
	t.Helper()
	sub, err := c.Subscribe(t.Context(), id, after)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

// turn subscribes from the session's head, sends wake, and returns the
// events until the turn ends.
func turn(t *testing.T, c session.Client, id session.ID, wake func() error) []session.Event {
	t.Helper()
	sub := subscribe(t, c, id, head(t, c, id))
	if err := wake(); err != nil {
		t.Fatal(err)
	}
	evs := untilEnd(t, sub)
	_ = sub.Close()
	return evs
}

// untilEnd reads sub until the turn completes, parks or fails, and returns
// every event it read.
func untilEnd(t *testing.T, sub session.Subscription) []session.Event {
	t.Helper()
	var got []session.Event
	timeout := time.After(turnTimeout)
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

// eventually reports whether done holds within the turn timeout.
func eventually(t *testing.T, done func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(turnTimeout)
	for !done() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

func last(evs []session.Event) session.Event {
	if len(evs) == 0 {
		return session.Event{}
	}
	return evs[len(evs)-1]
}

// durable are the events of evs that are not deltas.
func durable(evs []session.Event) []session.Event {
	return slices.DeleteFunc(slices.Clone(evs), func(ev session.Event) bool { return ev.Delta })
}

func types(evs []session.Event) []session.EventType {
	out := make([]session.EventType, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

// toolResult is the content of the first tool result in evs.
func toolResult(evs []session.Event) string {
	for _, ev := range evs {
		if ev.Type == session.EventToolResult {
			return ev.Content
		}
	}
	return ""
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
