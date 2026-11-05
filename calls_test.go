package tacklr_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
)

// answered is c with answer to its park, for its next run.
func answered(c tacklr.Call, answer string) tacklr.Call {
	c.Answer = []byte(answer)
	return c
}

// parkType is the interrupt type a parked call waits on.
func parkType(t *testing.T, c tacklr.Call) string {
	t.Helper()
	if c.Park == nil {
		t.Fatalf("call %+v did not park", c)
	}
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(c.Park.Data, &p); err != nil {
		t.Fatal(err)
	}
	return p.Type
}

// TestRunCall_aCallsWritesFromEachRunLandAtCommit: a call writes, parks,
// and on its run after resume reads its earlier writes and writes again; a
// permission prompt answered allow-always is remembered. A call reads its
// own writes over the committed state, and a value JSON cannot hold is its
// tool's error. Neither Tool step changes the session; the round's Commit
// applies every write in order, and a later call of the gated tool runs
// without a prompt. When a host interceptor runs a call a second time, the
// second run sees the first run's writes: create_plan finds the plan it
// made, and a tool that the user rejected always is rejected again without
// a prompt.
func TestRunCall_aCallsWritesFromEachRunLandAtCommit(t *testing.T) {
	t.Parallel()
	gate := tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "gate-ok", nil }})
	note := tacklr.NewTool(tacklr.ToolConfig{Name: "note", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		if _, err := cc.Park(&tacklr.UserSelectionInterrupt{Options: []tacklr.UserChoice{{Title: "A"}, {Title: "B"}}}); err != nil {
			cc.StateDelete("old")
			return "", errors.Join(cc.StateSet("k", "before"), err)
		}
		k, _ := cc.StateGet("k")
		_, old := cc.StateGet("old")
		kept, _ := cc.StateGet("kept")
		if err := errors.Join(cc.StateSet("k", "after"), cc.StateSet("new", 1)); err != nil {
			return "", err
		}
		mine, _ := cc.StateGet("k")
		return fmt.Sprintf("k=%v old=%v kept=%v mine=%v", k, old, kept, mine), nil
	}})
	keep := tacklr.NewTool(tacklr.ToolConfig{Name: "keep", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		return "kept", cc.StateSet("fn", func() {})
	}})
	shut := tacklr.NewTool(tacklr.ToolConfig{Name: "shut", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "shut-ok", nil }})
	// twice runs a call of shut or create_plan a second time when its first
	// run does not park, as a host that runs a call again does.
	twice := func(ctx context.Context, inv tacklr.ToolInvocation, next tacklr.ToolCallFunc) (tacklr.ToolOutcome, error) {
		out, err := next(ctx, inv)
		if _, parked := errors.AsType[tacklr.Interrupt](err); parked || !slices.Contains([]string{"shut", "create_plan"}, inv.Tool.Name()) {
			return out, err
		}
		return next(ctx, inv)
	}
	model, _ := scripted(t)
	h := newHarness(t, "writes", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{gate, note, keep, shut}, ToolInterceptors: []tacklr.ToolInterceptor{twice}})
	out := newStream(t).out
	if err := h.Commit(t.Context(), tacklr.CommitIn{State: map[string]any{"old": true, "kept": true}, Users: []*message.Message{userMsg("go")}}, out); err != nil {
		t.Fatal(err)
	}
	tcs := []message.ToolCall{call("n1", "note", "{}"), call("g1", "gate", "{}")}
	r := tacklr.NewRound(reply(tcs...))
	answers := []string{`{"selectionIdx":1}`, `{"optionId":"allow-always"}`}

	for i := range tcs {
		parked, err := h.RunCall(t.Context(), r.Calls[i], out)
		if err != nil || parked.Park == nil {
			t.Fatalf("%s = %+v %v, want a park", tcs[i].Name, parked, err)
		}
		if r.Calls[i], err = h.RunCall(t.Context(), answered(parked, answers[i]), out); err != nil {
			t.Fatal(err)
		}
	}
	shutParked, shutParkErr := h.RunCall(t.Context(), tacklr.NewCall(call("s1", "shut", "{}"), 0), out)
	shutAgain, shutErr := h.RunCall(t.Context(), answered(shutParked, `{"optionId":"reject-always"}`), out)
	planTwice := result(t, h, call("p1", "create_plan", `{"plan":"doc","todos":[{"title":"a"}]}`))
	unsaved := checkpoint(t, h).state
	commitErr := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)
	again := result(t, h, call("g2", "gate", "{}"))
	unkept := result(t, h, call("c1", "keep", "{}"))

	if got := r.Calls[0].Body.Text(); got != "k=before old=false kept=true mine=after" {
		t.Fatalf("note after resume = %q, want its own writes over the committed state", got)
	}
	if want := map[string]any{"old": true, "kept": true}; !maps.Equal(unsaved, want) {
		t.Fatalf("state before the commit = %v, want %v: the Tool steps change nothing", unsaved, want)
	}
	if want := map[string]any{"k": "after", "kept": true, "new": float64(1)}; commitErr != nil || !maps.Equal(checkpoint(t, h).state, want) {
		t.Fatalf("state after the commit = %v %v, want %v", checkpoint(t, h).state, commitErr, want)
	}
	if again != "gate-ok" || !strings.Contains(unkept, `state "fn"`) {
		t.Fatalf("gate again = %q, keep = %q; want the gate to run without a prompt and the value named", again, unkept)
	}
	if shutParkErr != nil || shutErr != nil || parkType(t, shutParked) != "tool_permission" || !shutAgain.Done || !strings.Contains(shutAgain.Body.Text(), "rejected by the user") {
		t.Fatalf("shut run twice = %+v %v %v, want the second run rejected by the call's own reject-always", shutAgain, shutParkErr, shutErr)
	}
	if !strings.Contains(planTwice, "active plan already exists") {
		t.Fatalf("create_plan run twice = %q, want the second run to find the call's own plan", planTwice)
	}
}

// TestRunCall_callsOfOneReplyRunAtOnceOnOneHarness: two calls of one reply
// run at the same time on one harness; each sees only its own state writes,
// and both land at commit in call order.
func TestRunCall_callsOfOneReplyRunAtOnceOnOneHarness(t *testing.T) {
	t.Parallel()
	var both sync.WaitGroup
	both.Add(2)
	set := tacklr.NewTool(tacklr.ToolConfig{Name: "set", Handler: func(_ context.Context, args struct {
		Value string `json:"value"`
	}, cc *tacklr.CallContext) (string, error) {
		if err := cc.StateSet("k", args.Value); err != nil {
			return "", err
		}
		both.Done()
		both.Wait() // the other call has written k too
		seen, _ := cc.StateGet("k")
		return seen.(string), nil
	}})
	model, _ := scripted(t)
	h := newHarness(t, "at-once", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{set}})
	prompt(t, h, "go")
	out := newStream(t).out
	r := tacklr.NewRound(reply(call("a", "set", `{"value":"a"}`), call("b", "set", `{"value":"b"}`)))

	var runs sync.WaitGroup
	errs := make([]error, len(r.Calls))
	for i := range r.Calls {
		runs.Go(func() { r.Calls[i], errs[i] = h.RunCall(t.Context(), r.Calls[i], out) })
	}
	runs.Wait()
	commitErr := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)

	if err := errors.Join(errs[0], errs[1], commitErr); err != nil {
		t.Fatal(err)
	}
	state := checkpoint(t, h)
	if got := contents(state.window[len(state.window)-2:]); !slices.Equal(got, []string{"a", "b"}) || state.state["k"] != "b" {
		t.Fatalf("results %q, committed k = %v; want each call to see only its own write, applied in call order", got, state.state["k"])
	}
}

// hostChoice is an interrupt type a host defines and does not register.
type hostChoice struct{}

func (hostChoice) TypeName() string           { return "host_choice" }
func (hostChoice) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (hostChoice) Return([]byte) error        { return nil }
func (hostChoice) Error() string              { return "host choice" }

// TestAskUserChoice_parksTheCallUntilTheUserAnswers: ask_user_choice parks
// its call with the question; a runtime stores the parked call as JSON and
// reads it back. An invalid answer fails the step and leaves it parked,
// and the call's run after a valid answer has the user's choice as its
// result. An answer to a call that is not parked fails the step. A body
// that is not one, or parks on a type this process cannot read, does not
// decode. A bad question, or a park of a type no one registered, is the
// tool's error.
func TestAskUserChoice_parksTheCallUntilTheUserAnswers(t *testing.T) {
	t.Parallel()
	host := tacklr.NewTool(tacklr.ToolConfig{Name: "host", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		_, err := cc.Park(hostChoice{})
		return "", err
	}})
	model, _ := scripted(t)
	h := newHarness(t, "ask", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{host}})
	out := newStream(t).out
	ask := func(id, args string) tacklr.Call {
		c, err := h.RunCall(t.Context(), tacklr.NewCall(call(id, "ask_user_choice", args), 0), out)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	bad := map[string]string{
		`{"question":" ","choices":[{"title":"a"},{"title":"b"}]}`: "question is required",
		`{"question":"q","choices":[{"title":"a"}]}`:               "at least 2 choices",
		`{"question":"q","choices":[{"title":"a"},{"title":" "}]}`: "title is required",
		`{"question":"q","choices":[{"title":"a"},{"title":"a"}]}`: "duplicate choice title",
	}
	parked := ask("q1", `{"question":"Which approach?","choices":[{"title":"Fast","is_recommended":true},{"title":"Careful","description":"more tests"}]}`)
	plain := ask("q2", `{"question":"Which?","choices":[{"title":"a"},{"title":"b"}]}`)
	stored, err := json.Marshal(parked)
	if err != nil {
		t.Fatal(err)
	}
	parked = tacklr.Call{}
	if err := json.Unmarshal(stored, &parked); err != nil {
		t.Fatal(err)
	}
	var unreadable []string
	for _, body := range []string{`[`, `{"park":1}`, `{"park":{"type":"nope","data":{}}}`, `{"park":{"type":"tool_permission","data":[]}}`} {
		if json.Unmarshal([]byte(body), new(tacklr.CallBody)) == nil {
			unreadable = append(unreadable, body)
		}
	}

	_, invalid := h.RunCall(t.Context(), answered(parked, `{}`), out)
	chosen, chosenErr := h.RunCall(t.Context(), answered(parked, `{"selectionIdx":1}`), out)
	plainChosen, _ := h.RunCall(t.Context(), answered(plain, `{"selectionIdx":0}`), out)
	_, notParked := h.RunCall(t.Context(), answered(tacklr.NewCall(call("q3", "ask_user_choice", "{}"), 0), `{"selectionIdx":0}`), out)

	for args, want := range bad {
		if got := ask("b", args).Body.Text(); !strings.Contains(got, want) {
			t.Errorf("ask %s = %q, want %q", args, got, want)
		}
	}
	if parkType(t, parked) != "user_selection_choice" || !strings.Contains(string(parked.Park.Data), "Which approach?") || parked.Park.InterruptID != "q1" {
		t.Fatalf("park = %+v, want the question for the user", parked.Park)
	}
	if len(unreadable) > 0 {
		t.Fatalf("bodies %q decoded, want each to fail", unreadable)
	}
	body, _ := json.Marshal(chosen.Body)
	if text, err := tacklr.ResultText(body); err != nil || text != chosen.Body.Text() {
		t.Fatalf("result text of the stored body = %q %v, want %q", text, err, chosen.Body.Text())
	}
	if invalid == nil || chosenErr != nil || chosen.Body.Text() != `User selected "Careful" — more tests` || plainChosen.Body.Text() != `User selected "a"` {
		t.Fatalf("invalid answer %v; chosen %q %v and %q; want the step to fail, then each choice as the result", invalid, chosen.Body.Text(), chosenErr, plainChosen.Body.Text())
	}
	if !errors.Is(notParked, tacklr.ErrInterruptNotFound) || !strings.Contains(result(t, h, call("h1", "host", "{}")), `"host_choice" is not registered`) {
		t.Fatalf("answer to a call that is not parked = %v, want ErrInterruptNotFound; and an unregistered park named", notParked)
	}
}

// TestOnCall_permissionParksUntilTheUserAnswers: a permission-gated call
// parks tool_permission; after allow-once the handler runs, and the next
// call of the tool parks again. A tool with two OnCall layers parks on the
// first, then on the second, and runs once. A user's reject-always denies
// the tool; a host grant runs a tool without a park, but the user's
// reject-always still wins over it.
func TestOnCall_permissionParksUntilTheUserAnswers(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	runs := map[string]int{}
	tool := func(name string, onCall ...tacklr.OnCallFunc) *tacklr.Tool {
		return tacklr.NewTool(tacklr.ToolConfig{Name: name, OnCall: onCall, Handler: func(_ context.Context, args struct {
			Path string `json:"path"`
		}) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			runs[name]++
			return name + " ran on " + args.Path, nil
		}})
	}
	skip := func(tacklr.ToolInvocation) tacklr.Interrupt { return nil }
	choose := func(tacklr.ToolInvocation) tacklr.Interrupt {
		return &tacklr.UserSelectionInterrupt{Question: "which?", Options: []tacklr.UserChoice{{Title: "a"}, {Title: "b"}}}
	}
	model, _ := scripted(t)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{
		tool("mutate", skip, tacklr.ToolPermissionOnCall),
		tool("two", tacklr.ToolPermissionOnCall, choose),
		tool("gate", tacklr.ToolPermissionOnCall),
	}}
	h := newHarness(t, "permission", opts)
	prompt(t, h, "go")
	out := newStream(t).out
	run := func(h *tacklr.Harness, c tacklr.Call) tacklr.Call {
		t.Helper()
		c, err := h.RunCall(t.Context(), c, out)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	parked := run(h, tacklr.NewCall(call("m1", "mutate", `{"path":"/a"}`), 0))
	allowed := run(h, answered(parked, `{"optionId":"allow-once"}`))
	parkedAgain := run(h, tacklr.NewCall(call("m2", "mutate", `{"path":"/a"}`), 0))
	two := run(h, tacklr.NewCall(call("t1", "two", `{"path":"/b"}`), 0))
	second := run(h, answered(two, `{"optionId":"allow-once"}`))
	both := run(h, answered(second, `{"selectionIdx":1}`))
	r := tacklr.NewRound(reply(call("g1", "gate", "{}")))
	r.Calls[0] = run(h, answered(run(h, r.Calls[0]), `{"optionId":"reject-always"}`))
	commitErr := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)
	rows, _ := h.Checkpoint()
	opts.AllowTools = []string{"mutate", "gate"}
	grants := newHarness(t, "permission", opts)
	restoreErr := grants.RestoreCheckpoint(rows)
	granted := run(grants, tacklr.NewCall(call("m3", "mutate", `{"path":"/c"}`), 0))
	overGrant := run(grants, tacklr.NewCall(call("g2", "gate", "{}"), 0))

	if parkType(t, parked) != "tool_permission" || allowed.Body.Text() != "mutate ran on /a" || parkType(t, parkedAgain) != "tool_permission" {
		t.Fatalf("allow-once = %q, then %+v; want the run, then a new park", allowed.Body.Text(), parkedAgain.Park)
	}
	if parkType(t, two) != "tool_permission" || parkType(t, second) != "user_selection_choice" || both.Body.Text() != "two ran on /b" || runs["two"] != 1 {
		t.Fatalf("two layers = %q after %d runs, want one park per layer, then one run", both.Body.Text(), runs["two"])
	}
	if commitErr != nil || restoreErr != nil || !strings.Contains(r.Calls[0].Body.Text(), "rejected by the user") || granted.Body.Text() != "mutate ran on /c" || !strings.Contains(overGrant.Body.Text(), "rejected by the user") {
		t.Fatalf("reject-always = %q; granted %q; reject-always over a grant %q (%v %v); want the denial, the grant's run, then the denial", r.Calls[0].Body.Text(), granted.Body.Text(), overGrant.Body.Text(), commitErr, restoreErr)
	}
}
