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

// note parks on a choice; its run after the answer reports what it reads
// of the state, writes k and new, and reads its own write back.
var note = tacklr.NewTool(tacklr.ToolConfig{Name: "note", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
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

// noted is a harness whose state holds old and kept, and a round of one
// note call that ran, parked, and ran again after its answer.
func noted(t *testing.T) (*tacklr.Harness, *tacklr.Round) {
	t.Helper()
	model, _ := scripted(t)
	h := newHarness(t, "writes", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{note}})
	out := newStream(t).out
	if err := h.Commit(t.Context(), tacklr.CommitIn{State: map[string]any{"old": true, "kept": true}, Users: []*message.Message{userMsg("go")}}, out); err != nil {
		t.Fatal(err)
	}
	r := tacklr.NewRound(reply(call("n1", "note", "{}")))
	parked, err := h.RunCall(t.Context(), r.Calls[0], out)
	if err != nil {
		t.Fatal(err)
	}
	if r.Calls[0], err = h.RunCall(t.Context(), answered(parked, `{"selectionIdx":1}`), out); err != nil {
		t.Fatal(err)
	}
	return h, r
}

// TestRunCall_aCallReadsItsOwnWritesOverTheCommittedState: a call that
// wrote before it parked reads those writes on its run after the answer,
// and its new writes over the committed state.
func TestRunCall_aCallReadsItsOwnWritesOverTheCommittedState(t *testing.T) {
	t.Parallel()

	_, r := noted(t)

	if got := r.Calls[0].Body.Text(); got != "k=before old=false kept=true mine=after" {
		t.Fatalf("note after resume = %q, want its own writes over the committed state", got)
	}
}

// TestRunCall_aToolStepChangesNoStateUntilItsRoundCommits: the state is as
// committed until the round's Commit applies the call's writes in order.
func TestRunCall_aToolStepChangesNoStateUntilItsRoundCommits(t *testing.T) {
	t.Parallel()
	h, r := noted(t)
	before := checkpoint(t, h).state

	err := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, newStream(t).out)

	if want := map[string]any{"old": true, "kept": true}; !maps.Equal(before, want) {
		t.Fatalf("state before the commit = %v, want %v", before, want)
	}
	if want := map[string]any{"k": "after", "kept": true, "new": float64(1)}; err != nil || !maps.Equal(checkpoint(t, h).state, want) {
		t.Fatalf("state after the commit = %v %v, want %v", checkpoint(t, h).state, err, want)
	}
}

// TestRunCall_anAllowAlwaysAnswerLandsAtCommit: once the round of a call
// answered allow-always commits, a later call of the tool runs without a
// prompt.
func TestRunCall_anAllowAlwaysAnswerLandsAtCommit(t *testing.T) {
	t.Parallel()
	gate := tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "gate-ok", nil }})
	model, _ := scripted(t)
	h := newHarness(t, "allow-always", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{gate}})
	prompt(t, h, "go")
	out := newStream(t).out
	r := tacklr.NewRound(reply(call("g1", "gate", "{}")))
	parked, err := h.RunCall(t.Context(), r.Calls[0], out)
	if err != nil {
		t.Fatal(err)
	}
	if r.Calls[0], err = h.RunCall(t.Context(), answered(parked, `{"optionId":"allow-always"}`), out); err != nil {
		t.Fatal(err)
	}

	err = h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)

	if again := result(t, h, call("g2", "gate", "{}")); err != nil || again != "gate-ok" {
		t.Fatalf("gate after the commit = %q %v, want it to run without a prompt", again, err)
	}
}

// TestCallContext_aStateValueJSONCannotHoldIsTheToolsError: a state value
// that JSON cannot hold fails the tool, naming the key.
func TestCallContext_aStateValueJSONCannotHoldIsTheToolsError(t *testing.T) {
	t.Parallel()
	keep := tacklr.NewTool(tacklr.ToolConfig{Name: "keep", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		return "kept", cc.StateSet("fn", func() {})
	}})
	model, _ := scripted(t)
	h := newHarness(t, "unkept", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{keep}})

	got := result(t, h, call("c1", "keep", "{}"))

	if !strings.Contains(got, `state "fn"`) {
		t.Fatalf("keep = %q, want the value's key named", got)
	}
}

// TestRunCall_aSecondRunOfACallSeesItsFirstRunsWrites: when a host
// interceptor runs a call twice, the second run sees the first run's
// writes: create_plan finds the plan it made, and a gated tool its own
// reject-always answer, so it is rejected without a second prompt.
func TestRunCall_aSecondRunOfACallSeesItsFirstRunsWrites(t *testing.T) {
	t.Parallel()
	twice := func(ctx context.Context, inv tacklr.ToolInvocation, next tacklr.ToolCallFunc) (tacklr.ToolOutcome, error) {
		out, err := next(ctx, inv)
		if _, parked := errors.AsType[tacklr.Interrupt](err); parked {
			return out, err
		}
		return next(ctx, inv)
	}
	gate := tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "gate-ok", nil }})
	model, _ := scripted(t)
	h := newHarness(t, "twice", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{gate}, ToolInterceptors: []tacklr.ToolInterceptor{twice}})
	prompt(t, h, "go")
	out := newStream(t).out
	parked, err := h.RunCall(t.Context(), tacklr.NewCall(call("g1", "gate", "{}"), 0), out)
	if err != nil {
		t.Fatal(err)
	}

	plan := result(t, h, call("p1", "create_plan", `{"plan":"doc","todos":[{"title":"a"}]}`))
	gated, err := h.RunCall(t.Context(), answered(parked, `{"optionId":"reject-always"}`), out)

	if !strings.Contains(plan, "active plan already exists") {
		t.Fatalf("create_plan run twice = %q, want the second run to find the call's own plan", plan)
	}
	if err != nil || !gated.Done || !strings.Contains(gated.Body.Text(), "rejected by the user") {
		t.Fatalf("gate run twice = %+v %v, want the second run rejected by the call's own answer", gated, err)
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

// asked is a harness with the host tool, and its run of ask_user_choice
// with args, stored as JSON and read back as a runtime does.
func asked(t *testing.T, args string) (*tacklr.Harness, tacklr.Call) {
	t.Helper()
	host := tacklr.NewTool(tacklr.ToolConfig{Name: "host", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		_, err := cc.Park(hostChoice{})
		return "", err
	}})
	model, _ := scripted(t)
	h := newHarness(t, "ask", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{host}})
	c, err := h.RunCall(t.Context(), tacklr.NewCall(call("q1", "ask_user_choice", args), 0), newStream(t).out)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var parked tacklr.Call
	if err := json.Unmarshal(stored, &parked); err != nil {
		t.Fatal(err)
	}
	return h, parked
}

// TestAskUserChoice_parksTheCallOnTheQuestion: ask_user_choice parks its
// call on the question for the user, by the call's id.
func TestAskUserChoice_parksTheCallOnTheQuestion(t *testing.T) {
	t.Parallel()

	_, parked := asked(t, `{"question":"Which approach?","choices":[{"title":"Fast"},{"title":"Careful"}]}`)

	if parkType(t, parked) != "user_selection_choice" || !strings.Contains(string(parked.Park.Data), "Which approach?") || parked.Park.InterruptID != "q1" || parked.Body.Text() != "" {
		t.Fatalf("park = %+v, want the question for the user and no result yet", parked.Park)
	}
}

// TestAskUserChoice_aBadQuestionIsTheModelsToCorrect: a question
// ask_user_choice cannot ask is the tool's error, not a park.
func TestAskUserChoice_aBadQuestionIsTheModelsToCorrect(t *testing.T) {
	t.Parallel()

	_, c := asked(t, `{"question":"q","choices":[{"title":"a"}]}`)

	if c.Park != nil || !strings.Contains(c.Body.Text(), "at least 2 choices") {
		t.Fatalf("ask = %+v %q, want the bad question named", c.Park, c.Body.Text())
	}
}

// TestAskUserChoice_theChoiceIsTheCallsResult: the call's run after the
// user's answer has the choice, with its description when it has one, as
// its result, which a reader of the stored body reads back.
func TestAskUserChoice_theChoiceIsTheCallsResult(t *testing.T) {
	t.Parallel()
	for answer, want := range map[string]string{
		`{"selectionIdx":0}`: `User selected "Fast"`,
		`{"selectionIdx":1}`: `User selected "Careful" — more tests`,
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()
			h, parked := asked(t, `{"question":"Which?","choices":[{"title":"Fast"},{"title":"Careful","description":"more tests"}]}`)

			chosen, err := h.RunCall(t.Context(), answered(parked, answer), newStream(t).out)

			body, _ := json.Marshal(chosen.Body)
			text, textErr := tacklr.ResultText(body)
			if err != nil || textErr != nil || chosen.Body.Text() != want || text != want {
				t.Fatalf("chosen = %q (stored %q) %v %v, want %q", chosen.Body.Text(), text, err, textErr, want)
			}
		})
	}
}

// TestRunCall_anInvalidAnswerFailsTheStepAndLeavesTheCallParked: an
// answer the park refuses fails the step; the call still waits on it.
func TestRunCall_anInvalidAnswerFailsTheStepAndLeavesTheCallParked(t *testing.T) {
	t.Parallel()
	h, parked := asked(t, `{"question":"Which?","choices":[{"title":"a"},{"title":"b"}]}`)

	_, err := h.RunCall(t.Context(), answered(parked, `{}`), newStream(t).out)

	chosen, chosenErr := h.RunCall(t.Context(), answered(parked, `{"selectionIdx":0}`), newStream(t).out)
	if err == nil || chosenErr != nil || chosen.Body.Text() != `User selected "a"` {
		t.Fatalf("invalid answer = %v, then %q %v; want the step to fail and the park to take a valid answer", err, chosen.Body.Text(), chosenErr)
	}
}

// TestRunCall_anAnswerToACallThatIsNotParkedFails: an answer to a call
// that does not wait on one fails with ErrInterruptNotFound.
func TestRunCall_anAnswerToACallThatIsNotParkedFails(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	h := newHarness(t, "not-parked", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})

	_, err := h.RunCall(t.Context(), answered(tacklr.NewCall(call("q3", "ask_user_choice", "{}"), 0), `{"selectionIdx":0}`), newStream(t).out)

	if !errors.Is(err, tacklr.ErrInterruptNotFound) {
		t.Fatalf("answer = %v, want ErrInterruptNotFound", err)
	}
}

// TestCallContext_aParkOfAnUnregisteredTypeIsTheToolsError: a park on a
// type no one registered could not be read back, so it fails the tool.
func TestCallContext_aParkOfAnUnregisteredTypeIsTheToolsError(t *testing.T) {
	t.Parallel()
	h, _ := asked(t, `{"question":"q","choices":[{"title":"a"},{"title":"b"}]}`)

	got := result(t, h, call("h1", "host", "{}"))

	if !strings.Contains(got, `"host_choice" is not registered`) {
		t.Fatalf("host = %q, want the unregistered type named", got)
	}
}

// TestCallBody_refusesABodyItCannotRead: a body that is not JSON, or parks
// on a type this process cannot read, does not decode.
func TestCallBody_refusesABodyItCannotRead(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"not JSON":                 `[`,
		"a park that is no object": `{"park":1}`,
		"an unknown park type":     `{"park":{"type":"nope","data":{}}}`,
		"park data of no shape":    `{"park":{"type":"tool_permission","data":[]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := json.Unmarshal([]byte(body), new(tacklr.CallBody))

			if err == nil {
				t.Fatalf("body %s decoded, want it refused", body)
			}
		})
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
