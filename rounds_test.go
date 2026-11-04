package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/brain"
)

// roundsHarness is a turn whose model answers each request with the calls
// of calls, then "done"; gate parks for permission and free does not. runs
// counts each tool's runs.
func roundsHarness(t *testing.T, opts AgentOptions, calls ...message.ToolCall) (*Harness, AgentOptions, map[string]int) {
	t.Helper()
	runs := map[string]int{}
	gate := NewTool(ToolConfig{Name: "gate", OnCall: []OnCallFunc{ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { runs["gate"]++; return "gate-ok", nil }})
	free := NewTool(ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { runs["free"]++; return "free-ok", nil }})
	asked := false
	opts.Model = &scriptedModel{InvokeFn: func(_ context.Context, _ []*message.Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		if asked {
			ch <- LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		asked = true
		ch <- LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: calls, IsComplete: true}
	}}
	opts.Tools = append(opts.Tools, gate, free)
	h := mustNewHarness(t, opts)
	return h, opts, runs
}

// replyRound runs one round the way a runtime does: commit the prompt,
// infer, run each call, and commit the round once each call has its
// result. It returns the round and the commit's error.
func replyRound(t *testing.T, h *Harness) (*Round, error) {
	t.Helper()
	out := drainEventCh()
	if err := h.Commit(t.Context(), CommitIn{Users: []*message.Message{userMsg("go")}}, out); err != nil {
		t.Fatal(err)
	}
	reply, err := h.Infer(t.Context(), out)
	if err != nil {
		t.Fatal(err)
	}
	r := NewRound(reply)
	for i, call := range reply.Calls {
		if r.Outcomes[i], err = h.RunTool(t.Context(), ToolRun{Call: call}, out); err != nil {
			t.Fatal(err)
		}
	}
	if !r.Complete() {
		return r, nil
	}
	return r, h.Commit(t.Context(), CommitIn{Round: r}, out)
}

func contents(msgs []*message.Message) []string {
	var out []string
	for _, m := range msgs {
		out = append(out, m.Content)
	}
	return out
}

// TestCommit_aPromptPastAnOpenRoundCancelsItsOpenCalls: a round committed
// with a call that has no result (a prompt moved past it) gives that call a
// cancelled result, then commits the prompt. A commit that fails changes
// nothing.
func TestCommit_aPromptPastAnOpenRoundCancelsItsOpenCalls(t *testing.T) {
	h, _, _ := roundsHarness(t, AgentOptions{sessionID: "cancel-round", MaxWindowSize: 8192},
		toolCall("g1", "gate", "{}"), toolCall("f1", "free", "{}"))
	r, _ := replyRound(t, h)
	before := contents(h.context.Messages())
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	in := CommitIn{Round: r, Users: []*message.Message{userMsg("instead")}}

	failedErr := h.Commit(cancelled, in, drainEventCh())
	unchanged := contents(h.context.Messages())
	err := h.Commit(t.Context(), in, drainEventCh())

	got := contents(h.context.Messages())
	if !errors.Is(failedErr, context.Canceled) || len(unchanged) != len(before) {
		t.Fatalf("commit with a cancelled context = %v, window %q, want it to fail and leave the window", failedErr, unchanged)
	}
	if err != nil || len(got) != 5 || got[2] != CancelledToolResultContent || got[3] != "free-ok" || got[4] != "instead" {
		t.Fatalf("window = %q %v, want the cancelled gate, the free result, then the new prompt", got, err)
	}
}

// keptResults is an Overflow in memory: it keeps each result under
// <session>/<call id>, or fails every Keep.
type keptResults struct {
	full map[string]string
	fail bool
}

func (k *keptResults) Keep(_ context.Context, sessionID, callID, text string) (string, error) {
	if k.fail {
		return "", errors.New("store is down")
	}
	p := sessionID + "/" + callID
	k.full[p] = text
	return p, nil
}

// TestCommitRound_trimsAResultTooLargeForTheWindow: a result too large for
// the window even with nothing left to summarize is cut to fit. With an
// Overflow its full text is first kept and the cut result says where;
// without, the note says it was not kept. A Keep that fails fails the step.
func TestCommitRound_trimsAResultTooLargeForTheWindow(t *testing.T) {
	full := strings.Repeat("x", 2000) + "END"
	cases := []struct {
		name     string
		overflow *keptResults
		want     string
	}{
		{name: "kept", overflow: &keptResults{full: map[string]string{}}, want: "The full output is at trim/b1; read it with start and end."},
		{name: "not kept", want: "The full output was not kept."},
		{name: "a keep that fails", overflow: &keptResults{fail: true}, want: "stage full tool result"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			big := NewTool(ToolConfig{Name: "big", Handler: func(context.Context) (string, error) { return full, nil }})
			opts := AgentOptions{sessionID: "trim", MaxWindowSize: 500, Tools: []*Tool{big}}
			if tc.overflow != nil {
				opts.Overflow = tc.overflow
			}
			h, _, _ := roundsHarness(t, opts, toolCall("b1", "big", "{}"))

			_, err := replyRound(t, h)

			if tc.overflow != nil && tc.overflow.fail {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("commit = %v, want %q", err, tc.want)
				}
				return
			}
			window := h.context.Messages()
			result := window[len(window)-1]
			if err != nil || !strings.HasPrefix(result.Content, "xxx") || !strings.HasSuffix(result.Content, tc.want+"]") || len(result.Content) >= len(full) {
				t.Fatalf("result = %q %v, want it cut with %q", result.Content, err, tc.want)
			}
			if tc.overflow != nil && tc.overflow.full["trim/b1"] != full {
				t.Fatalf("kept = %q, want the full result", tc.overflow.full["trim/b1"])
			}
		})
	}
}

// TestRunTool_aChildResultIsTheCallsResultWhenItsEpisodeCannotBeKept: a
// specialist's result is the call's result even when the Brain cannot keep
// it as an Episode (the catalog has no Episode kind).
func TestRunTool_aChildResultIsTheCallsResultWhenItsEpisodeCannotBeKept(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{Kind: "Fact", IsParent: true}))
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewHarness(t, AgentOptions{sessionID: "episode-fails", MaxWindowSize: 8192, Model: &scriptedModel{}, Brain: eng, SearchNamespace: storetest.Namespace(t, "org", "acme")})
	call := toolCall("sp1", "spawn_specialist", "{}")
	h.context.Restore([]*message.Message{userMsg("go")})
	r := NewRound(Reply{Messages: []*message.Message{{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{call}}}, Calls: []message.ToolCall{call}})

	r.Outcomes[0], err = h.RunTool(t.Context(), ToolRun{Call: call, Child: &session.JobResult{Output: "found it"}}, drainEventCh())
	commitErr := h.Commit(t.Context(), CommitIn{Round: r}, drainEventCh())

	if got := contents(h.context.Messages()); err != nil || commitErr != nil || len(got) != 3 || got[2] != "found it" {
		t.Fatalf("window = %q %v %v, want the specialist's result committed", got, err, commitErr)
	}
}

// TestRunTool_aCallsWritesFromEachRunLandAtCommit: a call writes, parks,
// and on its run after resume writes again; a permission prompt answered
// allow-always is remembered. Neither Tool step changes the session; the
// round's Commit applies every write in order, and a later call of the
// gated tool runs without a prompt.
func TestRunTool_aCallsWritesFromEachRunLandAtCommit(t *testing.T) {
	note := NewTool(ToolConfig{Name: "note", Handler: func(_ context.Context, cc *CallContext) (string, error) {
		if _, err := cc.Park(&UserSelectionInterrupt{Options: []UserChoice{{Title: "A"}, {Title: "B"}}}); err != nil {
			return "", errors.Join(cc.StateSet("k", "before"), err)
		}
		cc.StateDelete("old")
		return "noted", errors.Join(cc.StateSet("k", "after"), cc.StateSet("new", 1))
	}})
	h, _, runs := roundsHarness(t, AgentOptions{sessionID: "writes", MaxWindowSize: 8192, Tools: []*Tool{note}})
	out := drainEventCh()
	if err := h.Commit(t.Context(), CommitIn{State: map[string]any{"old": true}, Users: []*message.Message{userMsg("go")}}, out); err != nil {
		t.Fatal(err)
	}
	calls := []message.ToolCall{toolCall("n1", "note", "{}"), toolCall("g1", "gate", "{}")}
	r := NewRound(Reply{Messages: []*message.Message{{Role: message.RoleAssistant, ToolCalls: calls}}, Calls: calls})
	answers := []string{`{"selectionIdx":1}`, `{"optionId":"allow-always"}`}

	for i, call := range calls {
		parked, err := h.RunTool(t.Context(), ToolRun{Call: call}, out)
		if err != nil || parked.Park == nil {
			t.Fatalf("%s = %+v %v, want a park", call.Name, parked, err)
		}
		if r.Outcomes[i], err = h.RunTool(t.Context(), ToolRun{Call: call, Prior: &parked, Answer: []byte(answers[i])}, out); err != nil {
			t.Fatal(err)
		}
	}
	_, written := h.session.state("k")
	commitErr := h.Commit(t.Context(), CommitIn{Round: r}, out)
	again, againErr := h.RunTool(t.Context(), ToolRun{Call: toolCall("g2", "gate", "{}")}, out)

	k, _ := h.session.state("k")
	_, old := h.session.state("old")
	n, _ := h.session.state("new")
	if written || commitErr != nil || k != "after" || old || n != float64(1) {
		t.Fatalf("before commit k set %v; after commit %v: k=%v old=%v new=%v; want the Tool steps to change nothing and every write to land", written, commitErr, k, old, n)
	}
	if againErr != nil || !again.Done || runs["gate"] != 2 {
		t.Fatalf("gate again = %+v %v after %d runs, want it to run without a prompt", again, againErr, runs["gate"])
	}
}

// TestRunTool_failsOnAnOutcomeItCannotReadOrAStateValueItCannotKeep: a
// prior outcome with a park of a type this worker does not register, or
// that is not an outcome, fails the step, and so does its Commit; a tool
// that stores a value JSON cannot encode gets the error as its result.
func TestRunTool_failsOnAnOutcomeItCannotReadOrAStateValueItCannotKeep(t *testing.T) {
	keep := NewTool(ToolConfig{Name: "keep", Handler: func(_ context.Context, cc *CallContext) (string, error) {
		return "kept", cc.StateSet("fn", func() {})
	}})
	h, _, _ := roundsHarness(t, AgentOptions{sessionID: "unreadable", MaxWindowSize: 8192, Tools: []*Tool{keep}})
	call := toolCall("c1", "gate", "{}")
	for name, record := range map[string]string{
		"not an outcome":       `[`,
		"a park not an object": `{"park":1}`,
		"an unknown park type": `{"park":{"type":"nope","data":{}}}`,
		"park data that fails": `{"park":{"type":"tool_permission","data":[]}}`,
	} {
		prior := CallOutcome{Call: call, Record: json.RawMessage(record)}
		if _, err := h.RunTool(t.Context(), ToolRun{Call: call, Prior: &prior}, drainEventCh()); err == nil {
			t.Errorf("%s: run = nil, want it to fail", name)
		}
	}
	bad := NewRound(Reply{Messages: []*message.Message{{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{call}}}, Calls: []message.ToolCall{call}})
	bad.Outcomes[0] = CallOutcome{Call: call, Done: true, Record: json.RawMessage(`[`)}
	if err := h.Commit(t.Context(), CommitIn{Round: bad}, drainEventCh()); err == nil {
		t.Error("commit of an unreadable outcome = nil, want it to fail")
	}
	o, err := h.RunTool(t.Context(), ToolRun{Call: toolCall("c2", "keep", "{}")}, drainEventCh())
	rec, _ := record(&o)
	if err != nil || !o.Done || rec.Result == nil || !strings.Contains(rec.Result.Content, `state "fn"`) {
		t.Fatalf("keep = %+v %v, want an error result naming the value", rec.Result, err)
	}
}

// TestRunTool_aSearchCursorCarriesToTheNextStepThroughCommit: a search
// leaves the rest of its results as the session's cursor; the Tool step
// saves nothing, the round's Commit keeps the cursor, and the next step's
// search with more continues it.
func TestRunTool_aSearchCursorCarriesToTheNextStepThroughCommit(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithConfig(brain.EngineConfig{DefaultLimit: 1}))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", "cursor")
	for _, title := range []string{"cursor one", "cursor two"} {
		if _, err := eng.Save(t.Context(), brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: title, Content: "cursor"}); err != nil {
			t.Fatal(err)
		}
	}
	h, _, _ := roundsHarness(t, AgentOptions{sessionID: "cursor", MaxWindowSize: 8192, Brain: eng, SearchNamespace: ns})
	h.context.Restore([]*message.Message{userMsg("go")})
	first := toolCall("s1", "search", `{"query":"cursor"}`)
	r := NewRound(Reply{Messages: []*message.Message{{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{first}}}, Calls: []message.ToolCall{first}})

	r.Outcomes[0], err = h.RunTool(t.Context(), ToolRun{Call: first}, drainEventCh())
	before, beforeErr := h.RunTool(t.Context(), ToolRun{Call: toolCall("s2", "search", `{"more":true}`)}, drainEventCh())
	commitErr := h.Commit(t.Context(), CommitIn{Round: r}, drainEventCh())
	more, moreErr := h.RunTool(t.Context(), ToolRun{Call: toolCall("s3", "search", `{"more":true}`)}, drainEventCh())

	rec, _ := record(&more)
	if err != nil || commitErr != nil || moreErr != nil || rec.Result == nil || !strings.Contains(rec.Result.Content, "cursor") {
		t.Fatalf("search %v, commit %v, more %v %+v; want the next step to continue the committed cursor", err, commitErr, moreErr, rec.Result)
	}
	if beforeRec, _ := record(&before); beforeErr != nil || !strings.Contains(beforeRec.Result.Content, "no more results") {
		t.Fatalf("more before the commit = %v %+v, want no cursor yet: the Tool step saved nothing", beforeErr, beforeRec.Result)
	}
}

// TestRunTool_callsOfOneReplyRunAtOnceOnOneHarness: two calls of one reply
// run at the same time on one harness; each sees only its own state writes,
// and both land at commit in call order.
func TestRunTool_callsOfOneReplyRunAtOnceOnOneHarness(t *testing.T) {
	var both sync.WaitGroup
	both.Add(2)
	set := NewTool(ToolConfig{Name: "set", Handler: func(_ context.Context, args struct {
		Value string `json:"value"`
	}, cc *CallContext) (string, error) {
		if err := cc.StateSet("k", args.Value); err != nil {
			return "", err
		}
		both.Done()
		both.Wait() // the other call has written k too
		seen, _ := cc.StateGet("k")
		return seen.(string), nil
	}})
	h := mustNewHarness(t, AgentOptions{MaxWindowSize: 8192, Model: &scriptedModel{}, Tools: []*Tool{set}})
	out := drainEventCh()
	if err := h.Commit(t.Context(), CommitIn{Users: []*message.Message{userMsg("go")}}, out); err != nil {
		t.Fatal(err)
	}
	calls := []message.ToolCall{toolCall("a", "set", `{"value":"a"}`), toolCall("b", "set", `{"value":"b"}`)}
	r := NewRound(Reply{Messages: []*message.Message{{Role: message.RoleAssistant, ToolCalls: calls}}, Calls: calls})

	var runs sync.WaitGroup
	errs := make([]error, len(calls))
	for i, call := range calls {
		runs.Go(func() { r.Outcomes[i], errs[i] = h.RunTool(t.Context(), ToolRun{Call: call}, out) })
	}
	runs.Wait()
	commitErr := h.Commit(t.Context(), CommitIn{Round: r}, out)
	k, _ := h.session.state("k")

	if err := errors.Join(errs[0], errs[1], commitErr); err != nil {
		t.Fatal(err)
	}
	window := h.context.Messages()
	if got := contents(window[len(window)-2:]); got[0] != "a" || got[1] != "b" {
		t.Fatalf("results = %q, want each call to see only its own write", got)
	}
	if k != "b" {
		t.Fatalf("committed k = %v, want the writes applied in call order", k)
	}
}

// TestCommit_countsTokensOnlyNearTheWindow: with the provider's count of
// the last request, a round far below the window commits without a
// CountTokens request, and one near it is counted.
func TestCommit_countsTokensOnlyNearTheWindow(t *testing.T) {
	for _, tc := range []struct {
		name        string
		inputTokens int
		wantCounts  int
	}{
		{"far below the window", 100, 0},
		{"near the window", 7000, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counts := 0
			model := &scriptedModel{
				InvokeFn: func(_ context.Context, _ []*message.Message, _ []*Tool, ch chan<- LLMResponseChunk) {
					ch <- LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: []message.ToolCall{toolCall("f1", "free", `{}`)}, InputTokens: tc.inputTokens, IsComplete: true}
				},
				CountTokensFn: func(context.Context, []*message.Message, []*Tool) (int, error) { counts++; return tc.inputTokens, nil },
			}
			free := NewTool(ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { return "free-ok", nil }})
			h := mustNewHarness(t, AgentOptions{MaxWindowSize: 8192, Model: model, Tools: []*Tool{free}})
			out := drainEventCh()
			reply, err := h.Infer(t.Context(), out)
			if err != nil {
				t.Fatal(err)
			}
			r := NewRound(reply)
			if r.Outcomes[0], err = h.RunTool(t.Context(), ToolRun{Call: reply.Calls[0]}, out); err != nil {
				t.Fatal(err)
			}
			counts = 0

			err = h.Commit(t.Context(), CommitIn{Round: r}, out)

			if err != nil || counts != tc.wantCounts {
				t.Fatalf("commit = %v with %d CountTokens requests, want %d", err, counts, tc.wantCounts)
			}
		})
	}
}

// TestSchedule_aSessionAtItsChildLimitGetsACorrection: a start past the
// runtime's limit on running children is the model's to correct; a child
// already running is not started again, so it does not count twice.
func TestSchedule_aSessionAtItsChildLimitGetsACorrection(t *testing.T) {
	h := mustNewHarness(t, AgentOptions{MaxWindowSize: 1024, Model: &scriptedModel{}})
	h.jobs = []string{"ci"}
	call := toolCall("c1", "x", `{}`)
	running := ChildID(h.sessionId, "ci", "other", false)
	cc := newCallContext(drainEventCh(), h, ToolRun{Call: call, Children: []string{running}, MaxChildren: 1}, roundCall{Call: call}, callWrites{})

	_, err := cc.Schedule(t.Context(), JobRequest{Name: "ci", Task: "build"})

	if !errors.Is(err, ErrCorrection) || !strings.Contains(err.Error(), "already runs 1 jobs") {
		t.Fatalf("schedule past the limit = %v, want a correction naming the limit", err)
	}
}

func TestRunTool_aToolFailureIsItsCallsResultInTheWindow(t *testing.T) {
	boom := NewTool(ToolConfig{Name: "boom", Handler: func(context.Context) (string, error) {
		return "", fmt.Errorf("upstream: the search provider failed: %w", ErrFailed)
	}})
	h, _, _ := roundsHarness(t, AgentOptions{sessionID: "boom", MaxWindowSize: 8192, Tools: []*Tool{boom}}, toolCall("b1", "boom", "{}"))

	_, err := replyRound(t, h)

	var result *message.Message
	for _, m := range h.context.Messages() {
		if m.Role == message.RoleTool {
			result = m
		}
	}
	if err != nil || result == nil || !strings.Contains(result.Content, "search provider failed") {
		t.Fatalf("commit %v, tool result %+v; want the failure as the call's result", err, result)
	}
}
