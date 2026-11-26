package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// TestActivities_aCommitRunAgainAfterItSavedAppliesNothingAgain: the
// Commit of a round whose two calls ran as their own Tool steps applies it,
// and a Commit run again after it saved (its result lost) finds the
// snapshot past its base and applies nothing again: the model's next
// request has each call's result once.
func TestActivities_aCommitRunAgainAfterItSavedAppliesNothingAgain(t *testing.T) {
	t.Parallel()
	// Arrange
	var seen []string
	model := testkit.ScriptedModel(t, func(msgs []*message.Message) tacklr.LLMResponseChunk {
		if seen = testkit.ToolResults(msgs); len(seen) > 0 {
			return testkit.Text("done")
		}
		return testkit.Calls(testkit.Call("a", "set", `{"v":"a"}`), testkit.Call("b", "set", `{"v":"b"}`))
	})
	set := tacklr.NewTool(tacklr.ToolConfig{Name: "set", Handler: func(_ context.Context, args struct {
		V string `json:"v"`
	}, cc *tacklr.CallContext) (string, error) {
		return "set " + args.V, cc.StateSet("last", args.V)
	}})
	acts := newActs(tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{set}}, session.NewMemoryEventLog(), true)
	step := stepSession{SessionID: "writer"}
	rev, err := acts.Commit(t.Context(), commitInput{stepSession: step, In: tacklr.CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: "go"}}}})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := acts.Inference(t.Context(), step)
	if err != nil {
		t.Fatal(err)
	}
	round := tacklr.NewRound(reply)
	for i, call := range reply.Calls {
		if round.Calls[i], err = acts.Tool(t.Context(), toolInput{stepSession: step, Call: tacklr.NewCall(call, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	in := commitInput{stepSession: step, Base: rev, In: tacklr.CommitIn{Round: round}}

	// Act
	committed, firstErr := acts.Commit(t.Context(), in)
	again, againErr := acts.Commit(t.Context(), in)
	_, inferErr := acts.Inference(t.Context(), step)

	// Assert
	if firstErr != nil || againErr != nil || inferErr != nil || again != committed {
		t.Fatalf("commits %v %v (revisions %s, %s), next request %v; want the second to find the first's revision", firstErr, againErr, committed, again, inferErr)
	}
	if !slices.Equal(seen, []string{"set a", "set b"}) {
		t.Fatalf("the next request's results %q; want each call's result once, in call order", seen)
	}
}

// TestActivityError_retriesOnlyWhatCanClear: a network error, a refusal and
// a stale checkpoint retry. A cancel is a cancel, not a failure. Anything
// else fails on its first attempt. A refusal, the token limit and a request
// over the context window cross the boundary typed with their kind. The test workflow environment does not
// cancel a running step, so this small test holds the mapping.
func TestActivityError_retriesOnlyWhatCanClear(t *testing.T) {
	t.Parallel()
	// Arrange
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for name, tc := range map[string]struct {
		ctx       context.Context
		err       error
		retry     bool
		canceled  bool
		permanent bool
		kind      session.FailKind
	}{
		"no error":                          {ctx: t.Context()},
		"a network error":                   {ctx: t.Context(), err: fmt.Errorf("dial: %w", tacklr.ErrNetwork), retry: true},
		"a refusal":                         {ctx: t.Context(), err: session.ErrModelRefused, retry: true, kind: session.FailRefused},
		"the token limit":                   {ctx: t.Context(), err: session.ErrMaxTokens, permanent: true, kind: session.FailMaxTokens},
		"a request over the context window": {ctx: t.Context(), err: session.ErrContextLength, permanent: true, kind: session.FailContextLength},
		"a stale checkpoint":                {ctx: t.Context(), err: session.ErrStaleCheckpoint, retry: true},
		"a cancelled step":                  {ctx: cancelled, err: errors.New("stopped"), canceled: true},
		"a missing api key":                 {ctx: t.Context(), err: tacklr.ErrApiKeyNotSet, permanent: true},
	} {
		// Act
		got := activityError(tc.ctx, tc.err)
		app, isApp := errors.AsType[*temporal.ApplicationError](got)
		permanent := isApp && app.NonRetryable()
		canceled := temporal.IsCanceledError(got)
		retry := got != nil && !canceled && !permanent
		kind := session.FailKind("")
		if isApp {
			kind = session.FailKind(app.Type())
		}
		// Assert
		if retry != tc.retry || canceled != tc.canceled || permanent != tc.permanent || kind != tc.kind {
			t.Errorf("%s = %v, want retry %v cancel %v permanent %v kind %q", name, got, tc.retry, tc.canceled, tc.permanent, tc.kind)
		}
	}
}
