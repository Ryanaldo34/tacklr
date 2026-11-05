package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// TestActivities_commitIsTheOnlyWriterAndAppliesItsInputOnce: Tool steps
// of two calls that change shared state at once save nothing and return
// small outcomes, each call's result kept once in the store (a Tool step
// run again leaves the same one record); the Commit of their round reads the
// results back and applies both calls' writes in call order; and a Commit
// run again after it saved (its result lost) finds the snapshot past its
// base and applies nothing again.
func TestActivities_commitIsTheOnlyWriterAndAppliesItsInputOnce(t *testing.T) {
	// Arrange
	calls := []message.ToolCall{{ID: "a", CallID: "a", Name: "set", Arguments: `{"v":"a"}`}, {ID: "b", CallID: "b", Name: "set", Arguments: `{"v":"b"}`}}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: calls}
	})
	set := tacklr.NewTool(tacklr.ToolConfig{Name: "set", Handler: func(_ context.Context, args struct {
		V string `json:"v"`
	}, cc *tacklr.CallContext) (string, error) {
		if err := cc.StateSet("by-"+args.V, true); err != nil {
			return "", err
		}
		return "set", cc.StateSet("last", args.V)
	}})
	acts := newActs(tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{set}}, session.NewMemoryEventLog(), true)
	id := session.ID("writer")
	rev, err := acts.Commit(t.Context(), commitInput{stepSession: stepSession{SessionID: id}, In: tacklr.CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: "go"}}}})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := acts.Inference(t.Context(), stepSession{SessionID: id})
	if err != nil {
		t.Fatal(err)
	}
	round := tacklr.NewRound(reply)
	for i, call := range reply.Calls {
		if round.Calls[i], err = acts.Tool(t.Context(), toolInput{stepSession: stepSession{SessionID: id}, Call: tacklr.NewCall(call, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	retried, err := acts.Tool(t.Context(), toolInput{stepSession: stepSession{SessionID: id}, Call: tacklr.NewCall(reply.Calls[0], 0)})
	if err != nil {
		t.Fatal(err)
	}
	bodies, err := acts.Store.Calls(t.Context(), id, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	ran, _ := json.Marshal(round.Calls[0])
	rerun, _ := json.Marshal(retried)
	if string(rerun) != string(ran) || retried.Body != nil || !retried.Done || len(bodies) != 2 {
		t.Fatalf("call %+v, again %+v, bodies %d; want small calls whose bodies are stored, the same again, one body per call", round.Calls[0], retried, len(bodies))
	}
	_, afterTools, err := acts.Store.Load(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	in := commitInput{stepSession: stepSession{SessionID: id}, Base: rev, In: tacklr.CommitIn{Round: round}}

	// Act
	committed, firstErr := acts.Commit(t.Context(), in)
	again, againErr := acts.Commit(t.Context(), in)

	// Assert
	cp, _, err := acts.Store.Load(t.Context(), id)
	if err != nil || firstErr != nil || againErr != nil {
		t.Fatal(err, firstErr, againErr)
	}
	if afterTools != rev || again != committed {
		t.Fatalf("revisions: after the commit %s, tools %s, commit %s, again %s; want tools to save nothing and the second commit to apply nothing", rev, afterTools, committed, again)
	}
	state, results := map[string]string{}, 0
	for _, row := range cp {
		switch row.Kind {
		case "state":
			state[row.Key] = string(row.Value)
		case "message":
			results += strings.Count(string(row.Value), `"content":"set"`)
		}
	}
	if !maps.Equal(state, map[string]string{"by-a": "true", "by-b": "true", "last": `"b"`}) {
		t.Fatalf("checkpoint state %v; want both calls' writes, the later call's last", state)
	}
	if results != 2 {
		t.Fatalf("checkpoint has %d tool results, want the round committed once", results)
	}
}

// TestActivityError_retriesOnlyWhatCanClear: a network error, a refusal and
// a stale checkpoint retry. A cancel is a cancel, not a failure. Anything
// else fails on its first attempt. A refusal and the token limit cross the
// boundary typed with their kind. The test workflow environment does not
// cancel a running step, so this small test holds the mapping.
func TestActivityError_retriesOnlyWhatCanClear(t *testing.T) {
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
		"no error":           {ctx: t.Context()},
		"a network error":    {ctx: t.Context(), err: fmt.Errorf("dial: %w", tacklr.ErrNetwork), retry: true},
		"a refusal":          {ctx: t.Context(), err: session.ErrModelRefused, retry: true, kind: session.FailRefused},
		"the token limit":    {ctx: t.Context(), err: session.ErrMaxTokens, permanent: true, kind: session.FailMaxTokens},
		"a stale checkpoint": {ctx: t.Context(), err: session.ErrStaleCheckpoint, retry: true},
		"a cancelled step":   {ctx: cancelled, err: errors.New("stopped"), canceled: true},
		"a missing api key":  {ctx: t.Context(), err: tacklr.ErrApiKeyNotSet, permanent: true},
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
