package worker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/loop"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/workspace"
)

func TestActivities_childTurnUsesParentSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-parent"), 0o644); err != nil {
		t.Fatal(err)
	}
	var gotToken string
	open := vfs.Tree(vfs.At("docs", vfs.Local(dir)))
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok", IsComplete: true}
	}), MaxWindowSize: 8192}
	store := session.NewMemorySecretStorage()
	parentAuth := session.AuthContext{Bindings: []mount.Binding{{
		Provider: "local",
		Params:   map[string]string{mount.ParamName: "docs"},
		Auth:     mount.Credential{Token: "parent-tok"},
	}}}
	if err := store.Put(t.Context(), "parent", session.Secrets{Auth: parentAuth}); err != nil {
		t.Fatal(err)
	}
	acts := newActs(agent, session.NewMemoryEventLog(), true)
	acts.Workspace = workspace.Config{Open: func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
		if len(req.Bindings) > 0 {
			gotToken = req.Bindings[0].Auth.Token
		}
		return open(ctx, sessionID, req)
	}}
	acts.Secrets = store
	if _, err := acts.Commit(t.Context(), commitInput{
		stepSession: stepSession{SessionID: "child", Identity: loop.Identity{
			Parent: "parent",
			Mounts: adapter.ApplyAuth(nil, parentAuth),
		}},
		In: tacklr.CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: "hi"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if gotToken != "parent-tok" {
		t.Fatalf("workspace token=%q", gotToken)
	}
}

// TestActivities_commitIsTheOnlyWriterAndAppliesItsInputOnce: Tool steps
// of two calls that change shared state at once save nothing and return
// small outcomes, each call's result kept once in the store (a Tool step
// run again leaves the same one record); the Commit of their round reads the
// results back and applies both calls' writes in call order; and a Commit
// run again after it saved (its result lost) finds the snapshot past its
// base and applies nothing again.
func TestActivities_commitIsTheOnlyWriterAndAppliesItsInputOnce(t *testing.T) {
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
		if round.Outcomes[i], err = acts.Tool(t.Context(), toolInput{stepSession: stepSession{SessionID: id}, Run: tacklr.ToolRun{Call: call}}); err != nil {
			t.Fatal(err)
		}
	}
	retried, err := acts.Tool(t.Context(), toolInput{stepSession: stepSession{SessionID: id}, Run: tacklr.ToolRun{Call: reply.Calls[0]}})
	if err != nil {
		t.Fatal(err)
	}
	records, err := acts.Store.Outcomes(t.Context(), id, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retried, round.Outcomes[0]) || retried.Record != nil || !retried.Stored || len(records) != 2 {
		t.Fatalf("outcome %+v, again %+v, records %d; want small stored outcomes, the same again, one record per call", round.Outcomes[0], retried, len(records))
	}
	_, afterTools, err := acts.Store.Load(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	in := commitInput{stepSession: stepSession{SessionID: id}, Base: rev, In: tacklr.CommitIn{Round: round}}

	committed, firstErr := acts.Commit(t.Context(), in)
	again, againErr := acts.Commit(t.Context(), in)

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

// downLog is a fallback event log whose store is down: it takes no event.
type downLog struct{ session.EventLog }

var errLogDown = errors.New("event log down")

func (downLog) Append(context.Context, session.ID, string, session.Event) error {
	return errLogDown
}

// TestActivities_aStepWithNoWayToFinishFailsAtOnce: a job no worker
// registered, and an event the log cannot take, fail their step on its
// first attempt with the cause.
func TestActivities_aStepWithNoWayToFinishFailsAtOnce(t *testing.T) {
	acts := newActs(tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}, downLog{}, true)
	_, jobErr := acts.RunJob(t.Context(), runJobInput{Name: "nope"})
	emitErr := acts.EmitEvent(t.Context(), emitEventInput{SessionID: "s", Event: session.Event{Type: session.EventComplete}})
	for name, tc := range map[string]struct{ err, want error }{
		"an unregistered worker": {jobErr, tacklr.ErrNotFound},
		"a log that is down":     {emitErr, errLogDown},
	} {
		app, ok := errors.AsType[*temporal.ApplicationError](tc.err)
		if !ok || !app.NonRetryable() || !errors.Is(tc.err, tc.want) {
			t.Errorf("%s = %v, want a permanent %v", name, tc.err, tc.want)
		}
	}
}

// TestActivityError_retriesOnlyWhatCanClear: a network error, a refusal and
// a stale checkpoint retry; a cancel is a cancel, not a failure; anything
// else fails on its first attempt. A refusal and the token limit cross the
// boundary typed with their kind.
func TestActivityError_retriesOnlyWhatCanClear(t *testing.T) {
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
		got := activityError(tc.ctx, tc.err)
		app, isApp := errors.AsType[*temporal.ApplicationError](got)
		permanent := isApp && app.NonRetryable()
		canceled := temporal.IsCanceledError(got)
		retry := got != nil && !canceled && !permanent
		kind := session.FailKind("")
		if isApp {
			kind = session.FailKind(app.Type())
		}
		if retry != tc.retry || canceled != tc.canceled || permanent != tc.permanent || kind != tc.kind {
			t.Errorf("%s = %v, want retry %v cancel %v permanent %v kind %q", name, got, tc.retry, tc.canceled, tc.permanent, tc.kind)
		}
	}
}
