package tacklr_test

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// TestCheckpoint_restoresEveryRowItSaved: a checkpoint of a session with a
// window and the provider's count of it, a plan, a permission answer, a
// search rest and user state
// restores into a new harness as the same rows; a new session's checkpoint
// restores as an empty session.
func TestCheckpoint_restoresEveryRowItSaved(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithConfig(brain.EngineConfig{DefaultLimit: 1}))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", "acme")
	for _, title := range []string{"fact one", "fact two"} {
		if _, err := eng.Save(t.Context(), brain.Scope{Namespace: ns}, brain.Object{Kind: "Fact", Title: title}); err != nil {
			t.Fatal(err)
		}
	}
	gate := tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "ok", nil }})
	model, _ := scripted(t, []tacklr.LLMResponseChunk{calls(call("g1", "gate", "{}"), call("s1", "search", `{"query":"fact"}`))})
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 100000, Brain: eng, SearchNamespace: ns, Tools: []*tacklr.Tool{gate}}
	fresh, err := newHarness(t, "rows", opts).Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	h := planned(t, "rows", opts, tacklr.Todo{Title: "check"})
	out := newStream(t).out
	asked, err := h.Infer(t.Context(), out)
	if err != nil {
		t.Fatal(err)
	}
	r := tacklr.NewRound(asked)
	parked, _ := h.RunCall(t.Context(), r.Calls[0], out)
	r.Calls[0], _ = h.RunCall(t.Context(), answered(parked, `{"optionId":"allow-always"}`), out)
	r.Calls[1], _ = h.RunCall(t.Context(), r.Calls[1], out)
	if err := h.Commit(t.Context(), tacklr.CommitIn{State: map[string]any{"tier": "pro"}, Round: r}, out); err != nil {
		t.Fatal(err)
	}
	rows, err := h.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, r := range rows {
		kinds[r.Kind] = true
	}

	restored := newHarness(t, "rows", opts)
	restoreErr := restored.RestoreCheckpoint(rows)
	again, _ := restored.Checkpoint()
	emptyErr := h.RestoreCheckpoint(fresh)
	emptied, _ := h.Checkpoint()

	if len(kinds) != 8 || restoreErr != nil || !slices.EqualFunc(again, rows, func(a, b session.CheckpointEntry) bool {
		return a.Kind == b.Kind && a.Key == b.Key && string(a.Value) == string(b.Value)
	}) {
		t.Fatalf("restore = %v giving %+v; want every row of each kind of %+v back", restoreErr, again, rows)
	}
	if emptyErr != nil || len(emptied) != 1 || emptied[0].Kind != "version" {
		t.Fatalf("restore a new session's checkpoint = %v giving %+v, want only its version row", emptyErr, emptied)
	}
}

// TestCheckpoint_failsOnAStateValueItCannotKeep: a user state value JSON
// cannot hold fails the checkpoint, naming its key, instead of losing it.
func TestCheckpoint_failsOnAStateValueItCannotKeep(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	h := newHarness(t, "bad", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})
	if err := h.Commit(t.Context(), tacklr.CommitIn{State: map[string]any{"bad": make(chan int)}}, newStream(t).out); err != nil {
		t.Fatal(err)
	}

	_, err := h.Checkpoint()

	if err == nil || !strings.Contains(err.Error(), `checkpoint state bad`) {
		t.Fatalf("checkpoint = %v, want an encoding error naming the key", err)
	}
}

// TestRestoreCheckpoint_refusesABadCheckpoint: restore refuses a
// checkpoint of no or another version, with a corrupt message or row, an
// invalid window, or a row of a kind it does not know.
func TestRestoreCheckpoint_refusesABadCheckpoint(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	good, err := newHarness(t, "good", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}).Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	with := func(kind, key, value string) []session.CheckpointEntry {
		return []session.CheckpointEntry{good[0], {Kind: kind, Key: key, Value: []byte(value)}}
	}
	for name, rows := range map[string][]session.CheckpointEntry{
		"of no version":                 nil,
		"of another version":            {{Kind: "version", Value: []byte(strconv.Itoa(5))}},
		"with a corrupt message":        with("message", "00000000", "x"),
		"with an invalid window":        with("message", "00000000", `{"role":"nobody"}`),
		"with a corrupt row":            with("todo", "00000000", "x"),
		"with a row of an unknown kind": with("bogus", "", "x"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "restore", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})

			err := h.RestoreCheckpoint(rows)

			if err == nil {
				t.Fatalf("a checkpoint %s was accepted", name)
			}
		})
	}
}

// TestValidateMessages_refusesAMessageAModelCannotRead: a nil message, a
// call result on a message that is not a tool's, a tool message with no
// call, or an unknown role is not a valid window.
func TestValidateMessages_refusesAMessageAModelCannotRead(t *testing.T) {
	t.Parallel()
	for name, m := range map[string]*message.Message{
		"nil":                        nil,
		"a user message with a call": {Role: message.RoleUser, ToolCallID: "c1"},
		"a tool result of no call":   {Role: message.RoleTool, Content: "x"},
		"an unknown role":            {Role: "nobody"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tacklr.ValidateMessages([]*message.Message{m})

			if err == nil {
				t.Fatalf("%s is a valid window", name)
			}
		})
	}
}

// TestCheckpoint_aCommitChangesOnlyTheRowsItTouched: a checkpoint is rows,
// so a commit that appends to the window and sets one state key changes
// only those rows; the rest stay as they were saved.
func TestCheckpoint_aCommitChangesOnlyTheRowsItTouched(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	h := newHarness(t, "touched", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})
	commit := func(in tacklr.CommitIn) []session.CheckpointEntry {
		t.Helper()
		if err := h.Commit(t.Context(), in, newStream(t).out); err != nil {
			t.Fatal(err)
		}
		rows, err := h.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	first := commit(tacklr.CommitIn{Users: []*message.Message{userMsg("one")}, State: map[string]any{"tier": "free"}})

	changes, _ := session.Diff(session.DigestOf(first), commit(tacklr.CommitIn{Users: []*message.Message{userMsg("two")}, State: map[string]any{"tier": "pro"}}))

	var changed []string
	for _, c := range changes {
		changed = append(changed, c.Kind+"/"+c.Key)
	}
	if want := []string{"message/00000001", "state/tier"}; !slices.Equal(changed, want) {
		t.Fatalf("changed rows %v; want the new message and the changed key only", changed)
	}
}
