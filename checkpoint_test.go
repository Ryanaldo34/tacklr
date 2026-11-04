package tacklr

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// TestCheckpoint_failsOnStateItCannotKeepOrABadCheckpoint: a user state value
// JSON cannot hold fails the checkpoint instead of losing it, and restore
// refuses a checkpoint that is empty, corrupt, of another version, holds an
// invalid window, or a row of a kind it does not know.
func TestCheckpoint_failsOnStateItCannotKeepOrABadCheckpoint(t *testing.T) {
	h := mustNewHarness(t, AgentOptions{Model: &scriptedModel{}})
	h.session.setState(map[string]any{"bad": make(chan int)})
	row := func(kind, key, value string) session.CheckpointEntry {
		return session.CheckpointEntry{Kind: kind, Key: key, Value: []byte(value)}
	}
	version := row(rowVersion, "", strconv.Itoa(checkpointVersion))

	_, saveErr := h.Checkpoint()

	if saveErr == nil || !strings.Contains(saveErr.Error(), "checkpoint") {
		t.Fatalf("checkpoint = %v, want an encoding error", saveErr)
	}
	for name, rows := range map[string][]session.CheckpointEntry{
		"empty":             nil,
		"corrupt":           {version, row(rowMessage, position(0), "x")},
		"other version":     {row(rowVersion, "", strconv.Itoa(checkpointVersion-1))},
		"an invalid window": {version, row(rowMessage, position(0), `{"role":"tool","content":"x"}`)},
		"an unknown kind":   {version, row("bogus", "", "x")},
	} {
		if err := h.RestoreCheckpoint(rows); err == nil {
			t.Errorf("%s checkpoint was accepted", name)
		}
	}
}

// TestCheckpoint_aCommitChangesOnlyTheRowsItTouched: a checkpoint is rows,
// so a commit that appends to the window and sets one state key changes
// only those rows; the rest stay as they were saved.
func TestCheckpoint_aCommitChangesOnlyTheRowsItTouched(t *testing.T) {
	h := mustNewHarness(t, AgentOptions{Model: &scriptedModel{}})
	out := drainEventCh()
	commit := func(in CommitIn) []session.CheckpointEntry {
		t.Helper()
		if err := h.Commit(t.Context(), in, out); err != nil {
			t.Fatal(err)
		}
		rows, err := h.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	first := commit(CommitIn{Users: []*message.Message{userMsg("one")}, State: map[string]any{"tier": "free"}})

	changes, _ := session.Diff(session.DigestOf(first), commit(CommitIn{Users: []*message.Message{userMsg("two")}, State: map[string]any{"tier": "pro"}}))

	var changed []string
	for _, c := range changes {
		changed = append(changed, c.Kind+"/"+c.Key)
	}
	if want := []string{rowMessage + "/" + position(1), rowState + "/tier"}; !slices.Equal(changed, want) {
		t.Fatalf("changed rows %v; want the new message and the changed key only", changed)
	}
}
