package tacklr

import (
	"strings"
	"testing"
)

// TestCheckpoint_aValueThatCannotBeEncodedFailsTheCheckpoint: a user state
// value JSON cannot hold fails the checkpoint instead of losing it.
func TestCheckpoint_aValueThatCannotBeEncodedFailsTheCheckpoint(t *testing.T) {
	h := mustNewHarness(t, AgentOptions{Model: &scriptedModel{}})
	h.session.setState(map[string]any{"bad": make(chan int)})

	_, err := h.Checkpoint()

	if err == nil || !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("checkpoint = %v, want an encoding error", err)
	}
}

// TestRestoreCheckpoint_anEmptyCheckpointDoesNotRestore: a session never
// saved has no checkpoint, which does not restore.
func TestRestoreCheckpoint_anEmptyCheckpointDoesNotRestore(t *testing.T) {
	err := mustNewHarness(t, AgentOptions{Model: &scriptedModel{}}).RestoreCheckpoint(nil)

	if err == nil {
		t.Fatal("an empty checkpoint restored")
	}
}
