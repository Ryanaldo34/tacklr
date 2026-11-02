package tacklr

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestCheckpoint_aValueThatCannotBeEncodedFailsTheCheckpoint: a user state
// value JSON cannot hold fails the checkpoint instead of losing it.
func TestCheckpoint_aValueThatCannotBeEncodedFailsTheCheckpoint(t *testing.T) {
	h := mustNewTurnManager(t, AgentOptions{Model: &scriptedModel{}})
	h.session.setState(map[string]any{"bad": make(chan int)})

	_, err := h.Checkpoint()

	if err == nil || !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("checkpoint = %v, want an encoding error", err)
	}
}

// TestSessionCheckpoint_aZeroCheckpointIsNull: a session never saved
// encodes as null, and null decodes back to it, which does not restore.
func TestSessionCheckpoint_aZeroCheckpointIsNull(t *testing.T) {
	raw, err := json.Marshal(SessionCheckpoint{})
	var back SessionCheckpoint
	decodeErr := json.Unmarshal([]byte("null"), &back)
	restoreErr := mustNewTurnManager(t, AgentOptions{Model: &scriptedModel{}}).RestoreCheckpoint(back)

	if err != nil || string(raw) != "null" || decodeErr != nil || restoreErr == nil {
		t.Fatalf("zero = %s %v, decode %v, restore %v", raw, err, decodeErr, restoreErr)
	}
}
