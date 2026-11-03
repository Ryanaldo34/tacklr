package tacklr

import (
	"fmt"
	"strings"
	"testing"
)

// TestCheckpoint_failsOnStateItCannotKeepOrABadCheckpoint: a user state value
// JSON cannot hold fails the checkpoint instead of losing it, and restore
// refuses a checkpoint that is empty, corrupt, of another version, or holds
// an invalid window.
func TestCheckpoint_failsOnStateItCannotKeepOrABadCheckpoint(t *testing.T) {
	h := mustNewHarness(t, AgentOptions{Model: &scriptedModel{}})
	h.session.setState(map[string]any{"bad": make(chan int)})
	v := checkpointVersion

	_, saveErr := h.Checkpoint()

	if saveErr == nil || !strings.Contains(saveErr.Error(), "checkpoint") {
		t.Fatalf("checkpoint = %v, want an encoding error", saveErr)
	}
	for name, raw := range map[string]string{
		"empty":             "",
		"corrupt":           fmt.Sprintf(`{"version":%d,"window":"x"}`, v),
		"other version":     fmt.Sprintf(`{"version":%d,"window":[]}`, v-1),
		"an invalid window": fmt.Sprintf(`{"version":%d,"window":[{"role":"tool","content":"x"}]}`, v),
	} {
		if err := h.RestoreCheckpoint([]byte(raw)); err == nil {
			t.Errorf("%s checkpoint was accepted", name)
		}
	}
}
