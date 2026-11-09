package orchestrator_test

import (
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/orchestrator"
)

// TestTakeCancel_takesTheFirstCancelAndThePromptsBeforeIt: the cancel and
// the prompts queued before it go; a resume before it and every wake after
// it stay, in order. A queue with no cancel is left as it is.
func TestTakeCancel_takesTheFirstCancelAndThePromptsBeforeIt(t *testing.T) {
	t.Parallel()
	prompt := func(text string) session.Wake {
		return session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: text}}
	}
	resume := session.Wake{Kind: session.WakeResume}
	cancel := session.Wake{Kind: session.WakeCancel}
	queue := []session.Wake{prompt("steer"), resume, cancel, prompt("next"), cancel}

	kept, taken, ok := orchestrator.TakeCancel(queue)

	if !ok || !slices.Equal(taken, []int{0, 2}) {
		t.Fatalf("taken = %v, ok = %v; want the steering prompt and the first cancel", taken, ok)
	}
	if len(kept) != 3 || kept[0].Kind != session.WakeResume || kept[1].Prompt.Text != "next" || kept[2].Kind != session.WakeCancel {
		t.Fatalf("kept = %+v, want the resume, the next prompt and the second cancel", kept)
	}
	none := []session.Wake{prompt("only")}
	if kept, taken, ok := orchestrator.TakeCancel(none); ok || taken != nil || len(kept) != 1 {
		t.Fatalf("a queue with no cancel = %+v %v %v, want it unchanged", kept, taken, ok)
	}
}
