package tacklr

import (
	"strings"
	"testing"
)

// TestFallbackHandoffContent_listsEachTodoOfThePlan: a handoff made from
// the plan says it is work in progress and lists each todo with its
// status and description.
func TestFallbackHandoffContent_listsEachTodoOfThePlan(t *testing.T) {
	t.Parallel()

	got := fallbackHandoffContent([]Todo{{Title: "First", Status: TodoStatusCompleted}, {Title: "Ship", Status: TodoStatusInProgress, Description: "finish"}})

	if !strings.Contains(got, "work in progress") || !strings.Contains(got, "- [completed] First: \n- [in_progress] Ship: finish\n") {
		t.Fatalf("fallback handoff = %q, want each todo listed", got)
	}
}
