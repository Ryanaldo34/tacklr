package tacklr

import (
	"strings"
	"testing"
)

// TestUserSelection_refusesABadQuestion: a blank question, fewer than two
// choices, a blank title or a title given twice is the model's to fix.
func TestUserSelection_refusesABadQuestion(t *testing.T) {
	t.Parallel()
	two := []askUserChoiceOption{{Title: "a"}, {Title: "b"}}
	for want, args := range map[string]askUserChoiceArgs{
		"question is required":        {Question: " ", Choices: two},
		"at least 2 choices":          {Question: "q", Choices: two[:1]},
		"choice 1: title is required": {Question: "q", Choices: []askUserChoiceOption{{Title: "a"}, {Title: " "}}},
		`duplicate choice title "a"`:  {Question: "q", Choices: []askUserChoiceOption{{Title: "a"}, {Title: " a "}}},
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()

			_, err := userSelection(args)

			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("userSelection = %v, want %q", err, want)
			}
		})
	}
}

// TestUserSelection_asksWithTrimmedTitles: a good question parks with each
// choice's title trimmed and its description and recommendation kept.
func TestUserSelection_asksWithTrimmedTitles(t *testing.T) {
	t.Parallel()

	got, err := userSelection(askUserChoiceArgs{Question: "Which?", Choices: []askUserChoiceOption{{Title: " Fast ", IsRecommended: true}, {Title: "Careful", Description: "more tests"}}})

	if want := []UserChoice{{Title: "Fast", IsRecommended: true}, {Title: "Careful", Description: "more tests"}}; err != nil || got.Question != "Which?" || len(got.Options) != 2 || got.Options[0] != want[0] || got.Options[1] != want[1] {
		t.Fatalf("userSelection = %+v %v, want %+v", got, err, want)
	}
}

// TestCreatePlanChange_refusesAPlanItCannotInstall: a plan with no
// document, with no todos, or over an active plan names what is wrong.
func TestCreatePlanChange_refusesAPlanItCannotInstall(t *testing.T) {
	t.Parallel()
	for want, tc := range map[string]struct {
		existing []Todo
		args     createTodosArgs
	}{
		"plan document text is required": {nil, createTodosArgs{Plan: "  ", Todos: []Todo{{Title: "a"}}}},
		"at least one todo":              {nil, createTodosArgs{Plan: "valid"}},
		"active plan already exists":     {[]Todo{{Title: "a"}}, createTodosArgs{Plan: "valid", Todos: []Todo{{Title: "b"}}}},
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()

			_, _, _, err := createPlanChange(tc.args)(tc.existing, "")

			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("create = %v, want %q", err, want)
			}
		})
	}
}

// TestEditPlanChange_refusesAnEditThatDoesNotApply: an edit with no plan,
// an unchanged document, an order out of bounds, a title not in the plan,
// or a completed todo to delete names what is wrong.
func TestEditPlanChange_refusesAnEditThatDoesNotApply(t *testing.T) {
	t.Parallel()
	plan := []Todo{{Title: "a"}, {Title: "b", Status: TodoStatusCompleted}}
	for want, tc := range map[string]struct {
		plan []Todo
		args editTodosArgs
	}{
		"no plan exists":                    {nil, editTodosArgs{ToDelete: []string{"a"}}},
		"plan document is unchanged":        {plan, editTodosArgs{Plan: " doc "}},
		"order 3 is out of bounds":          {plan, editTodosArgs{ToAdd: []todoEdit{{Todo: Todo{Title: "x"}, Order: 3}}}},
		`todo "x" not found`:                {plan, editTodosArgs{ToDelete: []string{"x"}}},
		`cannot delete completed todo: "b"`: {plan, editTodosArgs{ToDelete: []string{"a", "b"}}},
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()

			_, _, _, err := editPlanChange(tc.args)(append([]Todo(nil), tc.plan...), "doc")

			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("edit = %v, want %q", err, want)
			}
		})
	}
}

// TestCompleteTodoChange_refusesATodoItCannotComplete: a completion with
// no plan, of a title not in it, or of a completed todo names what is
// wrong.
func TestCompleteTodoChange_refusesATodoItCannotComplete(t *testing.T) {
	t.Parallel()
	plan := []Todo{{Title: "A"}, {Title: "B", Status: TodoStatusCompleted}}
	for want, tc := range map[string]struct {
		plan  []Todo
		title string
	}{
		"no plan exists":                {nil, "A"},
		`todo "Z" not found`:            {plan, "Z"},
		`todo "B" is already completed`: {plan, "B"},
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()

			_, _, _, err := completeTodoChange(completeTodoArgs{Title: tc.title})(append([]Todo(nil), tc.plan...), "doc")

			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("complete = %v, want %q", err, want)
			}
		})
	}
}

// TestFormatPlan_listsEachTodoWithItsStatusAndDescription: list_plan
// numbers the todos, each with its status, and a description under a todo
// that has one.
func TestFormatPlan_listsEachTodoWithItsStatusAndDescription(t *testing.T) {
	t.Parallel()

	got := formatPlan([]Todo{{Title: "next", Status: TodoStatusInProgress, Description: "go"}, {Title: "later", Status: TodoStatusPending}})

	if want := "Active plan (2 todos):\n1. [in_progress] next\n   Description: go\n2. [pending] later"; got != want {
		t.Fatalf("formatPlan = %q, want %q", got, want)
	}
}
