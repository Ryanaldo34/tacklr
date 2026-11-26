package tacklr_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

func titles(todos []tacklr.Todo) []string {
	var out []string
	for _, td := range todos {
		out = append(out, td.Title)
	}
	return out
}

// TestCreatePlan_installsThePlanAfterTheGoalAndStartsTheFirstOpenTodo:
// create_plan installs the plan document after the goal, starts the first
// open todo (leaving the rest pending) and streams the todos.
func TestCreatePlan_installsThePlanAfterTheGoalAndStartsTheFirstOpenTodo(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	h := newHarness(t, "create", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})
	prompt(t, h, "goal")
	out := newStream(t)
	r := tacklr.NewRound(reply(call("p2", "create_plan", `{"plan":"CoS: ship it","todos":[{"title":"done","status":"completed"},{"title":"next"},{"title":"later"}]}`)))
	var err error
	if r.Calls[0], err = h.RunCall(t.Context(), r.Calls[0], out.out); err != nil {
		t.Fatal(err)
	}

	err = h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out.out)

	state := checkpoint(t, h)
	var statuses []tacklr.TodoStatus
	for _, td := range state.todos {
		statuses = append(statuses, td.Status)
	}
	if err != nil || r.Calls[0].Body.Text() != "Plan created successfully" || len(state.window) != 2 || !tacklr.IsPlanDocument(state.window[1]) || !strings.HasSuffix(state.window[1].Content, "CoS: ship it") ||
		!slices.Equal(statuses, []tacklr.TodoStatus{tacklr.TodoStatusCompleted, tacklr.TodoStatusInProgress, tacklr.TodoStatusPending}) {
		t.Fatalf("create = %v %q, window %q, todos %+v; want the plan after the goal and the first open todo started", err, r.Calls[0].Body.Text(), contents(state.window), state.todos)
	}
	var streamed []tacklr.Todo
	for _, ev := range out.events() {
		if ev.Type == session.EventPlanUpdate {
			_ = json.Unmarshal(ev.Data, &streamed)
		}
	}
	if !slices.Equal(streamed, state.todos) {
		t.Fatalf("plan update = %+v, want the committed todos", streamed)
	}
}

// TestCreatePlan_unlocksWriteToolsAndThePlanList: before a plan, write
// tools are locked and list_plan has nothing to list; after it, both run.
func TestCreatePlan_unlocksWriteToolsAndThePlanList(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	mutate := tacklr.NewTool(tacklr.ToolConfig{Name: "mutate", Access: tacklr.ToolWriteAccess, Handler: func(context.Context) (string, error) { return "mutated", nil }})
	h := newHarness(t, "unlock", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{mutate}})
	prompt(t, h, "goal")
	locked, unlisted := result(t, h, call("w0", "mutate", "{}")), result(t, h, call("l0", "list_plan", "{}"))

	_, err := runRound(t, h, call("p", "create_plan", `{"plan":"doc","todos":[{"title":"a"}]}`))

	if !strings.Contains(locked, "write tools are locked until create_plan") || !strings.Contains(unlisted, "no plan exists") {
		t.Fatalf("before a plan = %q, %q; want the lock and nothing to list", locked, unlisted)
	}
	if listed := result(t, h, call("l1", "list_plan", "{}")); err != nil || result(t, h, call("w1", "mutate", "{}")) != "mutated" || !strings.HasPrefix(listed, "Active plan (1 todos):") {
		t.Fatalf("after the plan = %v, list %q; want write tools unlocked and the plan listed", err, listed)
	}
}

// TestEditPlan_addsAndDeletesTodos: edit_plan inserts a todo at its order
// and deletes another, keeping the document.
func TestEditPlan_addsAndDeletesTodos(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	h := planned(t, "edit", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}, tacklr.Todo{Title: "a"}, tacklr.Todo{Title: "b", Status: tacklr.TodoStatusCompleted})

	edited, err := runRound(t, h, call("e2", "edit_plan", `{"toAdd":[{"todo":{"title":"c","status":"pending"},"order":1}],"toDelete":["a"]}`))

	if state := checkpoint(t, h); err != nil || edited.Calls[0].Body.Text() != "Plan edited successfully" || !slices.Equal(titles(state.todos), []string{"c", "b"}) || state.plan != "ship" {
		t.Fatalf("edit = %v %q with todos %v, want c added and a deleted", err, edited.Calls[0].Body.Text(), titles(state.todos))
	}
}

// TestEditPlan_aRevisedDocumentHandsOffWithIt: an edit with a revised
// document hands off the window with the new document.
func TestEditPlan_aRevisedDocumentHandsOffWithIt(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t, []tacklr.LLMResponseChunk{text("the handoff")})
	h := planned(t, "revise", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}, tacklr.Todo{Title: "a"})

	revised, err := runRound(t, h, call("e3", "edit_plan", `{"plan":"new full plan"}`))

	if window := checkpoint(t, h).window; err != nil || revised.Calls[0].Body.Text() != "Plan edited successfully" || len(window) != 4 || !strings.HasSuffix(window[1].Content, "new full plan") || window[2].Content != "the handoff" {
		t.Fatalf("revise = %v, window %q; want a handoff with the revised plan", err, contents(window))
	}
}

// TestPlanTools_changesOfCallsThatRanAtOnceAllLand: each call of a round
// completes a todo from the same committed plan (as Tool steps that ran at
// once do); the round's Commit replays each change in call order, so every
// completion lands, and a change that no longer applies is skipped.
func TestPlanTools_changesOfCallsThatRanAtOnceAllLand(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	const n = 4
	todos := make([]tacklr.Todo, n)
	tcs := make([]message.ToolCall, n, n+1)
	for i := range n {
		todos[i] = tacklr.Todo{Title: fmt.Sprintf("t%d", i)}
		tcs[i] = call(fmt.Sprintf("c%d", i), "complete_todo", fmt.Sprintf(`{"title":"t%d"}`, i))
	}
	// A second completion of t0 succeeds on the plan it runs against; at
	// commit t0 is already completed, so that change is skipped.
	tcs = append(tcs, call("again", "complete_todo", `{"title":"t0"}`))
	h := planned(t, "plan-at-once", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}, todos...)

	_, err := runRound(t, h, tcs...)

	for _, td := range checkpoint(t, h).todos {
		if err != nil || td.Status != tacklr.TodoStatusCompleted {
			t.Errorf("todo %s is %s (commit %v), want every completion to land", td.Title, td.Status, err)
		}
	}
}
