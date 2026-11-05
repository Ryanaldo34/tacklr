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

// TestCreatePlan_installsThePlanAndUnlocksWriteTools: before a plan, write
// tools are locked and list_plan has nothing to list. create_plan
// installs the plan document after the goal, starts the first open todo
// (leaving the rest pending), streams the todos and unlocks write tools.
// An empty document, no todos, or a second plan is the model's to correct.
func TestCreatePlan_installsThePlanAndUnlocksWriteTools(t *testing.T) {
	model, _ := scripted(t)
	mutate := tacklr.NewTool(tacklr.ToolConfig{Name: "mutate", Access: tacklr.ToolWriteAccess, Handler: func(context.Context) (string, error) { return "mutated", nil }})
	h := newHarness(t, "create", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{mutate}})
	prompt(t, h, "goal")
	before := []string{
		result(t, h, call("w0", "mutate", "{}")),
		result(t, h, call("l0", "list_plan", "{}")),
		result(t, h, call("p0", "create_plan", `{"plan":"  ","todos":[{"title":"a"}]}`)),
		result(t, h, call("p1", "create_plan", `{"plan":"valid","todos":[]}`)),
	}
	out := newStream(t)
	tc := call("p2", "create_plan", `{"plan":"CoS: ship it","todos":[{"title":"done","status":"completed"},{"title":"next","description":"go"},{"title":"later"}]}`)
	r := tacklr.NewRound(reply(tc))
	var err error
	if r.Calls[0], err = h.RunCall(t.Context(), r.Calls[0], out.out); err != nil {
		t.Fatal(err)
	}

	err = h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out.out)

	state := checkpoint(t, h)
	if !strings.Contains(before[0], "write tools are locked until create_plan") || !strings.Contains(before[1], "no plan exists") ||
		!strings.Contains(before[2], "plan document text is required") || !strings.Contains(before[3], "at least one todo") {
		t.Fatalf("before a plan = %q, want the lock, nothing to list, and each bad plan named", before)
	}
	statuses := []tacklr.TodoStatus{}
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
	listed := result(t, h, call("l1", "list_plan", "{}"))
	if !containsAll(listed, "Active plan (3 todos):", "2. [in_progress] next\n   Description: go", "3. [pending] later") || result(t, h, call("w1", "mutate", "{}")) != "mutated" ||
		!strings.Contains(result(t, h, tc), "active plan already exists") {
		t.Fatalf("list = %q, want each todo; and write tools unlocked and no second plan", listed)
	}
}

// TestEditPlan_changesTheTodosOrHandsOffARevisedPlan: edit_plan adds, then
// deletes todos; a revised document hands off with it. Each rejected edit
// names what is wrong and leaves the plan as it was.
func TestEditPlan_changesTheTodosOrHandsOffARevisedPlan(t *testing.T) {
	model, _ := scripted(t, []tacklr.LLMResponseChunk{text("the handoff")})
	h := newHarness(t, "edit", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192})
	prompt(t, h, "goal")
	noPlan := result(t, h, call("e0", "edit_plan", `{"toDelete":["a"]}`))
	if _, err := runRound(t, h, call("p", "create_plan", `{"plan":"doc","todos":[{"title":"a"},{"title":"b","status":"completed"}]}`)); err != nil {
		t.Fatal(err)
	}
	rejected := map[string]string{
		`{"plan":" doc "}`: "unchanged",
		`{"toAdd":[{"todo":{"title":"x"},"order":3}]}`: "out of bounds",
		`{"toDelete":["x"]}`:                           `todo "x" not found`,
		`{"toDelete":["a","b"]}`:                       `cannot delete completed todo: "b"`,
	}
	var got []string
	for args := range rejected {
		got = append(got, result(t, h, call("e1", "edit_plan", args)))
	}

	edited, editErr := runRound(t, h, call("e2", "edit_plan", `{"toAdd":[{"todo":{"title":"c","status":"pending"},"order":1}],"toDelete":["a"]}`))
	afterEdit := checkpoint(t, h)
	revised, revisedErr := runRound(t, h, call("e3", "edit_plan", `{"plan":"new full plan"}`))

	if !strings.Contains(noPlan, "no plan exists") {
		t.Fatalf("edit with no plan = %q", noPlan)
	}
	for _, want := range rejected {
		if !slices.ContainsFunc(got, func(g string) bool { return strings.Contains(g, want) }) {
			t.Errorf("rejected edits = %q, want one naming %q", got, want)
		}
	}
	if editErr != nil || edited.Calls[0].Body.Text() != "Plan edited successfully" || !slices.Equal(titles(afterEdit.todos), []string{"c", "b"}) || afterEdit.plan != "doc" {
		t.Fatalf("edit = %v %q with todos %v, want c added and a deleted", editErr, edited.Calls[0].Body.Text(), titles(afterEdit.todos))
	}
	window := checkpoint(t, h).window
	if revisedErr != nil || revised.Calls[0].Body.Text() != "Plan edited successfully" || len(window) != 4 || !strings.HasSuffix(window[1].Content, "new full plan") || window[2].Content != "the handoff" {
		t.Fatalf("revise = %v, window %q; want a handoff with the revised plan", revisedErr, contents(window))
	}
}

// TestPlanTools_changesOfCallsThatRanAtOnceAllLand: each call of a round
// completes a todo from the same committed plan (as Tool steps that ran at
// once do); the round's Commit replays each change in call order, so every
// completion lands, and a change that no longer applies is skipped.
func TestPlanTools_changesOfCallsThatRanAtOnceAllLand(t *testing.T) {
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
