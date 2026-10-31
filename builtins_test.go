package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

// planToolsFixture holds builtin plan tools sharing one PlanStore.
type planToolsFixture struct {
	create, edit, complete, list *Tool
	store                        *planStore
}

func testPlanTools() planToolsFixture {
	sm := newSessionManager()
	return planToolsFixture{
		create:   newCreatePlanTool(sm),
		edit:     newEditPlanTool(sm),
		complete: newCompleteTodoTool(sm),
		list:     newListPlanTool(sm),
		store:    sm.Plan,
	}
}

func drainEventCh() chan StreamEvent {
	c := make(chan StreamEvent, 8)
	go func() {
		for range c {
		}
	}()
	return c
}

// planRT is a turn Runtime for plan/ask tool unit tests (events drained).
func planRT() HarnessRuntime {
	return newToolRuntime(drainEventCh(), newSessionManager(), nil)
}

// TestCreatePlanTool covers create_plan success and rejection return paths.
func TestCreatePlanTool(t *testing.T) {
	t.Run("installs the plan document", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		res, err := pt.create.invoke(context.Background(),
			`{"plan":"CoS: ship it","todos":[{"title":"a","status":"pending","description":"d"}]}`, rt)
		if err != nil {
			t.Fatal(err)
		}
		if res.Output != "Plan created successfully" || res.Effect != EffectInstallPlanDocument {
			t.Fatalf("res = %+v", res)
		}
		if len(pt.store.Get()) != 1 || pt.store.Get()[0].Title != "a" || pt.store.Document() != "CoS: ship it" {
			t.Fatalf("store = %+v doc=%q", pt.store.Get(), pt.store.Document())
		}
	})
	t.Run("rejects active plan and empty document", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		pt.store.set([]Todo{{Title: "existing", Status: TodoStatusInProgress}})
		_, err := pt.create.invoke(context.Background(),
			`{"plan":"draft","todos":[{"title":"new","status":"pending","description":""}]}`, rt)
		if err == nil || !strings.Contains(err.Error(), "active plan already exists") {
			t.Fatalf("err = %v", err)
		}
		if len(pt.store.Get()) != 1 || pt.store.Get()[0].Title != "existing" {
			t.Fatalf("plan mutated: %+v", pt.store.Get())
		}
		pt2, rt2 := testPlanTools(), planRT()
		_, err = pt2.create.invoke(context.Background(),
			`{"plan":"  ","todos":[{"title":"a","status":"pending","description":""}]}`, rt2)
		if err == nil || !strings.Contains(err.Error(), "plan document text is required") {
			t.Fatalf("err = %v", err)
		}
		_, err = pt2.create.invoke(context.Background(),
			`{"plan":"valid","todos":[]}`, rt2)
		if err == nil || !strings.Contains(err.Error(), "at least one todo") {
			t.Fatalf("empty todos err = %v", err)
		}
	})
	t.Run("starts the first open todo and leaves the rest pending", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		_, err := pt.create.invoke(context.Background(),
			`{"plan":"ship","todos":[{"title":"done","status":"completed","description":""},{"title":"next","status":"","description":"go"},{"title":"later","status":"","description":""}]}`, rt)
		if err != nil {
			t.Fatal(err)
		}
		plan := pt.store.Get()
		if len(plan) != 3 || plan[0].Status != TodoStatusCompleted || plan[1].Status != TodoStatusInProgress || plan[2].Status != TodoStatusPending {
			t.Fatalf("plan = %+v", plan)
		}
	})
}

// TestEditPlanTool covers each edit_plan return path. A rejected edit
// leaves the plan as it was.
func TestEditPlanTool(t *testing.T) {
	ab := []Todo{{Title: "a", Status: TodoStatusPending}, {Title: "b", Status: TodoStatusCompleted}}
	cases := []struct {
		name       string
		plan       []Todo
		args       string
		wantTitles []string
		wantEffect ToolResultEffect
		wantErr    string
	}{
		{name: "adds, then deletes", plan: ab, args: `{"toAdd":[{"todo":{"title":"c","status":"pending"},"order":1}],"toDelete":["a"]}`, wantTitles: []string{"c", "b"}},
		{name: "a revised document hands off", plan: ab, args: `{"plan":"new full plan"}`, wantTitles: []string{"a", "b"}, wantEffect: EffectHandoff},
		{name: "no plan", args: `{"toDelete":["a"]}`, wantErr: "no plan exists"},
		{name: "an unchanged document", plan: ab, args: `{"plan":" doc "}`, wantErr: "unchanged"},
		{name: "an order out of bounds", plan: ab, args: `{"toAdd":[{"todo":{"title":"x"},"order":3}]}`, wantErr: "out of bounds"},
		{name: "a missing title", plan: ab, args: `{"toDelete":["x"]}`, wantErr: `todo "x" not found`},
		{name: "a completed todo", plan: ab, args: `{"toDelete":["a","b"]}`, wantErr: `cannot delete completed todo: "b"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pt := testPlanTools()
			if tc.plan != nil {
				pt.store.set(slices.Clone(tc.plan))
				pt.store.setDocument("doc")
			}

			res, err := pt.edit.invoke(context.Background(), tc.args, planRT())

			titles := []string{}
			for _, todo := range pt.store.Get() {
				titles = append(titles, todo.Title)
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !slices.Equal(pt.store.Get(), tc.plan) {
					t.Fatalf("err = %v, plan %v, want %q and the plan unchanged", err, titles, tc.wantErr)
				}
				return
			}
			if err != nil || res.Output != "Plan edited successfully" || res.Effect != tc.wantEffect || !slices.Equal(titles, tc.wantTitles) {
				t.Fatalf("res = %+v %v, plan %v, want %v with effect %v", res, err, titles, tc.wantTitles, tc.wantEffect)
			}
		})
	}
}

func TestAskUserChoiceTool_raiseAndResume(t *testing.T) {
	sm := newSessionManager()
	sm.round = newOpenRound(nil, []ToolCall{{ID: "tc_ask", CallID: "tc_ask", Name: "ask_user_choice"}})
	rt := newToolRuntime(make(chan StreamEvent, 4), sm, nil).WithToolCallID("tc_ask")

	args, _ := json.Marshal(map[string]any{
		"question": "Which approach?",
		"choices": []map[string]any{
			{"title": "Fast", "description": "ship now", "is_recommended": true},
			{"title": "Careful", "description": "more tests"},
		},
	})

	_, err := askUserChoiceTool.invoke(context.Background(), string(args), rt)
	if err == nil {
		t.Fatal("first invoke should raise interrupt")
	}
	var intr Interrupt
	if !errors.As(err, &intr) {
		t.Fatalf("expected Interrupt, got %T %v", err, err)
	}
	var usi *UserSelectionInterrupt
	if !errors.As(err, &usi) || usi.Question != "Which approach?" {
		t.Errorf("interrupt question = %+v", usi)
	}
	// Answer and run again, as the harness does after resume.
	if err := sm.Resume("tc_ask", []byte(`{"selectionIdx":1}`)); err != nil {
		t.Fatal(err)
	}
	res, err := askUserChoiceTool.invoke(context.Background(), string(args), rt)
	got := res.Output
	if err != nil {
		t.Fatal(err)
	}
	want := `User selected "Careful" — more tests`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestCompleteTodoTool covers each complete_todo return path: a handoff
// while open work remains (past completed todos), else all done.
func TestCompleteTodoTool(t *testing.T) {
	plan := []Todo{
		{Title: "A", Status: TodoStatusInProgress},
		{Title: "B", Status: TodoStatusCompleted},
		{Title: "C", Description: "next", Status: TodoStatusPending},
	}
	cases := []struct {
		name       string
		plan       []Todo
		title      string
		wantEffect ToolResultEffect
		want       string
	}{
		{name: "open work remains", plan: plan, title: "A", wantEffect: EffectHandoff, want: `now starting "C"`},
		{name: "the last open todo", plan: plan[:2], title: "A", want: "All todos completed"},
		{name: "no plan", title: "A", want: "no plan exists"},
		{name: "a missing title", plan: plan, title: "Z", want: `todo "Z" not found`},
		{name: "an already completed todo", plan: plan, title: "B", want: `todo "B" is already completed`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pt := testPlanTools()
			if tc.plan != nil {
				pt.store.set(slices.Clone(tc.plan))
			}
			args, _ := json.Marshal(map[string]string{"title": tc.title})

			res, err := pt.complete.invoke(context.Background(), string(args), planRT())

			got := res.Output
			if err != nil {
				got = err.Error()
			}
			if !strings.Contains(got, tc.want) || res.Effect != tc.wantEffect {
				t.Fatalf("complete %s = %q with effect %v, want %q with effect %v", tc.title, got, res.Effect, tc.want, tc.wantEffect)
			}
		})
	}
}

// TestPlanTools_concurrentChangesAllLand covers tool calls of one reply that
// change the plan at once: every change lands.
func TestPlanTools_concurrentChangesAllLand(t *testing.T) {
	pt, rt := testPlanTools(), planRT()
	const n = 32
	todos := make([]Todo, n)
	for i := range todos {
		todos[i] = Todo{Title: fmt.Sprintf("t%d", i), Status: TodoStatusPending}
	}
	pt.store.set(todos)

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if _, err := pt.complete.invoke(context.Background(), fmt.Sprintf(`{"title":"t%d"}`, i), rt); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	for _, todo := range pt.store.Get() {
		if todo.Status != TodoStatusCompleted {
			t.Errorf("todo %s is %s, want every concurrent completion to land", todo.Title, todo.Status)
		}
	}
}
