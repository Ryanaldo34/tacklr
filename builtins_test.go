package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"
)

// planToolsFixture holds the builtin plan tools and one session's plan.
type planToolsFixture struct {
	create, edit, complete, list *Tool
	sm                           *sessionManager
	store                        *planStore
}

func testPlanTools() planToolsFixture {
	sm := newSessionManager()
	return planToolsFixture{
		create:   newCreatePlanTool(),
		edit:     newEditPlanTool(),
		complete: newCompleteTodoTool(),
		list:     newListPlanTool(),
		sm:       sm,
		store:    sm.Plan,
	}
}

// run runs tool as one call and commits what it wrote.
func (pt planToolsFixture) run(tool *Tool, args string) (ToolOutcome, error) {
	cc := callRuntime(pt.sm, "c1")
	out, err := tool.invoke(context.Background(), args, cc)
	cc.commit()
	return out, err
}

func drainEventCh() chan session.Event {
	c := make(chan session.Event, 8)
	go func() {
		for range c {
		}
	}()
	return c
}

// TestCreatePlanTool covers create_plan success and rejection return paths.
func TestCreatePlanTool(t *testing.T) {
	t.Run("installs the plan document", func(t *testing.T) {
		pt := testPlanTools()
		if _, err := pt.run(pt.list, `{}`); err == nil || !strings.Contains(err.Error(), "no plan exists") {
			t.Fatalf("list before a plan = %v, want no plan", err)
		}
		res, err := pt.run(pt.create, `{"plan":"CoS: ship it","todos":[{"title":"a","status":"pending","description":"d"}]}`)
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
		pt := testPlanTools()
		pt.store.set([]Todo{{Title: "existing", Status: TodoStatusInProgress}})
		_, err := pt.run(pt.create, `{"plan":"draft","todos":[{"title":"new","status":"pending","description":""}]}`)
		if err == nil || !strings.Contains(err.Error(), "active plan already exists") {
			t.Fatalf("err = %v", err)
		}
		if len(pt.store.Get()) != 1 || pt.store.Get()[0].Title != "existing" {
			t.Fatalf("plan mutated: %+v", pt.store.Get())
		}
		pt2 := testPlanTools()
		_, err = pt2.run(pt2.create, `{"plan":"  ","todos":[{"title":"a","status":"pending","description":""}]}`)
		if err == nil || !strings.Contains(err.Error(), "plan document text is required") {
			t.Fatalf("err = %v", err)
		}
		_, err = pt2.run(pt2.create, `{"plan":"valid","todos":[]}`)
		if err == nil || !strings.Contains(err.Error(), "at least one todo") {
			t.Fatalf("empty todos err = %v", err)
		}
	})
	t.Run("starts the first open todo and leaves the rest pending", func(t *testing.T) {
		pt := testPlanTools()
		_, err := pt.run(pt.create, `{"plan":"ship","todos":[{"title":"done","status":"completed","description":""},{"title":"next","status":"","description":"go"},{"title":"later","status":"","description":""}]}`)
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

			res, err := pt.run(pt.edit, tc.args)

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
	cc := callRuntime(newSessionManager(), "tc_ask")
	for args, want := range map[string]string{
		`{"question":" ","choices":[{"title":"a"},{"title":"b"}]}`: "question is required",
		`{"question":"q","choices":[{"title":"a"}]}`:               "at least 2 choices",
		`{"question":"q","choices":[{"title":"a"},{"title":" "}]}`: "title is required",
		`{"question":"q","choices":[{"title":"a"},{"title":"a"}]}`: "duplicate choice title",
	} {
		if _, err := askUserChoiceTool.invoke(context.Background(), args, cc); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ask %s = %v, want %q", args, err, want)
		}
	}

	args, _ := json.Marshal(map[string]any{
		"question": "Which approach?",
		"choices": []map[string]any{
			{"title": "Fast", "description": "ship now", "is_recommended": true},
			{"title": "Careful", "description": "more tests"},
		},
	})

	_, err := askUserChoiceTool.invoke(context.Background(), string(args), cc)
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
	if err := cc.resume([]byte(`{"selectionIdx":1}`)); err != nil {
		t.Fatal(err)
	}
	res, err := askUserChoiceTool.invoke(context.Background(), string(args), cc)
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

			res, err := pt.run(pt.complete, string(args))

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

// TestPlanTools_changesOfCallsThatRanAtOnceAllLand: each call of a round
// completes a todo from the same committed plan (as Tool steps that ran at
// once do); the round's Commit replays each change in call order, so every
// completion lands, and a change that no longer applies is skipped.
func TestPlanTools_changesOfCallsThatRanAtOnceAllLand(t *testing.T) {
	h := mustNewHarness(t, AgentOptions{sessionID: "plan-at-once", MaxWindowSize: 8192, Model: &scriptedModel{}})
	t.Cleanup(h.Close)
	const n = 4
	todos := make([]Todo, n)
	calls := make([]message.ToolCall, n, n+1)
	for i := range n {
		todos[i] = Todo{Title: fmt.Sprintf("t%d", i), Status: TodoStatusPending}
		calls[i] = toolCall(fmt.Sprintf("c%d", i), "complete_todo", fmt.Sprintf(`{"title":"t%d"}`, i))
	}
	// A second completion of t0 succeeded on the plan it ran against; at
	// commit t0 is already completed, so that change is skipped.
	calls = append(calls, toolCall("again", "complete_todo", `{"title":"t0"}`))
	h.session.Plan.set(todos)
	h.context.Restore([]*message.Message{userMsg("go")})
	r := NewRound(Reply{Messages: []*message.Message{{Role: message.RoleAssistant, ToolCalls: calls}}, Calls: calls})
	for i, call := range calls {
		o, err := h.RunTool(t.Context(), ToolRun{Call: call}, drainEventCh())
		if err != nil {
			t.Fatal(err)
		}
		r.Outcomes[i] = o
	}

	err := h.Commit(t.Context(), CommitIn{Round: r}, drainEventCh())

	if err != nil {
		t.Fatal(err)
	}
	for _, todo := range h.session.Plan.Get() {
		if todo.Status != TodoStatusCompleted {
			t.Errorf("todo %s is %s, want every completion to land", todo.Title, todo.Status)
		}
	}
}
