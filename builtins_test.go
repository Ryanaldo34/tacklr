package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
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
	t.Run("installs document and suppress effect", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		res, err := pt.create.invoke(context.Background(),
			`{"plan":"CoS: ship it","todos":[{"title":"a","status":"pending","description":"d"}]}`, rt)
		if err != nil {
			t.Fatal(err)
		}
		if res.output != "Plan created successfully" || res.disp.Effect != EffectInstallPlanDocument || !res.disp.SuppressWindowMessage {
			t.Fatalf("res = %+v", res)
		}
		if len(pt.store.Get()) != 1 || pt.store.Get()[0].Title != "a" || pt.store.Document() != "CoS: ship it" {
			t.Fatalf("store = %+v doc=%q", pt.store.Get(), pt.store.Document())
		}
	})
	t.Run("rejects active plan and empty document", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		pt.store.Set([]Todo{{Title: "existing", Status: TodoStatusInProgress}})
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
	t.Run("starts first non-completed todo", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		_, err := pt.create.invoke(context.Background(),
			`{"plan":"ship","todos":[{"title":"done","status":"completed","description":""},{"title":"next","status":"","description":"go"}]}`, rt)
		if err != nil {
			t.Fatal(err)
		}
		plan := pt.store.Get()
		if len(plan) != 2 || plan[0].Status != TodoStatusCompleted || plan[1].Status != TodoStatusInProgress {
			t.Fatalf("plan = %+v", plan)
		}
	})
}

func TestEditPlanTool_documentEffects(t *testing.T) {
	t.Run("identical rejected", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		pt.store.Set([]Todo{{Title: "a", Status: TodoStatusPending}})
		pt.store.SetDocument("same plan")
		_, err := pt.edit.invoke(context.Background(), `{"plan":"same plan","toAdd":[],"toDelete":[]}`, rt)
		if err == nil || !strings.Contains(err.Error(), "unchanged") {
			t.Fatalf("err = %v", err)
		}
		if pt.store.Document() != "same plan" || pt.store.ConsumeDocumentUpdated() {
			t.Fatal("document or updated flag wrong")
		}
	})
	t.Run("revision handoff and consumes updated flag", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		pt.store.Set([]Todo{{Title: "a", Status: TodoStatusPending}})
		pt.store.SetDocument("old")
		res, err := pt.edit.invoke(context.Background(), `{"plan":"new full plan","toAdd":[],"toDelete":[]}`, rt)
		if err != nil || res.output != "Plan edited successfully" || res.disp.Effect != EffectHandoff {
			t.Fatalf("res=%+v err=%v", res, err)
		}
		if pt.store.Document() != "new full plan" || pt.store.ConsumeDocumentUpdated() {
			t.Fatal("document or flag")
		}
	})
}

func TestEditPlanTool(t *testing.T) {
	pending := Todo{Title: "pending", Status: TodoStatusPending}
	completed := Todo{Title: "completed", Status: TodoStatusCompleted}
	inProgress := Todo{Title: "in_progress", Status: TodoStatusInProgress}

	tests := []struct {
		name    string
		plan    []Todo
		args    string
		want    string
		wantErr bool
		check   func(*testing.T, []Todo)
	}{
		{
			name: "add at beginning",
			plan: []Todo{pending, pending},
			args: `{"toAdd":[{"todo":{"title":"new","status":"pending","description":""},"order":0}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 3 || plan[0].Title != "new" {
					t.Errorf("expected [new, pending, pending], got %v", plan)
				}
			},
		},
		{
			name: "add at end",
			plan: []Todo{pending, pending},
			args: `{"toAdd":[{"todo":{"title":"new","status":"pending","description":""},"order":2}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 3 || plan[2].Title != "new" {
					t.Errorf("expected [pending, pending, new], got %v", plan)
				}
			},
		},
		{
			name: "add in middle",
			plan: []Todo{{Title: "a"}, {Title: "b"}},
			args: `{"toAdd":[{"todo":{"title":"mid","status":"pending","description":""},"order":1}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 3 || plan[1].Title != "mid" {
					t.Errorf("expected [a, mid, b], got %v", plan)
				}
			},
		},
		{
			name:    "add with negative order",
			plan:    []Todo{pending},
			args:    `{"toAdd":[{"todo":{"title":"x","status":"pending","description":""},"order":-1}]}`,
			wantErr: true,
		},
		{
			name:    "add with order past end",
			plan:    []Todo{pending},
			args:    `{"toAdd":[{"todo":{"title":"x","status":"pending","description":""},"order":2}]}`,
			wantErr: true,
		},
		{
			name: "delete pending todo",
			plan: []Todo{pending, inProgress},
			args: `{"toDelete":["pending"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 1 || plan[0].Title != "in_progress" {
					t.Errorf("expected [in_progress], got %v", plan)
				}
			},
		},
		{
			name:    "delete completed todo",
			plan:    []Todo{pending, completed, inProgress},
			args:    `{"toDelete":["completed"]}`,
			wantErr: true,
		},
		{
			name:    "delete nonexistent todo",
			plan:    []Todo{pending},
			args:    `{"toDelete":["does_not_exist"]}`,
			wantErr: true,
		},
		{
			name: "delete multiple",
			plan: []Todo{{Title: "a"}, {Title: "b"}, {Title: "c"}},
			args: `{"toDelete":["a","c"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 1 || plan[0].Title != "b" {
					t.Errorf("expected [b], got %v", plan)
				}
			},
		},
		{
			name: "add and delete combined",
			plan: []Todo{{Title: "a"}, {Title: "b"}},
			args: `{"toDelete":["a"],"toAdd":[{"todo":{"title":"c","status":"pending","description":""},"order":1}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 2 || plan[0].Title != "c" || plan[1].Title != "b" {
					t.Errorf("expected [c, b], got %v", plan)
				}
			},
		},
		{
			name:    "delete empty string title",
			plan:    []Todo{pending, inProgress},
			args:    `{"toDelete":[""]}`,
			wantErr: true,
		},
		{
			name: "add todo with empty title",
			plan: []Todo{pending},
			args: `{"toAdd":[{"todo":{"title":"","status":"pending","description":""},"order":0}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 2 || plan[0].Title != "" {
					t.Errorf("expected empty title at index 0, got %v", plan)
				}
			},
		},
		{
			name: "delete from multiple todos with same title deletes first",
			plan: []Todo{{Title: "dup"}, {Title: "dup"}, {Title: "other"}},
			args: `{"toDelete":["dup"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 2 || plan[0].Title != "dup" || plan[1].Title != "other" {
					t.Errorf("expected [dup, other], got %v", plan)
				}
			},
		},
		{
			name: "add at order equal to plan length (append)",
			plan: []Todo{{Title: "a"}, {Title: "b"}},
			args: `{"toAdd":[{"todo":{"title":"c","status":"pending","description":""},"order":2}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 3 || plan[2].Title != "c" {
					t.Errorf("expected [a, b, c], got %v", plan)
				}
			},
		},
		{
			name: "add to empty plan at order 0",
			plan: []Todo{},
			args: `{"toAdd":[{"todo":{"title":"first","status":"pending","description":""},"order":0}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 1 || plan[0].Title != "first" {
					t.Errorf("expected [first], got %v", plan)
				}
			},
		},
		{
			name:    "add to empty plan at order > 0 fails",
			plan:    []Todo{},
			args:    `{"toAdd":[{"todo":{"title":"x","status":"pending","description":""},"order":1}]}`,
			wantErr: true,
		},
		{
			name: "delete all todos from plan",
			plan: []Todo{{Title: "a"}, {Title: "b"}, {Title: "c"}},
			args: `{"toDelete":["a","b","c"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 0 {
					t.Errorf("expected empty plan, got %v", plan)
				}
			},
		},
		{
			name: "add multiple todos in sequence",
			plan: []Todo{{Title: "a"}},
			args: `{"toAdd":[{"todo":{"title":"b","status":"pending","description":""},"order":1},{"todo":{"title":"c","status":"pending","description":""},"order":2}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []Todo) {
				if len(plan) != 3 || plan[0].Title != "a" || plan[1].Title != "b" || plan[2].Title != "c" {
					t.Errorf("expected [a, b, c], got %v", plan)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := testPlanTools()
			rt := planRT()
			pt.store.Set(tt.plan)

			res, err := pt.edit.invoke(context.Background(), tt.args, rt)
			got := res.output
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if got != tt.want {
					t.Errorf("got %q, want %q", got, tt.want)
				}
				if tt.check != nil {
					tt.check(t, pt.store.Get())
				}
			}
		})
	}
}

func TestAskUserChoiceTool_raiseAndResume(t *testing.T) {
	sm := newSessionManager()
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
	var intr interrupt.Interrupt
	if !errors.As(err, &intr) {
		t.Fatalf("expected Interrupt, got %T %v", err, err)
	}
	var usi *interrupt.UserSelectionInterrupt
	if !errors.As(err, &usi) || usi.Question != "Which approach?" {
		t.Errorf("interrupt question = %+v", usi)
	}
	if !sm.HasPendingInterrupt() {
		t.Fatal("Park must write pending")
	}

	// Resolve and re-invoke (harness re-execution pattern).
	if _, err := sm.Resume("tc_ask", []byte(`{"selectionIdx":1}`)); err != nil {
		t.Fatal(err)
	}
	res, err := askUserChoiceTool.invoke(context.Background(), string(args), rt)
	got := res.output
	if err != nil {
		t.Fatal(err)
	}
	want := `User selected "Careful" — more tests`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestListPlanTool_exactListing(t *testing.T) {
	pt := testPlanTools()
	rt := planRT()
	pt.store.Set([]Todo{
		{Title: "Exact Title One", Status: TodoStatusCompleted, Description: "done work"},
		{Title: "Exact Title Two", Status: TodoStatusInProgress, Description: "now"},
		{Title: "Exact Title Three", Status: TodoStatusPending},
	})

	res, err := pt.list.invoke(context.Background(), `{}`, rt)
	got := res.output
	if err != nil {
		t.Fatal(err)
	}
	// Titles verbatim, order preserved, statuses and descriptions present.
	wantParts := []string{
		"Active plan (3 todos):",
		"1. [completed] Exact Title One",
		"Description: done work",
		"2. [in_progress] Exact Title Two",
		"Description: now",
		"3. [pending] Exact Title Three",
	}
	for _, p := range wantParts {
		if !strings.Contains(got, p) {
			t.Fatalf("list_plan output missing %q\n---\n%s", p, got)
		}
	}
	// No empty description line for the third item.
	if strings.Count(got, "Description:") != 2 {
		t.Fatalf("expected 2 description lines, got output:\n%s", got)
	}
}

func TestTurnManager_installPlanDocumentRequiresWindow(t *testing.T) {
	h := mustNewTurnManager(t, AgentOptions{Model: &mockStrategy{}, Config: Config{MaxWindowSize: 8192}})
	t.Cleanup(h.Close)
	h.session.Plan.SetDocument("PROJECT PLAN")
	err := h.applyBatchToolResultEffect(context.Background(), EffectInstallPlanDocument)
	if err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("empty window install: %v", err)
	}
}

// TestCompleteTodo_effectsByRemainingWork: handoff only while open work remains;
// completing the last open todo (sole or after already-done siblings) is EffectNone.
func TestCompleteTodo_effectsByRemainingWork(t *testing.T) {
	rt := planRT()
	cases := []struct {
		name       string
		plan       []Todo
		title      string
		wantEffect ToolResultEffect
		wantSubstr string
	}{
		{
			name: "mid-plan advances next",
			plan: []Todo{
				{Title: "A", Status: TodoStatusInProgress},
				{Title: "B", Description: "next", Status: TodoStatusPending},
			},
			title:      "A",
			wantEffect: EffectHandoff,
			wantSubstr: `starting "B"`,
		},
		{
			name: "last open with completed siblings",
			plan: []Todo{
				{Title: "A", Status: TodoStatusInProgress},
				{Title: "B", Status: TodoStatusCompleted},
				{Title: "C", Status: TodoStatusCompleted},
			},
			title:      "A",
			wantEffect: EffectNone,
			wantSubstr: "All todos completed",
		},
		{
			name: "sole item",
			plan: []Todo{
				{Title: "Only", Status: TodoStatusInProgress},
			},
			title:      "Only",
			wantEffect: EffectNone,
			wantSubstr: "All todos completed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pt := testPlanTools()
			pt.store.Set(append([]Todo(nil), tc.plan...))
			args, _ := json.Marshal(map[string]string{"title": tc.title})
			res, err := pt.complete.invoke(context.Background(), string(args), rt)
			if err != nil {
				t.Fatal(err)
			}
			if res.disp.Effect != tc.wantEffect {
				t.Fatalf("effect = %v, want %v", res.disp.Effect, tc.wantEffect)
			}
			if !strings.Contains(res.output, tc.wantSubstr) {
				t.Fatalf("output = %q, want substr %q", res.output, tc.wantSubstr)
			}
		})
	}
}
