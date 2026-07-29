package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestCreatePlanTool_rejectsWhenActivePlanExists(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	rt.PlanSet([]control.Todo{
		{Title: "existing", Status: streaming.TodoStatusInProgress},
	})

	_, err := createPlanTool.Invoke(context.Background(), `{"todos":[{"title":"new","status":"pending","description":""}]}`, rt)
	if err == nil {
		t.Fatal("expected error when create_plan is called with an active plan")
	}
	if !strings.Contains(err.Error(), "active plan already exists") {
		t.Fatalf("error = %v, want mention of active plan", err)
	}
	// Original plan must be unchanged.
	plan := rt.PlanGet()
	if len(plan) != 1 || plan[0].Title != "existing" {
		t.Fatalf("plan = %v, want original single todo", plan)
	}
}

func TestCreatePlanTool_createsWhenNoPlan(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()

	got, err := createPlanTool.Invoke(context.Background(), `{"todos":[{"title":"a","status":"pending","description":"d"}]}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if got != "Plan created successfully" {
		t.Fatalf("got %q", got)
	}
	plan := rt.PlanGet()
	if len(plan) != 1 || plan[0].Title != "a" {
		t.Fatalf("plan = %v", plan)
	}
}

func TestEditPlanTool(t *testing.T) {
	pending := control.Todo{Title: "pending", Status: streaming.TodoStatusPending}
	completed := control.Todo{Title: "completed", Status: streaming.TodoStatusCompleted}
	inProgress := control.Todo{Title: "in_progress", Status: streaming.TodoStatusInProgress}

	tests := []struct {
		name    string
		plan    []control.Todo
		args    string
		want    string
		wantErr bool
		check   func(*testing.T, []control.Todo)
	}{
		{
			name: "add at beginning",
			plan: []control.Todo{pending, pending},
			args: `{"toAdd":[{"todo":{"title":"new","status":"pending","description":""},"order":0}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 3 || plan[0].Title != "new" {
					t.Errorf("expected [new, pending, pending], got %v", plan)
				}
			},
		},
		{
			name: "add at end",
			plan: []control.Todo{pending, pending},
			args: `{"toAdd":[{"todo":{"title":"new","status":"pending","description":""},"order":2}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 3 || plan[2].Title != "new" {
					t.Errorf("expected [pending, pending, new], got %v", plan)
				}
			},
		},
		{
			name: "add in middle",
			plan: []control.Todo{{Title: "a"}, {Title: "b"}},
			args: `{"toAdd":[{"todo":{"title":"mid","status":"pending","description":""},"order":1}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 3 || plan[1].Title != "mid" {
					t.Errorf("expected [a, mid, b], got %v", plan)
				}
			},
		},
		{
			name:    "add with negative order",
			plan:    []control.Todo{pending},
			args:    `{"toAdd":[{"todo":{"title":"x","status":"pending","description":""},"order":-1}]}`,
			wantErr: true,
		},
		{
			name:    "add with order past end",
			plan:    []control.Todo{pending},
			args:    `{"toAdd":[{"todo":{"title":"x","status":"pending","description":""},"order":2}]}`,
			wantErr: true,
		},
		{
			name: "delete pending todo",
			plan: []control.Todo{pending, inProgress},
			args: `{"toDelete":["pending"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 1 || plan[0].Title != "in_progress" {
					t.Errorf("expected [in_progress], got %v", plan)
				}
			},
		},
		{
			name:    "delete completed todo",
			plan:    []control.Todo{pending, completed, inProgress},
			args:    `{"toDelete":["completed"]}`,
			wantErr: true,
		},
		{
			name:    "delete nonexistent todo",
			plan:    []control.Todo{pending},
			args:    `{"toDelete":["does_not_exist"]}`,
			wantErr: true,
		},
		{
			name: "delete multiple",
			plan: []control.Todo{{Title: "a"}, {Title: "b"}, {Title: "c"}},
			args: `{"toDelete":["a","c"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 1 || plan[0].Title != "b" {
					t.Errorf("expected [b], got %v", plan)
				}
			},
		},
		{
			name: "add and delete combined",
			plan: []control.Todo{{Title: "a"}, {Title: "b"}},
			args: `{"toDelete":["a"],"toAdd":[{"todo":{"title":"c","status":"pending","description":""},"order":1}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 2 || plan[0].Title != "c" || plan[1].Title != "b" {
					t.Errorf("expected [c, b], got %v", plan)
				}
			},
		},
		{
			name:    "delete empty string title",
			plan:    []control.Todo{pending, inProgress},
			args:    `{"toDelete":[""]}`,
			wantErr: true,
		},
		{
			name: "add todo with empty title",
			plan: []control.Todo{pending},
			args: `{"toAdd":[{"todo":{"title":"","status":"pending","description":""},"order":0}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 2 || plan[0].Title != "" {
					t.Errorf("expected empty title at index 0, got %v", plan)
				}
			},
		},
		{
			name: "delete from multiple todos with same title deletes first",
			plan: []control.Todo{{Title: "dup"}, {Title: "dup"}, {Title: "other"}},
			args: `{"toDelete":["dup"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 2 || plan[0].Title != "dup" || plan[1].Title != "other" {
					t.Errorf("expected [dup, other], got %v", plan)
				}
			},
		},
		{
			name: "add at order equal to plan length (append)",
			plan: []control.Todo{{Title: "a"}, {Title: "b"}},
			args: `{"toAdd":[{"todo":{"title":"c","status":"pending","description":""},"order":2}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 3 || plan[2].Title != "c" {
					t.Errorf("expected [a, b, c], got %v", plan)
				}
			},
		},
		{
			name: "add to empty plan at order 0",
			plan: []control.Todo{},
			args: `{"toAdd":[{"todo":{"title":"first","status":"pending","description":""},"order":0}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 1 || plan[0].Title != "first" {
					t.Errorf("expected [first], got %v", plan)
				}
			},
		},
		{
			name:    "add to empty plan at order > 0 fails",
			plan:    []control.Todo{},
			args:    `{"toAdd":[{"todo":{"title":"x","status":"pending","description":""},"order":1}]}`,
			wantErr: true,
		},
		{
			name: "delete all todos from plan",
			plan: []control.Todo{{Title: "a"}, {Title: "b"}, {Title: "c"}},
			args: `{"toDelete":["a","b","c"]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 0 {
					t.Errorf("expected empty plan, got %v", plan)
				}
			},
		},
		{
			name: "add multiple todos in sequence",
			plan: []control.Todo{{Title: "a"}},
			args: `{"toAdd":[{"todo":{"title":"b","status":"pending","description":""},"order":1},{"todo":{"title":"c","status":"pending","description":""},"order":2}]}`,
			want: "Plan edited successfully",
			check: func(t *testing.T, plan []control.Todo) {
				if len(plan) != 3 || plan[0].Title != "a" || plan[1].Title != "b" || plan[2].Title != "c" {
					t.Errorf("expected [a, b, c], got %v", plan)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := HarnessRuntime{}
			rt.EnsureInitialized()
			rt.PlanSet(tt.plan)

			got, err := editPlanTool.Invoke(context.Background(), tt.args, rt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if got != tt.want {
					t.Errorf("got %q, want %q", got, tt.want)
				}
				if tt.check != nil {
					tt.check(t, rt.PlanGet())
				}
			}
		})
	}
}

func TestAskUserChoiceTool_raiseAndResume(t *testing.T) {
	rt := control.NewRuntime(make(chan streaming.StreamEvent, 4), nil, nil)
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "tc_ask"

	args, _ := json.Marshal(map[string]any{
		"question": "Which approach?",
		"choices": []map[string]any{
			{"title": "Fast", "description": "ship now", "is_recommended": true},
			{"title": "Careful", "description": "more tests"},
		},
	})

	_, err := askUserChoiceTool.Invoke(context.Background(), string(args), rt)
	if err == nil {
		t.Fatal("first invoke should raise interrupt")
	}
	var intr control.Interrupt
	if !errors.As(err, &intr) {
		t.Fatalf("expected Interrupt, got %T %v", err, err)
	}
	if q := AskUserQuestionFromState(rt, "tc_ask"); q != "Which approach?" {
		t.Errorf("question state = %q", q)
	}

	// Resolve and re-invoke (harness re-execution pattern).
	if _, err := rt.ReturnInterrupt("tc_ask", []byte(`{"selectionIdx":1}`)); err != nil {
		t.Fatal(err)
	}
	got, err := askUserChoiceTool.Invoke(context.Background(), string(args), rt)
	if err != nil {
		t.Fatal(err)
	}
	want := `User selected "Careful" — more tests`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAskUserChoiceTool_validation(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "tc"

	cases := []struct {
		name string
		args string
	}{
		{"empty question", `{"question":"","choices":[{"title":"A"},{"title":"B"}]}`},
		{"one choice", `{"question":"q","choices":[{"title":"A"}]}`},
		{"duplicate titles", `{"question":"q","choices":[{"title":"A"},{"title":"A"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := askUserChoiceTool.Invoke(context.Background(), tc.args, rt)
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestAskUserChoiceTool_injectedAsBuiltin(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
	})
	if h.findTool("ask_user_choice", "") == nil {
		t.Fatal("ask_user_choice should be injected as a builtin")
	}
}

func TestListPlanTool_noPlan(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	_, err := listPlanTool.Invoke(context.Background(), `{}`, rt)
	if err == nil {
		t.Fatal("expected error when no plan exists")
	}
	if !strings.Contains(err.Error(), "no plan exists") {
		t.Fatalf("error = %v, want no plan exists", err)
	}
}

func TestListPlanTool_exactListing(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	rt.PlanSet([]control.Todo{
		{Title: "Exact Title One", Status: streaming.TodoStatusCompleted, Description: "done work"},
		{Title: "Exact Title Two", Status: streaming.TodoStatusInProgress, Description: "now"},
		{Title: "Exact Title Three", Status: streaming.TodoStatusPending},
	})

	got, err := listPlanTool.Invoke(context.Background(), `{}`, rt)
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

func TestListPlanTool_injectedAsBuiltin(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
	})
	if h.findTool("list_plan", "") == nil {
		t.Fatal("list_plan should be injected as a builtin")
	}
}
