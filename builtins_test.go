package tacklr

import (
	"context"
	"testing"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

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
			rt.Plan = tt.plan

			got, err := editPlanTool.Invoke(context.Background(), tt.args, &rt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if got != tt.want {
					t.Errorf("got %q, want %q", got, tt.want)
				}
				if tt.check != nil {
					tt.check(t, rt.Plan)
				}
			}
		})
	}
}