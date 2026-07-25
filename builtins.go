package tacklr

import (
	"context"
	"fmt"
	"slices"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

type createTodosArgs struct {
	Todos []control.Todo `json:"todos"`
}

type todoEdit struct {
	Todo  control.Todo `json:"todo"`
	Order int          `json:"order"`
}

type editTodosArgs struct {
	ToDelete []string   `json:"toDelete"`
	ToAdd    []todoEdit `json:"toAdd"`
}

type completeTodoArgs struct {
	Title string `json:"title"`
}

var createPlanTool = NewTool(ToolConfig{
	Name:        "create_plan",
	DisplayName: "Create Plan",
	Description: "Creates a plan in the form of a task or todo list to logically break up a complex task into smaller, manageable steps. Must be called before any real work begins after the planning process.",
	Category:    streaming.ToolCategoryThink,
	Handler: func(ctx context.Context, args createTodosArgs, runtime control.HarnessRuntime) (string, error) {
		runtime.PlanSet(args.Todos)
		return "Plan created successfully", nil
	},
})

var completeTodoTool = NewTool(ToolConfig{
	Name:        "complete_todo",
	DisplayName: "Complete Todo",
	Description: "Marks a todo as completed. Cannot complete a todo that is already completed or not found in the plan.",
	Category:    streaming.ToolCategoryThink,
	Handler: func(ctx context.Context, args completeTodoArgs, runtime control.HarnessRuntime) (string, error) {
		plan := runtime.PlanGet()
		if plan == nil {
			return "", fmt.Errorf("no plan exists")
		}
		for i, todo := range plan {
			if todo.Title == args.Title {
				if todo.Status == streaming.TodoStatusCompleted {
					return "", fmt.Errorf("todo %q is already completed", args.Title)
				}
				plan[i].Status = streaming.TodoStatusCompleted
				// Start next todo
				if len(plan)-1 > i {
					if plan[i+1].Status == streaming.TodoStatusCompleted {
						j := i + 2
						for j < len(plan) {
							if plan[j].Status != streaming.TodoStatusCompleted {
								plan[j].Status = streaming.TodoStatusInProgress
								runtime.PlanSet(plan)
								return fmt.Sprintf("Todo completed successfully, now starting %q with description: %q", plan[j].Title, plan[j].Description), nil
							}
							j++
						}
						runtime.PlanSet(plan)
						return "All todos completed successfully", nil
					}
					plan[i+1].Status = streaming.TodoStatusInProgress
					runtime.PlanSet(plan)
					return fmt.Sprintf("Todo completed successfully, now starting %q with description: %q", plan[i+1].Title, plan[i+1].Description), nil
				}
				runtime.PlanSet(plan)
				return "All todos completed successfully", nil
			}
		}
		return "", fmt.Errorf("todo %q not found in plan", args.Title)
	},
})

var editPlanTool = NewTool(ToolConfig{
	Name:        "edit_plan",
	DisplayName: "Edit Plan",
	Description: "Edits an existing plan by removing specified todos and/or adding new ones at a given position. Cannot delete or edit completed todos.",
	Category:    streaming.ToolCategoryThink,
	Handler: func(ctx context.Context, args editTodosArgs, runtime control.HarnessRuntime) (string, error) {
		plan := runtime.PlanGet()
		if plan == nil {
			return "", fmt.Errorf("no plan exists")
		}

		for _, todo := range args.ToAdd {
			if todo.Order < 0 || todo.Order > len(plan) {
				return "", fmt.Errorf("order %d is out of bounds (plan has %d items)", todo.Order, len(plan))
			}
			plan = slices.Insert(plan, todo.Order, todo.Todo)
		}

		for _, title := range args.ToDelete {
			found := false
			for i, t := range plan {
				if t.Title == title {
					if t.Status == streaming.TodoStatusCompleted {
						return "", fmt.Errorf("cannot delete completed todo: %q", title)
					}
					plan = slices.Delete(plan, i, i+1)
					found = true
					break
				}
			}
			if !found {
				return "", fmt.Errorf("todo %q not found in plan", title)
			}
		}
		runtime.PlanSet(plan)
		return "Plan edited successfully", nil
	},
})