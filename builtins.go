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

var createPlanTool = NewTool(ToolConfig{
	Name:        "create_plan",
	DisplayName: "Create Plan",
	Description: "Creates a plan in the form of a task or todo list to logically break up a complex task into smaller, manageable steps. Must be called before any real work begins after the planning process.",
	Category:    streaming.ToolCategoryThink,
	Handler: func(ctx context.Context, args createTodosArgs, runtime *control.HarnessRuntime) (string, error) {
		runtime.Plan = args.Todos
		return "Plan created successfully", nil
	},
	Access: ToolReadAccess,
})

var editPlanTool = NewTool(ToolConfig{
	Name:        "edit_plan",
	DisplayName: "Edit Plan",
	Description: "Edits an existing plan by removing specified todos and/or adding new ones at a given position. Cannot delete or edit completed todos.",
	Category:    streaming.ToolCategoryThink,
	Handler: func(ctx context.Context, args editTodosArgs, runtime *control.HarnessRuntime) (string, error) {
		for _, todo := range args.ToAdd {
			if todo.Order < 0 || todo.Order > len(runtime.Plan) {
				return "", fmt.Errorf("order %d is out of bounds (plan has %d items)", todo.Order, len(runtime.Plan))
			}
			runtime.Plan = slices.Insert(runtime.Plan, todo.Order, todo.Todo)
		}

		for _, title := range args.ToDelete {
			found := false
			for i, t := range runtime.Plan {
				if t.Title == title {
					if t.Status == streaming.TodoStatusCompleted {
						return "", fmt.Errorf("cannot delete completed todo: %q", title)
					}
					runtime.Plan = slices.Delete(runtime.Plan, i, i+1)
					found = true
					break
				}
			}
			if !found {
				return "", fmt.Errorf("todo %q not found in plan", title)
			}
		}
		return "Plan edited successfully", nil
	},
	Access: ToolWriteAccess,
})
