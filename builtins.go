package tacklr

import (
	"context"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

type todo struct {
	Title       string               `json:"title"`
	Status      streaming.TodoStatus `json:"status"`
	Description string               `json:"description"`
}

type createTodosArgs struct {
	Todos []todo `json:"todos"`
}

var createPlanTool = NewTool(ToolConfig{
	Name:        "create_plan",
	DisplayName: "Create Plan",
	Description: "Creates a plan in the form of a task or todo list to logically break up a complex task into smaller, manageable steps. Must be called before any real work begins after the planning process.",
	Category:    streaming.ToolCategoryThink,
	Handler: func(ctx context.Context, args createTodosArgs, runtime control.HarnessRuntime) (string, error) {
		runtime.StateSet("mode", "execute")
		runtime.StateSet("todos", args.Todos)
		return "Plan created successfully.", nil
	},
})
