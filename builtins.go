package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/interrupt"
)

type createTodosArgs struct {
	Plan  string `json:"plan" desc:"Full plaintext project plan (CoS, POS, WBS, scope, requirements). Required."`
	Todos []Todo `json:"todos" desc:"Linear todo list derived from the plan. At least one item."`
}

type todoEdit struct {
	Todo  Todo `json:"todo" desc:"Todo to insert."`
	Order int  `json:"order" desc:"0-based insertion index (0..len(plan))."`
}

type editTodosArgs struct {
	ToDelete []string   `json:"toDelete" desc:"Titles of incomplete todos to remove."`
	ToAdd    []todoEdit `json:"toAdd" desc:"Todos to insert at the given order."`
	Plan     string     `json:"plan" desc:"Optional. Full revised plaintext project plan. Omit or empty to leave the plan document unchanged. Must differ from the current plan when provided."`
}

type completeTodoArgs struct {
	Title string `json:"title" desc:"Exact todo title as stored in the plan list."`
}

type askUserChoiceOption struct {
	Title         string `json:"title" desc:"Short label shown to the user"`
	Description   string `json:"description" desc:"Optional longer explanation"`
	IsRecommended bool   `json:"is_recommended" desc:"Hint that this is the preferred option"`
}

type askUserChoiceArgs struct {
	Question string                `json:"question" desc:"What to ask the user"`
	Choices  []askUserChoiceOption `json:"choices" desc:"2 or more mutually exclusive options"`
}

var askUserChoiceTool = NewTool(ToolConfig{
	Name:        "ask_user_choice",
	DisplayName: "Ask: {question}",
	Description: "Park the turn and ask the user a multiple-choice question when you need a discrete decision before continuing. Returns the selected title (and description when one was given) after the user answers. Fails if the question is empty or fewer than two distinct choice titles are provided.",
	Category:    ToolCategoryThink,
	Handler: func(ctx context.Context, args askUserChoiceArgs, runtime HarnessRuntime) (string, error) {
		if strings.TrimSpace(args.Question) == "" {
			return "", fmt.Errorf("question is required")
		}
		if len(args.Choices) < 2 {
			return "", fmt.Errorf("at least 2 choices are required")
		}
		seen := make(map[string]struct{}, len(args.Choices))
		options := make([]interrupt.UserChoice, 0, len(args.Choices))
		for i, c := range args.Choices {
			title := strings.TrimSpace(c.Title)
			if title == "" {
				return "", fmt.Errorf("choice %d: title is required", i)
			}
			if _, ok := seen[title]; ok {
				return "", fmt.Errorf("duplicate choice title %q", title)
			}
			seen[title] = struct{}{}
			options = append(options, interrupt.UserChoice{
				Title:         title,
				Description:   c.Description,
				IsRecommended: c.IsRecommended,
			})
		}
		payload, err := json.Marshal(struct {
			Question string                 `json:"question"`
			Options  []interrupt.UserChoice `json:"options"`
		}{Question: args.Question, Options: options})
		if err != nil {
			return "", fmt.Errorf("marshal choices: %w", err)
		}

		intr, err := runtime.Park("user_selection_choice", payload)
		if err != nil {
			return "", err
		}
		usi, ok := intr.(*interrupt.UserSelectionInterrupt)
		if !ok || usi.ConfirmedChoice == nil {
			return "", fmt.Errorf("user selection missing confirmed choice")
		}
		choice := usi.ConfirmedChoice
		if choice.Description != "" {
			return fmt.Sprintf("User selected %q — %s", choice.Title, choice.Description), nil
		}
		return fmt.Sprintf("User selected %q", choice.Title), nil
	},
})

func newCreatePlanTool(sm *sessionManager) *Tool {
	return NewTool(ToolConfig{
		Name:        "create_plan",
		DisplayName: "Create Plan",
		Description: "Install the project's plan and the linear todo list derived from it. Call once at the start of multi-step work. On success returns \"Plan created successfully\", installs the plan in context, marks the first incomplete todo in-progress, and unlocks write and command tools. Fails if a plan already exists, plan text is empty, or todos is empty.",
		Category:    ToolCategoryThink,
		Handler: func(ctx context.Context, args createTodosArgs) (ToolOutcome, error) {
			if existing := sm.Plan.Get(); len(existing) > 0 {
				return ToolOutcome{}, fmt.Errorf("an active plan already exists (%d todos); use edit_plan to modify it or complete_todo to progress — do not call create_plan again", len(existing))
			}
			if strings.TrimSpace(args.Plan) == "" {
				return ToolOutcome{}, fmt.Errorf("plan document text is required")
			}
			if len(args.Todos) == 0 {
				return ToolOutcome{}, fmt.Errorf("plan must include at least one todo")
			}
			todos := make([]Todo, len(args.Todos))
			copy(todos, args.Todos)
			started := false
			for i := range todos {
				if todos[i].Status == TodoStatusCompleted {
					continue
				}
				if !started {
					todos[i].Status = TodoStatusInProgress
					started = true
				} else if todos[i].Status == "" {
					todos[i].Status = TodoStatusPending
				}
			}
			sm.Plan.SetDocument(args.Plan)
			sm.Plan.Set(todos)
			return ToolOutcome{
				Output:                "Plan created successfully",
				Effect:                EffectInstallPlanDocument,
				SuppressWindowMessage: true,
			}, nil
		},
	})
}

func newListPlanTool(sm *sessionManager) *Tool {
	return NewTool(ToolConfig{
		Name:        "list_plan",
		DisplayName: "List Plan",
		Description: "Return the active todo list exactly as stored: titles, statuses, and descriptions, in order. Call after a handoff or whenever titles or statuses are unclear so later plan edits match exactly. Returns the numbered list. Fails if no plan exists yet.",
		Category:    ToolCategoryRead,
		Handler: func(ctx context.Context, _ HarnessRuntime) (string, error) {
			plan := sm.Plan.Get()
			if len(plan) == 0 {
				return "", fmt.Errorf("no plan exists yet; call create_plan first")
			}
			var b strings.Builder
			fmt.Fprintf(&b, "Active plan (%d todos):\n", len(plan))
			for i, todo := range plan {
				fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, todo.Status, todo.Title)
				if todo.Description != "" {
					fmt.Fprintf(&b, "   Description: %s\n", todo.Description)
				}
			}
			return strings.TrimRight(b.String(), "\n"), nil
		},
	})
}

func newCompleteTodoTool(sm *sessionManager) *Tool {
	return NewTool(ToolConfig{
		Name:        "complete_todo",
		DisplayName: "Complete {title}",
		Description: "Mark a todo completed when its acceptance criteria are met. Before closing a research or discovery todo, durable findings and indexed files should already be saved so later work can find them. If open todos remain, the next incomplete item is marked in-progress, a handoff is written into context, and the return names the todo now starting. If this was the last open todo, returns that all todos completed and leaves context intact so you can give the user-facing answer. Fails if no plan exists, the title is missing from the plan, or the todo is already completed.",
		Category:    ToolCategoryEdit,
		Handler: func(ctx context.Context, args completeTodoArgs) (ToolOutcome, error) {
			plan := sm.Plan.Get()
			if plan == nil {
				return ToolOutcome{}, fmt.Errorf("no plan exists yet; call create_plan first")
			}
			// Handoff only when another open todo must be picked up. Completing the
			// final item should leave context intact so the agent can wrap up.
			handoff := func(msg string) (ToolOutcome, error) {
				return ToolOutcome{Output: msg, Effect: EffectHandoff}, nil
			}
			allDone := func(msg string) (ToolOutcome, error) {
				return ToolOutcome{Output: msg, Effect: EffectNone}, nil
			}
			for i, todo := range plan {
				if todo.Title == args.Title {
					if todo.Status == TodoStatusCompleted {
						return ToolOutcome{}, fmt.Errorf("todo %q is already completed; call list_plan and pick a pending title", args.Title)
					}
					plan[i].Status = TodoStatusCompleted
					if len(plan)-1 > i {
						if plan[i+1].Status == TodoStatusCompleted {
							j := i + 2
							for j < len(plan) {
								if plan[j].Status != TodoStatusCompleted {
									plan[j].Status = TodoStatusInProgress
									sm.Plan.Set(plan)
									return handoff(fmt.Sprintf("Todo completed successfully, now starting %q with description: %q", plan[j].Title, plan[j].Description))
								}
								j++
							}
							sm.Plan.Set(plan)
							return allDone("All todos completed successfully")
						}
						plan[i+1].Status = TodoStatusInProgress
						sm.Plan.Set(plan)
						return handoff(fmt.Sprintf("Todo completed successfully, now starting %q with description: %q", plan[i+1].Title, plan[i+1].Description))
					}
					sm.Plan.Set(plan)
					return allDone("All todos completed successfully")
				}
			}
			return ToolOutcome{}, fmt.Errorf("todo %q not found in plan; call list_plan and use an exact title", args.Title)
		},
	})
}

func newEditPlanTool(sm *sessionManager) *Tool {
	return NewTool(ToolConfig{
		Name:        "edit_plan",
		DisplayName: "Edit Plan",
		Description: "Change an existing plan's todos and, when needed, the plan document. On success returns \"Plan edited successfully\". If the plan document text changed, a handoff is written into context for remaining work. Fails if no plan exists, the plan document is unchanged, a delete title is missing, a completed todo is deleted, or order is out of bounds.",
		Category:    ToolCategoryEdit,
		Handler: func(ctx context.Context, args editTodosArgs) (ToolOutcome, error) {
			plan := sm.Plan.Get()
			if plan == nil {
				return ToolOutcome{}, fmt.Errorf("no plan exists yet; call create_plan first")
			}

			trimmedPlan := strings.TrimSpace(args.Plan)
			if trimmedPlan != "" {
				existing := strings.TrimSpace(sm.Plan.Document())
				if trimmedPlan == existing {
					return ToolOutcome{}, fmt.Errorf("plan document is unchanged; omit plan or provide a revised full plan")
				}
			}

			for _, todo := range args.ToAdd {
				if todo.Order < 0 || todo.Order > len(plan) {
					return ToolOutcome{}, fmt.Errorf("order %d is out of bounds (plan has %d items)", todo.Order, len(plan))
				}
				plan = slices.Insert(plan, todo.Order, todo.Todo)
			}

			for _, title := range args.ToDelete {
				found := false
				for i, t := range plan {
					if t.Title == title {
						if t.Status == TodoStatusCompleted {
							return ToolOutcome{}, fmt.Errorf("cannot delete completed todo: %q", title)
						}
						plan = slices.Delete(plan, i, i+1)
						found = true
						break
					}
				}
				if !found {
					return ToolOutcome{}, fmt.Errorf("todo %q not found in plan", title)
				}
			}
			sm.Plan.Set(plan)
			if trimmedPlan != "" {
				sm.Plan.SetDocument(args.Plan)
			}
			effect := EffectNone
			if sm.Plan.ConsumeDocumentUpdated() {
				effect = EffectHandoff
			}
			return ToolOutcome{Output: "Plan edited successfully", Effect: effect}, nil
		},
	})
}
