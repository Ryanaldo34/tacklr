package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

type createTodosArgs struct {
	Plan  string         `json:"plan" desc:"Full plaintext project plan (CoS, POS, WBS, scope, requirements). Required."`
	Todos []control.Todo `json:"todos"`
}

type todoEdit struct {
	Todo  control.Todo `json:"todo"`
	Order int          `json:"order"`
}

type editTodosArgs struct {
	ToDelete []string   `json:"toDelete"`
	ToAdd    []todoEdit `json:"toAdd"`
	Plan     string     `json:"plan" desc:"Optional. Full revised plaintext project plan. Omit or empty to leave the plan document unchanged. Must differ from the current plan when provided."`
}

type completeTodoArgs struct {
	Title string `json:"title"`
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

// internalSession is the harness-internal handle closed over by built-in tools.
// User tools never receive this — only public HarnessRuntime for DI/interrupts.
type internalSession struct {
	sm *control.SessionManager
	// emitPlanTodos streams StreamEventPlanUpdate after todo list changes (nil in unit tests).
	emitPlanTodos func([]control.Todo)
}

func (s internalSession) setTodos(todos []control.Todo) {
	s.sm.Plan().Set(todos)
	if s.emitPlanTodos != nil {
		s.emitPlanTodos(todos)
	}
}

var askUserChoiceTool = NewTool(ToolConfig{
	Name:        "ask_user_choice",
	DisplayName: "Ask User Choice",
	Description: "Ask the user a multiple-choice clarification question and wait for their selection. Use when you need a discrete decision before continuing. Provide clear, mutually exclusive options.",
	Category:    streaming.ToolCategoryThink,
	Handler: func(ctx context.Context, args askUserChoiceArgs, runtime control.HarnessRuntime) (string, error) {
		if strings.TrimSpace(args.Question) == "" {
			return "", fmt.Errorf("question is required")
		}
		if len(args.Choices) < 2 {
			return "", fmt.Errorf("at least 2 choices are required")
		}
		seen := make(map[string]struct{}, len(args.Choices))
		options := make([]control.UserChoice, 0, len(args.Choices))
		for i, c := range args.Choices {
			title := strings.TrimSpace(c.Title)
			if title == "" {
				return "", fmt.Errorf("choice %d: title is required", i)
			}
			if _, ok := seen[title]; ok {
				return "", fmt.Errorf("duplicate choice title %q", title)
			}
			seen[title] = struct{}{}
			options = append(options, control.UserChoice{
				Title:         title,
				Description:   c.Description,
				IsRecommended: c.IsRecommended,
			})
		}
		optionsJSON, err := json.Marshal(options)
		if err != nil {
			return "", fmt.Errorf("marshal choices: %w", err)
		}
		if runtime.CurrentToolCallID != "" {
			runtime.StateSet(askUserQuestionStateKey(runtime.CurrentToolCallID), args.Question)
		}

		intr, err := runtime.RaiseInterrupt("user_selection_choice", optionsJSON)
		if err != nil {
			return "", err
		}
		usi, ok := intr.(*control.UserSelectionInterrupt)
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

func askUserQuestionStateKey(toolCallID string) string {
	return "_ask_user_question:" + toolCallID
}

// AskUserQuestionFromState returns a question string stashed by ask_user_choice
// for the given tool call id, if any. Takes a pointer so concurrent Run teardown
// (SetOutputChannel) does not race a by-value Runtime copy.
func AskUserQuestionFromState(rt *control.HarnessRuntime, toolCallID string) string {
	if rt == nil || toolCallID == "" {
		return ""
	}
	v, ok := rt.StateGet(askUserQuestionStateKey(toolCallID))
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func newCreatePlanTool(s internalSession) *Tool {
	return NewTool(ToolConfig{
		Name:        "create_plan",
		DisplayName: "Create Plan",
		Description: "Creates a project plan document and a linear todo list derived from it. Pass the full plaintext plan in plan and the derived todos in todos. Call only when no active plan exists. If a plan is already active, use edit_plan or complete_todo instead of create_plan.",
		Category:    streaming.ToolCategoryThink,
		Handler: func(ctx context.Context, args createTodosArgs, _ control.HarnessRuntime) (string, error) {
			if existing := s.sm.Plan().Get(); len(existing) > 0 {
				return "", fmt.Errorf("an active plan already exists (%d todos); use edit_plan to modify it or complete_todo to progress — do not call create_plan again", len(existing))
			}
			if strings.TrimSpace(args.Plan) == "" {
				return "", fmt.Errorf("plan document text is required")
			}
			if len(args.Todos) == 0 {
				return "", fmt.Errorf("plan must include at least one todo")
			}
			todos := make([]control.Todo, len(args.Todos))
			copy(todos, args.Todos)
			started := false
			for i := range todos {
				if todos[i].Status == streaming.TodoStatusCompleted {
					continue
				}
				if !started {
					todos[i].Status = streaming.TodoStatusInProgress
					started = true
				} else if todos[i].Status == "" {
					todos[i].Status = streaming.TodoStatusPending
				}
			}
			s.sm.Plan().SetDocument(args.Plan)
			s.setTodos(todos)
			return "Plan created successfully", nil
		},
	})
}

func newListPlanTool(s internalSession) *Tool {
	return NewTool(ToolConfig{
		Name:        "list_plan",
		DisplayName: "List Plan",
		Description: "Returns the active plan todo list exactly as stored (titles, statuses, descriptions, in order). Use before complete_todo or edit_plan so titles match exactly. Call after a handoff or whenever plan titles are unclear.",
		Category:    streaming.ToolCategoryThink,
		Handler: func(ctx context.Context, _ control.HarnessRuntime) (string, error) {
			plan := s.sm.Plan().Get()
			if len(plan) == 0 {
				return "", fmt.Errorf("no plan exists")
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

func newCompleteTodoTool(s internalSession) *Tool {
	return NewTool(ToolConfig{
		Name:        "complete_todo",
		DisplayName: "Complete Todo",
		Description: "Marks a todo as completed by exact title (must match list_plan / create_plan titles). Cannot complete a todo that is already completed or not found in the plan.",
		Category:    streaming.ToolCategoryThink,
		Handler: func(ctx context.Context, args completeTodoArgs, _ control.HarnessRuntime) (string, error) {
			plan := s.sm.Plan().Get()
			if plan == nil {
				return "", fmt.Errorf("no plan exists")
			}
			for i, todo := range plan {
				if todo.Title == args.Title {
					if todo.Status == streaming.TodoStatusCompleted {
						return "", fmt.Errorf("todo %q is already completed", args.Title)
					}
					plan[i].Status = streaming.TodoStatusCompleted
					if len(plan)-1 > i {
						if plan[i+1].Status == streaming.TodoStatusCompleted {
							j := i + 2
							for j < len(plan) {
								if plan[j].Status != streaming.TodoStatusCompleted {
									plan[j].Status = streaming.TodoStatusInProgress
									s.setTodos(plan)
									return fmt.Sprintf("Todo completed successfully, now starting %q with description: %q", plan[j].Title, plan[j].Description), nil
								}
								j++
							}
							s.setTodos(plan)
							return "All todos completed successfully", nil
						}
						plan[i+1].Status = streaming.TodoStatusInProgress
						s.setTodos(plan)
						return fmt.Sprintf("Todo completed successfully, now starting %q with description: %q", plan[i+1].Title, plan[i+1].Description), nil
					}
					s.setTodos(plan)
					return "All todos completed successfully", nil
				}
			}
			return "", fmt.Errorf("todo %q not found in plan", args.Title)
		},
	})
}

func newEditPlanTool(s internalSession) *Tool {
	return NewTool(ToolConfig{
		Name:        "edit_plan",
		DisplayName: "Edit Plan",
		Description: "Edits an existing plan by removing and/or adding todos. Optionally replace the full plaintext plan document via plan (must differ from the current document). Omit plan when only changing todos. Cannot delete completed todos.",
		Category:    streaming.ToolCategoryThink,
		Handler: func(ctx context.Context, args editTodosArgs, _ control.HarnessRuntime) (string, error) {
			plan := s.sm.Plan().Get()
			if plan == nil {
				return "", fmt.Errorf("no plan exists")
			}

			trimmedPlan := strings.TrimSpace(args.Plan)
			if trimmedPlan != "" {
				existing := strings.TrimSpace(s.sm.Plan().Document())
				if trimmedPlan == existing {
					return "", fmt.Errorf("plan document is unchanged; omit plan or provide a revised full plan")
				}
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
			s.setTodos(plan)
			if trimmedPlan != "" {
				s.sm.Plan().SetDocument(args.Plan)
			}
			return "Plan edited successfully", nil
		},
	})
}
