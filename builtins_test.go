package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	_, err := createPlanTool.Invoke(context.Background(), `{"plan":"draft","todos":[{"title":"new","status":"pending","description":""}]}`, rt)
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

	got, err := createPlanTool.Invoke(context.Background(), `{"plan":"CoS: ship it","todos":[{"title":"a","status":"pending","description":"d"}]}`, rt)
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
	if rt.PlanDocumentGet() != "CoS: ship it" {
		t.Fatalf("plan document = %q", rt.PlanDocumentGet())
	}
}

func TestCreatePlanTool_rejectsEmptyPlanDocument(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	_, err := createPlanTool.Invoke(context.Background(), `{"plan":"  ","todos":[{"title":"a","status":"pending","description":""}]}`, rt)
	if err == nil || !strings.Contains(err.Error(), "plan document text is required") {
		t.Fatalf("err = %v", err)
	}
}

func TestEditPlanTool_identicalPlanDocument_rejected(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	rt.PlanSet([]control.Todo{{Title: "a", Status: streaming.TodoStatusPending}})
	rt.PlanDocumentSet("same plan")

	_, err := editPlanTool.Invoke(context.Background(), `{"plan":"same plan","toAdd":[],"toDelete":[]}`, rt)
	if err == nil || !strings.Contains(err.Error(), "unchanged") {
		t.Fatalf("err = %v", err)
	}
	if rt.PlanDocumentGet() != "same plan" {
		t.Fatalf("document changed: %q", rt.PlanDocumentGet())
	}
	if rt.ConsumePlanDocumentUpdated() {
		t.Fatal("should not mark updated on rejected identical plan")
	}
}

func TestEditPlanTool_newPlanDocument_setsUpdatedFlag(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	rt.PlanSet([]control.Todo{{Title: "a", Status: streaming.TodoStatusPending}})
	rt.PlanDocumentSet("old")

	got, err := editPlanTool.Invoke(context.Background(), `{"plan":"new full plan","toAdd":[],"toDelete":[]}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if got != "Plan edited successfully" {
		t.Fatalf("got %q", got)
	}
	if rt.PlanDocumentGet() != "new full plan" {
		t.Fatalf("document = %q", rt.PlanDocumentGet())
	}
	if !rt.ConsumePlanDocumentUpdated() {
		t.Fatal("expected updated flag for handoff hook")
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
	if q := AskUserQuestionFromState(&rt, "tc_ask"); q != "Which approach?" {
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

// --- Parent harness outcomes for builtin plan / ask_user tools ---

// TestRun_completeTodo_skipsAlreadyCompletedNext: completing a todo when the
// next items are already completed advances past them or finishes the plan.
func TestRun_completeTodo_skipsAlreadyCompletedNext(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			switch n {
			case 1:
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "p1", CallID: "p1", Name: "create_plan",
						Arguments: `{"plan":"P","todos":[{"title":"A","description":"a"},{"title":"B","description":"b","status":"completed"},{"title":"C","description":"c"}]}`,
					}},
					IsComplete: true,
				}
			case 2:
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "c1", CallID: "c1", Name: "complete_todo",
						Arguments: `{"title":"A"}`,
					}},
					IsComplete: true,
				}
			default:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  testStore(t),
	})
	h.SessionId = "plan-skip"
	events, err := h.Run(context.Background(), "plan")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "starting \"C\"") && !hasToolResultContent(got, "Todo completed") {
		t.Fatalf("want complete_todo to advance past completed B, got %+v", summarizeEvents(got))
	}
	plan := h.Runtime.PlanGet()
	if len(plan) != 3 {
		t.Fatalf("plan len = %d", len(plan))
	}
	if plan[2].Status != streaming.TodoStatusInProgress && plan[2].Status != streaming.TodoStatusCompleted {
		// After complete A, C should be in_progress (B was already completed).
		t.Fatalf("plan statuses = %v %v %v", plan[0].Status, plan[1].Status, plan[2].Status)
	}
}

// TestRun_planToolValidationOutcomes: empty create_plan, missing complete_todo, list_plan.
func TestRun_planToolValidationOutcomes(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			switch n {
			case 1:
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "p1", CallID: "p1", Name: "create_plan",
						Arguments: `{"plan":"P","todos":[]}`,
					}},
					IsComplete: true,
				}
			case 2:
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "p2", CallID: "p2", Name: "create_plan",
						Arguments: `{"plan":"Only plan","todos":[{"title":"Only","description":"d"}]}`,
					}},
					IsComplete: true,
				}
			case 3:
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "c1", CallID: "c1", Name: "complete_todo",
						Arguments: `{"title":"Missing"}`,
					}},
					IsComplete: true,
				}
			case 4:
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "l1", CallID: "l1", Name: "list_plan",
						Arguments: `{}`,
					}},
					IsComplete: true,
				}
			default:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "plan")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "at least one todo") {
		t.Fatalf("want empty plan error, got %+v", summarizeEvents(got))
	}
	if !hasToolResultContent(got, "not found") {
		t.Fatalf("want missing todo error, got %+v", summarizeEvents(got))
	}
	if !hasToolResultContent(got, "Active plan") {
		t.Fatalf("want list_plan listing, got %+v", summarizeEvents(got))
	}
}

// TestRun_askUserChoice_withoutDescription_formatsSelection: selection without
// a description still returns a clear confirmation string after resume.

// TestRun_askUserChoice_withoutDescription_formatsSelection: selection without
// a description still returns a clear confirmation string after resume.
func TestRun_askUserChoice_withoutDescription_formatsSelection(t *testing.T) {
	optionsJSON := `[{"title":"A","description":"","isRecommended":true},{"title":"B","description":"","isRecommended":false}]`
	// Drive via ask_user_choice builtin after create_plan unlock is not needed
	// (ask is think category). Resume via ReturnFromInterrupt.
	var phase int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			phase++
			if phase == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
						Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  testStore(t),
	})
	h.SessionId = "ask-sess"
	events, err := h.Run(context.Background(), "ask")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range events {
		if ev.Type == StreamEventInterrupt {
			var env struct {
				InterruptId string `json:"interruptId"`
			}
			_ = json.Unmarshal(ev.Data, &env)
			interruptID = env.InterruptId
		}
	}
	if interruptID == "" {
		t.Fatal("expected interrupt")
	}
	// Question stashed for protocol elicitation.
	if q := AskUserQuestionFromState(&h.Runtime, "ask1"); q != "Pick?" {
		t.Fatalf("question = %q", q)
	}
	if AskUserQuestionFromState(&h.Runtime, "") != "" {
		t.Fatal("empty tool call id should yield empty question")
	}
	if AskUserQuestionFromState(&h.Runtime, "missing") != "" {
		t.Fatal("unknown tool call should yield empty question")
	}

	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"selectionIdx":0}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(resumed)
	if !hasToolResultContent(got, `User selected "A"`) {
		t.Fatalf("want selection text, got %+v", summarizeEvents(got))
	}
	_ = optionsJSON
}

func TestRun_listPlan_noPlan_toolError(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "l1", CallID: "l1", Name: "list_plan", Arguments: `{}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "list")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "no plan") {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

func TestRun_completeTodo_alreadyCompleted_toolError(t *testing.T) {
	// Pre-seed a plan with A already completed; completing A again is a tool error.
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "c1", CallID: "c1", Name: "complete_todo",
						Arguments: `{"title":"A"}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	h.Runtime.PlanSet([]control.Todo{
		{Title: "A", Status: streaming.TodoStatusCompleted},
		{Title: "B", Status: streaming.TodoStatusInProgress},
	})
	events, err := h.Run(context.Background(), "plan")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "already completed") {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

func TestRun_editPlan_noPlan_toolError(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "e1", CallID: "e1", Name: "edit_plan",
						Arguments: `{"toDelete":["x"]}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "edit")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "no plan") {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

func TestRun_askUserChoice_emptyChoiceTitle_toolError(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "a1", CallID: "a1", Name: "ask_user_choice",
						Arguments: `{"question":"q","choices":[{"title":"  "},{"title":"B"}]}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "ask")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "title is required") {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

// --- Message stream assembler via parent turn ---

// TestCompleteTodo_allRemainingCompleted: completing last open todo when later
// ones are already completed reports all-done.
func TestCompleteTodo_allRemainingCompleted(t *testing.T) {
	rt := control.NewRuntime(nil, nil, nil)
	rt.EnsureInitialized()
	rt.PlanSet([]control.Todo{
		{Title: "A", Status: streaming.TodoStatusInProgress},
		{Title: "B", Status: streaming.TodoStatusCompleted},
		{Title: "C", Status: streaming.TodoStatusCompleted},
	})
	got, err := completeTodoTool.Invoke(context.Background(), `{"title":"A"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "All todos completed") {
		t.Fatalf("got %q", got)
	}
}

func TestRun_createPlan_installsPlanDocumentAndPrunesWindow(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "p1", CallID: "p1", Name: "create_plan",
						Arguments: `{"plan":"CoS: ship quality","todos":[{"title":"A","status":"pending","description":"d"}]}`,
					}},
					IsComplete: true,
				}
				return
			}
			if len(msgs) != 2 || msgs[0].Role != RoleUser || !isPlanDocument(msgs[1]) {
				ch <- LLMResponseChunk{Type: StreamEventError, Content: fmt.Sprintf("bad window: %+v", msgs)}
				return
			}
			if rawPlanFromDocumentMessage(msgs[1]) != "CoS: ship quality" {
				ch <- LLMResponseChunk{Type: StreamEventError, Content: "plan body mismatch"}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "executing", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  testStore(t),
	})
	events, err := h.Run(context.Background(), "build the thing")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	for _, ev := range got {
		if ev.Type == StreamEventError {
			t.Fatalf("events=%+v", summarizeEvents(got))
		}
	}
	if len(h.Messages()) < 2 || !isPlanDocument(h.Messages()[1]) {
		t.Fatalf("window = %+v", h.Messages())
	}
	if h.Runtime.PlanDocumentGet() != "CoS: ship quality" {
		t.Fatalf("document = %q", h.Runtime.PlanDocumentGet())
	}
}

func TestRun_completeTodo_withPlanDocument_preservesFullPlan(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			switch n {
			case 1:
				ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "c1", CallID: "c1", Name: "complete_todo", Arguments: `{"title":"A"}`},
				}, IsComplete: true}
			case 2:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "handoff notes", IsComplete: true}
			default:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "next", IsComplete: true}
			}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  testStore(t),
	})
	h.Runtime.PlanSet([]control.Todo{
		{Title: "A", Status: streaming.TodoStatusInProgress},
		{Title: "B", Status: streaming.TodoStatusPending},
	})
	h.Runtime.PlanDocumentSet("FULL PLAN DRAFT")

	events, err := h.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)

	if len(h.Messages()) < 4 {
		t.Fatalf("window len = %d, window=%+v", len(h.Messages()), h.Messages())
	}
	if !isPlanDocument(h.Messages()[1]) || rawPlanFromDocumentMessage(h.Messages()[1]) != "FULL PLAN DRAFT" {
		t.Fatalf("plan = %+v", h.Messages()[1])
	}
	if h.Messages()[2].Role != RoleDeveloper || h.Messages()[2].Content != "handoff notes" {
		t.Fatalf("handoff = %+v", h.Messages()[2])
	}
	if h.Messages()[3].Content != continuePlanNudge {
		t.Fatalf("nudge = %+v", h.Messages()[3])
	}
}

func TestRun_editPlan_planChange_triggersHandoff(t *testing.T) {
	var n int
	var handoffSawPlan bool
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			switch n {
			case 1:
				ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "e1", CallID: "e1", Name: "edit_plan",
						Arguments: `{"plan":"revised blueprint","toAdd":[],"toDelete":[]}`},
				}, IsComplete: true}
			case 2:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "post-edit handoff", IsComplete: true}
			default:
				for _, m := range msgs {
					if isPlanDocument(m) && rawPlanFromDocumentMessage(m) == "revised blueprint" {
						handoffSawPlan = true
					}
				}
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
			}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  testStore(t),
	})
	h.Runtime.PlanSet([]control.Todo{
		{Title: "A", Status: streaming.TodoStatusInProgress},
		{Title: "B", Status: streaming.TodoStatusPending},
	})
	h.Runtime.PlanDocumentSet("old blueprint")

	events, err := h.Run(context.Background(), "revise")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)
	if h.Runtime.PlanDocumentGet() != "revised blueprint" {
		t.Fatalf("document = %q", h.Runtime.PlanDocumentGet())
	}
	if len(h.Messages()) < 3 || !isPlanDocument(h.Messages()[1]) {
		t.Fatalf("window = %+v", h.Messages())
	}
	if rawPlanFromDocumentMessage(h.Messages()[1]) != "revised blueprint" {
		t.Fatalf("plan msg = %+v", h.Messages()[1])
	}
	if !handoffSawPlan {
		t.Fatal("continue turn should see revised plan document in window")
	}
}

// TestRun_completeTodo_noPlan_toolError: complete_todo without a plan fails clearly.
func TestRun_completeTodo_noPlan_toolError(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{
					Type: StreamEventFunctionCall,
					ToolCalls: []ToolCall{{
						ID: "c1", CallID: "c1", Name: "complete_todo",
						Arguments: `{"title":"A"}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	events, err := h.Run(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(events)
	if !hasToolResultContent(got, "no plan") {
		t.Fatalf("%+v", summarizeEvents(got))
	}
}

// TestNewTool_depthLimitAndSkipTypes: deeply nested schema degrades safely;
// time.Time fields are skipped in schemas.

func TestRun_completeTodo_persistsPlanInStore(t *testing.T) {
	store := testStore(t)
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "call_ct", CallID: "call_ct", Name: "complete_todo", Arguments: `{"title":"Ship"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			if invokeCount == 2 {
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "handoff body", IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "continued", IsComplete: true}
		},
	}

	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Store:  store,
	})
	ah.SessionId = "sess-plan-persist"
	ah.Runtime.PlanSet([]control.Todo{
		{Title: "Ship", Status: streaming.TodoStatusInProgress},
	})

	ch, err := ah.Run(context.Background(), "finish ship")
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}

	// Reload via NewAgentFromSession and assert plan status survived the checkpoint.
	restored, err := NewAgentFromSession(context.Background(), "sess-plan-persist", AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		Store:  store,
	})
	if err != nil {
		t.Fatalf("NewAgentFromSession: %v", err)
	}
	plan := restored.Runtime.PlanGet()
	if len(plan) != 1 {
		t.Fatalf("restored plan len = %d, want 1", len(plan))
	}
	if plan[0].Title != "Ship" || plan[0].Status != streaming.TodoStatusCompleted {
		t.Fatalf("restored plan = %+v, want Ship completed", plan[0])
	}
}
