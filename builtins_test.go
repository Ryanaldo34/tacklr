package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	session "github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// planToolsFixture holds builtin plan tools sharing one PlanStore.
type planToolsFixture struct {
	create, edit, complete, list *Tool
	store                        *session.PlanStore
}

func testPlanTools() planToolsFixture {
	sm := session.NewSessionManager()
	return planToolsFixture{
		create:   newCreatePlanTool(sm),
		edit:     newEditPlanTool(sm),
		complete: newCompleteTodoTool(sm),
		list:     newListPlanTool(sm),
		store:    sm.Plan,
	}
}

func drainEventCh() chan streaming.StreamEvent {
	c := make(chan streaming.StreamEvent, 8)
	go func() {
		for range c {
		}
	}()
	return c
}

// planRT is a turn Runtime for plan/ask tool unit tests (events drained).
func planRT() HarnessRuntime {
	return session.NewRuntime(drainEventCh(), session.NewSessionManager())
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
		pt.store.Set([]Todo{{Title: "existing", Status: streaming.TodoStatusInProgress}})
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
		if len(plan) != 2 || plan[0].Status != streaming.TodoStatusCompleted || plan[1].Status != streaming.TodoStatusInProgress {
			t.Fatalf("plan = %+v", plan)
		}
	})
}

func TestEditPlanTool_documentEffects(t *testing.T) {
	t.Run("identical rejected", func(t *testing.T) {
		pt, rt := testPlanTools(), planRT()
		pt.store.Set([]Todo{{Title: "a", Status: streaming.TodoStatusPending}})
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
		pt.store.Set([]Todo{{Title: "a", Status: streaming.TodoStatusPending}})
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
	pending := Todo{Title: "pending", Status: streaming.TodoStatusPending}
	completed := Todo{Title: "completed", Status: streaming.TodoStatusCompleted}
	inProgress := Todo{Title: "in_progress", Status: streaming.TodoStatusInProgress}

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
	sm := session.NewSessionManager()
	rt := session.NewRuntime(make(chan streaming.StreamEvent, 4), sm)
	rt = rt.WithToolCallID("tc_ask")

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

	// Resolve and re-invoke (harness re-execution pattern).
	if _, err := sm.ReturnInterrupt("tc_ask", []byte(`{"selectionIdx":1}`)); err != nil {
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
		{Title: "Exact Title One", Status: streaming.TodoStatusCompleted, Description: "done work"},
		{Title: "Exact Title Two", Status: streaming.TodoStatusInProgress, Description: "now"},
		{Title: "Exact Title Three", Status: streaming.TodoStatusPending},
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

// --- Parent harness outcomes for builtin plan / ask_user tools ---

// TestRun_completeTodo_skipsAlreadyCompletedNext: completing advances past already-done siblings.
func TestRun_planToolHappyAndErrorPaths(t *testing.T) {
	model := sequentialToolModel(
		[]ToolCall{toolCall("p1", "create_plan", `{"plan":"P","todos":[]}`)},
		[]ToolCall{toolCall("p2", "create_plan", `{"plan":"Only plan","todos":[{"title":"Only","description":"d"}]}`)},
		[]ToolCall{toolCall("c1", "complete_todo", `{"title":"Missing"}`)},
		[]ToolCall{toolCall("l1", "list_plan", `{}`)},
	)
	_, got := runPrompt(t, model, AgentOptions{})
	requireToolResult(t, got, "at least one todo")
	requireToolResult(t, got, "not found")
	requireToolResult(t, got, "Active plan")

	for _, c := range []struct {
		name string
		call ToolCall
		seed func(*AgentHarness)
		want string
	}{
		{"list no plan", toolCall("l1", "list_plan", `{}`), nil, "no plan"},
		{"edit no plan", toolCall("e1", "edit_plan", `{"toDelete":["x"]}`), nil, "no plan"},
		{"complete no plan", toolCall("c1", "complete_todo", `{"title":"A"}`), nil, "no plan"},
		{"complete already done", toolCall("c1", "complete_todo", `{"title":"A"}`), func(h *AgentHarness) {
			h.session.Plan.Set([]Todo{
				{Title: "A", Status: streaming.TodoStatusCompleted},
				{Title: "B", Status: streaming.TodoStatusInProgress},
			})
		}, "already completed"},
		{"ask empty choice title", toolCall("a1", "ask_user_choice",
			`{"question":"q","choices":[{"title":"  "},{"title":"B"}]}`), nil, "title is required"},
	} {
		t.Run(c.name, func(t *testing.T) {
			model := sequentialToolModel([]ToolCall{c.call})
			opts := AgentOptions{Model: model, Config: Config{MaxWindowSize: 8192}}
			h := mustNewAgent(t, opts)
			t.Cleanup(h.Close)
			if c.seed != nil {
				c.seed(h)
			}
			events, err := h.Run(context.Background(), "hi")
			if err != nil {
				t.Fatal(err)
			}
			requireToolResult(t, drainEvents(events), c.want)
		})
	}
}

func TestAgentHarness_installPlanDocumentRequiresWindow(t *testing.T) {
	h := mustNewAgent(t, AgentOptions{Model: &mockStrategy{}, Config: Config{MaxWindowSize: 8192}})
	t.Cleanup(h.Close)
	h.session.Plan.SetDocument("PROJECT PLAN")
	err := h.applyBatchToolResultEffect(context.Background(), EffectInstallPlanDocument)
	if err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("empty window install: %v", err)
	}
}

// TestRun_askUserChoice_withoutDescription_formatsSelection: selection without
// a description still returns a clear confirmation after resume; also probes
// AskUserQuestion state helpers.
func TestRun_askUserChoice_withoutDescription_formatsSelection(t *testing.T) {
	model := sequentialToolModel([]ToolCall{toolCall("ask1", "ask_user_choice",
		`{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`)})
	h := mustNewAgent(t, AgentOptions{
		Model: model, Config: Config{MaxWindowSize: 8192},
	})
	t.Cleanup(h.Close)
	h.sessionId = "ask-sess"
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
	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"selectionIdx":0}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	requireToolResult(t, drainEvents(resumed), `User selected "A"`)
}

// --- Message stream assembler via parent turn ---

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
				{Title: "A", Status: streaming.TodoStatusInProgress},
				{Title: "B", Description: "next", Status: streaming.TodoStatusPending},
			},
			title:      "A",
			wantEffect: EffectHandoff,
			wantSubstr: `starting "B"`,
		},
		{
			name: "last open with completed siblings",
			plan: []Todo{
				{Title: "A", Status: streaming.TodoStatusInProgress},
				{Title: "B", Status: streaming.TodoStatusCompleted},
				{Title: "C", Status: streaming.TodoStatusCompleted},
			},
			title:      "A",
			wantEffect: EffectNone,
			wantSubstr: "All todos completed",
		},
		{
			name: "sole item",
			plan: []Todo{
				{Title: "Only", Status: streaming.TodoStatusInProgress},
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

func TestRun_createPlan_installsPlanDocumentAndPrunesWindow(t *testing.T) {
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					toolCall("p1", "create_plan",
						`{"plan":"CoS: ship quality","todos":[{"title":"A","status":"pending","description":"d"}]}`),
				}, IsComplete: true}
				return
			}
			// Second turn: window must be user + installed plan document only.
			if len(msgs) != 2 || msgs[0].Role != RoleUser || rawPlanFromDocumentMessage(msgs[1]) != "CoS: ship quality" {
				ch <- LLMResponseChunk{Type: StreamEventError, Content: fmt.Sprintf("bad window: %+v", msgs)}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "executing", IsComplete: true}
		},
	}
	h, got := runPrompt(t, strategy, AgentOptions{})
	if hasEventType(got, StreamEventError) {
		t.Fatalf("events=%+v", summarizeEvents(got))
	}
	if h.session.Plan.Document() != "CoS: ship quality" || !isPlanDocument(h.Messages()[1]) {
		t.Fatalf("doc=%q window=%+v", h.session.Plan.Document(), h.Messages())
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
					toolCall("c1", "complete_todo", `{"title":"A"}`),
				}, IsComplete: true}
			case 2:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "handoff notes", IsComplete: true}
			default:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "next", IsComplete: true}
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Model: strategy, Config: Config{MaxWindowSize: 8192},
	})
	t.Cleanup(h.Close)
	h.session.Plan.Set([]Todo{
		{Title: "A", Status: streaming.TodoStatusInProgress},
		{Title: "B", Status: streaming.TodoStatusPending},
	})
	h.session.Plan.SetDocument("FULL PLAN DRAFT")
	events, err := h.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)
	msgs := h.Messages()
	if len(msgs) < 4 {
		t.Fatalf("window len = %d: %+v", len(msgs), msgs)
	}
	if rawPlanFromDocumentMessage(msgs[1]) != "FULL PLAN DRAFT" {
		t.Fatalf("plan = %+v", msgs[1])
	}
	if msgs[2].Role != RoleDeveloper || msgs[2].Content != "handoff notes" {
		t.Fatalf("handoff = %+v", msgs[2])
	}
	if msgs[3].Content != continuePlanNudge {
		t.Fatalf("nudge = %+v", msgs[3])
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
					toolCall("e1", "edit_plan", `{"plan":"revised blueprint","toAdd":[],"toDelete":[]}`),
				}, IsComplete: true}
			case 2:
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "post-edit handoff", IsComplete: true}
			default:
				for _, m := range msgs {
					if rawPlanFromDocumentMessage(m) == "revised blueprint" {
						handoffSawPlan = true
					}
				}
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
			}
		},
	}
	h := mustNewAgent(t, AgentOptions{
		Model: strategy, Config: Config{MaxWindowSize: 8192},
	})
	t.Cleanup(h.Close)
	h.session.Plan.Set([]Todo{
		{Title: "A", Status: streaming.TodoStatusInProgress},
		{Title: "B", Status: streaming.TodoStatusPending},
	})
	h.session.Plan.SetDocument("old blueprint")
	events, err := h.Run(context.Background(), "revise")
	if err != nil {
		t.Fatal(err)
	}
	_ = drainEvents(events)
	if h.session.Plan.Document() != "revised blueprint" {
		t.Fatalf("document = %q", h.session.Plan.Document())
	}
	if rawPlanFromDocumentMessage(h.Messages()[1]) != "revised blueprint" {
		t.Fatalf("plan msg = %+v", h.Messages()[1])
	}
	if !handoffSawPlan {
		t.Fatal("continue turn should see revised plan document in window")
	}
}

func TestRun_completeTodo_persistsPlanInStore(t *testing.T) {
	// complete → handoff model message → continue message
	var n int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			n++
			switch n {
			case 1:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					toolCall("call_ct", "complete_todo", `{"title":"Ship"}`),
				}, IsComplete: true}
			case 2:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "handoff body", IsComplete: true}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "continued", IsComplete: true}
			}
		},
	}

	ah := mustNewAgent(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	ah.sessionId = "sess-plan-persist"
	ah.session.Plan.Set([]Todo{
		{Title: "Ship", Status: streaming.TodoStatusInProgress},
	})

	ch, err := ah.Run(context.Background(), "finish ship")
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}

	restored := reloadHarness(t, ah, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
	})
	plan := restored.session.Plan.Get()
	if len(plan) != 1 {
		t.Fatalf("restored plan len = %d, want 1", len(plan))
	}
	if plan[0].Title != "Ship" || plan[0].Status != streaming.TodoStatusCompleted {
		t.Fatalf("restored plan = %+v, want Ship completed", plan[0])
	}
}
