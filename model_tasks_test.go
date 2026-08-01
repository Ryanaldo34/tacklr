package tacklr

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestDefaultModelTasks_Absorb_underPressure_summarizes(t *testing.T) {
	var invokeCount int
	model := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return contentTokenEstimate(msgs), nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			invokeCount++
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "SUMMARY", IsComplete: true}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, Content: strings.Repeat("x", 80)},
	})
	tasks := NewDefaultModelTasks(model, cm, DefaultContextPolicy(), 100)
	res, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "more"}, nil, "restored")
	if err != nil {
		t.Fatal(err)
	}
	if invokeCount != 1 {
		t.Fatalf("invokeCount = %d, want 1", invokeCount)
	}
	if len(cm.Messages()) < 2 || cm.Messages()[0].Content != "goal" {
		t.Fatalf("%+v", cm.Messages())
	}
	var sawSummary bool
	for _, m := range cm.Messages() {
		if m != nil && m.Role == RoleAssistant && m.Content == "SUMMARY" {
			sawSummary = true
		}
	}
	if !sawSummary {
		t.Fatalf("expected SUMMARY: %+v", cm.Messages())
	}
	if len(res.SummaryChunks) == 0 {
		t.Error("expected streamed summary chunks")
	}
	if len(model.systemPrompts) == 0 || model.systemPrompts[len(model.systemPrompts)-1] != "restored" {
		t.Fatalf("system prompts = %v", model.systemPrompts)
	}
}

func TestDefaultModelTasks_Absorb_underThreshold_appendsOnly(t *testing.T) {
	var invokeCount int
	model := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return 1, nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			invokeCount++
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{{Role: RoleUser, Content: "hi"}})
	tasks := NewDefaultModelTasks(model, cm, DefaultContextPolicy(), 1000)
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "there"}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if invokeCount != 0 {
		t.Fatal("should not invoke under threshold")
	}
	if len(cm.Messages()) != 2 || cm.Messages()[1].Content != "there" {
		t.Fatalf("%+v", cm.Messages())
	}
}

func TestDefaultModelTasks_Handoff_windowShape(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "h", Content: "HANDOFF_BODY", IsComplete: true}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{
		{Role: RoleUser, Content: "build it"},
		{Role: RoleAssistant, Content: "working"},
	})
	tasks := NewDefaultModelTasks(model, cm, DefaultContextPolicy(), 8192)
	err := tasks.Handoff(context.Background(), []control.Todo{
		{Title: "A", Status: streaming.TodoStatusCompleted},
		{Title: "B", Status: streaming.TodoStatusInProgress},
	}, "", nil, "sys")
	if err != nil {
		t.Fatal(err)
	}
	res := cm.Messages()
	if len(res) != 3 {
		t.Fatalf("len = %d, want 3", len(res))
	}
	if res[0].Content != "build it" || res[1].Content != "HANDOFF_BODY" || res[2].Content != continuePlanNudge {
		t.Fatalf("%+v", res)
	}
}

func TestDefaultModelTasks_Handoff_includesFullPlanDocument(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "HANDOFF_BODY", IsComplete: true}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{
		{Role: RoleUser, Content: "build it"},
		{Role: RoleAssistant, Content: "noise"},
	})
	tasks := NewDefaultModelTasks(model, cm, DefaultContextPolicy(), 8192)
	err := tasks.Handoff(context.Background(), []control.Todo{
		{Title: "A", Status: streaming.TodoStatusCompleted},
		{Title: "B", Status: streaming.TodoStatusInProgress},
	}, "CoS: done right", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	res := cm.Messages()
	if len(res) != 4 || !isPlanDocument(res[1]) || rawPlanFromDocumentMessage(res[1]) != "CoS: done right" {
		t.Fatalf("%+v", res)
	}
}

func TestDefaultModelTasks_Absorb_countTokensError(t *testing.T) {
	model := &mockStrategy{
		countTokensFn: func(context.Context, []*Message, []*Tool) (int, error) {
			return 0, fmt.Errorf("token service unavailable")
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{{Role: RoleUser, Content: "x"}})
	tasks := NewDefaultModelTasks(model, cm, DefaultContextPolicy(), 100)
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "y"}, nil, "")
	if err == nil || !strings.Contains(err.Error(), "token service unavailable") {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultModelTasks_Absorb_streamFitSummaryFalse_noChunks(t *testing.T) {
	model := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return contentTokenEstimate(msgs), nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "SUM", IsComplete: true}
		},
	}
	policy := DefaultContextPolicy()
	policy.StreamFitSummary = false
	cm := NewModelContextManager()
	cm.Restore([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, Content: strings.Repeat("x", 80)},
	})
	tasks := NewDefaultModelTasks(model, cm, policy, 100)
	res, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "more"}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.SummaryChunks) != 0 {
		t.Fatalf("chunks = %d", len(res.SummaryChunks))
	}
}

func TestDefaultModelTasks_Handoff_allTodosDone_noNudge(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done handoff", IsComplete: true}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{{Role: RoleUser, Content: "x"}})
	tasks := NewDefaultModelTasks(model, cm, DefaultContextPolicy(), 8192)
	err := tasks.Handoff(context.Background(), []control.Todo{
		{Title: "Only", Status: streaming.TodoStatusCompleted},
	}, "", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(cm.Messages()) != 2 {
		t.Fatalf("len = %d", len(cm.Messages()))
	}
}

func TestDefaultModelTasks_Turn_setsSystemPrompt(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{{Role: RoleUser, Content: "hi"}})
	tasks := NewDefaultModelTasks(model, cm, DefaultContextPolicy(), 8192)
	ch, err := tasks.Turn(context.Background(), nil, "agent-sys")
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	if len(model.systemPrompts) == 0 || model.systemPrompts[len(model.systemPrompts)-1] != "agent-sys" {
		t.Fatalf("prompts = %v", model.systemPrompts)
	}
}

func TestDefaultModelTasks_Handoff_errors(t *testing.T) {
	cm := NewModelContextManager()
	tasks := NewDefaultModelTasks(nil, cm, DefaultContextPolicy(), 8192)
	if err := tasks.Handoff(context.Background(), nil, "", nil, ""); err == nil || !strings.Contains(err.Error(), "model") {
		t.Fatalf("err = %v", err)
	}
	tasks = NewDefaultModelTasks(&mockStrategy{}, cm, DefaultContextPolicy(), 8192)
	if err := tasks.Handoff(context.Background(), nil, "", nil, ""); err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultModelTasks_Absorb_countTokensDuringCompressSearch(t *testing.T) {
	calls := 0
	strategy := &mockStrategy{
		countTokensFn: func(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
			calls++
			if len(msgs) >= 4 {
				return 80, nil
			}
			return 10, nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "sum", IsComplete: true}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{
		{Role: RoleUser, Content: "a1"},
		{Role: RoleAssistant, Content: "a2"},
		{Role: RoleUser, Content: "a3"},
		{Role: RoleAssistant, Content: "a4"},
		{Role: RoleUser, Content: "a5"},
	})
	tasks := NewDefaultModelTasks(strategy, cm, ContextPolicy{PressureRatio: 0.5, CompressFraction: 0.05, StreamFitSummary: false}, 50)
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "q"}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if calls < 2 {
		t.Fatalf("expected multiple CountTokens, got %d", calls)
	}
}
