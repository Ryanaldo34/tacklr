package tacklr

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestModelContextManager_Fit_underPressure_summarizes(t *testing.T) {
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
	mgr := NewModelContextManager()
	window := []*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, Content: strings.Repeat("x", 80)},
	}
	res, err := mgr.Fit(context.Background(), FitInput{
		Window:              window,
		NewMsg:              &Message{Role: RoleUser, Content: "more"},
		MaxSize:             100,
		Policy:              DefaultContextPolicy(),
		Model:               model,
		RestoreSystemPrompt: "restored",
	})
	if err != nil {
		t.Fatal(err)
	}
	if invokeCount != 1 {
		t.Fatalf("invokeCount = %d, want 1", invokeCount)
	}
	if len(res.Window) < 2 {
		t.Fatalf("window too short: %+v", res.Window)
	}
	if res.Window[0].Content != "goal" {
		t.Errorf("first user = %q", res.Window[0].Content)
	}
	var sawSummary bool
	for _, m := range res.Window {
		if m != nil && m.Role == RoleAssistant && m.Content == "SUMMARY" {
			sawSummary = true
		}
	}
	if !sawSummary {
		t.Fatalf("expected SUMMARY in window: %+v", res.Window)
	}
	if len(res.Chunks) == 0 {
		t.Error("expected streamed summary chunks when StreamFitSummary=true")
	}
}

func TestModelContextManager_Fit_underThreshold_appendsOnly(t *testing.T) {
	var invokeCount int
	model := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return 1, nil
		},
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			invokeCount++
		},
	}
	mgr := NewModelContextManager()
	res, err := mgr.Fit(context.Background(), FitInput{
		Window:  []*Message{{Role: RoleUser, Content: "hi"}},
		NewMsg:  &Message{Role: RoleUser, Content: "there"},
		MaxSize: 1000,
		Policy:  DefaultContextPolicy(),
		Model:   model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if invokeCount != 0 {
		t.Fatal("should not invoke model under threshold")
	}
	if len(res.Window) != 2 || res.Window[1].Content != "there" {
		t.Fatalf("%+v", res.Window)
	}
}

func TestModelContextManager_Handoff_windowShape(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "h", Content: "HANDOFF_BODY", IsComplete: false}
			ch <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "h", IsComplete: true}
		},
	}
	mgr := NewModelContextManager()
	res, err := mgr.Handoff(context.Background(), HandoffInput{
		Window: []*Message{
			{Role: RoleUser, Content: "build it"},
			{Role: RoleAssistant, Content: "working"},
		},
		Plan: []control.Todo{
			{Title: "A", Status: streaming.TodoStatusCompleted},
			{Title: "B", Status: streaming.TodoStatusInProgress},
		},
		Model:               model,
		RestoreSystemPrompt: "sys",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Window) != 3 {
		t.Fatalf("window len = %d, want 3 (user, handoff, nudge)", len(res.Window))
	}
	if res.Window[0].Role != RoleUser || res.Window[0].Content != "build it" {
		t.Errorf("user = %+v", res.Window[0])
	}
	if res.Window[1].Role != RoleDeveloper || res.Window[1].Content != "HANDOFF_BODY" {
		t.Errorf("handoff = %+v", res.Window[1])
	}
	if res.Window[2].Role != RoleDeveloper || res.Window[2].Content != continuePlanNudge {
		t.Errorf("nudge = %+v", res.Window[2])
	}
}

func TestModelContextManager_Fit_countTokensError(t *testing.T) {
	model := &mockStrategy{
		countTokensFn: func(context.Context, []*Message, []*Tool) (int, error) {
			return 0, fmt.Errorf("token service unavailable")
		},
	}
	_, err := NewModelContextManager().Fit(context.Background(), FitInput{
		Window:  []*Message{{Role: RoleUser, Content: "x"}},
		NewMsg:  &Message{Role: RoleUser, Content: "y"},
		MaxSize: 100,
		Policy:  DefaultContextPolicy(),
		Model:   model,
	})
	if err == nil || !strings.Contains(err.Error(), "token service unavailable") {
		t.Fatalf("err = %v", err)
	}
}

func TestModelContextManager_Fit_streamFitSummaryFalse_noChunks(t *testing.T) {
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
	res, err := NewModelContextManager().Fit(context.Background(), FitInput{
		Window: []*Message{
			{Role: RoleUser, Content: "goal"},
			{Role: RoleAssistant, Content: strings.Repeat("x", 80)},
		},
		NewMsg:  &Message{Role: RoleUser, Content: "more"},
		MaxSize: 100,
		Policy:  policy,
		Model:   model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Chunks) != 0 {
		t.Fatalf("chunks = %d, want 0 when StreamFitSummary=false", len(res.Chunks))
	}
	var saw bool
	for _, m := range res.Window {
		if m != nil && m.Content == "SUM" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("summary still stored in window")
	}
}

func TestModelContextManager_Handoff_allTodosDone_noNudge(t *testing.T) {
	model := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done handoff", IsComplete: true}
		},
	}
	mgr := NewModelContextManager()
	res, err := mgr.Handoff(context.Background(), HandoffInput{
		Window: []*Message{{Role: RoleUser, Content: "x"}},
		Plan:   []control.Todo{{Title: "Only", Status: streaming.TodoStatusCompleted}},
		Model:  model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Window) != 2 {
		t.Fatalf("len = %d, want 2 without nudge", len(res.Window))
	}
}

func TestModelContextManager_Fit_nilNewMsg_returnsWindow(t *testing.T) {
	window := []*Message{{Role: RoleUser, Content: "keep"}}
	res, err := NewModelContextManager().Fit(context.Background(), FitInput{
		Window:  window,
		NewMsg:  nil,
		MaxSize: 100,
		Policy:  DefaultContextPolicy(),
		Model:   &mockStrategy{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Window) != 1 || res.Window[0].Content != "keep" {
		t.Fatalf("window = %+v", res.Window)
	}
}

func TestModelContextManager_Fit_cancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewModelContextManager().Fit(ctx, FitInput{
		Window:  nil,
		NewMsg:  &Message{Role: RoleUser, Content: "x"},
		MaxSize: 100,
		Policy:  DefaultContextPolicy(),
		Model:   &mockStrategy{},
	})
	if err == nil {
		t.Fatal("expected cancelled context error")
	}
}
