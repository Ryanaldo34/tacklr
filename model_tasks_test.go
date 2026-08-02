package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// ModelTasks, stream assembly, and model-stream span lifecycle outcomes.

func TestStreamAssembler_deltasAndComplete(t *testing.T) {
	asm := newStreamAssembler()
	asm.AddDelta(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "hel", IsComplete: false})
	asm.AddDelta(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "lo", IsComplete: false})
	// Empty complete uses buffer.
	got := asm.CompleteContent(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", IsComplete: true})
	if got != "hello" {
		t.Fatalf("CompleteContent = %q, want hello", got)
	}
	// Explicit content wins.
	got = asm.CompleteContent(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "x", IsComplete: true})
	if got != "x" {
		t.Fatalf("explicit content = %q", got)
	}
	msg := asm.MessageFromComplete(LLMResponseChunk{
		Type: StreamEventReasoning, MessageId: "r1", Content: "think", IsComplete: true,
	})
	if msg.Role != RoleReasoning || msg.Content != "think" || msg.MessageID != "r1" {
		t.Fatalf("%+v", msg)
	}
}

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
	err := tasks.Handoff(context.Background(), []Todo{
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
	err := tasks.Handoff(context.Background(), []Todo{
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
	err := tasks.Handoff(context.Background(), []Todo{
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

// TestDefaultModelTasks_Handoff_streamError_usesFallback: Azure response.failed
// during handoff must not fail complete_todo — window still gets a handoff message.
func TestDefaultModelTasks_Handoff_streamError_usesFallback(t *testing.T) {
	var sawTools []*Tool
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			sawTools = tools
			ch <- LLMResponseChunk{
				Type:       StreamEventError,
				Content:    "provider HTTP 200: stream ended without a usable response; status=failed",
				IsComplete: true,
			}
		},
	}
	cm := NewModelContextManager()
	cm.Restore([]*Message{{Role: RoleUser, Content: "ship feature"}})
	tools := []*Tool{NewTool(ToolConfig{Name: "web_search", Handler: func(ctx context.Context) (string, error) { return "", nil }})}
	tasks := NewDefaultModelTasks(strategy, cm, DefaultContextPolicy(), 8192)
	err := tasks.Handoff(context.Background(), []Todo{
		{Title: "A", Status: streaming.TodoStatusCompleted, Description: "done"},
		{Title: "B", Status: streaming.TodoStatusInProgress, Description: "next"},
	}, "plan doc", tools, "sys")
	if err != nil {
		t.Fatalf("handoff should soft-fail: %v", err)
	}
	if sawTools != nil {
		t.Fatalf("handoff must invoke without tools, got %d tools", len(sawTools))
	}
	msgs := cm.Messages()
	if len(msgs) < 2 {
		t.Fatalf("window = %d", len(msgs))
	}
	// developer handoff content should mention fallback / remaining work
	found := false
	for _, m := range msgs {
		if m.Role == RoleDeveloper && strings.Contains(m.Content, "fallback") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected fallback handoff message, got %+v", msgs)
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

// TestWatchModelStream_cancelEndsSpan: cancel mid-stream must finish the model
// span and close the consumer channel (no leaked open span).
func TestWatchModelStream_cancelEndsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := telemetry.ContextWithTracer(context.Background(), tp.Tracer("test"))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	in := make(chan LLMResponseChunk)
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{Messages: 1})
	out := watchModelStream(ctx, span, in)

	go func() {
		select {
		case in <- LLMResponseChunk{Type: StreamEventMessage, Content: "partial"}:
		case <-ctx.Done():
		}
		<-ctx.Done()
		close(in)
	}()

	select {
	case <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first chunk")
	}
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				goto closed
			}
		case <-deadline:
			t.Fatal("watchModelStream did not close after cancel")
		}
	}
closed:
	time.Sleep(20 * time.Millisecond)
	if len(sr.Ended()) != 1 {
		t.Fatalf("ended model spans = %d, want 1", len(sr.Ended()))
	}
	if sr.Ended()[0].Status().Code != codes.Error {
		t.Fatalf("cancelled model span status = %v, want Error", sr.Ended()[0].Status())
	}
}

// TestWatchModelStream_streamErrorEndsSpanEvenIfConsumerStops: after a provider
// error, the model span ends even if the harness stops reading (early return).
func TestWatchModelStream_streamErrorEndsSpanEvenIfConsumerStops(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := telemetry.ContextWithTracer(context.Background(), tp.Tracer("test"))
	in := make(chan LLMResponseChunk, 8)
	ctx, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 2, telemetry.WindowShape{})
	out := watchModelStream(ctx, span, in)

	in <- LLMResponseChunk{Type: StreamEventMessage, Content: "hi"}
	in <- LLMResponseChunk{Type: StreamEventError, Content: "provider boom", Error: errors.New("provider boom")}
	for i := 0; i < 32; i++ {
		in <- LLMResponseChunk{Type: StreamEventMessage, Content: "noise"}
	}
	close(in)

	// Deliver message + error, then abandon (agent_run modelFailed).
	for range 2 {
		select {
		case _, ok := <-out:
			if !ok {
				t.Fatal("out closed before error chunk")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout reading error path")
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(sr.Ended()) < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if len(sr.Ended()) != 1 {
		t.Fatalf("ended model spans = %d, want 1", len(sr.Ended()))
	}
	if sr.Ended()[0].Status().Code != codes.Error {
		t.Fatalf("error model span status = %v, want Error", sr.Ended()[0].Status())
	}
}
