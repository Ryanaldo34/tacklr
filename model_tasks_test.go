package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func TestStreamAssembler_accumulatesDeltasAndBuildsMessages(t *testing.T) {
	// Arrange
	asm := newStreamAssembler()

	// Act
	asm.AddDelta(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "hel"})
	asm.AddDelta(LLMResponseChunk{Type: StreamEventReasoning, MessageId: "r1", Content: "think"})
	asm.AddDelta(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "lo"})
	asm.AddDelta(LLMResponseChunk{Type: StreamEventFunctionCall, MessageId: "x", Content: "ignored"})
	completed := asm.MessageFromComplete(LLMResponseChunk{
		Type: StreamEventReasoning, MessageId: "r1", IsComplete: true,
	})

	// Assert
	if got := asm.CompleteContent(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1"}); got != "hello" {
		t.Fatalf("message content = %q", got)
	}
	if got := asm.CompleteContent(LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "explicit"}); got != "explicit" {
		t.Fatalf("explicit content = %q", got)
	}
	if completed.Role != RoleReasoning || completed.Content != "think" {
		t.Fatalf("reasoning message = %+v", completed)
	}
}

func TestDefaultModelTasks_respectsCancelledContext(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tasks := newDefaultModelTasks(&mockStrategy{}, NewModelContextManager(), DefaultContextPolicy(), 100)

	// Act
	_, turnErr := tasks.Turn(ctx, nil, "")
	_, absorbErr := tasks.Absorb(ctx, &Message{Role: RoleUser, Content: "x"}, nil, "")
	handoffErr := tasks.Handoff(ctx, nil, "", nil, "")

	// Assert
	if turnErr == nil || absorbErr == nil || handoffErr == nil {
		t.Fatalf("errors = %v %v %v", turnErr, absorbErr, handoffErr)
	}
}

func TestDefaultModelTasks_absorbFitReportsCountTokenErrors(t *testing.T) {
	// Arrange
	model := &mockStrategy{
		countTokensFn: func(context.Context, []*Message, []*Tool) (int, error) {
			return 0, fmt.Errorf("count failed")
		},
	}
	tasks := newDefaultModelTasks(model, NewModelContextManager(), DefaultContextPolicy(), 100)
	ctxMgr := NewModelContextManager()
	ctxMgr.Restore([]*Message{{Role: RoleUser, Content: "goal"}})
	tasks.context = ctxMgr

	// Act
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "next"}, nil, "")

	// Assert
	if err == nil || !strings.Contains(err.Error(), "count tokens") {
		t.Fatalf("error = %v", err)
	}
}

func TestDefaultModelTasks_absorbFitSkipsCompressWhenOnlyProtectedPrefix(t *testing.T) {
	// Arrange
	model := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return len(msgs) * 50, nil
		},
	}
	tasks := newDefaultModelTasks(model, NewModelContextManager(), ContextPolicy{
		PressureRatio:    0.5,
		CompressFraction: 0.25,
	}, 40)
	ctxMgr := NewModelContextManager()
	ctxMgr.Restore([]*Message{{Role: RoleUser, Content: strings.Repeat("x", 40)}})
	tasks.context = ctxMgr

	// Act
	result, err := tasks.Absorb(context.Background(), &Message{Role: RoleAssistant, Content: "reply"}, nil, "")

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SummaryChunks) != 0 {
		t.Fatalf("summary chunks = %d", len(result.SummaryChunks))
	}
	if len(tasks.context.Messages()) != 2 {
		t.Fatalf("window = %+v", tasks.context.Messages())
	}
}

func TestDefaultModelTasks_absorbFitCompressesOverMaxWindow(t *testing.T) {
	// Arrange
	var compressInvoked bool
	model := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			total := 0
			for _, msg := range msgs {
				if msg != nil {
					total += len(msg.Content)
				}
			}
			return total, nil
		},
		invokeFn: func(_ context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			if tools == nil {
				compressInvoked = true
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "summary", IsComplete: true}
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "unexpected", IsComplete: true}
		},
	}
	tasks := newDefaultModelTasks(model, NewModelContextManager(), ContextPolicy{
		PressureRatio:    0.5,
		CompressFraction: 0.5,
		StreamFitSummary: true,
	}, 20)
	ctxMgr := NewModelContextManager()
	ctxMgr.Restore([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, Content: strings.Repeat("a", 30)},
	})
	tasks.context = ctxMgr

	// Act
	result, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "follow-up"}, nil, "")

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if !compressInvoked {
		t.Fatal("expected compression invoke")
	}
	if len(result.SummaryChunks) == 0 {
		t.Fatal("expected streamed summary chunks")
	}
	window := tasks.context.Messages()
	if len(window) < 3 || !strings.Contains(window[1].Content, "summary") {
		t.Fatalf("compressed window = %+v", window)
	}
}

func TestDefaultModelTasks_absorbFitReturnsCompressStreamErrors(t *testing.T) {
	// Arrange
	model := &mockStrategy{
		countTokensFn: func(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
			return len(msgs) * 100, nil
		},
		invokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Error: errors.New("compress stream failed")}
		},
	}
	tasks := newDefaultModelTasks(model, NewModelContextManager(), ContextPolicy{
		PressureRatio:    0.5,
		CompressFraction: 0.5,
	}, 50)
	ctxMgr := NewModelContextManager()
	ctxMgr.Restore([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, Content: strings.Repeat("x", 80)},
	})
	tasks.context = ctxMgr

	// Act
	_, err := tasks.Absorb(context.Background(), &Message{Role: RoleUser, Content: "more"}, nil, "")

	// Assert
	if err == nil || !strings.Contains(err.Error(), "compress stream failed") {
		t.Fatalf("error = %v", err)
	}
}

func TestDefaultModelTasks_handoffUsesFallbackWhenModelFails(t *testing.T) {
	// Arrange
	model := &mockStrategy{
		invokeErr: errors.New("handoff invoke failed"),
	}
	tasks := newDefaultModelTasks(model, NewModelContextManager(), DefaultContextPolicy(), 8192)
	ctxMgr := NewModelContextManager()
	ctxMgr.Restore([]*Message{{Role: RoleUser, Content: "goal"}})
	tasks.context = ctxMgr
	plan := []Todo{{Title: "Ship", Description: "finish", Status: streaming.TodoStatusPending}}

	// Act
	err := tasks.Handoff(context.Background(), plan, "plan body", nil, "")

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	window := tasks.context.Messages()
	if len(window) < 3 || !strings.Contains(window[len(window)-2].Content, "fallback") {
		t.Fatalf("handoff window = %+v", window)
	}
}

func TestDefaultModelTasks_handoffUsesFallbackOnEmptyStream(t *testing.T) {
	// Arrange
	model := &mockStrategy{
		invokeFn: func(_ context.Context, _ []*Message, _ []*Tool, _ chan<- LLMResponseChunk) {
			// Leave the stream empty; mockStrategy closes the channel after invokeFn returns.
		},
	}
	tasks := newDefaultModelTasks(model, NewModelContextManager(), DefaultContextPolicy(), 8192)
	ctxMgr := NewModelContextManager()
	ctxMgr.Restore([]*Message{{Role: RoleUser, Content: "goal"}})
	tasks.context = ctxMgr

	// Act
	err := tasks.Handoff(context.Background(), []Todo{{Title: "T", Status: streaming.TodoStatusCompleted}}, "", nil, "")

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks.context.Messages()) != 2 || !strings.Contains(tasks.context.Messages()[1].Content, "fallback") {
		t.Fatalf("window = %+v", tasks.context.Messages())
	}
}

func TestHandoffGenerate_rejectsEmptyWindow(t *testing.T) {
	// Act
	_, _, err := handoffGenerate(context.Background(), nil, nil, "", &mockStrategy{}, nil)

	// Assert
	if err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("error = %v", err)
	}
}

func TestWatchModelStream_cancelsAndDrainsProviderChunks(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan LLMResponseChunk, 8)
	_, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{})

	// Act
	out := watchModelStream(ctx, span, in)
	in <- LLMResponseChunk{Type: StreamEventMessage, Content: "partial"}
	cancel()
	close(in)
	for range out {
	}

	// Assert — no hang; span ended via drain path.
}

func TestWatchModelStream_forwardBlockedByCancelledContext(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan LLMResponseChunk)
	_, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{})
	out := watchModelStream(ctx, span, in)

	// Act
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
		in <- LLMResponseChunk{Type: StreamEventMessage, Content: "late"}
		close(in)
	}()
	for range out {
	}
}

func TestFallbackHandoffContent_listsRemainingTodos(t *testing.T) {
	// Act
	content := fallbackHandoffContent([]Todo{
		{Title: "A", Description: "alpha", Status: streaming.TodoStatusPending},
	})

	// Assert
	if !strings.Contains(content, "[pending] A: alpha") {
		t.Fatalf("content = %q", content)
	}
}
