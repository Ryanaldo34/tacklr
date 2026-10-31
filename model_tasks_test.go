package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

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
		EncryptedContent: "gAAAAABasm",
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
	if completed.EncryptedContent != "gAAAAABasm" {
		t.Fatalf("encrypted_content = %q", completed.EncryptedContent)
	}
}

// handoffHarness is a turn whose window holds the goal and whose plan has
// todos, so a handoff has work to hand off.
func handoffHarness(t *testing.T, model InferenceStrategy, todos ...Todo) *TurnManager {
	t.Helper()
	h := mustNewTurnManager(t, AgentOptions{MaxWindowSize: 8192, Model: model})
	t.Cleanup(h.Close)
	h.context.Restore([]*Message{userMsg("goal")})
	h.session.Plan.set(todos)
	return h
}

// TestHandoff_fallsBackWhenTheModelGivesNoHandoff: a failed request, an
// empty stream, and a stream error each install the plan-derived handoff.
func TestHandoff_fallsBackWhenTheModelGivesNoHandoff(t *testing.T) {
	pending := Todo{Title: "Ship", Description: "finish", Status: TodoStatusPending}
	for name, model := range map[string]InferenceStrategy{
		// A model without a telemetry identity: spans start without one.
		"a failed request": struct{ InferenceStrategy }{&scriptedModel{InvokeErr: errors.New("handoff invoke failed")}},
		"an empty stream":  &scriptedModel{InvokeFn: func(context.Context, []*Message, []*Tool, chan<- LLMResponseChunk) {}},
		"a stream error": &scriptedModel{InvokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Content: "handoff stream failed"}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			h := handoffHarness(t, model, pending)

			err := h.handoff(t.Context())

			window := h.context.Messages()
			if err != nil || len(window) != 3 || !strings.Contains(window[1].Content, "[pending] Ship: finish") {
				t.Fatalf("handoff = %v, window %+v, want the goal, the fallback handoff and the continue nudge", err, window)
			}
		})
	}
}

func TestHandoff_failsOnACancelledContext(t *testing.T) {
	h := handoffHarness(t, &scriptedModel{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := h.handoff(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("handoff = %v, want the cancel", err)
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

// contentTokens counts a message's content length as its tokens.
func contentTokens(_ context.Context, msgs []*Message, _ []*Tool) (int, error) {
	n := 0
	for _, m := range msgs {
		n += len(m.Content)
	}
	return n, nil
}

func userMsg(content string) *Message { return &Message{Role: RoleUser, Content: content} }

// collapseHarness is a turn with a window of window and a MaxWindowSize of
// size.
func collapseHarness(t *testing.T, model *scriptedModel, size uint, window ...*Message) *TurnManager {
	t.Helper()
	h := mustNewTurnManager(t, AgentOptions{MaxWindowSize: size, Model: model})
	t.Cleanup(h.Close)
	h.context.Restore(window)
	return h
}

// TestCommitRound_fails covers each way a commit fails: a token count
// (first, after the summary, during a trim), the summary request or its
// stream, and a round too large with nothing to trim. The window keeps its
// committed rounds.
func TestCommitRound_fails(t *testing.T) {
	big := strings.Repeat("x", 80)
	failCount := func(after int) func(context.Context, []*Message, []*Tool) (int, error) {
		calls := 0
		return func(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
			if calls++; calls > after {
				return 0, errors.New("count failed")
			}
			return contentTokens(ctx, msgs, tools)
		}
	}
	summary := func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "s", IsComplete: true}
	}
	cases := []struct {
		name   string
		model  *scriptedModel
		window []*Message
		round  []*Message
		want   string
	}{
		{"the first count", &scriptedModel{CountTokensFn: failCount(0)}, []*Message{userMsg("goal")}, []*Message{userMsg("x")}, "count tokens"},
		{"after the summary", &scriptedModel{CountTokensFn: failCount(1), InvokeFn: summary}, []*Message{userMsg("goal"), userMsg(big)}, []*Message{userMsg(big)}, "count tokens"},
		{"during a trim", &scriptedModel{CountTokensFn: failCount(1)}, []*Message{userMsg("goal")}, []*Message{{Role: RoleTool, ToolCallID: "c1", Content: big + big}}, "count tokens"},
		{"the summary request", &scriptedModel{CountTokensFn: contentTokens, InvokeErr: errors.New("invoke failed")}, []*Message{userMsg("goal"), userMsg(big)}, []*Message{userMsg(big)}, "invoke failed"},
		{"the summary stream", &scriptedModel{CountTokensFn: contentTokens, InvokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Error: fmt.Errorf("%w: stream failed", ErrNetwork)}
		}}, []*Message{userMsg("goal"), userMsg(big)}, []*Message{userMsg(big)}, "stream failed"},
		{"a summary stream error without an error value", &scriptedModel{CountTokensFn: contentTokens, InvokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventError, Content: "provider said no"}
		}}, []*Message{userMsg("goal"), userMsg(big)}, []*Message{userMsg(big)}, "provider said no"},
		{"nothing to trim", &scriptedModel{CountTokensFn: contentTokens}, []*Message{userMsg("goal")}, []*Message{userMsg(big + big)}, "the round needs 164 tokens"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := collapseHarness(t, tc.model, 100, tc.window...)

			err := h.commitRound(t.Context(), tc.round, EffectNone)

			if err == nil || !strings.Contains(err.Error(), tc.want) || len(h.context.Messages()) != len(tc.window) {
				t.Fatalf("commit = %v with window %d, want %q and the window unchanged", err, len(h.context.Messages()), tc.want)
			}
		})
	}
}

// TestCommitRound_cutsAResultOnARuneBoundary: a cut result stays valid
// UTF-8 and says the full output was not kept.
func TestCommitRound_cutsAResultOnARuneBoundary(t *testing.T) {
	h := collapseHarness(t, &scriptedModel{CountTokensFn: contentTokens}, 400, userMsg("goal"))

	err := h.commitRound(t.Context(), []*Message{{Role: RoleTool, ToolCallID: "c1", Content: strings.Repeat("─", 400)}}, EffectNone)

	window := h.context.Messages()
	got := window[len(window)-1].Content
	if err != nil || !utf8.ValidString(got) || !strings.HasPrefix(got, "─") || !strings.Contains(got, "The full output was not kept.") {
		t.Fatalf("cut result = %q %v, want valid UTF-8 cut on a rune", got, err)
	}
}
