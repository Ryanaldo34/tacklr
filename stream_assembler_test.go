package tacklr

import "testing"

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
