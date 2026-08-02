package tacklr

import (
	"context"
	"testing"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// TestStdioWatchDog_turnCompletes: attaching telemetry.StdioWatchDog does not
// fail a normal harness turn (outcome: optional watchdog is safe to wire).
func TestStdioWatchDog_turnCompletes(t *testing.T) {
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, MessageId: "m1", Content: "ok", IsComplete: true}
		},
	}
	h := NewAgent(context.Background(), AgentOptions{
		Config:   Config{MaxWindowSize: 8192, SystemPrompt: "test"},
		Model:    strategy,
		WatchDog: telemetry.New(),
	})
	t.Cleanup(h.Close)

	events, err := h.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	var sawComplete bool
	for ev := range events {
		if ev.Type == StreamEventComplete {
			sawComplete = true
		}
		if ev.Type == StreamEventError {
			t.Fatalf("turn error: %v %s", ev.Error, ev.Content)
		}
	}
	if !sawComplete {
		t.Fatal("expected StreamEventComplete")
	}
}
