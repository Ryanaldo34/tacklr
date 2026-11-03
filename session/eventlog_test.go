package session

import (
	"context"
	"testing"
)

// TestMemoryEventLog_replaysThenTailsEverySubscriber: a subscriber replays
// the log past its offset without retries, then gets what is appended; a
// subscriber that reads late still gets every event; cancelling ends one
// subscriber, and closing the session ends the rest. An unknown session's
// head is 0.
func TestMemoryEventLog_replaysThenTailsEverySubscriber(t *testing.T) {
	log := NewMemoryEventLog()
	id := ID("s")
	head, headErr := log.Head(t.Context(), "unknown")
	for _, topic := range []string{TopicEvents, TopicRetry, TopicEvents} {
		if err := log.Append(t.Context(), id, topic, Event{Content: topic}); err != nil {
			t.Fatal(err)
		}
	}
	live, err := log.Subscribe(t.Context(), id, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancelled, err := log.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	replayed := (<-live).Content
	for n := range 100 {
		if err := log.Append(t.Context(), id, TopicEvents, Event{Content: string(rune('a' + n%26))}); err != nil {
			t.Fatal(err)
		}
	}
	got := 0
	for got < 100 {
		<-live
		got++
	}
	cancel()
	for range cancelled {
	}
	if err := log.CloseSession(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	_, open := <-live

	if head != 0 || headErr != nil || replayed != TopicEvents || got != 100 || open {
		t.Fatalf("head %d %v, replayed %q, tailed %d, still open %v; want 0, the event past the offset, all 100, closed", head, headErr, replayed, got, open)
	}
}
