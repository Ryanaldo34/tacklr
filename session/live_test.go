package session_test

import (
	"context"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

// TestMerge_sendsEachByteOfAMessageOnceInOrder: a reader of a merged
// stream gets a message's deltas in order and its complete event; a delta
// that repeats or skips text, or comes after the complete event, is not
// sent. A reader that falls behind the live deltas misses the ones after
// its buffer filled, and Publish does not wait for it. When the live
// deltas end, the log's events go on.
func TestMerge_sendsEachByteOfAMessageOnceInOrder(t *testing.T) {
	t.Parallel()
	// Arrange
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	l := session.NewLive()
	behind := l.Subscribe(ctx, "s")
	live := make(chan session.Event, 8)
	delta := func(content string, offset int) session.Event {
		return session.Event{Type: session.EventMessage, MessageID: "m", Delta: true, Content: content, Offset: offset}
	}
	log := make(chan session.Event)
	merged := session.Merge(ctx, log, live)

	// Act
	for _, ev := range []session.Event{delta("hel", 0), delta("hel", 0), delta("XX", 9), delta("lo", 3)} {
		live <- ev
	}
	var got []session.Event
	for range 2 {
		got = append(got, <-merged)
	}
	log <- session.Event{Type: session.EventMessage, MessageID: "m", Content: "hello"}
	got = append(got, <-merged)
	live <- delta("!", 5)
	close(live)
	log <- session.Event{Type: session.EventComplete}
	got = append(got, <-merged)
	for i := range 300 {
		l.Publish("s", delta("x", i))
	}

	// Assert
	want := []string{"hel", "lo", "hello", ""}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %v", got, want)
	}
	for i, ev := range got {
		if ev.Content != want[i] {
			t.Fatalf("event %d = %+v, want %q (all: %+v)", i, ev, want[i], got)
		}
	}
	if got[3].Type != session.EventComplete {
		t.Fatalf("last = %+v, want the log's complete event after the live deltas ended", got[3])
	}
	if n := len(behind); n != 256 {
		t.Fatalf("a reader that never read holds %d deltas, want its buffer full", n)
	}
}
