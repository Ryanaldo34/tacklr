package session_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

// TestEvent_aFailureDecodesWithItsKind: a turn's failure that crossed a
// log, a store or a wire as JSON decodes with an Error that errors.Is
// finds its kind's sentinel in, and with its text; an event that is not
// JSON fails to decode.
func TestEvent_aFailureDecodesWithItsKind(t *testing.T) {
	t.Parallel()
	// Arrange
	decode := func(ev session.Event) session.Event {
		t.Helper()
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		var out session.Event
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	// Act
	cancelled := decode(session.FailureEvent(context.Canceled))
	refused := decode(session.FailureEvent(errors.Join(session.ErrModelRefused, errors.New("not that"))))
	// A step's failure carries only its Error until it is encoded.
	long := decode(session.Event{Type: session.EventError, Error: session.ErrMaxTokens})
	broke := decode(session.FailureEvent(errors.New("model broke")))
	message := decode(session.Event{Type: session.EventMessage, Content: "hi"})
	garbled := json.Unmarshal([]byte(`{"Type":7}`), new(session.Event))

	// Assert
	if !errors.Is(cancelled.Error, context.Canceled) || !errors.Is(refused.Error, session.ErrModelRefused) || refused.Error.Error() != refused.Fail {
		t.Fatalf("cancelled = %v, refused = %v; want each kind's sentinel and its text", cancelled.Error, refused.Error)
	}
	if !errors.Is(long.Error, session.ErrMaxTokens) || long.FailKind != session.FailMaxTokens {
		t.Fatalf("a step's failure decodes as %+v, want its kind", long)
	}
	if broke.Error == nil || broke.Error.Error() != "model broke" || broke.FailKind != "" {
		t.Fatalf("a failure of no kind decodes as %+v, want its text", broke)
	}
	if message.Error != nil || message.Content != "hi" {
		t.Fatalf("a message decodes as %+v, want it as it was", message)
	}
	if garbled == nil {
		t.Fatal("an event that is not JSON decoded, want an error")
	}
}
