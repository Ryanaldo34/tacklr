package tacklr

import (
	"errors"
	"testing"
)

func TestSessionManager_stateAndPlanAreIsolated(t *testing.T) {
	sm := newSessionManager()
	rt := newSessionRuntime(func() chan StreamEvent {
		c := make(chan StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm)
	if err := rt.StateSet("_plan", "host value"); err != nil {
		t.Fatal(err)
	}
	if value, ok := rt.StateGet("_plan"); !ok || value != "host value" {
		t.Fatal("host state was not stored independently")
	}
	if err := rt.StateSet("ok", 1); err != nil {
		t.Fatal(err)
	}
	if v, ok := rt.StateGet("ok"); !ok || v != 1 {
		t.Fatal("user state")
	}
	rt.StateDelete("ok")
	if _, ok := rt.StateGet("ok"); ok {
		t.Fatal("deleted")
	}
	rt.StateDelete("_plan")
	if sm.Plan.HasActive() {
		t.Fatal("no plan yet")
	}
	sm.Plan.set([]Todo{{Title: "x", Status: TodoStatusPending}})
	if !sm.Plan.HasActive() {
		t.Fatal("active plan")
	}
}

// TestRuntime_emitAndState_channels covers EmitUpdate and session State.
func TestRuntime_emitAndState_channels(t *testing.T) {
	ch := make(chan StreamEvent, 2)
	sm := newSessionManager()
	rt := newSessionRuntime(ch, sm)
	rt = rt.WithToolCallID("id1")
	rt.EmitUpdate("hello")
	ev := <-ch
	if ev.Type != StreamEventToolUpdate || ev.Content != "hello" || ev.MessageID != "id1" {
		t.Fatalf("update = %+v", ev)
	}

	// Full channel drops non-blocking.
	full := make(chan StreamEvent)
	rtFull := newSessionRuntime(full, sm)
	rtFull.EmitUpdate("drop")

	// SessionManager state without a turn bus.
	if err := sm.StateSet("z", true); err != nil {
		t.Fatal(err)
	}
	if v, ok := sm.StateGet("z"); !ok || v != true {
		t.Fatal("session state")
	}
	sm.StateDelete("z")
}

// TestSession_parkResumeAndRunAgain is a call's park lifecycle on its open
// round: park, an invalid answer leaves it unanswered, a valid one answers
// it, and the call's next run takes the answer.
func TestSession_parkResumeAndRunAgain(t *testing.T) {
	sm := newSessionManager()
	sm.round = newOpenRound(nil, []ToolCall{{ID: "tc1", CallID: "tc1"}})
	rt := newSessionRuntime(make(chan StreamEvent, 4), sm).WithToolCallID("tc1")
	outside := newSessionRuntime(make(chan StreamEvent, 4), sm).WithToolCallID("tc9")

	_, unknownType := rt.Park("nope", nil)
	_, outsideErr := outside.Park("user_selection_choice", []byte(`[{"title":"A"}]`))
	nilErr := sm.Park("tc1", nil)
	_, parked := rt.Park("user_selection_choice", []byte(`[{"title":"A"},{"title":"B"}]`))
	invalid := sm.Resume("tc1", []byte(`{}`))
	_, beforeAnswer := sm.TakeAnswer("tc1", "user_selection_choice")
	missing := sm.Resume("tc9", []byte(`{"selectionIdx":0}`))
	answerErr := sm.Resume("tc1", []byte(`{"selectionIdx":1}`))
	twice := sm.Resume("tc1", []byte(`{"selectionIdx":1}`))
	got, runErr := rt.Park("user_selection_choice", []byte(`[{"title":"A"},{"title":"B"}]`))

	var intr Interrupt
	if unknownType == nil || outsideErr == nil || nilErr == nil || !errors.As(parked, &intr) {
		t.Fatalf("unknown type %v, outside the round %v, nil %v, park %v", unknownType, outsideErr, nilErr, parked)
	}
	if invalid == nil || beforeAnswer || !errors.Is(missing, ErrInterruptNotFound) || answerErr != nil || !errors.Is(twice, ErrInterruptNotFound) {
		t.Fatalf("invalid %v, taken before an answer %v, missing %v, answer %v, twice %v", invalid, beforeAnswer, missing, answerErr, twice)
	}
	if usi, ok := got.(*UserSelectionInterrupt); runErr != nil || !ok || usi.ConfirmedChoice.Title != "B" || sm.round.Calls[0].Park != nil {
		t.Fatalf("run after resume = %+v %v, want the answer taken", got, runErr)
	}
}
