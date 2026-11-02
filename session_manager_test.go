package tacklr

import (
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

// TestCall_readsItsOwnWritesAndCommitAppliesThem: a call sees its own state
// writes over the committed state, the committed state does not change
// until its writes are applied, and a value JSON cannot encode is refused.
func TestCall_readsItsOwnWritesAndCommitAppliesThem(t *testing.T) {
	sm := newSessionManager()
	sm.setState(map[string]any{"k": "committed", "gone": 1, "kept": true})
	rt := callRuntime(sm, nil, "c1")

	setErr := rt.StateSet("k", "mine")
	rt.StateDelete("gone")
	badErr := rt.StateSet("fn", func() {})
	mine, _ := rt.StateGet("k")
	_, gone := rt.StateGet("gone")
	kept, _ := rt.StateGet("kept")
	before, _ := sm.state("k")
	rt.commit()
	after, _ := sm.state("k")
	_, goneAfter := sm.state("gone")

	if setErr != nil || mine != "mine" || gone || kept != true {
		t.Fatalf("the call read k=%v gone=%v kept=%v (set %v), want its own writes over the committed state", mine, gone, kept, setErr)
	}
	if badErr == nil {
		t.Fatal("a value JSON cannot encode was stored")
	}
	if before != "committed" || after != "mine" || goneAfter {
		t.Fatalf("committed k = %v before and %v after, gone after = %v, want the writes applied only at commit", before, after, goneAfter)
	}
}

// TestRuntime_emitUpdateNamesTheCallAndNeverBlocks covers EmitUpdate.
func TestRuntime_emitUpdateNamesTheCallAndNeverBlocks(t *testing.T) {
	ch := make(chan StreamEvent, 1)
	rt := newHarnessRuntime(ch, nil, newSessionManager(), roundCall{Call: ToolCall{ID: "id1", CallID: "id1"}}, callWrites{})

	rt.EmitUpdate("hello")
	rt.EmitUpdate("dropped: the channel is full")

	if ev := <-ch; ev.Type != StreamEventToolUpdate || ev.Content != "hello" || ev.MessageID != "id1" {
		t.Fatalf("update = %+v", ev)
	}
}

// TestCall_parkResumeAndRunAgain is a call's park lifecycle: park, an
// invalid answer leaves it unanswered, a valid one answers it, and the
// call's next run takes the answer.
func TestCall_parkResumeAndRunAgain(t *testing.T) {
	rt := callRuntime(newSessionManager(), nil, "tc1")

	_, unknownType := rt.Park("nope", nil)
	noPark := rt.resume([]byte(`{"selectionIdx":0}`))
	_, parked := rt.Park("user_selection_choice", []byte(`[{"title":"A"},{"title":"B"}]`))
	invalid := rt.resume([]byte(`{}`))
	_, beforeAnswer := rt.takeAnswer("user_selection_choice")
	answerErr := rt.resume([]byte(`{"selectionIdx":1}`))
	twice := rt.resume([]byte(`{"selectionIdx":1}`))
	got, runErr := rt.Park("user_selection_choice", []byte(`[{"title":"A"},{"title":"B"}]`))

	var intr Interrupt
	if unknownType == nil || !errors.Is(noPark, ErrInterruptNotFound) || !errors.As(parked, &intr) {
		t.Fatalf("unknown type %v, answer with no park %v, park %v", unknownType, noPark, parked)
	}
	if invalid == nil || beforeAnswer || answerErr != nil || !errors.Is(twice, ErrInterruptNotFound) {
		t.Fatalf("invalid %v, taken before an answer %v, answer %v, twice %v", invalid, beforeAnswer, answerErr, twice)
	}
	if usi, ok := got.(*UserSelectionInterrupt); runErr != nil || !ok || usi.ConfirmedChoice.Title != "B" || rt.call.Park != nil {
		t.Fatalf("run after resume = %+v %v, want the answer taken", got, runErr)
	}
}

// TestCall_readsItsOwnAnswersCursorsAndPlan: a call reads back its own
// permission answer, search cursor and plan changes before they commit; a
// once-answer is not remembered.
func TestCall_readsItsOwnAnswersCursorsAndPlan(t *testing.T) {
	sm := newSessionManager()
	c := callRuntime(sm, nil, "c1")
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: mustNS(t, "org", "acme")}
	for _, title := range []string{"one fact", "two fact"} {
		if _, err := eng.Save(t.Context(), scope, brain.Object{Kind: "Fact", Title: title}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := eng.Search(t.Context(), scope, brain.SearchRequest{Query: "fact", Limit: 1})
	if err != nil || page.Rest.Empty() {
		t.Fatalf("search = %+v %v, want a page with more", page, err)
	}

	c.rememberAnswer(&ToolPermissionInterrupt{ToolName: "once", SelectedKind: PermissionAllowOnce})
	c.rememberAnswer(&ToolPermissionInterrupt{ToolName: "w", SelectedKind: PermissionRejectAlways})
	c.rememberAnswer(&ToolPermissionInterrupt{ToolName: "w", SelectedKind: PermissionAllowAlways})
	c.setRest("search", page.Rest)
	created, createErr := c.changePlan(planOp{Create: &createTodosArgs{Plan: "p", Todos: []Todo{{Title: "a"}, {Title: "b"}}}})
	completed, completeErr := c.changePlan(planOp{Complete: &completeTodoArgs{Title: "a"}})
	todos, doc := c.plan()

	if c.decision("w") != permAllowAlways || c.decision("once") != permNone || sm.Permissions.Decision("w") != permNone {
		t.Fatalf("decisions w=%v once=%v committed w=%v, want the call's last always-answer and nothing committed", c.decision("w"), c.decision("once"), sm.Permissions.Decision("w"))
	}
	if c.rest("search").Empty() || !sm.Search.Rest("search").Empty() {
		t.Fatal("rest: want the call's own cursor, and none committed")
	}
	if createErr != nil || completeErr != nil || created.Effect != EffectInstallPlanDocument || completed.Effect != EffectHandoff {
		t.Fatalf("plan changes = %+v %v, %+v %v", created, createErr, completed, completeErr)
	}
	if doc != "p" || len(todos) != 2 || todos[0].Status != TodoStatusCompleted || todos[1].Status != TodoStatusInProgress || sm.Plan.HasActive() {
		t.Fatalf("plan the call sees = %+v %q, committed active %v, want both changes on top of an empty committed plan", todos, doc, sm.Plan.HasActive())
	}
}
