package session_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// TestCheckpointer_roundTrip is the durable session outcome: plan, user state,
// and window survive Capture → Apply on a fresh manager.
func TestCheckpointer_roundTrip(t *testing.T) {
	sm := session.NewSessionManager()
	sm.Plan().SetDocument("plan")
	sm.Plan().Set([]streaming.Todo{{Title: "A", Status: streaming.TodoStatusPending}})
	rt := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm)
	rt.StateSet("k", "v")

	cp, err := session.NewCheckpointer().Capture(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "hi"}},
		sm,
		map[string]stores.PendingToolCall{},
		map[string]string{},
	)
	if err != nil {
		t.Fatal(err)
	}
	sm2 := session.NewSessionManager()
	applied, err := session.NewCheckpointer().Apply(*cp, sm2)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Window) != 1 {
		t.Fatalf("window=%d", len(applied.Window))
	}
	if sm2.Plan().Document() != "plan" {
		t.Fatal("plan doc")
	}
	rt2 := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm2)
	if v, ok := rt2.StateGet("k"); !ok || v != "v" {
		t.Fatalf("state %v %v", v, ok)
	}
}

// TestCheckpointer_nilManager_errors is the Capture/Apply guard outcome.
func TestCheckpointer_nilManager_errors(t *testing.T) {
	if _, err := session.NewCheckpointer().Capture(nil, nil, nil, nil); err == nil {
		t.Fatal("want capture error")
	}
	if _, err := session.NewCheckpointer().Apply(stores.SessionCheckpoint{}, nil); err == nil {
		t.Fatal("want apply error")
	}
}

// TestCheckpointer_applyNilMaps_defaults empty pending/interrupt maps.
func TestCheckpointer_applyNilMaps_defaults(t *testing.T) {
	sm := session.NewSessionManager()
	// Zero checkpoint has nil maps; Apply must normalize them.
	applied, err := session.NewCheckpointer().Apply(stores.SessionCheckpoint{
		ContextWindow: []*streaming.Message{{Role: streaming.RoleUser, Content: "x"}},
	}, sm)
	if err != nil {
		t.Fatal(err)
	}
	if applied.PendingToolCalls == nil || applied.InterruptToRequester == nil {
		t.Fatal("expected non-nil default maps")
	}
}

// TestPlanStore_lifecycle covers set/get/document/updated/export/load outcomes.
func TestPlanStore_lifecycle(t *testing.T) {
	p := session.NewPlanStore()
	if p.HasActive() {
		t.Fatal("empty has active")
	}
	p.Set([]streaming.Todo{{Title: "t1", Status: streaming.TodoStatusPending}})
	if !p.HasActive() || len(p.Get()) != 1 {
		t.Fatal("set/get")
	}
	p.SetDocument("first")
	if p.ConsumeDocumentUpdated() {
		t.Fatal("initial document should not mark updated")
	}
	p.SetDocument("second")
	if !p.ConsumeDocumentUpdated() {
		t.Fatal("edit should mark updated")
	}
	if p.ConsumeDocumentUpdated() {
		t.Fatal("consume clears flag")
	}

	state := map[string]any{}
	p.ExportInto(state)
	if state["_plan"] == nil || state["_plan_document"] != "second" {
		t.Fatalf("export = %#v", state)
	}

	p2 := session.NewPlanStore()
	// JSON-shaped todos (checkpoint reload path).
	state["_plan"] = []any{map[string]any{"title": "fromJSON", "status": "pending"}}
	state["_plan_document"] = "reloaded"
	state["_plan_document_updated"] = true
	p2.LoadFromState(state)
	if p2.Document() != "reloaded" || len(p2.Get()) != 1 || p2.Get()[0].Title != "fromJSON" {
		t.Fatalf("load = doc %q plan %#v", p2.Document(), p2.Get())
	}
	if !p2.ConsumeDocumentUpdated() {
		t.Fatal("want reloaded updated flag")
	}

	p.Set(nil)
	if p.Get() != nil {
		t.Fatal("clear plan")
	}
	empty := map[string]any{}
	p.ExportInto(empty)
	if _, ok := empty["_plan"]; ok {
		t.Fatal("cleared plan should not export todos")
	}

	session.StripPlanKeys(state)
	if session.IsReservedRuntimeStateKey("_plan") != true {
		t.Fatal("reserved key")
	}
	if session.IsReservedRuntimeStateKey("user") {
		t.Fatal("user key not reserved")
	}
	session.StripPlanKeys(nil)
}

// TestSessionManager_stateAndPlan_guards reserved keys on a live manager.
func TestSessionManager_stateAndPlan_guards(t *testing.T) {
	sm := session.NewSessionManager()
	rt := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm)
	if err := rt.StateSet("_plan", "blocked"); err == nil {
		t.Fatal("reserved key should error on set")
	}
	if _, ok := rt.StateGet("_plan"); ok {
		t.Fatal("reserved key blocked on get")
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
	rt.StateDelete("_plan") // no-op on reserved
	if sm.HasActivePlan() {
		t.Fatal("no plan yet")
	}
	sm.Plan().Set([]streaming.Todo{{Title: "x", Status: streaming.TodoStatusPending}})
	if !sm.HasActivePlan() {
		t.Fatal("active plan")
	}
}

// TestRuntime_interrupts_raiseReturnAdopt is the interrupt lifecycle outcome
// through the public Runtime facade (raise → pending → return → take resolved).
func TestRuntime_interrupts_raiseReturnAdopt(t *testing.T) {
	sm := session.NewSessionManager()
	ch := make(chan streaming.StreamEvent, 4)
	rt := session.NewRuntime(ch, sm)
	rt = rt.WithToolCallID("tc1")

	// Unknown type.
	if _, err := rt.RaiseInterrupt("nope", nil); err == nil {
		t.Fatal("want unknown type error")
	}

	// Raise parks as error; pending true.
	_, err := rt.RaiseInterrupt("user_selection_choice", []byte(`[{"title":"A"},{"title":"B"}]`))
	if err == nil {
		t.Fatal("raise returns interrupt as error")
	}
	var asIntr interrupt.Interrupt
	if !errors.As(err, &asIntr) {
		t.Fatalf("want interrupt error, got %v", err)
	}
	if !rt.HasPendingInterrupt() {
		t.Fatal("pending")
	}
	if _, ok := rt.PendingInterrupt("tc1"); !ok {
		t.Fatal("pending by id")
	}

	// Invalid payload validation.
	if _, err := rt.ReturnInterrupt("tc1", []byte(`{}`)); err == nil {
		t.Fatal("want invalid payload")
	}
	if _, err := rt.ReturnInterrupt("missing", []byte(`{"selectionIdx":0}`)); err == nil {
		t.Fatal("want not found")
	}

	// Valid return → resolved.
	got, err := rt.ReturnInterrupt("tc1", []byte(`{"selectionIdx":1}`))
	if err != nil {
		t.Fatal(err)
	}
	usi, ok := got.(*interrupt.UserSelectionInterrupt)
	if !ok || usi.ConfirmedChoice == nil || usi.ConfirmedChoice.Title != "B" {
		t.Fatalf("choice = %+v", got)
	}
	resolved, ok := rt.TakeResolvedInterrupt("tc1")
	if !ok || resolved == nil {
		t.Fatal("take resolved")
	}
	if _, ok := rt.TakeResolvedInterrupt("tc1"); ok {
		t.Fatal("already taken")
	}

	// Raise after resolve short-circuits with resolved value (second raise path).
	rt = rt.WithToolCallID("tc2")
	_, err = rt.RaiseInterrupt("user_selection_choice", []byte(`[{"title":"X"}]`))
	if err == nil {
		t.Fatal("park")
	}
	if _, err := rt.ReturnInterrupt("tc2", []byte(`{"selectionIdx":0}`)); err != nil {
		t.Fatal(err)
	}
	// Second raise with same tool call id returns resolved without re-parking.
	intr, err := rt.RaiseInterrupt("user_selection_choice", []byte(`[{"title":"Y"}]`))
	if err != nil || intr == nil {
		t.Fatalf("want resolved return, err=%v intr=%v", err, intr)
	}

	// Adopt: first parks, second after resolve returns resolved.
	rt = rt.WithToolCallID("tc3")
	parked := &interrupt.UserSelectionInterrupt{Options: []interrupt.UserChoice{{Title: "Z"}}}
	_, err = rt.AdoptInterrupt(parked)
	if err == nil {
		t.Fatal("adopt parks as error")
	}
	if _, err := rt.AdoptInterrupt(nil); err == nil {
		t.Fatal("nil adopt")
	}
	rt = rt.WithToolCallID("")
	if _, err := rt.AdoptInterrupt(parked); err == nil {
		t.Fatal("empty tool call id")
	}
	rt = rt.WithToolCallID("tc3")
	if _, err := rt.ReturnInterrupt("tc3", []byte(`{"selectionIdx":0}`)); err != nil {
		t.Fatal(err)
	}
	got, err = rt.AdoptInterrupt(&interrupt.UserSelectionInterrupt{Options: []interrupt.UserChoice{{Title: "again"}}})
	if err != nil || got == nil {
		t.Fatalf("adopt resolved path err=%v got=%v", err, got)
	}

	// Tool permission raise + resolve.
	rt = rt.WithToolCallID("perm")
	_, err = rt.RaiseInterrupt("tool_permission", []byte(`{"toolName":"rm"}`))
	if err == nil {
		t.Fatal("park permission")
	}
	if _, err := rt.ReturnInterrupt("perm", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
}

// TestRuntime_emitAndState_channels covers EmitUpdate/PlanUpdate non-blocking paths.
func TestRuntime_emitAndState_channels(t *testing.T) {
	ch := make(chan streaming.StreamEvent, 2)
	sm := session.NewSessionManager()
	rt := session.NewRuntime(ch, sm)
	rt = rt.WithToolCallID("id1")
	rt.EmitUpdate("hello")
	ev := <-ch
	if ev.Type != streaming.StreamEventToolUpdate || ev.Content != "hello" || ev.MessageID != "id1" {
		t.Fatalf("update = %+v", ev)
	}
	rt.EmitPlanUpdate([]session.Todo{{Title: "a", Status: streaming.TodoStatusPending}})
	ev = <-ch
	if ev.Type != streaming.StreamEventPlanUpdate || len(ev.Data) == 0 {
		t.Fatalf("plan = %+v", ev)
	}

	// Full channel drops non-blocking.
	full := make(chan streaming.StreamEvent)
	rtFull := session.NewRuntime(full, sm)
	rtFull.EmitUpdate("drop")
	rtFull.EmitPlanUpdate(nil)

	// SessionManager state without a turn bus.
	if err := sm.StateSet("z", true); err != nil {
		t.Fatal(err)
	}
	if v, ok := sm.StateGet("z"); !ok || v != true {
		t.Fatal("session state")
	}
	sm.StateDelete("z")
}

// TestSessionManager_snapshotLoadInterrupts_roundTrip clones pending interrupts
// into checkpoint JSON and reloads them.
func TestSessionManager_snapshotLoadInterrupts_roundTrip(t *testing.T) {
	sm := session.NewSessionManager()
	rt := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm)
	rt = rt.WithToolCallID("c1")
	_, _ = rt.RaiseInterrupt("user_selection_choice", []byte(`[{"title":"A"}]`))
	rt.StateSet("u", "v")
	// reserved key should not appear as user state in snapshot
	sm.Plan().SetDocument("doc")
	sm.Plan().Set([]streaming.Todo{{Title: "T", Status: streaming.TodoStatusPending}})

	state, pending, resolved := sm.SnapshotDurable()
	if state["u"] != "v" || state["_plan_document"] != "doc" {
		t.Fatalf("state=%#v", state)
	}
	if len(pending) != 1 || len(resolved) != 0 {
		t.Fatalf("pending=%d resolved=%d", len(pending), len(resolved))
	}
	pendJSON, err := json.Marshal(pending)
	if err != nil {
		t.Fatal(err)
	}
	resJSON, err := json.Marshal(resolved)
	if err != nil {
		t.Fatal(err)
	}

	sm2 := session.NewSessionManager()
	sm2.LoadUserAndPlanState(state)
	if err := sm2.LoadInterruptsJSON(pendJSON, resJSON); err != nil {
		t.Fatal(err)
	}
	if sm2.Plan().Document() != "doc" {
		t.Fatal("plan doc reload")
	}
	rt2 := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm2)
	if v, ok := rt2.StateGet("u"); !ok || v != "v" {
		t.Fatal("user state reload")
	}
	if !sm2.HasPendingInterrupt() {
		t.Fatal("pending reload")
	}

	// Bad JSON paths.
	if err := sm2.LoadInterruptsJSON([]byte(`{`), nil); err == nil {
		t.Fatal("want pending unmarshal error")
	}
	if err := sm2.LoadInterruptsJSON(nil, []byte(`{`)); err == nil {
		t.Fatal("want resolved unmarshal error")
	}
	if err := sm2.LoadInterruptsJSON(nil, nil); err != nil {
		t.Fatal(err)
	}
	// null interrupt map
	if err := sm2.LoadInterruptsJSON([]byte(`null`), []byte(`null`)); err != nil {
		t.Fatal(err)
	}
}

// TestInterruptMap_unknownType_errors on polymorphic unmarshal.
func TestInterruptMap_unknownType_errors(t *testing.T) {
	// Capture with a real interrupt then corrupt type via raw JSON.
	sm := session.NewSessionManager()
	rt := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm)
	rt = rt.WithToolCallID("x")
	_, _ = rt.RaiseInterrupt("tool_permission", []byte(`{"toolName":"t"}`))
	_, pending, _ := sm.SnapshotDurable()
	b, err := json.Marshal(pending)
	if err != nil {
		t.Fatal(err)
	}
	// Replace type name.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	// Build envelope with unknown type.
	bad, _ := json.Marshal(map[string]any{
		"x": map[string]any{"type": "not_registered", "data": map[string]any{}},
	})
	sm3 := session.NewSessionManager()
	if err := sm3.LoadInterruptsJSON(bad, nil); err == nil {
		t.Fatal("want unknown type")
	}
	// Bad data body for known type.
	badData, _ := json.Marshal(map[string]any{
		"x": map[string]any{"type": "tool_permission", "data": "not-an-object"},
	})
	if err := sm3.LoadInterruptsJSON(badData, nil); err == nil {
		t.Fatal("want data unmarshal error")
	}
}

// TestCheckpointer_withPendingToolMaps_roundTrip keeps park maps.
func TestCheckpointer_withPendingToolMaps_roundTrip(t *testing.T) {
	sm := session.NewSessionManager()
	ptc := map[string]stores.PendingToolCall{
		"t1": {ToolCall: &streaming.ToolCall{ID: "t1", Name: "x"}, InterruptActive: true},
	}
	itr := map[string]string{"intr1": "t1"}
	cp, err := session.NewCheckpointer().Capture(nil, sm, ptc, itr)
	if err != nil {
		t.Fatal(err)
	}
	sm2 := session.NewSessionManager()
	applied, err := session.NewCheckpointer().Apply(*cp, sm2)
	if err != nil {
		t.Fatal(err)
	}
	if !applied.PendingToolCalls["t1"].InterruptActive || applied.InterruptToRequester["intr1"] != "t1" {
		t.Fatalf("%+v", applied)
	}
}
