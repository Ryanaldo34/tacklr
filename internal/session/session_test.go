package session_test

import (
	"encoding/json"
	"strings"
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
	sm.Plan.SetDocument("plan")
	sm.Plan.Set([]streaming.Todo{{Title: "A", Status: streaming.TodoStatusPending}})
	rt := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
		go func() {
			for range c {
			}
		}()
		return c
	}(), sm)
	rt.StateSet("k", "v")

	cp, err := session.CaptureCheckpoint(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "hi"}},
		sm,
		map[string]stores.PendingToolCall{},
	)
	if err != nil {
		t.Fatal(err)
	}
	sm2 := session.NewSessionManager()
	applied, err := session.ApplyCheckpoint(*cp, sm2)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Window) != 1 {
		t.Fatalf("window=%d", len(applied.Window))
	}
	if sm2.Plan.Document() != "plan" {
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

// TestCheckpointer_resolvedToolPermissionKeepsAllowed: Temporal reconstructs
// the harness between ApplyResume and RunToolCall; Allowed must survive.
func TestCheckpointer_resolvedToolPermissionKeepsAllowed(t *testing.T) {
	sm := session.NewSessionManager()
	parked := &interrupt.ToolPermissionInterrupt{
		ToolName: "sensitive",
		Options:  interrupt.DefaultPermissionOptions(),
	}
	if err := sm.Park("call_sens", parked); err == nil {
		t.Fatal("park should return interrupt as error")
	}
	if _, err := sm.Resume("call_sens", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}

	cp, err := session.CaptureCheckpoint(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "run"}},
		sm,
		map[string]stores.PendingToolCall{
			"call_sens": {ToolCall: &streaming.ToolCall{ID: "call_sens", Name: "sensitive"}, InterruptActive: false},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	sm2 := session.NewSessionManager()
	applied, err := session.ApplyCheckpoint(*cp, sm2)
	if err != nil {
		t.Fatal(err)
	}
	ptc, ok := applied.PendingToolCalls["call_sens"]
	if !ok || ptc.InterruptActive || ptc.ToolCall == nil {
		t.Fatalf("pending after apply: %+v", applied.PendingToolCalls)
	}
	got, ok := sm2.TakeResolved("call_sens")
	if !ok {
		t.Fatal("re-entry TakeResolved")
	}
	perm, ok := got.(*interrupt.ToolPermissionInterrupt)
	if !ok || !perm.Allowed || perm.SelectedOptionID != "allow-once" {
		t.Fatalf("Allowed lost across checkpoint: %+v", got)
	}
}

func TestCheckpointer_rejectsCorruptWindowBeforeApplyingState(t *testing.T) {
	// Arrange
	sm := session.NewSessionManager()
	if err := sm.StateSet("existing", "kept"); err != nil {
		t.Fatal(err)
	}
	checkpoint := stores.SessionCheckpoint{
		ContextWindow: []*streaming.Message{nil},
	}

	// Act
	_, err := session.ApplyCheckpoint(checkpoint, sm)

	// Assert
	if err == nil || !strings.Contains(err.Error(), "invalid context window") {
		t.Fatalf("Apply error = %v", err)
	}
	if value, ok := sm.StateGet("existing"); !ok || value != "kept" {
		t.Fatalf("state changed after rejected checkpoint: %v %v", value, ok)
	}
}

// TestCheckpointer_nilManager_errors is the Capture/Apply guard outcome.
func TestCheckpointer_nilManager_errors(t *testing.T) {
	if _, err := session.CaptureCheckpoint(nil, nil, nil); err == nil {
		t.Fatal("want capture error")
	}
	if _, err := session.ApplyCheckpoint(stores.SessionCheckpoint{}, nil); err == nil {
		t.Fatal("want apply error")
	}
}

func TestCheckpointer_rejectsInvalidWindowOnCapture(t *testing.T) {
	// Arrange
	sm := session.NewSessionManager()

	// Act
	_, err := session.CaptureCheckpoint(
		[]*streaming.Message{nil},
		sm,
		nil,
	)

	// Assert
	if err == nil || !strings.Contains(err.Error(), "invalid context window") {
		t.Fatalf("Capture error = %v", err)
	}
}

func TestCheckpointer_captureRejectsUnmarshallableUserState(t *testing.T) {
	sm := session.NewSessionManager()
	if err := sm.StateSet("bad", make(chan int)); err != nil {
		t.Fatal(err)
	}
	_, err := session.CaptureCheckpoint(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "go"}},
		sm,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "checkpoint user state") {
		t.Fatalf("Capture error = %v", err)
	}
}

func TestCheckpointer_applyRejectsUnsupportedVersion(t *testing.T) {
	// Arrange
	sm := session.NewSessionManager()
	checkpoint := stores.SessionCheckpoint{
		ContextWindow: []*streaming.Message{{Role: streaming.RoleUser, Content: "x"}},
	}.WithVersion(stores.CheckpointVersion + 1)

	// Act
	_, err := session.ApplyCheckpoint(checkpoint, sm)

	// Assert
	if err == nil || !strings.Contains(err.Error(), "unsupported checkpoint version") {
		t.Fatalf("Apply error = %v", err)
	}
}

// TestCheckpointer_applyNilMaps_defaults empty pending/interrupt maps.
func TestCheckpointer_applyNilMaps_defaults(t *testing.T) {
	sm := session.NewSessionManager()
	checkpoint := stores.SessionCheckpoint{
		ContextWindow: []*streaming.Message{{Role: streaming.RoleUser, Content: "x"}},
	}.WithVersion(stores.CheckpointVersion)
	applied, err := session.ApplyCheckpoint(checkpoint, sm)
	if err != nil {
		t.Fatal(err)
	}
	if applied.PendingToolCalls == nil {
		t.Fatal("expected non-nil pending map")
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
	todos, ok := p.ConsumeTodosUpdated()
	if !ok || len(todos) != 1 || todos[0].Title != "t1" {
		t.Fatalf("todos updated = %#v ok=%v", todos, ok)
	}
	if _, ok := p.ConsumeTodosUpdated(); ok {
		t.Fatal("consume clears todos flag")
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

	p.Set(nil)
	if p.Get() != nil {
		t.Fatal("clear plan")
	}
	if cleared, ok := p.ConsumeTodosUpdated(); !ok || cleared != nil {
		t.Fatalf("clear should notify, got %#v ok=%v", cleared, ok)
	}
}

func TestSessionManager_stateAndPlanAreIsolated(t *testing.T) {
	sm := session.NewSessionManager()
	rt := session.NewRuntime(func() chan streaming.StreamEvent {
		c := make(chan streaming.StreamEvent, 8)
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
	sm.Plan.Set([]streaming.Todo{{Title: "x", Status: streaming.TodoStatusPending}})
	if !sm.Plan.HasActive() {
		t.Fatal("active plan")
	}
}

// TestSession_interrupts_parkResumeReentry is the interrupt lifecycle:
// Park writes pending; invalid payload leaves the park; Resume then TakeResolved.
func TestSession_interrupts_parkResumeReentry(t *testing.T) {
	sm := session.NewSessionManager()
	ch := make(chan streaming.StreamEvent, 4)
	rt := session.NewRuntime(ch, sm)
	rt = rt.WithToolCallID("tc1")

	// Unknown type.
	if _, err := rt.Park("nope", nil); err == nil {
		t.Fatal("want unknown type error")
	}

	_, err := rt.Park("user_selection_choice", []byte(`[{"title":"A"},{"title":"B"}]`))
	if err == nil {
		t.Fatal("park returns interrupt as error")
	}
	if !sm.HasPendingInterrupt() {
		t.Fatal("Park must write pending")
	}
	if len(sm.Pending()) != 1 {
		t.Fatal("Pending()")
	}
	if _, ok := sm.PendingInterrupt("tc1"); !ok {
		t.Fatal("pending by id")
	}

	// Invalid payload leaves the park in place.
	if _, err := sm.Resume("tc1", []byte(`{}`)); err == nil {
		t.Fatal("want invalid payload")
	}
	if !sm.HasPendingInterrupt() {
		t.Fatal("invalid payload must leave park")
	}
	if _, err := sm.Resume("missing", []byte(`{"selectionIdx":0}`)); err == nil {
		t.Fatal("want not found")
	}

	// Valid resume → resolved.
	got, err := sm.Resume("tc1", []byte(`{"selectionIdx":1}`))
	if err != nil {
		t.Fatal(err)
	}
	usi, ok := got.(*interrupt.UserSelectionInterrupt)
	if !ok || usi.ConfirmedChoice == nil || usi.ConfirmedChoice.Title != "B" {
		t.Fatalf("choice = %+v", got)
	}
	resolved, ok := sm.TakeResolved("tc1")
	if !ok || resolved == nil {
		t.Fatal("take resolved")
	}
	if _, ok := sm.TakeResolved("tc1"); ok {
		t.Fatal("already taken")
	}

	// Park after resolve short-circuits with resolved value (re-entry).
	rt = rt.WithToolCallID("tc2")
	_, err = rt.Park("user_selection_choice", []byte(`[{"title":"X"}]`))
	if err == nil {
		t.Fatal("park")
	}
	if _, err := sm.Resume("tc2", []byte(`{"selectionIdx":0}`)); err != nil {
		t.Fatal(err)
	}
	intr, err := rt.Park("user_selection_choice", []byte(`[{"title":"Y"}]`))
	if err != nil || intr == nil {
		t.Fatalf("want resolved return, err=%v intr=%v", err, intr)
	}

	parked := &interrupt.UserSelectionInterrupt{Options: []interrupt.UserChoice{{Title: "Z"}}}
	if err = sm.Park("tc3", parked); err == nil {
		t.Fatal("Park returns interrupt as error")
	}
	if err := sm.Park("tc3", nil); err == nil {
		t.Fatal("nil park")
	}
	if err := sm.Park("", parked); err == nil {
		t.Fatal("empty tool call id")
	}
	if _, err := sm.Resume("tc3", []byte(`{"selectionIdx":0}`)); err != nil {
		t.Fatal(err)
	}
	got, ok = sm.TakeResolved("tc3")
	if !ok || got == nil {
		t.Fatalf("TakeResolved after resume got=%v", got)
	}

	// Tool permission park + resume.
	rt = rt.WithToolCallID("perm")
	_, err = rt.Park("tool_permission", []byte(`{"toolName":"rm"}`))
	if err == nil {
		t.Fatal("park permission")
	}
	if _, err := sm.Resume("perm", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}

	sm.ClearInterrupts()
	rt = rt.WithToolCallID("sm1")
	_, err = rt.Park("tool_permission", []byte(`{"toolName":"ls"}`))
	if err == nil {
		t.Fatal("park after ClearInterrupts")
	}
	if _, ok := sm.PendingInterrupt("sm1"); !ok {
		t.Fatal("PendingInterrupt after Park")
	}
	if _, err := sm.Resume("sm1", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
}

// TestRuntime_emitAndState_channels covers EmitUpdate and session State.
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

	// Full channel drops non-blocking.
	full := make(chan streaming.StreamEvent)
	rtFull := session.NewRuntime(full, sm)
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

func TestSessionManager_checkpointInterruptsRoundTrip(t *testing.T) {
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
	_, parkErr := rt.Park("user_selection_choice", []byte(`[{"title":"A"}]`))
	if parkErr == nil {
		t.Fatal("park")
	}
	rt.StateSet("u", "v")
	sm.Plan.SetDocument("doc")
	sm.Plan.Set([]streaming.Todo{{Title: "T", Status: streaming.TodoStatusPending}})

	checkpoint, err := session.CaptureCheckpoint(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "go"}},
		sm,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	sm2 := session.NewSessionManager()
	if _, err := session.ApplyCheckpoint(*checkpoint, sm2); err != nil {
		t.Fatal(err)
	}
	if sm2.Plan.Document() != "doc" {
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
	_, parkErr := rt.Park("tool_permission", []byte(`{"toolName":"t"}`))
	if parkErr == nil {
		t.Fatal("park")
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
	cp, err := session.CaptureCheckpoint(nil, sm, ptc)
	if err != nil {
		t.Fatal(err)
	}
	sm2 := session.NewSessionManager()
	applied, err := session.ApplyCheckpoint(*cp, sm2)
	if err != nil {
		t.Fatal(err)
	}
	if !applied.PendingToolCalls["t1"].InterruptActive {
		t.Fatalf("%+v", applied)
	}
}
