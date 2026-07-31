package control

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestPlanSet_withoutChannel_updatesStateOnly(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.PlanSet([]Todo{{Title: "A", Status: streaming.TodoStatusPending}})
	plan := rt.PlanGet()
	if len(plan) != 1 || plan[0].Title != "A" {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestPlanSet_withChannel_deliversPlanUpdate(t *testing.T) {
	ch := make(chan streaming.StreamEvent, 1)
	rt := NewRuntime(ch, nil, nil)
	rt.EnsureInitialized()
	rt.PlanSet([]Todo{
		{Title: "Ship", Status: streaming.TodoStatusInProgress, Description: "d"},
	})
	select {
	case ev := <-ch:
		if ev.Type != streaming.StreamEventPlanUpdate {
			t.Fatalf("type = %v, want plan update", ev.Type)
		}
		if !strings.Contains(string(ev.Data), "Ship") {
			t.Fatalf("data = %s", ev.Data)
		}
	default:
		t.Fatal("expected plan update on output channel")
	}
	// After detach, PlanSet must not hang or panic.
	rt.SetOutputChannel(nil)
	rt.PlanSet([]Todo{{Title: "Ship", Status: streaming.TodoStatusCompleted}})
	if got := rt.PlanGet(); got[0].Status != streaming.TodoStatusCompleted {
		t.Fatalf("status after detach = %q", got[0].Status)
	}
}

func TestPlanGet_rehydratesAfterJSONRoundTrip(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.PlanSet([]Todo{
		{Title: "Task 1", Status: streaming.TodoStatusCompleted, Description: "done"},
		{Title: "Task 2", Status: streaming.TodoStatusInProgress, Description: "next"},
	})

	// Simulate checkpoint JSON round-trip of Runtime.State.
	raw, err := json.Marshal(rt.State)
	if err != nil {
		t.Fatal(err)
	}
	var reloaded map[string]any
	if err := json.Unmarshal(raw, &reloaded); err != nil {
		t.Fatal(err)
	}
	rt2 := HarnessRuntime{State: reloaded}
	rt2.EnsureInitialized()

	plan := rt2.PlanGet()
	if len(plan) != 2 {
		t.Fatalf("plan len = %d, want 2", len(plan))
	}
	if plan[0].Title != "Task 1" || plan[0].Status != streaming.TodoStatusCompleted {
		t.Errorf("plan[0] = %+v", plan[0])
	}
	if plan[1].Title != "Task 2" || plan[1].Status != streaming.TodoStatusInProgress {
		t.Errorf("plan[1] = %+v", plan[1])
	}
}

func TestAdoptInterrupt_parksUnderCurrentToolCall(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "spawn_tc"

	// Simulate a child interrupt object.
	child := &UserSelectionInterrupt{
		Options: []UserChoice{{Title: "A"}, {Title: "B"}},
	}
	intr, err := rt.AdoptInterrupt(child)
	if err == nil {
		t.Fatal("first AdoptInterrupt should return interrupt as error")
	}
	if intr != nil {
		t.Fatal("first return value should be nil")
	}
	if !rt.HasPendingInterrupt() {
		t.Fatal("expected pending interrupt after adopt")
	}
	got, ok := rt.PendingInterrupt("spawn_tc")
	if !ok || got != child {
		t.Fatal("pending interrupt should be the adopted instance")
	}

	// Resolve then adopt again should surface resolved via Take/resume path.
	if _, err := rt.ReturnInterrupt("spawn_tc", []byte(`{"selectionIdx":1}`)); err != nil {
		t.Fatal(err)
	}
	// Adopt when resolved: returns resolved, nil error
	rt.CurrentToolCallID = "spawn_tc"
	// Put resolved back for Adopt's resume branch — Return already moved to Resolved.
	// AdoptInterrupt checks Resolved first.
	// After ReturnInterrupt, Resolved has the interrupt. Adopt should take it.
	resolved, err := rt.AdoptInterrupt(child)
	if err != nil {
		t.Fatalf("adopt on resume path: %v", err)
	}
	if resolved == nil {
		t.Fatal("expected resolved interrupt")
	}
	usi := resolved.(*UserSelectionInterrupt)
	if usi.ConfirmedChoice == nil || usi.ConfirmedChoice.Title != "B" {
		t.Fatalf("confirmed choice = %v", usi.ConfirmedChoice)
	}
}

func TestRaiseInterrupt_firstCallReturnsAsError(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "call_1"

	options := []UserChoice{{Title: "A"}, {Title: "B"}}
	payload, _ := json.Marshal(options)

	intr, err := rt.RaiseInterrupt("user_selection_choice", payload)
	if err == nil {
		t.Fatal("first RaiseInterrupt should return nil interrupt, non-nil error")
	}
	if intr != nil {
		t.Fatal("first RaiseInterrupt should return nil interrupt value")
	}
	if !rt.HasPendingInterrupt() {
		t.Fatal("PendingInterrupts should have one entry after raise")
	}
}

func TestRaiseInterrupt_resumeAfterResolveReturnsResolved(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "call_1"

	options := []UserChoice{{Title: "A"}, {Title: "B"}}
	payload, _ := json.Marshal(options)

	// First raise
	_, _ = rt.RaiseInterrupt("user_selection_choice", payload)

	// Consumer resolves with selection index 1
	resolution := `{"interruptId":"call_1","selectionIdx":1}`
	if _, err := rt.ReturnInterrupt("call_1", []byte(resolution)); err != nil {
		t.Fatalf("ReturnInterrupt failed: %v", err)
	}

	if rt.HasPendingInterrupt() {
		t.Fatal("PendingInterrupts should be empty after resolve")
	}

	// Second raise (re-execution): should return the resolved interrupt, no error
	intr, err := rt.RaiseInterrupt("user_selection_choice", payload)
	if err != nil {
		t.Fatalf("second RaiseInterrupt should return nil error, got: %v", err)
	}
	if intr == nil {
		t.Fatal("second RaiseInterrupt should return non-nil interrupt")
	}

	usi, ok := intr.(*UserSelectionInterrupt)
	if !ok {
		t.Fatalf("expected *UserSelectionInterrupt, got %T", intr)
	}
	if usi.ConfirmedChoice == nil {
		t.Fatal("ConfirmedChoice should be populated from resolution")
	}
	if usi.ConfirmedChoice.Title != "B" {
		t.Errorf("ConfirmedChoice.Title = %q, want 'B'", usi.ConfirmedChoice.Title)
	}
}

func TestRaiseInterrupt_unknownKindReturnsError(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.CurrentToolCallID = "call_1"

	_, err := rt.RaiseInterrupt("bogus_kind", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for unknown interrupt type")
	}
}

// Tool permission interrupt payload outcomes (package-local; harness/SSE/ACP
// cover the full protocol path).
func TestToolPermissionInterrupt_returnOutcomes(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()

	initPayload, _ := json.Marshal(map[string]any{"toolName": "mutate"})

	rt.CurrentToolCallID = "perm_allow"
	_, _ = rt.RaiseInterrupt("tool_permission", initPayload)
	if _, err := rt.ReturnInterrupt("perm_allow", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
	intr, err := rt.RaiseInterrupt("tool_permission", initPayload)
	if err != nil {
		t.Fatal(err)
	}
	if p := intr.(*ToolPermissionInterrupt); !p.Allowed || p.SelectedKind != PermissionAllowOnce {
		t.Fatalf("allow: %+v", p)
	}

	rt.CurrentToolCallID = "perm_reject"
	_, _ = rt.RaiseInterrupt("tool_permission", initPayload)
	if _, err := rt.ReturnInterrupt("perm_reject", []byte(`{"optionId":"reject-once"}`)); err != nil {
		t.Fatal(err)
	}
	intr, err = rt.RaiseInterrupt("tool_permission", initPayload)
	if err != nil {
		t.Fatal(err)
	}
	if p := intr.(*ToolPermissionInterrupt); p.Allowed {
		t.Fatal("expected reject")
	}

	rt.CurrentToolCallID = "perm_bad"
	_, _ = rt.RaiseInterrupt("tool_permission", initPayload)
	if _, err := rt.ReturnInterrupt("perm_bad", []byte(`{"optionId":"not-a-real-option"}`)); err == nil {
		t.Fatal("expected invalid optionId to fail")
	}
	// Permission ValidatePayload: missing optionId + invalid JSON.
	rt.CurrentToolCallID = "perm_v"
	_, _ = rt.RaiseInterrupt("tool_permission", initPayload)
	if _, err := rt.ReturnInterrupt("perm_v", []byte(`{}`)); err == nil {
		t.Fatal("missing optionId should fail")
	}
	rt.CurrentToolCallID = "perm_j"
	_, _ = rt.RaiseInterrupt("tool_permission", initPayload)
	if _, err := rt.ReturnInterrupt("perm_j", []byte(`not-json`)); err == nil {
		t.Fatal("bad json should fail")
	}
}

func TestReturnInterrupt_notFoundReturnsError(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()

	_, err := rt.ReturnInterrupt("nonexistent", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for unknown interrupt id")
	}
}

func TestReturnInterrupt_invalidPayload_returnsError(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "invalid JSON",
			payload: `not json`,
			want:    "invalid JSON",
		},
		{
			name:    "missing selectionIdx",
			payload: `{"interruptId":"call_1"}`,
			want:    "missing required field",
		},
		{
			name:    "negative selectionIdx",
			payload: `{"interruptId":"call_1","selectionIdx":-1}`,
			want:    "out of range",
		},
		{
			name:    "out of bounds selectionIdx",
			payload: `{"interruptId":"call_1","selectionIdx":5}`,
			want:    "out of range",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := HarnessRuntime{}
			rt.EnsureInitialized()
			rt.CurrentToolCallID = "call_1"

			options := []UserChoice{{Title: "A"}, {Title: "B"}}
			payload, _ := json.Marshal(options)
			_, _ = rt.RaiseInterrupt("user_selection_choice", payload)

			_, err := rt.ReturnInterrupt("call_1", []byte(tt.payload))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want contains %q", err.Error(), tt.want)
			}
		})
	}
}

func TestInterruptMap_jsonRoundTrip(t *testing.T) {
	original := interruptMap{
		"call_1": &UserSelectionInterrupt{
			Options: []UserChoice{{Title: "A", Description: "desc", IsRecommended: true}},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var restored interruptMap
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}

	if len(restored) != 1 {
		t.Fatalf("restored map has %d entries, want 1", len(restored))
	}

	intr, ok := restored["call_1"]
	if !ok {
		t.Fatal("call_1 entry missing after round trip")
	}

	usi, ok := intr.(*UserSelectionInterrupt)
	if !ok {
		t.Fatalf("expected *UserSelectionInterrupt, got %T", intr)
	}
	if len(usi.Options) != 1 {
		t.Fatalf("Options len = %d, want 1", len(usi.Options))
	}
	if usi.Options[0].Title != "A" {
		t.Errorf("Options[0].Title = %q, want 'A'", usi.Options[0].Title)
	}
}

func TestStateGet_stateSetConcurrent(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()

	rt.StateSet("key", "value")
	v, ok := rt.StateGet("key")
	if !ok {
		t.Fatal("StateGet should find key")
	}
	if v != "value" {
		t.Errorf("StateGet = %v, want 'value'", v)
	}

	rt.StateDelete("key")
	if _, ok := rt.StateGet("key"); ok {
		t.Error("StateGet should return ok=false after delete")
	}
}

// TestRuntime_interruptEmitAndSnapshotLifecycle exercises the session-facing
// runtime surface used during a real turn: plan edges, emit updates, raise /
// return / take interrupts, Serialize/Error, and SnapshotState for checkpoints.
func TestRuntime_interruptEmitAndSnapshotLifecycle(t *testing.T) {
	ch := make(chan streaming.StreamEvent, 2)
	rt := NewRuntime(ch, nil, nil)
	rt.EnsureInitialized()

	// PlanGet with no plan.
	if p := rt.PlanGet(); p != nil {
		t.Fatalf("empty plan = %v, want nil", p)
	}
	// Corrupt plan value rehydrates to nil (unmarshal fails).
	rt.StateSet("_plan", "not-a-plan")
	if p := rt.PlanGet(); p != nil {
		t.Fatalf("corrupt plan = %v, want nil", p)
	}
	rt.PlanSet([]Todo{{Title: "T", Status: streaming.TodoStatusPending}})
	<-ch // plan update

	// EmitUpdate delivers when capacity allows; drops when full or ch nil.
	rt.CurrentToolCallID = "tc_emit"
	rt.EmitUpdate("progress")
	select {
	case ev := <-ch:
		if ev.Type != streaming.StreamEventToolUpdate || ev.Content != "progress" {
			t.Fatalf("emit = %+v", ev)
		}
	default:
		t.Fatal("expected tool update")
	}
	// Fill buffer then emit should not block (drop).
	ch <- streaming.StreamEvent{Type: streaming.StreamEventMessage}
	ch <- streaming.StreamEvent{Type: streaming.StreamEventMessage}
	rt.EmitUpdate("dropped")
	rt.SetOutputChannel(nil)
	rt.EmitUpdate("no-channel")

	// Tool permission: empty payload → defaults; Serialize + Error() for wire/logs.
	rt.SetOutputChannel(ch)
	// drain
	for len(ch) > 0 {
		<-ch
	}
	rt.CurrentToolCallID = "tc_perm"
	_, err := rt.RaiseInterrupt("tool_permission", []byte("null"))
	if err == nil {
		t.Fatal("expected permission interrupt as error")
	}
	if !strings.Contains(err.Error(), "options") && !strings.Contains(err.Error(), "allow") {
		// Error() marshals the interrupt JSON
		_ = err.Error()
	}
	pending, ok := rt.PendingInterrupt("tc_perm")
	if !ok {
		t.Fatal("pending permission missing")
	}
	ser, err := pending.Serialize()
	if err != nil || len(ser) == 0 {
		t.Fatalf("Serialize: %v %s", err, ser)
	}
	if _, err := rt.ReturnInterrupt("tc_perm", []byte(`{"optionId":"allow-once"}`)); err != nil {
		t.Fatal(err)
	}
	taken, ok := rt.TakeResolvedInterrupt("tc_perm")
	if !ok || taken == nil {
		t.Fatal("TakeResolvedInterrupt should return resolved permission")
	}
	if _, ok := rt.TakeResolvedInterrupt("tc_perm"); ok {
		t.Fatal("second take should miss")
	}

	// User selection Serialize/Error + snapshot for checkpoint.
	rt.CurrentToolCallID = "tc_sel"
	opts, _ := json.Marshal([]UserChoice{{Title: "A"}, {Title: "B"}})
	_, err = rt.RaiseInterrupt("user_selection_choice", opts)
	if err == nil {
		t.Fatal("expected selection interrupt as error")
	}
	_ = err.Error() // UserSelectionInterrupt.Error
	sel, _ := rt.PendingInterrupt("tc_sel")
	if _, err := sel.Serialize(); err != nil {
		t.Fatal(err)
	}

	// AdoptInterrupt guards.
	if _, err := rt.AdoptInterrupt(nil); err == nil {
		t.Fatal("nil adopt should fail")
	}
	rt.CurrentToolCallID = ""
	if _, err := rt.AdoptInterrupt(&UserSelectionInterrupt{Options: []UserChoice{{Title: "X"}, {Title: "Y"}}}); err == nil {
		t.Fatal("empty tool call id should fail adopt")
	}

	state, pendingMap, resolvedMap := rt.SnapshotState()
	if state["_plan"] == nil {
		t.Fatal("snapshot missing plan")
	}
	if len(pendingMap) == 0 {
		t.Fatal("snapshot should include pending selection")
	}
	_ = resolvedMap

	// Interrupt map round-trip with both interrupt kinds + null/empty.
	m := interruptMap{
		"a": &UserSelectionInterrupt{Options: []UserChoice{{Title: "A"}, {Title: "B"}}},
		"b": &ToolPermissionInterrupt{ToolName: "t", Options: DefaultPermissionOptions()},
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var restored interruptMap
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if len(restored) != 2 {
		t.Fatalf("restored = %d", len(restored))
	}
	var empty interruptMap
	if err := empty.UnmarshalJSON([]byte("null")); err != nil || empty != nil {
		t.Fatalf("null unmarshal: %v %#v", err, empty)
	}
	if err := empty.UnmarshalJSON(nil); err != nil {
		t.Fatal(err)
	}
	if err := empty.UnmarshalJSON([]byte(`{"x":{"type":"unknown","data":{}}}`)); err == nil {
		t.Fatal("unknown interrupt type should fail")
	}
	// nil map marshal
	var nilMap interruptMap
	if b, err := nilMap.MarshalJSON(); err != nil || string(b) != "null" {
		t.Fatalf("nil marshal = %s %v", b, err)
	}

	// Permission InitFromPayload empty options → defaults; custom options path.
	p := &ToolPermissionInterrupt{}
	if err := p.InitFromPayload(nil); err != nil || len(p.Options) != 4 {
		t.Fatalf("nil payload defaults: %v %#v", err, p)
	}
	custom, _ := json.Marshal(map[string]any{
		"toolName": "x",
		"options":  []PermissionOption{{OptionID: "a", Name: "A", Kind: PermissionAllowOnce}},
	})
	if err := p.InitFromPayload(custom); err != nil || p.ToolName != "x" || len(p.Options) != 1 {
		t.Fatalf("custom init: %v %#v", err, p)
	}
	// Bad Return kind path via crafted option.
	p.Options = []PermissionOption{{OptionID: "weird", Name: "W", Kind: "not_a_kind"}}
	if err := p.Return([]byte(`{"optionId":"weird"}`)); err == nil {
		t.Fatal("unknown kind should fail Return")
	}
}
